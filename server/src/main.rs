
/*

	This file is part of DistDB.

	DistDB is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	DistDB is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  
    See the GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with DistDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>.

    The server application is distributed under the GNU Affero General Public License v3.0. 
    See the LICENSE file in the project root for more information.
	
	Written in 2026 by Sam Colak <sam@samcolak.com>
	For information on the author and contributors, see the DistDB 
	website (www.distdb.com) or the GitHub repository (www.github.com/dist-db).

    Copyright (c) 2026 Sam Colak. All rights reserved.

*/

use server::core::app::ServerApp;
use server::core::config::{
    ServerRuntimeConfig, DEFAULT_LOCAL_NODE_ID,
    DEFAULT_LOCAL_SWARM_ID,
};
use server::core::control::affinity::{
    execute_affinity_join_sequence, initialize_affinity_with_persistence,
    parse_affinity_startup_config, parse_server_list_from_args,
};
use server::core::control::connector_handler::{CatalogDispatcher, handle_connector_stream};
use server::core::control::outbound_transport::{
    configure_outbound_tls_state, send_service_request_to_addr,
};
use server::core::control::p2p_wire::{
    advertised_listen_addr_from_args, multiaddr_to_socket_addr,
    node_descriptor_to_peer_node,
};
use server::core::control::replication_sync::spawn_affinity_replication_task;
use server::core::control::tcp_transport::{
    TcpServerTransport, initialize_server_p2p_runtime, spawn_p2p_heartbeat_task,
    spawn_service_announce_task,
};
use server::core::control::tls_support::{
    build_tls_acceptor, build_tls_client_config, negotiate_connector_stream,
    parse_tls_config_from_args, parse_tls_mode_from_args,
};
use serverlib::core::cluster::NodeDescriptor;
use serverlib::core::identity::NodeId;
use serverlib::AffinityProcessor;
use peerlib::{ServiceMessage, ServerP2pRuntime};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn parse_tls_subject_alt_names_from_args(args: &[String]) -> Vec<String> {

    args
        .iter()
        .filter_map(|arg| arg.strip_prefix("tls_san="))
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()

}

fn parse_ca_root_from_args(args: &[String]) -> bool {

    if args.iter().any(|arg| arg == "ca_root") {
        return true;
    }

    args
        .iter()
        .find_map(|arg| arg.strip_prefix("ca_root="))
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes"
            )
        })
        .unwrap_or(false)

}

fn parse_advertised_services_from_args(args: &[String], ca_root_enabled: bool) -> Vec<String> {

    let mut services = args
        .iter()
        .filter_map(|arg| arg.strip_prefix("service="))
        .flat_map(|raw| raw.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    if services.is_empty() {
        services = vec![
            "sql.query".to_string(),
            "p2p.discovery".to_string(),
            "affinity.replication".to_string(),
            "tls.ca.distribution".to_string(),
        ];
    }

    if ca_root_enabled && !services.iter().any(|service| service == "tls.enrollment.issuer") {
        services.push("tls.enrollment.issuer".to_string());
    }

    services.sort();
    services.dedup();
    services

}

fn try_enroll_tls_from_peers(
    server_list: &[String],
    node_data_dir: &Path,
    node_id: &str,
    advertise_addr: &str,
    tls_extra_sans: &[String],
) -> Result<Option<serverlib::AutoTlsPaths>, String> {

    let enrollment = serverlib::build_tls_enrollment_request(
        node_id,
        advertise_addr,
        tls_extra_sans,
    )?;

    let request_id = format!(
        "tls-enroll-{}-{}",
        node_id,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| format!("system clock error: {err}"))?
            .as_millis()
    );

    for peer in server_list {
        let socket_addr = multiaddr_to_socket_addr(peer).unwrap_or_else(|| peer.to_string());
        let request = ServiceMessage::TlsCertEnrollRequest(peerlib::TlsCertEnrollRequest {
            request_id: request_id.clone(),
            requester_node_id: node_id.to_string(),
            csr_pem: enrollment.csr_pem.clone(),
        });

        let Ok(Some(ServiceMessage::TlsCertEnrollResponse(response))) =
            send_service_request_to_addr(&socket_addr, &request)
        else {
            continue;
        };

        if response.request_id != request_id {
            continue;
        }

        if !response.ok {
            log::debug!(
                "tls enrollment rejected by {}: {}",
                socket_addr,
                response
                    .error
                    .unwrap_or_else(|| "unknown enrollment error".to_string())
            );
            continue;
        }

        let Some(node_cert_pem) = response.node_cert_pem else {
            continue;
        };
        let Some(ca_cert_pem) = response.ca_cert_pem else {
            continue;
        };

        let installed = serverlib::install_signed_p2p_tls(
            node_data_dir,
            node_id,
            &enrollment.key_pem,
            &node_cert_pem,
            &ca_cert_pem,
        )?;

        log::info!(
            "tls enrollment succeeded via peer={} cert={} key={} ca={}",
            socket_addr,
            installed.cert_path.display(),
            installed.key_path.display(),
            installed.ca_path.display()
        );

        return Ok(Some(installed));
    }

    Ok(None)

}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    println!("Distdb server (www.distdb.com)");
    println!("Copyright (c) 2026 Sam Colak. All rights reserved.");

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring crypto provider");

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = std::env::args().collect::<Vec<_>>();
    let server_list = parse_server_list_from_args(&args);
    let affinity_config = parse_affinity_startup_config(&args);

    let node_id = args
        .iter()
        .find_map(|arg| arg.strip_prefix("node_id=").map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_LOCAL_NODE_ID.to_string());

    let swarm_id = args
        .iter()
        .find_map(|arg| arg.strip_prefix("swarm_id=").map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_LOCAL_SWARM_ID.to_string());

    let data_dir = args
        .iter()
        .find_map(|arg| arg.strip_prefix("datadir=").map(std::path::PathBuf::from))
        .unwrap_or_else(|| std::path::PathBuf::from("./data"));

    let listen_addr = args
        .iter()
        .find_map(|arg| arg.strip_prefix("listen_addr=").map(ToOwned::to_owned))
        .unwrap_or_else(|| "0.0.0.0".to_string());

    let port: u16 = args
        .iter()
        .find_map(|arg| arg.strip_prefix("port=").and_then(|v| v.parse().ok()))
        .unwrap_or(common::DEFAULT_SERVER_PORT);

    let advertise_addr = advertised_listen_addr_from_args(&args, &listen_addr);
    let ca_root_enabled = parse_ca_root_from_args(&args);
    let advertised_services = parse_advertised_services_from_args(&args, ca_root_enabled);
    let tls_extra_sans = parse_tls_subject_alt_names_from_args(&args);
    let node_data_dir = data_dir.join(&node_id);
    let tls_mode = parse_tls_mode_from_args(&args).map_err(|err| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, err)
    })?;

    let mut tls = parse_tls_config_from_args(&args);

    if matches!(tls_mode, common::TlsMode::Optional | common::TlsMode::Required)
        && (tls.cert_path.is_none() || tls.key_path.is_none() || tls.ca_path.is_none())
    {
        let mut resolved_tls: Option<serverlib::AutoTlsPaths> = None;

        if !ca_root_enabled
            && tls.cert_path.is_none()
            && tls.key_path.is_none()
            && tls.ca_path.is_none()
        {
            resolved_tls = try_enroll_tls_from_peers(
                &server_list,
                &node_data_dir,
                &node_id,
                &advertise_addr,
                &tls_extra_sans,
            )
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        }

        if resolved_tls.is_none() {
            resolved_tls = Some(
                serverlib::ensure_or_generate_p2p_tls(
                    &node_data_dir,
                    &node_id,
                    &advertise_addr,
                    &tls_extra_sans,
                )
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?,
            );
        }

        let auto_tls = resolved_tls.expect("resolved tls paths should be available");

        if tls.cert_path.is_none() {
            tls.cert_path = Some(auto_tls.cert_path.clone());
        }
        if tls.key_path.is_none() {
            tls.key_path = Some(auto_tls.key_path.clone());
        }
        if tls.ca_path.is_none() {
            tls.ca_path = Some(auto_tls.ca_path.clone());
        }

        log::info!(
            "auto-generated p2p tls material cert={} key={} ca={} extra_sans={}",
            tls.cert_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<none>".to_string()),
            tls.key_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<none>".to_string()),
            tls.ca_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<none>".to_string()),
            if tls_extra_sans.is_empty() {
                "<none>".to_string()
            } else {
                tls_extra_sans.join(",")
            }
        );
    }
    
    let tls_acceptor = if matches!(tls_mode, common::TlsMode::Optional | common::TlsMode::Required) {
        Some(build_tls_acceptor(&tls).map_err(|err| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, err)
        })?)
    } else {
        None
    };

    let outbound_tls_client_config = if matches!(tls_mode, common::TlsMode::Optional | common::TlsMode::Required) {

        match build_tls_client_config(&tls) {
            
            Ok(config) => Some(config),

            Err(err) if matches!(tls_mode, common::TlsMode::Optional) => {
                log::warn!(
                    "outbound optional tls client not configured (plaintext fallback enabled): {}",
                    err
                );
                None
            },
            Err(err) => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, err).into());
            }

        }

    } else {
        None
    };

    configure_outbound_tls_state(tls_mode, outbound_tls_client_config);

    log::info!(
        "starting server node_id={} with runtime config: data_dir={}, listen_addr={}, port={}, tls={}, ca_root={}, services={}",
        node_id,
        data_dir.display(),
        listen_addr,
        port,
        tls_mode.as_str(),
        ca_root_enabled,
        advertised_services.join(",")
    );

    if advertise_addr != listen_addr {
        log::info!(
            "server p2p advertise_addr resolved to {} (listen_addr was {})",
            advertise_addr,
            listen_addr
        );
    }

    let runtime = initialize_server_p2p_runtime(
        &node_id,
        &swarm_id,
            &advertise_addr,
        port,
        &server_list,
    )?;

    let peer_addrs = runtime
        .network()
        .discover_peers()
        .iter()
        .flat_map(|peer| peer.addrs.clone())
        .collect::<Vec<_>>();

    if !peer_addrs.is_empty() {
        log::info!(
            "serverlist bootstrap peers registered for kademlia: {}",
            peer_addrs.join(", ")
        );
    }

    let local_node = NodeDescriptor {
        id: NodeId(node_id.clone()),
            addrs: vec![format!("/ip4/{advertise_addr}/tcp/{port}")],
        is_local: true,
    };

    let discovered_peers_for_affinity = runtime.network().discover_peers();
    let local_peer_node = node_descriptor_to_peer_node(&local_node);

    let p2p_runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>> = Arc::new(Mutex::new(runtime));
    let p2p_heartbeat_task = spawn_p2p_heartbeat_task(Arc::clone(&p2p_runtime), local_node.clone());
    let p2p_service_announce_task = spawn_service_announce_task(
        Arc::clone(&p2p_runtime),
        local_node.clone(),
        advertised_services.clone(),
    );

    if ca_root_enabled && matches!(tls_mode, common::TlsMode::Optional | common::TlsMode::Required)
        && let Ok(Some(ca_cert_pem)) = serverlib::load_p2p_ca_pem(&node_data_dir) {
            
            let distribution = peerlib::TlsCaDistribution {
                issuer_node_id: local_node.id.0.clone(),
                ca_cert_pem,
            };

            let mut runtime = p2p_runtime.lock().await;
            for peer_addr in &peer_addrs {
                if let Err(err) = runtime.network_mut().send_message(
                    peer_addr,
                    ServiceMessage::TlsCaDistribution(distribution.clone()),
                ) {
                    log::debug!(
                        "p2p tls CA distribution send failed to {}: {}",
                        peer_addr,
                        err
                    );
                }
            }
        
        }

    let (affinity_processor, affinity_storage) = initialize_affinity_with_persistence(
        affinity_config.as_ref(),
        &local_peer_node,
        discovered_peers_for_affinity.clone(),
        &data_dir,
    );

    let affinity_processor: Arc<Mutex<Option<AffinityProcessor>>> = Arc::new(Mutex::new(affinity_processor));
    let affinity_storage = Arc::new(affinity_storage);

    let config = ServerRuntimeConfig {
        node_id,
        swarm_id,
        data_dir,
        listen_addrs: vec![format!("/ip4/{listen_addr}/tcp/{port}")],
        tls_mode,
        tls,
    };

    let app = Arc::new(RwLock::new(ServerApp::new(config)?));
    let bootstrap_ready = Arc::new(AtomicBool::new(false));

    let tcp_bind_addr = format!("{}:{}", listen_addr, port);
    let listener = TcpListener::bind(&tcp_bind_addr).await?;
    log::info!("connector request listener bound at {}", tcp_bind_addr);

    let app_for_listener = Arc::clone(&app);
    let catalog_dispatcher_for_listener = Arc::new(CatalogDispatcher::new(Arc::clone(&app)));
    let bootstrap_ready_for_listener = Arc::clone(&bootstrap_ready);
    let p2p_runtime_for_listener = Arc::clone(&p2p_runtime);
    let affinity_processor_for_listener = Arc::clone(&affinity_processor);
    let seen_node_announces = Arc::new(Mutex::new(HashSet::<String>::new()));
    let seen_node_announces_for_listener = Arc::clone(&seen_node_announces);
    let service_registry = Arc::new(Mutex::new(HashMap::<String, Vec<String>>::new()));
    
    {
        let mut registry = service_registry.lock().await;
        registry.insert(local_node.id.0.clone(), advertised_services.clone());
    }

    let service_registry_for_listener = Arc::clone(&service_registry);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let active_connections_for_listener = Arc::clone(&active_connections);
    let local_node_for_listener = local_node.clone();
    let node_data_dir_for_listener = node_data_dir.clone();
    let tls_acceptor_for_listener = tls_acceptor.clone();
    let ca_root_enabled_for_listener = ca_root_enabled;

    tokio::spawn(async move {

        loop {

            match listener.accept().await {

                Ok((stream, peer_addr)) => {
                    
                    let connection_id =
                        active_connections_for_listener.fetch_add(1, Ordering::SeqCst) + 1;
                    
                    log::info!(
                        "connector peer connected from {} (active_connections={})",
                        peer_addr,
                        connection_id
                    );
                    
                    let app = Arc::clone(&app_for_listener);
                    let bootstrap_ready = Arc::clone(&bootstrap_ready_for_listener);
                    let catalog_dispatcher = Arc::clone(&catalog_dispatcher_for_listener);
                    let p2p_runtime = Arc::clone(&p2p_runtime_for_listener);
                    let affinity_processor = Arc::clone(&affinity_processor_for_listener);
                    let seen_node_announces = Arc::clone(&seen_node_announces_for_listener);
                    let service_registry = Arc::clone(&service_registry_for_listener);
                    let active_connections = Arc::clone(&active_connections_for_listener);
                    let local_node = local_node_for_listener.clone();
                    let tls_acceptor = tls_acceptor_for_listener.clone();
                    let node_data_dir = node_data_dir_for_listener.clone();
                    let tls_mode = tls_mode;
                    let ca_root_enabled = ca_root_enabled_for_listener;

                    tokio::spawn(async move {

                        let stream = match negotiate_connector_stream(
                            stream,
                            &peer_addr.to_string(),
                            tls_mode,
                            tls_acceptor,
                        )
                        .await
                        {
                            Ok(stream) => stream,
                            Err(err) => {
                                log::warn!(
                                    "connector stream tls negotiation failed for {}: {}",
                                    peer_addr,
                                    err
                                );
                                let remaining = active_connections.fetch_sub(1, Ordering::SeqCst) - 1;
                                log::info!(
                                    "connector peer disconnected from {} (active_connections={})",
                                    peer_addr,
                                    remaining
                                );
                                return;
                            }
                        };

                        if let Err(err) = handle_connector_stream(
                            stream,
                            bootstrap_ready,
                            app,
                            catalog_dispatcher,
                            node_data_dir,
                            p2p_runtime,
                            affinity_processor,
                            seen_node_announces,
                            service_registry,
                            ca_root_enabled,
                            local_node,
                            peer_addr.to_string(),
                            connection_id,
                        )
                        .await
                        {
                            log::warn!(
                                "connector stream handling failed for {}: {}",
                                peer_addr,
                                err
                            );
                        }

                        let remaining = active_connections.fetch_sub(1, Ordering::SeqCst) - 1;
                        
                        log::info!(
                            "connector peer disconnected from {} (active_connections={})",
                            peer_addr,
                            remaining
                        );

                    });
                },

                Err(err) => {
                    log::warn!("listener accept failed: {}", err);
                }

            }
            
        }

    });

    let app_for_bootstrap = Arc::clone(&app);
    let bootstrap_result = tokio::task::spawn_blocking(move || {
        let mut app_guard = app_for_bootstrap.blocking_write();
        app_guard.bootstrap()?;
        let result = app_guard.run_wal_smoke_test()?;
        Ok::<(String, _), server::helpers::ServerAppError>((app_guard.node_id().to_string(), result))
    })
    .await
    .map_err(|err| format!("server bootstrap task failed to join: {err}"))?;

    let (bootstrapped_node_id, result) = bootstrap_result?;

    log::info!(
        "server runtime initialized for node={} with {} active WAL worker(s) and {} probe records",
        bootstrapped_node_id,
        result.active_workers,
        result.records_in_primary_table
    );

    bootstrap_ready.store(true, Ordering::SeqCst);
    log::info!("connector bootstrap gate opened; server is ready to accept requests");

    if let Some(config) = &affinity_config {
        
        execute_affinity_join_sequence(
            Arc::clone(&affinity_processor),
            Arc::clone(&affinity_storage),
            config,
            &local_peer_node,
            &discovered_peers_for_affinity,
        )
        .await;

        // Spawn replication task to execute sync phases
        let _replication_task = spawn_affinity_replication_task(
            Arc::clone(&affinity_processor),
            Arc::clone(&affinity_storage),
            Arc::clone(&app),
            Arc::clone(&p2p_runtime),
            config.clone(),
            local_peer_node.clone(),
        );

    }

    log::info!("server process is running; press Ctrl+C to shutdown");
    tokio::signal::ctrl_c().await?;
    log::info!("shutdown signal received");

    p2p_heartbeat_task.abort();
    p2p_service_announce_task.abort();

    app.write().await.shutdown()?;
    drop(p2p_runtime);
    
    Ok(())

}
