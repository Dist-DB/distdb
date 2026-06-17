
/*

	This file is part of DistDB.

	DistDB is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	DistDB is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with DistDB.  If not, see <http://www.gnu.org/licenses/>.

    The server application is distributed under the GNU General Public License. 
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
use server::core::control::connector_handler::handle_connector_stream;
use server::core::control::outbound_transport::configure_outbound_tls_state;
use server::core::control::p2p_wire::advertised_listen_addr_from_args;
use server::core::control::replication_sync::spawn_affinity_replication_task;
use server::core::control::tcp_transport::{
    TcpServerTransport, initialize_server_p2p_runtime, spawn_p2p_heartbeat_task,
};
use server::core::control::tls_support::{
    build_tls_acceptor, build_tls_client_config, negotiate_connector_stream,
    parse_tls_config_from_args, parse_tls_mode_from_args,
};
use serverlib::core::cluster::NodeDescriptor;
use serverlib::core::identity::NodeId;
use serverlib::{AffinityProcessor, ServerP2pRuntime};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

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
    let tls_mode = parse_tls_mode_from_args(&args).map_err(|err| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, err)
    })?;

    let tls = parse_tls_config_from_args(&args);
    
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
        "starting server node_id={} with runtime config: data_dir={}, listen_addr={}, port={}, tls={}",
        node_id,
        data_dir.display(),
        listen_addr,
        port,
        tls_mode.as_str()
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

    let p2p_runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>> = Arc::new(Mutex::new(runtime));
    let p2p_heartbeat_task = spawn_p2p_heartbeat_task(Arc::clone(&p2p_runtime), local_node.clone());

    let (affinity_processor, affinity_storage) = initialize_affinity_with_persistence(
        affinity_config.as_ref(),
        &local_node,
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

    let mut app = ServerApp::new(config)?;
    app.bootstrap()?;

    let result = app.run_wal_smoke_test()?;

    log::info!(
        "server runtime initialized for node={} with {} active WAL worker(s) and {} probe records",
        app.node_id(),
        result.active_workers,
        result.records_in_primary_table
    );

    let tcp_bind_addr = format!("{}:{}", listen_addr, port);
    let listener = TcpListener::bind(&tcp_bind_addr).await?;
    log::info!("connector request listener bound at {}", tcp_bind_addr);

    let app = Arc::new(Mutex::new(app));
    let app_for_listener = Arc::clone(&app);
    let p2p_runtime_for_listener = Arc::clone(&p2p_runtime);
    let affinity_processor_for_listener = Arc::clone(&affinity_processor);
    let seen_node_announces = Arc::new(Mutex::new(HashSet::<String>::new()));
    let seen_node_announces_for_listener = Arc::clone(&seen_node_announces);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let active_connections_for_listener = Arc::clone(&active_connections);
    let local_node_for_listener = local_node.clone();
    let tls_acceptor_for_listener = tls_acceptor.clone();

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
                    let p2p_runtime = Arc::clone(&p2p_runtime_for_listener);
                    let affinity_processor = Arc::clone(&affinity_processor_for_listener);
                    let seen_node_announces = Arc::clone(&seen_node_announces_for_listener);
                    let active_connections = Arc::clone(&active_connections_for_listener);
                    let local_node = local_node_for_listener.clone();
                    let tls_acceptor = tls_acceptor_for_listener.clone();
                    let tls_mode = tls_mode;

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
                            app,
                            p2p_runtime,
                            affinity_processor,
                            seen_node_announces,
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

    if let Some(config) = &affinity_config {
        
        execute_affinity_join_sequence(
            Arc::clone(&affinity_processor),
            Arc::clone(&affinity_storage),
            config,
            &local_node,
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
            local_node.clone(),
        );

    }

    log::info!("server process is running; press Ctrl+C to shutdown");
    tokio::signal::ctrl_c().await?;
    log::info!("shutdown signal received");

    p2p_heartbeat_task.abort();

    app.lock().await.shutdown()?;
    drop(p2p_runtime);
    
    Ok(())

}
