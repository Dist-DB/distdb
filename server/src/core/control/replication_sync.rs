use crate::core::app::ServerApp;
use crate::core::control::affinity::{
    AffinityStartupConfig, merge_affinity_documents_from_responses, send_affinity_join_requests,
};
use crate::core::control::outbound_transport::send_service_request_to_addr;
use crate::core::control::p2p_wire::multiaddr_to_socket_addr;
use crate::core::control::schema_catalog::apply_schema_definitions_to_local_database;
use crate::core::control::tcp_transport::TcpServerTransport;
use common::epoch_ms;
use serverlib::core::cluster::NodeDescriptor;
use serverlib::p2p::protocol::{AffinityReplicationAction, ServiceMessage};
use serverlib::{
    AffinityProcessor, AffinityStorage, ReplicationPhaseExecutor, ServerP2pRuntime,
    decode_wal_frame,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};

pub fn spawn_affinity_replication_task(
    affinity_processor: Arc<Mutex<Option<AffinityProcessor>>>,
    affinity_storage: Arc<AffinityStorage>,
    app: Arc<Mutex<ServerApp>>,
    p2p_runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    affinity_config: AffinityStartupConfig,
    local_node: NodeDescriptor,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(500));
        let mut executor = ReplicationPhaseExecutor::new();
        let mut last_affinity_refresh_at = std::time::Instant::now() - Duration::from_secs(30);
        let mut wal_cursors: HashMap<String, HashMap<String, serverlib::TransactionId>> = HashMap::new();
        let mut last_wal_sync_at: HashMap<String, std::time::Instant> = HashMap::new();

        loop {
            ticker.tick().await;

            let mut processor = affinity_processor.lock().await;

            if let Some(ref mut proc) = processor.as_mut() {
                if let serverlib::AffinityProcessorState::Syncing(_) = proc.state() {
                    match proc.build_sync_plan() {
                        Ok(plan) => {
                            let current_idx = executor.current_sync_index();

                            if let Some(step) = plan.get(current_idx) {
                                if matches!(step.phase, serverlib::AffinitySyncPhase::SchemaCatalog)
                                    && let Some(database_id) = &step.database_id
                                {
                                    let affinity_id = proc
                                        .document()
                                        .map(|doc| doc.affinity_id.clone())
                                        .unwrap_or_default();

                                    if let Err(err) = execute_live_schema_catalog_sync(
                                        &app,
                                        &p2p_runtime,
                                        &affinity_id,
                                        database_id,
                                        step.schema_identifier.unwrap_or(0),
                                        proc.document().and_then(|doc| {
                                            doc.databases
                                                .iter()
                                                .find(|db| db.database_id == *database_id)
                                                .and_then(|db| db.schema_hash.clone())
                                        }),
                                        proc.document().and_then(|doc| {
                                            doc.databases
                                                .iter()
                                                .find(|db| db.database_id == *database_id)
                                                .map(|db| db.database_name.clone())
                                        }),
                                    )
                                    .await
                                    {
                                        log::warn!(
                                            "live schema catalog sync failed affinity_id={} database_id={}: {}",
                                            affinity_id,
                                            database_id,
                                            err
                                        );
                                    }
                                }

                                if matches!(step.phase, serverlib::AffinitySyncPhase::WalCatchup)
                                    && let Some(database_id) = &step.database_id
                                {
                                    let affinity_id = proc
                                        .document()
                                        .map(|doc| doc.affinity_id.clone())
                                        .unwrap_or_default();

                                    let db_cursors = wal_cursors.get(database_id).cloned();

                                    match execute_live_wal_catchup_sync(
                                        &app,
                                        &p2p_runtime,
                                        &affinity_id,
                                        database_id,
                                        None,
                                        db_cursors.as_ref(),
                                        None,
                                    )
                                    .await
                                    {
                                        Ok(updated) => {
                                            if !updated.is_empty() {
                                                wal_cursors.insert(database_id.clone(), updated);
                                            }
                                        }

                                        Err(err) => {
                                            log::warn!(
                                                "live WAL catchup sync failed affinity_id={} database_id={}: {}",
                                                affinity_id,
                                                database_id,
                                                err
                                            );
                                        }
                                    }
                                }
                            }

                            match executor.execute_next_phase(proc, &plan) {
                                Ok(completed) => {
                                    if let Some(checkpoint) = proc.checkpoint()
                                        && let Err(err) = affinity_storage.save_checkpoint(checkpoint)
                                    {
                                        log::error!("failed to save checkpoint after replication phase: {}", err);
                                    }

                                    if completed {
                                        proc.set_ready();
                                        log::info!("affinity replication completed, processor is ready");

                                        if let Some(doc) = proc.document()
                                            && let Err(err) = affinity_storage.save(doc)
                                        {
                                            log::error!("failed to save final affinity document: {}", err);
                                        }
                                    } else {
                                        log::debug!("affinity replication phase completed, continuing to next phase");
                                    }
                                }

                                Err(err) => {
                                    log::error!("affinity replication phase failed: {}", err);
                                    proc.set_degraded(format!("replication failed: {}", err));
                                }
                            }
                        }

                        Err(err) => {
                            log::warn!("failed to build replication sync plan: {}", err);
                        }
                    }
                }

                if let serverlib::AffinityProcessorState::Ready = proc.state() {
                    if last_affinity_refresh_at.elapsed() >= Duration::from_secs(2) {
                        last_affinity_refresh_at = std::time::Instant::now();

                        let discovered_peers = {
                            let runtime = p2p_runtime.lock().await;
                            runtime.network().discover_peers()
                        };

                        let responses = send_affinity_join_requests(
                            &affinity_config,
                            &local_node,
                            &discovered_peers,
                        );

                        if !responses.is_empty() && let Some(base_doc) = proc.document() {
                            let mut merged_doc = base_doc.clone();
                            let previous_database_count = merged_doc.databases.len();
                            merge_affinity_documents_from_responses(&mut merged_doc, responses);

                            if merged_doc != *base_doc {
                                proc.apply_affinity_document(merged_doc.clone());

                                if let Err(err) = affinity_storage.save(&merged_doc) {
                                    log::error!(
                                        "failed to save refreshed affinity document: {}",
                                        err
                                    );
                                }

                                if merged_doc.databases.len() > previous_database_count {
                                    log::info!(
                                        "refreshed affinity document added databases old_count={} new_count={}",
                                        previous_database_count,
                                        merged_doc.databases.len()
                                    );
                                    executor.reset();
                                } else {
                                    proc.set_ready();
                                }
                            }
                        }
                    }

                    let Some(document) = proc.document() else {
                        continue;
                    };

                    let affinity_id = document.affinity_id.clone();
                    let database_ids = document
                        .databases
                        .iter()
                        .map(|db| db.database_id.clone())
                        .collect::<Vec<_>>();

                    let peer_targets = {
                        let runtime = p2p_runtime.lock().await;
                        runtime
                            .network()
                            .discover_peers()
                            .into_iter()
                            .filter(|peer| !peer.is_local)
                            .flat_map(|peer| peer.addrs)
                            .collect::<Vec<_>>()
                    };

                    for database_id in database_ids {
                        let should_sync = last_wal_sync_at
                            .get(&database_id)
                            .map(|last| last.elapsed() >= Duration::from_secs(5))
                            .unwrap_or(true);

                        if !should_sync {
                            continue;
                        }

                        last_wal_sync_at.insert(database_id.clone(), std::time::Instant::now());

                        let db_cursors = wal_cursors.get(&database_id).cloned();

                        for target_addr in &peer_targets {
                            match execute_live_wal_catchup_sync(
                                &app,
                                &p2p_runtime,
                                &affinity_id,
                                &database_id,
                                None,
                                db_cursors.as_ref(),
                                Some(target_addr),
                            )
                            .await
                            {
                                Ok(updated) => {
                                    if !updated.is_empty() {
                                        wal_cursors.insert(database_id.clone(), updated);
                                    }
                                }

                                Err(err) => {
                                    log::warn!(
                                        "continuous WAL catchup failed affinity_id={} database_id={} target={}: {}",
                                        affinity_id,
                                        database_id,
                                        target_addr,
                                        err
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

pub async fn execute_live_schema_catalog_sync(
    app: &Arc<Mutex<ServerApp>>,
    p2p_runtime: &Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    affinity_id: &str,
    database_id: &str,
    expected_schema_identifier: u64,
    expected_schema_hash: Option<String>,
    expected_database_name: Option<String>,
) -> Result<(), String> {
    if affinity_id.is_empty() {
        return Ok(());
    }

    let request_id = format!("schema-sync-{}-{}", database_id, epoch_ms!());

    if let Some(database_name) = expected_database_name.as_deref()
        && !database_name.is_empty()
        && database_name != database_id
    {
        let mut app_guard = app.lock().await;
        let _ = app_guard.set_affinity_catalog_database_name(database_id, database_name);
    }

    let peer_targets = {
        let runtime = p2p_runtime.lock().await;
        let peers = runtime.network().discover_peers();

        let mut targets = Vec::new();
        for peer in peers {
            if peer.is_local {
                continue;
            }

            for addr in peer.addrs {
                targets.push(addr);
            }
        }
        targets
    };

    if peer_targets.is_empty() {
        log::debug!(
            "no remote peers available for schema sync affinity_id={} database_id={}",
            affinity_id,
            database_id
        );
        return Ok(());
    }

    let mut applied_any = false;
    for target in peer_targets {
        let Some(socket_addr) = multiaddr_to_socket_addr(&target) else {
            continue;
        };

        let message = ServiceMessage::SchemaCatalogRequest(
            serverlib::p2p::protocol::SchemaCatalogRequest {
                request_id: request_id.clone(),
                affinity_id: affinity_id.to_string(),
                database_id: database_id.to_string(),
                expected_schema_identifier,
                expected_schema_hash: expected_schema_hash.clone(),
            },
        );

        log::debug!(
            "sending affinity replication action={} to={} affinity_id={} database_id={}",
            AffinityReplicationAction::SchemaCatalogRequest.as_str(),
            socket_addr,
            affinity_id,
            database_id
        );

        let Ok(Some(ServiceMessage::SchemaCatalogResponse(response))) =
            send_service_request_to_addr(&socket_addr, &message)
        else {
            continue;
        };

        if response.request_id != request_id {
            continue;
        }

        if !response.ok {
            let err = response
                .error
                .unwrap_or_else(|| "unknown schema sync error".to_string());
            log::warn!(
                "peer rejected schema catalog request affinity_id={} database_id={}: {}",
                affinity_id,
                database_id,
                err
            );
            continue;
        }

        let mut app_guard = app.lock().await;
        apply_schema_definitions_to_local_database(
            &mut app_guard,
            database_id,
            &response.schema_definitions,
        )?;
        if !response.database_name.is_empty() && response.database_name != database_id {
            let _ = app_guard.set_affinity_catalog_database_name(database_id, &response.database_name);
        }
        applied_any = true;
    }

    if applied_any {
        log::info!(
            "applied remote schema catalog affinity_id={} database_id={}",
            affinity_id,
            database_id
        );
    }

    Ok(())
}

pub async fn execute_live_wal_catchup_sync(
    app: &Arc<Mutex<ServerApp>>,
    p2p_runtime: &Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    affinity_id: &str,
    database_id: &str,
    from_transaction_id: Option<serverlib::TransactionId>,
    from_stream_transaction_ids: Option<&HashMap<String, serverlib::TransactionId>>,
    target_addr: Option<&str>,
) -> Result<HashMap<String, serverlib::TransactionId>, String> {
    if affinity_id.is_empty() {
        return Ok(HashMap::new());
    }

    let request_id = format!("wal-sync-{}-{}", database_id, epoch_ms!());

    let peer_targets = {
        let runtime = p2p_runtime.lock().await;
        let peers = runtime.network().discover_peers();

        let mut targets = Vec::new();
        for peer in peers {
            if peer.is_local {
                continue;
            }

            for addr in peer.addrs {
                if target_addr.map(|target| target == addr).unwrap_or(true) {
                    targets.push(addr);
                }
            }
        }

        targets
    };

    if peer_targets.is_empty() {
        log::debug!(
            "no remote peers available for WAL catchup affinity_id={} database_id={}",
            affinity_id,
            database_id
        );
        return Ok(HashMap::new());
    }

    let mut max_seen_by_stream: HashMap<String, u64> = HashMap::new();
    let mut imported = Vec::new();

    for target in peer_targets {
        let Some(socket_addr) = multiaddr_to_socket_addr(&target) else {
            continue;
        };

        let message = ServiceMessage::TransactionsSinceRequest(
            serverlib::p2p::protocol::TransactionsSinceRequest {
                request_id: request_id.clone(),
                affinity_id: affinity_id.to_string(),
                database_id: database_id.to_string(),
                from_transaction_id,
                from_stream_transaction_ids: from_stream_transaction_ids
                    .map(|map| map.iter().map(|(k, v)| (k.clone(), *v)).collect())
                    .unwrap_or_default(),
            },
        );

        log::debug!(
            "sending affinity replication action={} to={} affinity_id={} database_id={} from_tx={:?}",
            AffinityReplicationAction::TransactionsSinceRequest.as_str(),
            socket_addr,
            affinity_id,
            database_id,
            from_transaction_id
        );

        let Ok(Some(ServiceMessage::TransactionsSinceResponse(response))) =
            send_service_request_to_addr(&socket_addr, &message)
        else {
            continue;
        };

        if response.request_id != request_id {
            continue;
        }

        if !response.ok {
            continue;
        }

        if !response.transactions.is_empty() {
            log::info!(
                "live WAL catchup received frames affinity_id={} database_id={} from_peer={} frame_count={}",
                affinity_id,
                database_id,
                socket_addr,
                response.transactions.len()
            );
        }

        for encoded in response.transactions {
            match decode_wal_frame(&encoded) {
                Ok(frame) => {
                    max_seen_by_stream
                        .entry(frame.0.clone())
                        .and_modify(|current| {
                            if frame.1.id.0 > *current {
                                *current = frame.1.id.0;
                            }
                        })
                        .or_insert(frame.1.id.0);
                    imported.push(frame);
                }
                Err(err) => {
                    log::warn!("failed decoding WAL frame during catchup: {}", err);
                }
            }
        }
    }

    if imported.is_empty() {
        return Ok(HashMap::new());
    }

    log::info!(
        "live WAL catchup importing frames affinity_id={} database_id={} frame_count={}",
        affinity_id,
        database_id,
        imported.len()
    );

    let mut app_guard = app.lock().await;
    app_guard.import_wal_records(database_id, imported)?;

    Ok(max_seen_by_stream
        .into_iter()
        .map(|(stream, tx)| (stream, serverlib::TransactionId(tx)))
        .collect())
}
