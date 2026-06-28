use crate::core::app::ServerApp;
use crate::core::control::affinity::build_database_schema_summaries_from_app;
use crate::core::control::p2p_wire::{decode_service_message, multiaddr_to_socket_addr};
use crate::core::control::outbound_transport::send_service_message_to_addr;
use crate::core::control::schema_catalog::{
    build_schema_definitions_for_database, resolve_schema_catalog, schema_catalog_signature,
};
use crate::core::control::session::{extract_auth_token, ServerConnectionSession};
use crate::core::control::tcp_transport::TcpServerTransport;
use crate::core::control::tls_support::BoxedConnectorStream;
use crate::core::control::wire_io::{write_response_frame, write_service_message_to_stream};
use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, FieldDef, FieldIndex,
    FieldType, MutationResult, QueryResult, QueryTimings,
};
use serverlib::core::cluster::NodeDescriptor;
use serverlib::core::identity::NodeId;
use serverlib::p2p::protocol::{
    AffinityJoinResponse, DataSnapshotResponse, SchemaCatalogResponse, ServiceMessage,
    TlsCertEnrollResponse,
    TransactionsSinceResponse,
};
use serverlib::{
    AffinityProcessor, ConcurrentWalManager, DatabaseCatalog, DatabaseEntity,
    DatabaseEntityAspect, DatabaseEntityKind, DatabaseId, ServerP2pEvent, ServerP2pRuntime,
    encode_wal_frame,
    import_p2p_ca_pem_if_missing, sign_tls_enrollment_csr,
};
use common::helpers::p2p::{
    decode_ca_bootstrap_request, encode_ca_bootstrap_response, is_ca_bootstrap_frame,
    CaBootstrapResponse,
};
use common::helpers::format::FileKind;
use common::helpers::list_files;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, mpsc, oneshot};

const SERVER_PASSWORD_CHALLENGE_REQUEST_ID: &str = "__p2p_password_challenge__";
const SERVER_PEER_DISCOVERY_SQL: &str = "__distdb_show_server_peers__";
const SERVER_BOOTSTRAP_STATUS_SQL: &str = "__distdb_bootstrap_status__";
const SERVER_SHOW_ENTITIES_SQL: &str = "__distdb_show_entities__";
const SERVER_SHOW_CATALOG_WORKERS_SQL: &str = "__distdb_show_catalog_workers__";

fn entity_kind_name(kind: DatabaseEntityKind) -> &'static str {
    match kind {
        DatabaseEntityKind::Table => "table",
        DatabaseEntityKind::View => "view",
        DatabaseEntityKind::Relationship => "relationship",
        DatabaseEntityKind::Trigger => "trigger",
        DatabaseEntityKind::StoredProcedure => "stored_procedure",
    }
}

fn append_catalog_entity_rows(
    rows: &mut Vec<Vec<Vec<u8>>>,
    resolved_database_id: &str,
    catalog: &DatabaseCatalog,
    bootstrap_ready: bool,
) {
    let mut catalog_status = catalog.status().to_string().to_ascii_lowercase();
    if !bootstrap_ready && catalog_status == "load" {
        catalog_status = "indexing".to_string();
    }

    rows.push(vec![
        resolved_database_id.as_bytes().to_vec(),
        catalog.database_name().as_bytes().to_vec(),
        b"database".to_vec(),
        resolved_database_id.as_bytes().to_vec(),
        catalog_status.into_bytes(),
        if bootstrap_ready {
            b"loaded".to_vec()
        } else {
            b"loading".to_vec()
        },
        b"n/a".to_vec(),
    ]);

    let mut entities = catalog.entities_iter().collect::<Vec<_>>();
    entities.sort_by(|(left_id, _), (right_id, _)| left_id.cmp(right_id));

    for (_entity_id, entity) in entities {
        let mut entity_status = entity.status().to_string().to_ascii_lowercase();
        if !bootstrap_ready && entity_status == "load" {
            entity_status = "indexing".to_string();
        }
        if bootstrap_ready && entity_status == "load" {
            entity_status = "ready".to_string();
        }

        match entity {
            DatabaseEntity::Table(table) => {
                rows.push(vec![
                    resolved_database_id.as_bytes().to_vec(),
                    catalog.database_name().as_bytes().to_vec(),
                    b"table".to_vec(),
                    table.table_id.clone().into_bytes(),
                    entity_status.into_bytes(),
                    if bootstrap_ready {
                        b"loaded".to_vec()
                    } else {
                        b"loading".to_vec()
                    },
                    table.indexes.len().to_string().into_bytes(),
                ]);

                let mut indexes = table
                    .indexes
                    .values()
                    .map(|index| index.index_id.0.clone())
                    .collect::<Vec<_>>();
                indexes.sort();

                for index_id in indexes {
                    rows.push(vec![
                        resolved_database_id.as_bytes().to_vec(),
                        catalog.database_name().as_bytes().to_vec(),
                        b"index".to_vec(),
                        index_id.into_bytes(),
                        if bootstrap_ready {
                            b"ready".to_vec()
                        } else {
                            b"load".to_vec()
                        },
                        if bootstrap_ready {
                            b"loaded".to_vec()
                        } else {
                            b"loading".to_vec()
                        },
                        b"n/a".to_vec(),
                    ]);
                }
            }
            _ => {
                rows.push(vec![
                    resolved_database_id.as_bytes().to_vec(),
                    catalog.database_name().as_bytes().to_vec(),
                    entity_kind_name(entity.kind()).as_bytes().to_vec(),
                    entity.name().as_bytes().to_vec(),
                    entity_status.into_bytes(),
                    if bootstrap_ready {
                        b"loaded".to_vec()
                    } else {
                        b"loading".to_vec()
                    },
                    b"n/a".to_vec(),
                ]);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CatalogWorkerStats {
    pub catalog_id: String,
    pub queue_depth: usize,
    pub active_requests: usize,
    pub routed_sessions: usize,
}

#[derive(Clone)]
struct CatalogWorkerHandle {
    sender: mpsc::Sender<CatalogDispatchMessage>,
    queue_depth: Arc<std::sync::atomic::AtomicUsize>,
    active: Arc<std::sync::atomic::AtomicUsize>,
}

struct CatalogDispatchMessage {
    request: ConnectorRequest,
    session_id: String,
    connection_id: usize,
    response_tx: oneshot::Sender<ConnectorResponse>,
}

#[derive(Clone)]
pub struct CatalogDispatcher {
    app: Arc<Mutex<ServerApp>>,
    workers: Arc<Mutex<HashMap<String, CatalogWorkerHandle>>>,
    session_routes: Arc<Mutex<HashMap<String, String>>>,
}

impl CatalogDispatcher {
    pub fn new(app: Arc<Mutex<ServerApp>>) -> Self {
        Self {
            app,
            workers: Arc::new(Mutex::new(HashMap::new())),
            session_routes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn worker_for_catalog(&self, catalog_id: &str) -> CatalogWorkerHandle {
        let mut workers = self.workers.lock().await;
        if let Some(worker) = workers.get(catalog_id) {
            return worker.clone();
        }

        let (tx, mut rx) = mpsc::channel::<CatalogDispatchMessage>(512);
        let queue_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = Arc::clone(&self.app);
        let queue_depth_for_worker = Arc::clone(&queue_depth);
        let active_for_worker = Arc::clone(&active);
        let catalog_id_for_worker = catalog_id.to_string();

        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                queue_depth_for_worker.fetch_sub(1, Ordering::SeqCst);
                active_for_worker.fetch_add(1, Ordering::SeqCst);

                let response = execute_app_request_for_session(
                    &app,
                    &message.request,
                    &message.session_id,
                    message.connection_id,
                )
                .await;

                active_for_worker.fetch_sub(1, Ordering::SeqCst);

                if message.response_tx.send(response).is_err() {
                    log::debug!(
                        "catalog worker dropped response for catalog={} because requester is gone",
                        catalog_id_for_worker
                    );
                }
            }
        });

        let worker = CatalogWorkerHandle {
            sender: tx,
            queue_depth,
            active,
        };

        workers.insert(catalog_id.to_string(), worker.clone());
        worker
    }

    pub async fn dispatch(
        &self,
        catalog_id: &str,
        request: ConnectorRequest,
        session_id: String,
        connection_id: usize,
    ) -> Result<ConnectorResponse, String> {
        let worker = self.worker_for_catalog(catalog_id).await;
        let (response_tx, response_rx) = oneshot::channel::<ConnectorResponse>();

        {
            let mut routes = self.session_routes.lock().await;
            routes.insert(session_id.clone(), catalog_id.to_string());
        }

        worker.queue_depth.fetch_add(1, Ordering::SeqCst);

        if worker
            .sender
            .send(CatalogDispatchMessage {
                request,
                session_id,
                connection_id,
                response_tx,
            })
            .await
            .is_err()
        {
            worker.queue_depth.fetch_sub(1, Ordering::SeqCst);
            return Err("catalog worker is unavailable".to_string());
        }

        response_rx
            .await
            .map_err(|_| "catalog worker failed to reply".to_string())
    }

    pub async fn worker_stats(&self) -> Vec<CatalogWorkerStats> {
        let workers = self.workers.lock().await;
        let routes = self.session_routes.lock().await;

        let mut session_counts = HashMap::<String, usize>::new();
        for catalog_id in routes.values() {
            *session_counts.entry(catalog_id.clone()).or_insert(0) += 1;
        }

        let mut stats = workers
            .iter()
            .map(|(catalog_id, handle)| CatalogWorkerStats {
                catalog_id: catalog_id.clone(),
                queue_depth: handle.queue_depth.load(Ordering::SeqCst),
                active_requests: handle.active.load(Ordering::SeqCst),
                routed_sessions: *session_counts.get(catalog_id).unwrap_or(&0),
            })
            .collect::<Vec<_>>();

        stats.sort_by(|lhs, rhs| lhs.catalog_id.cmp(&rhs.catalog_id));
        stats
    }
}

fn request_catalog_route_key(request: &ConnectorRequest) -> Option<String> {
    let ConnectorCommand::Query { query } = &request.command else {
        return None;
    };

    let database_id = common::normalize_identifier!(query.database_id.clone());
    if database_id.is_empty() {
        return None;
    }

    Some(database_id)
}

async fn execute_app_request_for_session(
    app: &Arc<Mutex<ServerApp>>,
    request: &ConnectorRequest,
    session_id: &str,
    connection_id: usize,
) -> ConnectorResponse {
    let mut app = app.lock().await;

    if app.get_session(session_id).is_none() {
        app.init_session(
            session_id.to_string(),
            connection_id,
            "root".to_string(),
        );
    }

    app.handle_connector_request_for_session(request, session_id)
}

pub fn node_announce_dedup_key(node: &NodeDescriptor) -> String {
    format!("{}|{}", node.id.0, node.addrs.join(","))
}

pub fn is_server_peer_discovery_query(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();

    normalized == SERVER_PEER_DISCOVERY_SQL || normalized == "show server peers"
}

pub fn is_bootstrap_status_query(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
    normalized == SERVER_BOOTSTRAP_STATUS_SQL || normalized == "show bootstrap status"
}

pub fn is_show_entities_query(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
    normalized == SERVER_SHOW_ENTITIES_SQL || normalized == "show entities"
}

pub fn is_show_catalog_workers_query(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
    normalized == SERVER_SHOW_CATALOG_WORKERS_SQL || normalized == "show catalog workers"
}

pub fn maybe_bootstrap_status_response(
    request: &ConnectorRequest,
    bootstrap_ready: bool,
) -> Option<ConnectorResponse> {
    let ConnectorCommand::Query { query } = &request.command else {
        return None;
    };

    if !is_bootstrap_status_query(&query.sql) {
        return None;
    }

    let (mode, entities_state, indexes_state, message) = if bootstrap_ready {
        (
            "full",
            "loaded",
            "loaded",
            "server bootstrap complete; full database command set is enabled",
        )
    } else {
        (
            "limited",
            "loading",
            "loading",
            "server is bootstrapping; only connectivity and status/discovery commands are enabled",
        )
    };

    let response = ConnectorResponse::applied(
        request.request_id.clone(),
        ConnectorResult::Query(QueryResult {
            columns: vec![
                FieldDef {
                    seqno: 1,
                    field_name: "bootstrap_ready".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 2,
                    field_name: "session_mode".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 3,
                    field_name: "entities_state".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 4,
                    field_name: "indexes_state".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 5,
                    field_name: "database_commands_enabled".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 6,
                    field_name: "message".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
            ],
            rows: vec![vec![
                if bootstrap_ready { b"true".to_vec() } else { b"false".to_vec() },
                mode.as_bytes().to_vec(),
                entities_state.as_bytes().to_vec(),
                indexes_state.as_bytes().to_vec(),
                if bootstrap_ready { b"true".to_vec() } else { b"false".to_vec() },
                message.as_bytes().to_vec(),
            ]],
            timings: QueryTimings {
                server_parse_ms: 0,
                server_execute_ms: 0,
                server_total_ms: 0,
                network_round_trip_ms: None,
                cache: None,
            },
        }),
    );

    Some(response)
}

fn request_allowed_during_bootstrap(request: &ConnectorRequest) -> bool {
    match &request.command {
        ConnectorCommand::Query { query } => {
            is_server_peer_discovery_query(&query.sql)
                || is_bootstrap_status_query(&query.sql)
                || is_show_entities_query(&query.sql)
                || is_show_catalog_workers_query(&query.sql)
                || extract_auth_token(&query.sql).is_some()
        }
        _ => false,
    }
}

pub async fn maybe_show_catalog_workers_response(
    request: &ConnectorRequest,
    catalog_dispatcher: &Arc<CatalogDispatcher>,
    bootstrap_ready: bool,
) -> Option<ConnectorResponse> {
    let ConnectorCommand::Query { query } = &request.command else {
        return None;
    };

    if !is_show_catalog_workers_query(&query.sql) {
        return None;
    }

    let stats = catalog_dispatcher.worker_stats().await;
    let mut rows = Vec::new();

    if stats.is_empty() {
        rows.push(vec![
            b"*".to_vec(),
            b"0".to_vec(),
            b"0".to_vec(),
            b"0".to_vec(),
            b"idle".to_vec(),
            if bootstrap_ready { b"true".to_vec() } else { b"false".to_vec() },
        ]);
    } else {
        for stat in stats {
            rows.push(vec![
                stat.catalog_id.into_bytes(),
                stat.queue_depth.to_string().into_bytes(),
                stat.active_requests.to_string().into_bytes(),
                stat.routed_sessions.to_string().into_bytes(),
                b"running".to_vec(),
                if bootstrap_ready { b"true".to_vec() } else { b"false".to_vec() },
            ]);
        }
    }

    Some(ConnectorResponse::applied(
        request.request_id.clone(),
        ConnectorResult::Query(QueryResult {
            columns: vec![
                FieldDef {
                    seqno: 1,
                    field_name: "catalog_id".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 2,
                    field_name: "queue_depth".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 3,
                    field_name: "active_requests".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 4,
                    field_name: "routed_sessions".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 5,
                    field_name: "worker_state".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 6,
                    field_name: "bootstrap_ready".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
            ],
            rows,
            timings: QueryTimings {
                server_parse_ms: 0,
                server_execute_ms: 0,
                server_total_ms: 0,
                network_round_trip_ms: None,
                cache: None,
            },
        }),
    ))
}

pub async fn maybe_show_entities_response(
    request: &ConnectorRequest,
    app: &Arc<Mutex<ServerApp>>,
    node_data_dir: &Path,
    bootstrap_ready: bool,
) -> Option<ConnectorResponse> {
    let ConnectorCommand::Query { query } = &request.command else {
        return None;
    };

    if !is_show_entities_query(&query.sql) {
        return None;
    }

    let normalized_sql = query
        .sql
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_lowercase();
    let database_filter = if normalized_sql == SERVER_SHOW_ENTITIES_SQL {
        common::normalize_identifier!(query.database_id.clone())
    } else {
        String::new()
    };

    let mut rows = Vec::new();

    let app_guard = if bootstrap_ready {
        Some(app.lock().await)
    } else {
        app.try_lock().ok()
    };

    let Some(app) = app_guard else {
        let discovered_catalog_paths = list_files(node_data_dir)
            .ok()
            .into_iter()
            .flat_map(|files| files.into_iter())
            .filter(|file| {
                file.extension()
                    .and_then(|value| value.to_str())
                    == Some(FileKind::Catalog.extension())
            })
            .collect::<Vec<_>>();

        let wal = ConcurrentWalManager::with_data_dir(node_data_dir.to_path_buf());
        let mut discovered_catalogs = Vec::<DatabaseCatalog>::new();

        for catalog_path in discovered_catalog_paths {
            let stem = catalog_path
                .file_stem()
                .and_then(|value| value.to_str())
                .map(|value| common::normalize_identifier!(value));

            let Some(stem) = stem else {
                continue;
            };

            let mut catalog = match DatabaseCatalog::load_from_path(&catalog_path) {
                Ok(catalog) => catalog,
                Err(_) => DatabaseCatalog::from_file_stem(&stem),
            };

            let wal_id = catalog.database_id.0.clone();
            if let Err(err) = catalog.replay_entity_construction_from_log(&wal_id, &wal) {
                log::debug!(
                    "show entities bootstrap fallback replay failed for database_id={} err={}",
                    wal_id,
                    err
                );
            }

            discovered_catalogs.push(catalog);
        }

        discovered_catalogs.sort_by(|left, right| left.database_id.0.cmp(&right.database_id.0));
        discovered_catalogs.dedup_by(|left, right| left.database_id.0 == right.database_id.0);

        let catalog_ids = discovered_catalogs
            .iter()
            .map(|catalog| catalog.database_id.0.clone())
            .collect::<Vec<_>>();

        if database_filter.is_empty() {
            
            if discovered_catalogs.is_empty() {
                rows.push(vec![
                    b"*".to_vec(),
                    Vec::new(),
                    b"system".to_vec(),
                    b"bootstrap".to_vec(),
                    b"busy".to_vec(),
                    b"loading".to_vec(),
                    b"n/a".to_vec(),
                ]);
            } else {
                for catalog in discovered_catalogs {
                    let catalog_id = catalog.database_id.0.clone();
                    append_catalog_entity_rows(&mut rows, &catalog_id, &catalog, false);
                }
            }

        } else {

            let matches_filter = if catalog_ids.iter().any(|id| id == &database_filter) {
                true
            } else {
                DatabaseId::from_database_name(&database_filter)
                    .ok()
                    .map(|id| catalog_ids.iter().any(|catalog_id| catalog_id == &id.0))
                    .unwrap_or(false)
            };

            if !matches_filter {
                return Some(ConnectorResponse::rejected(
                    request.request_id.clone(),
                    format!("show entities failed: catalog/database '{}' is not loaded", database_filter),
                ));
            }

            let resolved_database_id = if catalog_ids.iter().any(|id| id == &database_filter) {
                database_filter.clone()
            } else {
                DatabaseId::from_database_name(&database_filter)
                    .ok()
                    .map(|id| id.0)
                    .filter(|id| catalog_ids.iter().any(|catalog_id| catalog_id == id))
                    .unwrap_or_else(|| database_filter.clone())
            };

            if let Some(catalog) = discovered_catalogs
                .iter()
                .find(|catalog| catalog.database_id.0 == resolved_database_id)
            {
                append_catalog_entity_rows(&mut rows, &resolved_database_id, catalog, false);
            }
        }

        let response = ConnectorResponse::applied(
            request.request_id.clone(),
            ConnectorResult::Query(QueryResult {
                columns: vec![
                    FieldDef {
                        seqno: 1,
                        field_name: "database_id".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                        metadata: None,
                    },
                    FieldDef {
                        seqno: 2,
                        field_name: "database_name".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                        metadata: None,
                    },
                    FieldDef {
                        seqno: 3,
                        field_name: "entity_type".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                        metadata: None,
                    },
                    FieldDef {
                        seqno: 4,
                        field_name: "entity_id".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                        metadata: None,
                    },
                    FieldDef {
                        seqno: 5,
                        field_name: "status".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                        metadata: None,
                    },
                    FieldDef {
                        seqno: 6,
                        field_name: "load_state".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                        metadata: None,
                    },
                    FieldDef {
                        seqno: 7,
                        field_name: "index_count".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                        metadata: None,
                    },
                ],
                rows,
                timings: QueryTimings {
                    server_parse_ms: 0,
                    server_execute_ms: 0,
                    server_total_ms: 0,
                    network_round_trip_ms: None,
                    cache: None,
                },
            }),
        );

        return Some(response);
    };

    let target_database_ids = if database_filter.is_empty() {
        let mut all = app.catalogs().keys().cloned().collect::<Vec<_>>();
        all.sort();
        all
    } else {
        let resolved_database_id = if app.catalogs().contains_key(&database_filter) {
            database_filter.clone()
        } else {
            DatabaseId::from_database_name(&database_filter)
                .ok()
                .map(|id| id.0)
                .filter(|id| app.catalogs().contains_key(id))
                .unwrap_or_else(|| database_filter.clone())
        };

        if !app.catalogs().contains_key(&resolved_database_id) {
            return Some(ConnectorResponse::rejected(
                request.request_id.clone(),
                format!("show entities failed: catalog/database '{}' is not loaded", database_filter),
            ));
        }

        vec![resolved_database_id]
    };

    for resolved_database_id in target_database_ids {
        let Some(catalog) = app.catalogs().get(&resolved_database_id) else {
            continue;
        };

        append_catalog_entity_rows(&mut rows, &resolved_database_id, catalog, bootstrap_ready);
    }

    let response = ConnectorResponse::applied(
        request.request_id.clone(),
        ConnectorResult::Query(QueryResult {
            columns: vec![
                FieldDef {
                    seqno: 1,
                    field_name: "database_id".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 2,
                    field_name: "database_name".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 3,
                    field_name: "entity_type".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 4,
                    field_name: "entity_id".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 5,
                    field_name: "status".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 6,
                    field_name: "load_state".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 7,
                    field_name: "index_count".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
            ],
            rows,
            timings: QueryTimings {
                server_parse_ms: 0,
                server_execute_ms: 0,
                server_total_ms: 0,
                network_round_trip_ms: None,
                cache: None,
            },
        }),
    );

    Some(response)
}

pub async fn maybe_server_peer_discovery_response(
    request: &ConnectorRequest,
    p2p_runtime: &Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    local_node: &NodeDescriptor,
    service_registry: &Arc<Mutex<HashMap<String, Vec<String>>>>,
) -> Option<ConnectorResponse> {
    
    let ConnectorCommand::Query { query } = &request.command else {
        return None;
    };

    if !is_server_peer_discovery_query(&query.sql) {
        return None;
    }

    let mut peers = {
        let runtime = p2p_runtime.lock().await;
        runtime.network().discover_peers()
    };

    if is_valid_server_node(local_node) {
        peers.push(local_node.clone());
    }

    let mut seen_ids = HashSet::new();
    peers.retain(|peer| seen_ids.insert(peer.id.0.clone()));

    let service_snapshot = {
        let registry = service_registry.lock().await;
        registry.clone()
    };

    let rows = peers
        .into_iter()
        .map(|peer| {
            let services = service_snapshot
                .get(&peer.id.0)
                .cloned()
                .unwrap_or_default()
                .join(",");
            vec![
                peer.id.0.into_bytes(),
                peer.addrs.join(",").into_bytes(),
                services.into_bytes(),
            ]
        })
        .collect::<Vec<_>>();

    let response = ConnectorResponse::applied(
        request.request_id.clone(),
        ConnectorResult::Query(QueryResult {
            columns: vec![
                FieldDef {
                    seqno: 1,
                    field_name: "peer_id".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 2,
                    field_name: "addrs".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 3,
                    field_name: "services".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
            ],
            rows,
            timings: QueryTimings {
                server_parse_ms: 0,
                server_execute_ms: 0,
                server_total_ms: 0,
                network_round_trip_ms: None,
                cache: None,
            },
        }),
    );

    Some(response)
}

pub fn is_valid_server_node(node: &NodeDescriptor) -> bool {

    if node.id.0.trim().is_empty() {
        return false;
    }

    if node.addrs.is_empty() {
        return false;
    }

    node.addrs
        .iter()
        .all(|addr| multiaddr_to_socket_addr(addr).is_some())

}

#[expect(clippy::too_many_arguments, reason = "necessary for handling connector stream with access to app, p2p runtime, affinity processor, and connection context")]
pub async fn handle_connector_stream(
    mut stream: BoxedConnectorStream,
    bootstrap_ready: Arc<AtomicBool>,
    app: Arc<Mutex<ServerApp>>,
    catalog_dispatcher: Arc<CatalogDispatcher>,
    node_data_dir: std::path::PathBuf,
    p2p_runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    affinity_processor: Arc<Mutex<Option<AffinityProcessor>>>,
    seen_node_announces: Arc<Mutex<HashSet<String>>>,
    service_registry: Arc<Mutex<HashMap<String, Vec<String>>>>,
    ca_root_enabled: bool,
    local_node: NodeDescriptor,
    peer_addr: String,
    connection_id: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let mut session = ServerConnectionSession::new(peer_addr.clone(), connection_id);

    async fn rollback_active_session_transaction(app: &Arc<Mutex<ServerApp>>, session_id: &str) {
        let mut app = app.lock().await;
        if app.rollback_session_transaction(session_id) {
            log::info!(
                "rolled back active transaction due to disconnect session_id={}",
                session_id
            );
        }
    }

    if let Err(err) = write_response_frame(
        &mut stream,
        ConnectorResponse::rejected(
            SERVER_PASSWORD_CHALLENGE_REQUEST_ID,
            session.challenge_message(),
        ),
    )
    .await
    {
        rollback_active_session_transaction(&app, &session.session_id).await;
        return Err(err);
    }

    loop {

        let mut len_buf = [0u8; 4];

        if let Err(err) = stream.read_exact(&mut len_buf).await {
            if matches!(
                err.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) {
                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Ok(());
            }
            rollback_active_session_transaction(&app, &session.session_id).await;
            return Err(Box::new(err));
        }

        let frame_len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; frame_len];

        if let Err(err) = stream.read_exact(&mut payload).await {
            rollback_active_session_transaction(&app, &session.session_id).await;
            return Err(Box::new(err));
        }

        if is_ca_bootstrap_frame(&payload) {
            let node_data_dir = {
                let app_guard = app.lock().await;
                app_guard.node_data_dir().clone()
            };

            let response = if let Some(_req) = decode_ca_bootstrap_request(&payload) {
                match serverlib::load_p2p_ca_pem(&node_data_dir) {
                    Ok(Some(ca_cert_pem)) => CaBootstrapResponse {
                        ok: true,
                        ca_cert_pem: Some(ca_cert_pem),
                        error: None,
                    },
                    Ok(None) => CaBootstrapResponse {
                        ok: false,
                        ca_cert_pem: None,
                        error: Some("no CA cert is available on this node".to_string()),
                    },
                    Err(err) => CaBootstrapResponse {
                        ok: false,
                        ca_cert_pem: None,
                        error: Some(format!("failed loading CA cert: {err}")),
                    },
                }
            } else {
                CaBootstrapResponse {
                    ok: false,
                    ca_cert_pem: None,
                    error: Some("malformed CA bootstrap request".to_string()),
                }
            };

            if let Some(encoded) = encode_ca_bootstrap_response(&response) {
                let len = encoded.len() as u32;
                use tokio::io::AsyncWriteExt;
                let _ = stream.write_all(&len.to_le_bytes()).await;
                let _ = stream.write_all(&encoded).await;
            }

            log::debug!(
                "served CA bootstrap request from {} ok={}",
                peer_addr,
                response.ok
            );

            session.mark_disconnect();
            rollback_active_session_transaction(&app, &session.session_id).await;
            return Ok(());
        }

        if let Some(message) = decode_service_message(&payload) {
            if let ServiceMessage::NodeAnnounce(node) = &message && !is_valid_server_node(node) {
                log::debug!(
                    "ignoring invalid server node announce id='{}' addrs='{}' from {}",
                    node.id.0,
                    node.addrs.join(","),
                    peer_addr
                );
                continue;
            }

            let message_for_fanout = message.clone();
            let mut runtime = p2p_runtime.lock().await;
            if let Err(err) = runtime.handle_event(ServerP2pEvent::MessageReceived {
                from_peer_id: peer_addr.clone(),
                message,
            }) {
                log::debug!("server p2p message handling failed from {}: {}", peer_addr, err);
            }

            if let ServiceMessage::TlsCaDistribution(distribution) = &message_for_fanout {
                let node_data_dir = {
                    let app_guard = app.lock().await;
                    app_guard.node_data_dir().clone()
                };

                match import_p2p_ca_pem_if_missing(&node_data_dir, &distribution.ca_cert_pem) {
                    Ok(true) => {
                        log::info!(
                            "imported p2p CA certificate from issuer_node_id={} via peer={}",
                            distribution.issuer_node_id,
                            peer_addr
                        );
                    }
                    Ok(false) => {
                        log::debug!(
                            "ignored p2p CA distribution from issuer_node_id={} because local CA already exists",
                            distribution.issuer_node_id
                        );
                    }
                    Err(err) => {
                        log::warn!(
                            "failed importing p2p CA distribution from issuer_node_id={}: {}",
                            distribution.issuer_node_id,
                            err
                        );
                    }
                }

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                
                return Ok(());

            }

            if let ServiceMessage::TlsCertEnrollRequest(enroll_req) = &message_for_fanout {

                if !ca_root_enabled {
                    let response_message = ServiceMessage::TlsCertEnrollResponse(
                        TlsCertEnrollResponse {
                            request_id: enroll_req.request_id.clone(),
                            ok: false,
                            error: Some("tls enrollment disabled on this node; ca_root is not enabled".to_string()),
                            node_cert_pem: None,
                            ca_cert_pem: None,
                        },
                    );

                    if let Err(err) =
                        write_service_message_to_stream(&mut stream, &response_message).await
                    {
                        log::warn!(
                            "failed sending tls enrollment rejection to {}: {}",
                            peer_addr,
                            err
                        );
                    }

                    session.mark_disconnect();
                    rollback_active_session_transaction(&app, &session.session_id).await;
                    return Ok(());
                }
                
                let node_data_dir = {
                    let app_guard = app.lock().await;
                    app_guard.node_data_dir().clone()
                };

                let response = match sign_tls_enrollment_csr(&node_data_dir, &enroll_req.csr_pem) {
                    Ok((node_cert_pem, ca_cert_pem)) => TlsCertEnrollResponse {
                        request_id: enroll_req.request_id.clone(),
                        ok: true,
                        error: None,
                        node_cert_pem: Some(node_cert_pem),
                        ca_cert_pem: Some(ca_cert_pem),
                    },
                    Err(err) => TlsCertEnrollResponse {
                        request_id: enroll_req.request_id.clone(),
                        ok: false,
                        error: Some(err),
                        node_cert_pem: None,
                        ca_cert_pem: None,
                    },
                };

                let response_message = ServiceMessage::TlsCertEnrollResponse(response);
                if let Err(err) = write_service_message_to_stream(&mut stream, &response_message).await {
                    log::warn!(
                        "failed sending tls enrollment response to {}: {}",
                        peer_addr,
                        err
                    );
                }

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                
                return Ok(());

            }

            if let ServiceMessage::ServiceAnnounce(announcement) = &message_for_fanout {
                {
                    let mut services = service_registry.lock().await;
                    services.insert(announcement.node_id.clone(), announcement.services.clone());
                }

                let valid_addrs = announcement
                    .addrs
                    .iter()
                    .filter(|addr| multiaddr_to_socket_addr(addr).is_some())
                    .cloned()
                    .collect::<Vec<_>>();

                if !valid_addrs.is_empty() {
                    runtime.network_mut().upsert_discovered_peer(NodeDescriptor {
                        id: NodeId(announcement.node_id.clone()),
                        addrs: valid_addrs,
                        is_local: false,
                    });
                }

                log::debug!(
                    "received service announce node_id={} services={}",
                    announcement.node_id,
                    announcement.services.join(",")
                );

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Ok(());
            }

            if let ServiceMessage::AffinityJoinRequest(join_req) = &message_for_fanout {
                
                let requester_addrs = join_req
                    .requester_addrs
                    .iter()
                    .filter(|addr| multiaddr_to_socket_addr(addr).is_some())
                    .cloned()
                    .collect::<Vec<_>>();

                if !requester_addrs.is_empty() {
                    runtime.network_mut().upsert_discovered_peer(NodeDescriptor {
                        id: NodeId(join_req.requester_node_id.clone()),
                        addrs: requester_addrs,
                        is_local: false,
                    });
                }

                let summaries = if bootstrap_ready.load(Ordering::SeqCst) {
                    let app_guard = app.lock().await;
                    build_database_schema_summaries_from_app(&app_guard)
                } else if let Ok(app_guard) = app.try_lock() {
                    build_database_schema_summaries_from_app(&app_guard)
                } else {
                    Vec::new()
                };

                let processor_lock = affinity_processor.lock().await;

                let response = if let Some(processor) = processor_lock.as_ref() {

                    if let Some(doc) = processor.document() {
                        let mut merged_doc = doc.clone();
                        for summary in summaries {
                            merged_doc.upsert_database_schema(summary);
                        }

                        AffinityJoinResponse {
                            request_id: join_req.request_id.clone(),
                            ok: true,
                            error: None,
                            document: Some(merged_doc),
                        }

                    } else {

                        AffinityJoinResponse {
                            request_id: join_req.request_id.clone(),
                            ok: false,
                            error: Some("processor has no document yet".to_string()),
                            document: None,
                        }

                    }

                } else {

                    AffinityJoinResponse {
                        request_id: join_req.request_id.clone(),
                        ok: false,
                        error: Some("affinity not configured".to_string()),
                        document: None,
                    }
                
                };

                drop(processor_lock);

                let response_message = ServiceMessage::AffinityJoinResponse(response);
                if let Err(err) = write_service_message_to_stream(&mut stream, &response_message).await {
                    log::warn!(
                        "failed to send affinity join response to {}: {}",
                        peer_addr,
                        err
                    );
                }

                log::debug!(
                    "sent affinity join response to {} for request_id={}",
                    peer_addr,
                    join_req.request_id
                );

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                
                return Ok(());

            }

            if let ServiceMessage::SchemaCatalogRequest(schema_req) = &message_for_fanout {

                log::debug!(
                    "received schema catalog request from {} database={}",
                    peer_addr,
                    schema_req.database_id
                );

                let app_guard = if bootstrap_ready.load(Ordering::SeqCst) {
                    Some(app.lock().await)
                } else {
                    app.try_lock().ok()
                };

                let (ok, error, schema_identifier, schema_hash, schema_definitions, database_name) =
                    if let Some(app_guard) = app_guard {
                        let (ok, error, schema_definitions) =
                            match build_schema_definitions_for_database(&app_guard, &schema_req.database_id)
                            {
                                Ok(definitions) => (true, None, definitions),
                                Err(err) => (false, Some(err), Vec::new()),
                            };
                        let (schema_identifier, schema_hash) = resolve_schema_catalog(
                            &app_guard,
                            &schema_req.database_id,
                        )
                        .map(schema_catalog_signature)
                        .unwrap_or((0, None));
                        
                        let database_name = resolve_schema_catalog(&app_guard, &schema_req.database_id)
                            .map(|cat| cat.database_name().to_string())
                            .unwrap_or_default();

                        (ok, error, schema_identifier, schema_hash, schema_definitions, database_name)
                    } else {
                        (
                            false,
                            Some("catalog bootstrap in progress; retry shortly".to_string()),
                            0,
                            None,
                            Vec::new(),
                            String::new(),
                        )
                    };

                let response = SchemaCatalogResponse {
                    request_id: schema_req.request_id.clone(),
                    ok,
                    error,
                    schema_identifier,
                    schema_hash,
                    schema_definitions,
                    database_name,
                };

                let response_message = ServiceMessage::SchemaCatalogResponse(response);
                if let Err(err) = write_service_message_to_stream(&mut stream, &response_message).await {
                    log::warn!(
                        "failed to send schema catalog response to {}: {}",
                        peer_addr,
                        err
                    );
                }

                log::debug!(
                    "sent schema catalog response to {} for request_id={}",
                    peer_addr,
                    schema_req.request_id
                );

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Ok(());

            }

            if let ServiceMessage::DataSnapshotRequest(snapshot_req) = &message_for_fanout {
                
                log::debug!(
                    "received data snapshot request from {} database={} tables={:?}",
                    peer_addr,
                    snapshot_req.database_id,
                    snapshot_req.table_names
                );

                let snapshot_data: Vec<(String, Vec<String>)> = Vec::new();

                let response = DataSnapshotResponse {
                    request_id: snapshot_req.request_id.clone(),
                    ok: true,
                    error: None,
                    snapshot_data,
                };

                let response_message = ServiceMessage::DataSnapshotResponse(response);
                if let Err(err) = write_service_message_to_stream(&mut stream, &response_message).await {
                    log::warn!(
                        "failed to send data snapshot response to {}: {}",
                        peer_addr,
                        err
                    );
                }

                log::debug!(
                    "sent data snapshot response to {} for request_id={}",
                    peer_addr,
                    snapshot_req.request_id
                );

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                
                return Ok(());

            }

            if let ServiceMessage::TransactionsSinceRequest(txn_req) = &message_for_fanout {

                log::debug!(
                    "received transactions since request from {} database={} from_tx={:?}",
                    peer_addr,
                    txn_req.database_id,
                    txn_req.from_transaction_id
                );

                let app_guard = if bootstrap_ready.load(Ordering::SeqCst) {
                    Some(app.lock().await)
                } else {
                    app.try_lock().ok()
                };

                let stream_cursors = txn_req
                    .from_stream_transaction_ids
                    .iter()
                    .map(|(stream_id, tx_id)| (stream_id.clone(), *tx_id))
                    .collect::<HashMap<_, _>>();
                let (ok, error, transactions) = if let Some(app_guard) = app_guard {
                    match app_guard.export_wal_records_for_database(
                        &txn_req.database_id,
                        txn_req.from_transaction_id,
                        Some(&stream_cursors),
                    ) {
                        Ok(records) => {
                            let encoded = records
                                .into_iter()
                                .filter_map(|frame| match encode_wal_frame(&frame) {
                                    Ok(encoded) => Some(encoded),
                                    Err(err) => {
                                        log::warn!(
                                            "failed encoding WAL frame for response: {}",
                                            err
                                        );
                                        None
                                    }
                                })
                                .collect::<Vec<_>>();
                            (true, None, encoded)
                        }
                        Err(err) => (false, Some(err), Vec::new()),
                    }
                } else {
                    (
                        false,
                        Some("catalog bootstrap in progress; retry shortly".to_string()),
                        Vec::new(),
                    )
                };

                let response = TransactionsSinceResponse {
                    request_id: txn_req.request_id.clone(),
                    ok,
                    error,
                    transactions,
                };

                let response_message = ServiceMessage::TransactionsSinceResponse(response);

                if let Err(err) = write_service_message_to_stream(&mut stream, &response_message).await {
                    log::warn!(
                        "failed to send transactions since response to {}: {}",
                        peer_addr,
                        err
                    );
                }

                log::debug!(
                    "sent transactions since response to {} for request_id={}",
                    peer_addr,
                    txn_req.request_id
                );

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                
                return Ok(());

            }

            if let ServiceMessage::NodeAnnounce(node) = message_for_fanout {

                let dedup_key = node_announce_dedup_key(&node);
                let should_fanout = {
                    let mut seen = seen_node_announces.lock().await;
                    seen.insert(dedup_key)
                };

                if should_fanout {
                    for announce_addr in &node.addrs {
                        let Some(target_addr) = multiaddr_to_socket_addr(announce_addr) else {
                            continue;
                        };

                        if let Err(err) = send_service_message_to_addr(
                            &target_addr,
                            &ServiceMessage::NodeAnnounce(local_node.clone()),
                        ) {
                            log::debug!(
                                "server p2p direct announce reply to {} failed: {}",
                                target_addr,
                                err
                            );
                        }
                    }

                    let mut target_addrs = HashSet::new();
                    for peer in runtime.network().discover_peers() {
                        for addr in peer.addrs {
                            if !node.addrs.contains(&addr) {
                                target_addrs.insert(addr);
                            }
                        }
                    }

                    drop(runtime);

                    for target in target_addrs {
                        let Some(target_addr) = multiaddr_to_socket_addr(&target) else {
                            continue;
                        };

                        if let Err(err) = send_service_message_to_addr(
                            &target_addr,
                            &ServiceMessage::NodeAnnounce(node.clone()),
                        ) {
                            log::debug!(
                                "server p2p fanout announce to {} failed: {}",
                                target_addr,
                                err
                            );
                        }
                    }
                }

            }

            session.mark_disconnect();
            rollback_active_session_transaction(&app, &session.session_id).await;

            return Ok(());

        }

        let request = match bincode::deserialize::<ConnectorRequest>(&payload) {
            Ok(request) => request,
            Err(_) => {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err("invalid connector or p2p frame payload".into());
            }
        };

        log::debug!(
            "server handling connector request_id={} from {}",
            request.request_id,
            peer_addr
        );

        if let Some(response) =
            maybe_server_peer_discovery_response(&request, &p2p_runtime, &local_node, &service_registry).await
        {
            if let Err(err) = write_response_frame(&mut stream, response).await {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err(err);
            }
            continue;
        }

        if let Some(response) = maybe_bootstrap_status_response(
            &request,
            bootstrap_ready.load(Ordering::SeqCst),
        ) {
            if let Err(err) = write_response_frame(&mut stream, response).await {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err(err);
            }
            continue;
        }

        if let Some(response) = maybe_show_entities_response(
            &request,
            &app,
            &node_data_dir,
            bootstrap_ready.load(Ordering::SeqCst),
        )
        .await
        {
            if let Err(err) = write_response_frame(&mut stream, response).await {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err(err);
            }
            continue;
        }

        if let Some(response) = maybe_show_catalog_workers_response(
            &request,
            &catalog_dispatcher,
            bootstrap_ready.load(Ordering::SeqCst),
        )
        .await
        {
            if let Err(err) = write_response_frame(&mut stream, response).await {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err(err);
            }
            continue;
        }

        if !session.authenticated {

            let auth_outcome = match &request.command {
                ConnectorCommand::Query { query } => {
                    extract_auth_token(&query.sql).map(|token| session.authenticate_if_valid_token(token))
                }
                _ => None,
            };

            let response = match auth_outcome {
                Some(true) => {
                    // Auth success establishes transport-level session only.
                    // Catalog-bound session state is initialized lazily on first
                    // command that actually enters the application core.
                    ConnectorResponse::applied(
                        request.request_id,
                        ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
                    )
                }
                Some(false) => ConnectorResponse::rejected(request.request_id, "invalid password"),
                None => ConnectorResponse::rejected(
                    request.request_id,
                    "authentication required; run `password <password>;` first",
                ),
            };

            if let Err(err) = write_response_frame(&mut stream, response).await {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err(err);
            }

            continue;

        }

        if !bootstrap_ready.load(Ordering::SeqCst)
            && !request_allowed_during_bootstrap(&request)
        {
            let response = ConnectorResponse::rejected(
                request.request_id.clone(),
                "server is bootstrapping; limited session mode allows only password setup, server peer discovery, entity status (`show entities;`), catalog worker status (`show catalog workers;`), and bootstrap status (`show bootstrap status;`)".to_string(),
            );

            if let Err(err) = write_response_frame(&mut stream, response).await {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err(err);
            }

            log::info!(
                "connector request limited while bootstrapping from {} request_id={}",
                peer_addr,
                request.request_id
            );

            continue;
        }

        session.record_request(&request);

        let response = if let Some(catalog_id) = request_catalog_route_key(&request) {
            match catalog_dispatcher
                .dispatch(
                    &catalog_id,
                    request.clone(),
                    session.session_id.clone(),
                    connection_id,
                )
                .await
            {
                Ok(response) => response,
                Err(err) => ConnectorResponse::rejected(
                    request.request_id.clone(),
                    format!("catalog dispatch failed: {}", err),
                ),
            }
        } else {
            execute_app_request_for_session(&app, &request, &session.session_id, connection_id).await
        };

        if let Err(err) = write_response_frame(&mut stream, response).await {
            rollback_active_session_transaction(&app, &session.session_id).await;
            return Err(err);
        }

    }

}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn node_announce_dedup_key_is_stable() {
        let node = NodeDescriptor {
            id: NodeId("sam01".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        };

        let key1 = node_announce_dedup_key(&node);
        let key2 = node_announce_dedup_key(&node);
        assert_eq!(key1, key2);
    }

    #[test]
    fn is_valid_server_node_requires_non_empty_id_and_multiaddrs() {

        let valid = NodeDescriptor {
            id: NodeId("sam01".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        };
        
        assert!(is_valid_server_node(&valid));

        let empty_id = NodeDescriptor {
            id: NodeId("".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        };

        assert!(!is_valid_server_node(&empty_id));

        let bad_addr = NodeDescriptor {
            id: NodeId("sam01".to_string()),
            addrs: vec!["127.0.0.1:4001".to_string()],
            is_local: false,
        };
        
        assert!(!is_valid_server_node(&bad_addr));

    }

    #[test]
    fn is_server_peer_discovery_query_detects_internal_and_alias() {
        assert!(is_server_peer_discovery_query("__distdb_show_server_peers__"));
        assert!(is_server_peer_discovery_query("show server peers"));
        assert!(is_server_peer_discovery_query("SHOW SERVER PEERS;"));
        assert!(!is_server_peer_discovery_query("show peers"));
    }

    #[test]
    fn is_bootstrap_status_query_detects_internal_and_alias() {
        assert!(is_bootstrap_status_query("__distdb_bootstrap_status__"));
        assert!(is_bootstrap_status_query("show bootstrap status"));
        assert!(is_bootstrap_status_query("SHOW BOOTSTRAP STATUS;"));
        assert!(!is_bootstrap_status_query("show bootstrap"));
    }

    #[test]
    fn is_show_entities_query_detects_internal_and_alias() {
        assert!(is_show_entities_query("__distdb_show_entities__"));
        assert!(is_show_entities_query("show entities"));
        assert!(is_show_entities_query("SHOW ENTITIES;"));
        assert!(!is_show_entities_query("show entity"));
    }

    #[test]
    fn is_show_catalog_workers_query_detects_internal_and_alias() {
        assert!(is_show_catalog_workers_query("__distdb_show_catalog_workers__"));
        assert!(is_show_catalog_workers_query("show catalog workers"));
        assert!(is_show_catalog_workers_query("SHOW CATALOG WORKERS;"));
        assert!(!is_show_catalog_workers_query("show catalog worker"));
    }
}
