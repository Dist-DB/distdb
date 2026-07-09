
    use super::*;
    use crate::core::config::ServerRuntimeConfig;
    use connector::ResponseStatus;
    use serverlib::DatabaseId;

    fn unique_test_data_dir(prefix: &str) -> std::path::PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{}-{}-{}", prefix, std::process::id(), suffix))
    }

    fn request(request_id: &str, command: ConnectorCommand) -> ConnectorRequest {
        ConnectorRequest {
            request_id: request_id.to_string(),
            command,
        }
    }

    fn ensure_applied(response: &ConnectorResponse, context: &str) {
        assert!(
            matches!(response.status, ResponseStatus::Applied),
            "{}: expected applied, got {:?}",
            context,
            response
        );
    }

    fn seed_catalog(
        app: &mut ServerApp,
        session_id: &str,
        database_name: &str,
        create_table_sql: &str,
        insert_sqls: Vec<String>,
    ) -> String {

        ensure_applied(
            &app.handle_connector_request_for_session(
                &request(
                    &format!("create-db-{}", database_name),
                    ConnectorCommand::CreateDatabase {
                        database_name: database_name.to_string(),
                    },
                ),
                session_id,
            ),
            "create database",
        );

        let database_id = DatabaseId::from_database_name(database_name)
            .expect("database id should be valid")
            .0;

        ensure_applied(
            &app.handle_connector_request_for_session(
                &request(
                    &format!("create-table-{}", database_name),
                    ConnectorCommand::Query {
                        query: connector::DataQuery {
                            database_id: database_id.clone(),
                            sql: create_table_sql.to_string(),
                        },
                    },
                ),
                session_id,
            ),
            "create table",
        );

        for (idx, sql) in insert_sqls.into_iter().enumerate() {

            ensure_applied(
                &app.handle_connector_request_for_session(
                    &request(
                        &format!("insert-{}-{}", database_name, idx),
                        ConnectorCommand::Query {
                            query: connector::DataQuery {
                                database_id: database_id.clone(),
                                sql,
                            },
                        },
                    ),
                    session_id,
                ),
                "insert row",
            );

        }

        database_id

    }

    #[tokio::test]
    async fn multi_catalog_query_fanout_merges_rows() {

        let data_dir = unique_test_data_dir("distdb-multi-cat-merge");
        let config = ServerRuntimeConfig::default_local_with_data_dir(data_dir);
        let mut app = ServerApp::new(config).expect("server app should initialize");
        let session_id = "session-multi";
        app.init_session(session_id.to_string(), 1, "root".to_string());

        let db_a = seed_catalog(
            &mut app,
            session_id,
            "alpha",
            "create table users (name text)",
            vec!["insert into users (name) values ('alice')".to_string()],
        );

        let db_b = seed_catalog(
            &mut app,
            session_id,
            "beta",
            "create table users (name text)",
            vec!["insert into users (name) values ('bob')".to_string()],
        );

        let app = Arc::new(RwLock::new(app));
        let dispatcher = Arc::new(CatalogDispatcher::new(Arc::clone(&app)));

        let request = request(
            "multi-read",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: String::new(),
                    sql: format!(
                        "select name from users /* from {}.users join {}.users */",
                        db_a,
                        db_b
                    ),
                },
            },
        );

        let response = maybe_dispatch_multi_catalog_query(&request, session_id, "root", 1, &dispatcher)
            .await
            .expect("multi-catalog coordinator should handle request");

        assert!(matches!(response.status, ResponseStatus::Applied));

        let ConnectorResult::Query(result) = response.result else {
            panic!("expected query result");
        };

        let mut values = result
            .rows
            .iter()
            .filter_map(|row| row.first())
            .map(|value| String::from_utf8_lossy(value).to_string())
            .collect::<Vec<_>>();
        values.sort();

        let mut unique = values.clone();
        unique.dedup();

        assert_eq!(unique, vec!["alice".to_string(), "bob".to_string()]);
        assert!(values.len() >= 2, "expected at least two merged rows");

    }

    #[tokio::test]
    async fn multi_catalog_query_rejects_schema_mismatch() {

        let data_dir = unique_test_data_dir("distdb-multi-cat-mismatch");
        let config = ServerRuntimeConfig::default_local_with_data_dir(data_dir);
        let mut app = ServerApp::new(config).expect("server app should initialize");
        let session_id = "session-mismatch";
        app.init_session(session_id.to_string(), 1, "root".to_string());

        let db_a = seed_catalog(
            &mut app,
            session_id,
            "gamma",
            "create table users (name text)",
            vec!["insert into users (name) values ('alice')".to_string()],
        );

        let db_b = seed_catalog(
            &mut app,
            session_id,
            "delta",
            "create table users (name text, age bigint)",
            vec!["insert into users (name, age) values ('bob', 42)".to_string()],
        );

        let app = Arc::new(RwLock::new(app));
        let dispatcher = Arc::new(CatalogDispatcher::new(Arc::clone(&app)));

        let request = request(
            "multi-mismatch",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: String::new(),
                    sql: format!(
                        "select * from users /* from {}.users join {}.users */",
                        db_a,
                        db_b
                    ),
                },
            },
        );

        let response = maybe_dispatch_multi_catalog_query(&request, session_id, "root", 1, &dispatcher)
            .await
            .expect("multi-catalog coordinator should handle request");

        assert!(matches!(response.status, ResponseStatus::Rejected));

        let ConnectorResult::Error(message) = response.result else {
            panic!("expected error result");
        };

        assert!(
            message.contains("mismatched schemas"),
            "expected mismatch error, got: {}",
            message
        );

    }

    #[tokio::test]
    async fn multi_catalog_query_rejects_write_statement() {

        let data_dir = unique_test_data_dir("distdb-multi-cat-write");
        let config = ServerRuntimeConfig::default_local_with_data_dir(data_dir);
        let app = Arc::new(RwLock::new(
            ServerApp::new(config).expect("server app should initialize"),
        ));
        let dispatcher = Arc::new(CatalogDispatcher::new(Arc::clone(&app)));

        let request = request(
            "multi-write",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: String::new(),
                    sql: "update users set name = 'x' /* from a.users join b.users */".to_string(),
                },
            },
        );

        let response = maybe_dispatch_multi_catalog_query(&request, "session-write", "root", 1, &dispatcher)
            .await
            .expect("multi-catalog coordinator should handle request");

        assert!(matches!(response.status, ResponseStatus::Rejected));

        let ConnectorResult::Error(message) = response.result else {
            panic!("expected error result");
        };

        assert!(
            message.contains("read-only queries only"),
            "expected write rejection, got: {}",
            message
        );

    }

    #[tokio::test]
    async fn policy_snapshot_multi_catalog_write_rejection_message() {

        let data_dir = unique_test_data_dir("distdb-policy-snapshot-write");
        let config = ServerRuntimeConfig::default_local_with_data_dir(data_dir);
        let app = Arc::new(RwLock::new(
            ServerApp::new(config).expect("server app should initialize"),
        ));
        let dispatcher = Arc::new(CatalogDispatcher::new(Arc::clone(&app)));

        let request = request(
            "policy-write",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: String::new(),
                    sql: "update users set name = 'x' /* from a.users join b.users */".to_string(),
                },
            },
        );

        let response = maybe_dispatch_multi_catalog_query(&request, "session-policy", "root", 1, &dispatcher)
            .await
            .expect("multi-catalog coordinator should handle request");

        let ConnectorResult::Error(message) = response.result else {
            panic!("expected error result");
        };

        assert_eq!(
            message,
            "multi-catalog coordination currently supports read-only queries only"
        );

    }

    #[tokio::test]
    async fn policy_snapshot_multi_catalog_schema_mismatch_message() {

        let data_dir = unique_test_data_dir("distdb-policy-snapshot-mismatch");
        let config = ServerRuntimeConfig::default_local_with_data_dir(data_dir);
        let mut app = ServerApp::new(config).expect("server app should initialize");
        let session_id = "session-policy-mismatch";
        app.init_session(session_id.to_string(), 1, "root".to_string());

        let db_a = seed_catalog(
            &mut app,
            session_id,
            "policy_gamma",
            "create table users (name text)",
            vec!["insert into users (name) values ('alice')".to_string()],
        );

        let db_b = seed_catalog(
            &mut app,
            session_id,
            "policy_delta",
            "create table users (name text, age bigint)",
            vec!["insert into users (name, age) values ('bob', 42)".to_string()],
        );

        let app = Arc::new(RwLock::new(app));
        let dispatcher = Arc::new(CatalogDispatcher::new(Arc::clone(&app)));

        let request = request(
            "policy-mismatch",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: String::new(),
                    sql: format!(
                        "select * from users /* from {}.users join {}.users */",
                        db_a,
                        db_b
                    ),
                },
            },
        );

        let response = maybe_dispatch_multi_catalog_query(&request, session_id, "root", 1, &dispatcher)
            .await
            .expect("multi-catalog coordinator should handle request");

        let ConnectorResult::Error(message) = response.result else {
            panic!("expected error result");
        };

        assert_eq!(
            message,
            "multi-catalog query produced mismatched schemas across catalogs"
        );

    }

    #[tokio::test]
    async fn policy_snapshot_multi_catalog_catalog_status_rejection_message() {

        let data_dir = unique_test_data_dir("distdb-policy-snapshot-status");
        let config = ServerRuntimeConfig::default_local_with_data_dir(data_dir);
        let app = Arc::new(RwLock::new(
            ServerApp::new(config).expect("server app should initialize"),
        ));
        let dispatcher = Arc::new(CatalogDispatcher::new(Arc::clone(&app)));

        let request = request(
            "policy-status",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: String::new(),
                    sql: "select * from users /* from alpha.users join beta.users */".to_string(),
                },
            },
        );

        let response = maybe_dispatch_multi_catalog_query(&request, "session-policy", "root", 1, &dispatcher)
            .await
            .expect("multi-catalog coordinator should handle request");

        let ConnectorResult::Error(message) = response.result else {
            panic!("expected error result");
        };

        assert_eq!(
            message,
            "multi-catalog query failed in catalog 'alpha' with status Rejected"
        );

    }

    #[test]
    fn routing_compliance_schema_and_mutation_are_catalog_bound() {

        let schema_request = request(
            "schema-route",
            ConnectorCommand::Schema {
                database_id: " Orders_DB ".to_string(),
                command: connector::SchemaCommand::DropTable {
                    table_id: "users".to_string(),
                },
            },
        );

        let mutation_request = request(
            "mutation-route",
            ConnectorCommand::Mutation {
                database_id: " Inventory_DB ".to_string(),
                mutation: connector::DataMutation::Delete {
                    table_id: "users".to_string(),
                    predicate_sql: None,
                },
            },
        );

        assert_eq!(
            request_catalog_route_key(&schema_request),
            Some("orders_db".to_string())
        );
        assert_eq!(
            request_catalog_route_key(&mutation_request),
            Some("inventory_db".to_string())
        );

    }

    #[test]
    fn routing_compliance_query_with_explicit_database_id_is_catalog_bound() {

        let query_request = request(
            "query-explicit-db",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: " SALES_DB ".to_string(),
                    sql: "select * from users".to_string(),
                },
            },
        );

        assert_eq!(
            request_catalog_route_key(&query_request),
            Some("sales_db".to_string())
        );

    }

    #[test]
    fn routing_compliance_query_infers_single_catalog_from_sql_when_session_db_is_empty() {

        let query_request = request(
            "query-infer-single",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: String::new(),
                    sql: "select * from alpha.users".to_string(),
                },
            },
        );

        assert_eq!(
            request_catalog_route_key(&query_request),
            Some("alpha".to_string())
        );

    }

    #[test]
    fn routing_compliance_query_with_multi_catalog_sql_has_no_single_route_key() {

        let query_request = request(
            "query-infer-multi",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: String::new(),
                    sql: "select * from alpha.users join beta.users on alpha.users.id = beta.users.id".to_string(),
                },
            },
        );

        assert_eq!(request_catalog_route_key(&query_request), None);

    }

    #[tokio::test]
    async fn routing_compliance_multi_catalog_coordinator_skips_non_multi_paths() {

        let data_dir = unique_test_data_dir("distdb-routing-compliance");
        let config = ServerRuntimeConfig::default_local_with_data_dir(data_dir);
        let app = Arc::new(RwLock::new(
            ServerApp::new(config).expect("server app should initialize"),
        ));
        let dispatcher = Arc::new(CatalogDispatcher::new(Arc::clone(&app)));

        let explicit_db_request = request(
            "coordinator-explicit-db",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "alpha".to_string(),
                    sql: "select * from users".to_string(),
                },
            },
        );

        let single_catalog_request = request(
            "coordinator-single-catalog",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: String::new(),
                    sql: "select * from alpha.users".to_string(),
                },
            },
        );

        let explicit_db_result = maybe_dispatch_multi_catalog_query(
            &explicit_db_request,
            "session-a",
            "root",
            1,
            &dispatcher,
        )
        .await;

        let single_catalog_result = maybe_dispatch_multi_catalog_query(
            &single_catalog_request,
            "session-b",
            "root",
            2,
            &dispatcher,
        )
        .await;

        assert!(
            explicit_db_result.is_none(),
            "coordinator should skip requests with explicit database id"
        );
        assert!(
            single_catalog_result.is_none(),
            "coordinator should skip single-catalog SQL"
        );

    }

    #[test]
    fn node_announce_dedup_key_is_stable() {
        let node = PeerNode {
            id: "sam01".to_string(),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        };

        let key1 = node_announce_dedup_key(&node);
        let key2 = node_announce_dedup_key(&node);
        assert_eq!(key1, key2);
    }

    #[test]
    fn is_valid_server_node_requires_non_empty_id_and_multiaddrs() {

        let valid = PeerNode {
            id: "sam01".to_string(),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        };
        
        assert!(is_valid_server_node(&valid));

        let empty_id = PeerNode {
            id: "".to_string(),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        };

        assert!(!is_valid_server_node(&empty_id));

        let bad_addr = PeerNode {
            id: "sam01".to_string(),
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

