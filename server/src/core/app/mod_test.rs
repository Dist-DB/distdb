use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use super::*;
use crate::core::config::ServerRuntimeConfig;
use crate::core::control::session::{
    configure_bootstrap_crypto_context,
    encode_set_password_wal_payload,
    reset_bootstrap_password_for_tests,
    ServerConnectionSession,
};
use crate::core::mappings::perf::QueryTimingThresholds;
use common::helpers::format::FileKind;
use common::helpers::utils::md5_hash;
use connector::{
    ConnectorClient, ConnectorCommand, ConnectorError, ConnectorRequest, ConnectorResponse, ConnectorResult,
    ConnectorTransport, ResponseStatus,
};
use serverlib::engine::database::transaction::TransactionLog;
use serverlib::{
    DatabaseCatalog, DatabaseIndex, DatabaseIndexKind, EntityMetadata, EntityMetadataPayload, FieldDef,
    FieldIndex, FieldType, ObjectStatus, SchemaChangePayload, SqlDefinitionAction,
    SqlDefinitionPayload, SqlObjectKind, TableSchema, TransactionId, TransactionKind, RuntimeIndexStore,
    TransactionRecord, UserId,
};
use serverlib::decode_row_payload;
use serverlib::render_stored_field_value;

#[derive(Debug)]
struct InProcessServerTransport {
    app: RefCell<ServerApp>,
}

impl ConnectorTransport for InProcessServerTransport {
    fn request(&self, request: &ConnectorRequest) -> Result<ConnectorResponse, ConnectorError> {
        Ok(self.app.borrow_mut().handle_connector_request(request))
    }
}

fn table_stream_id(app: &ServerApp, database_id: &str, table_id: &str) -> String {
    app.catalogs
        .get(database_id)
        .and_then(|catalog| catalog.entity_wal_stream_id(table_id))
        .unwrap_or_else(|| table_id.to_string())
}

#[test]
fn query_requires_select_privilege_for_non_root_user() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-select-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let query = ConnectorRequest {
        request_id: "select-main".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show databases".to_string(),
            },
        },
    };

    let response = app.handle_connector_request_for_session(&query, "alice-session");
    assert_eq!(response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected permission error result");
    };

    assert!(message.contains("permission denied"));

}

#[test]
fn query_allows_user_with_select_privilege() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-select-allowed-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;

    let catalog = app
        .catalogs
        .get_mut(&main_id)
        .expect("main catalog should exist");

    let mut alice_acl = serverlib::AccountAclEntry::new(serverlib::UserId("alice".to_string()), "main");
    alice_acl.append_privilege(serverlib::engine::security::AccountPrivilege::Select);
    catalog.upsert_account_acl_entry(alice_acl);

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let query = ConnectorRequest {
        request_id: "select-main".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show databases".to_string(),
            },
        },
    };

    let response = app.handle_connector_request_for_session(&query, "alice-session");
    assert_eq!(response.status, ResponseStatus::Applied);

}

#[test]
fn query_object_acl_allows_only_granted_objects() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-object-acl-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let create_users_table = ConnectorRequest {
        request_id: "create-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    };

    let create_users_table_response =
        app.handle_connector_request_for_session(&create_users_table, "root-session");
    assert_eq!(create_users_table_response.status, ResponseStatus::Applied);

    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;

    let catalog = app
        .catalogs
        .get_mut(&main_id)
        .expect("main catalog should exist");

    let mut alice_acl = serverlib::AccountAclEntry::new(serverlib::UserId("alice".to_string()), "main");
    alice_acl.append_object_privilege("users", serverlib::engine::security::AccountPrivilege::Select);
    catalog.upsert_account_acl_entry(alice_acl);

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let users_query = ConnectorRequest {
        request_id: "show-users-columns".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let users_query_response = app.handle_connector_request_for_session(&users_query, "alice-session");
    assert_eq!(users_query_response.status, ResponseStatus::Applied);

    let orders_query = ConnectorRequest {
        request_id: "show-orders-columns".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    let orders_query_response = app.handle_connector_request_for_session(&orders_query, "alice-session");
    assert_eq!(orders_query_response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(message) = orders_query_response.result else {
        panic!("expected permission error result");
    };

    assert!(message.contains("missing 'SELECT' on object 'main.orders'"));

}

#[test]
fn query_join_requires_privileges_for_all_referenced_objects() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-object-join-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let create_users_table = ConnectorRequest {
        request_id: "create-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    };

    let create_users_table_response =
        app.handle_connector_request_for_session(&create_users_table, "root-session");
    assert_eq!(create_users_table_response.status, ResponseStatus::Applied);

    let create_orders_table = ConnectorRequest {
        request_id: "create-orders".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table orders (id bigint not null primary key, user_id bigint not null)"
                    .to_string(),
            },
        },
    };

    let create_orders_table_response =
        app.handle_connector_request_for_session(&create_orders_table, "root-session");
    assert_eq!(create_orders_table_response.status, ResponseStatus::Applied);

    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;

    let catalog = app
        .catalogs
        .get_mut(&main_id)
        .expect("main catalog should exist");

    let mut alice_acl = serverlib::AccountAclEntry::new(serverlib::UserId("alice".to_string()), "main");
    alice_acl.append_object_privilege("users", serverlib::engine::security::AccountPrivilege::Select);
    catalog.upsert_account_acl_entry(alice_acl);

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let join_query = ConnectorRequest {
        request_id: "join-query".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select u.id from users u inner join orders o on u.id = o.user_id".to_string(),
            },
        },
    };

    let join_query_response = app.handle_connector_request_for_session(&join_query, "alice-session");
    assert_eq!(join_query_response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(message) = join_query_response.result else {
        panic!("expected permission error result");
    };

    assert!(message.contains("missing 'SELECT' on object 'main.orders'"));

}

#[test]
fn grant_and_revoke_queries_update_object_acl_access() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-grant-revoke-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let create_users_table = ConnectorRequest {
        request_id: "create-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    };

    let create_users_table_response =
        app.handle_connector_request_for_session(&create_users_table, "root-session");
    assert_eq!(create_users_table_response.status, ResponseStatus::Applied);

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let show_users_columns = ConnectorRequest {
        request_id: "show-users-columns-initial".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let before_grant = app.handle_connector_request_for_session(&show_users_columns, "alice-session");
    assert_eq!(before_grant.status, ResponseStatus::Rejected);

    let grant = ConnectorRequest {
        request_id: "grant-users-select".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to alice".to_string(),
            },
        },
    };

    let grant_response = app.handle_connector_request_for_session(&grant, "root-session");
    assert_eq!(grant_response.status, ResponseStatus::Applied);

    let main_wal_id = app.resolve_catalog_wal_stream_for_database("main");
    let security_records_after_grant = app
        .wal
        .since_kinds(&main_wal_id, None, &[TransactionKind::SecurityChange]);

    assert!(!security_records_after_grant.is_empty());

    let grant_acl_payload = security_records_after_grant
        .last()
        .and_then(|record| record.payload_logical())
        .expect("grant security WAL payload should exist");

    let grant_acl_entry = ServerApp::decode_account_acl_wal_payload(grant_acl_payload)
        .expect("grant security WAL payload should decode as ACL payload");

    assert_eq!(grant_acl_entry.user_id.0, "alice");
    assert!(grant_acl_entry.has_privilege_for_object(
        serverlib::engine::security::AccountPrivilege::Select,
        Some("users"),
    ));

    let after_grant = app.handle_connector_request_for_session(&show_users_columns, "alice-session");
    assert_eq!(after_grant.status, ResponseStatus::Applied);

    let revoke = ConnectorRequest {
        request_id: "revoke-users-select".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on users from alice".to_string(),
            },
        },
    };

    let revoke_response = app.handle_connector_request_for_session(&revoke, "root-session");
    assert_eq!(revoke_response.status, ResponseStatus::Applied);

    let security_records_after_revoke = app
        .wal
        .since_kinds(&main_wal_id, None, &[TransactionKind::SecurityChange]);

    assert!(security_records_after_revoke.len() > security_records_after_grant.len());

    let revoke_acl_payload = security_records_after_revoke
        .last()
        .and_then(|record| record.payload_logical())
        .expect("revoke security WAL payload should exist");

    let revoke_acl_entry = ServerApp::decode_account_acl_wal_payload(revoke_acl_payload)
        .expect("revoke security WAL payload should decode as ACL payload");

    assert_eq!(revoke_acl_entry.user_id.0, "alice");
    assert!(!revoke_acl_entry.has_privilege_for_object(
        serverlib::engine::security::AccountPrivilege::Select,
        Some("users"),
    ));

    let after_revoke = app.handle_connector_request_for_session(&show_users_columns, "alice-session");
    assert_eq!(after_revoke.status, ResponseStatus::Rejected);

}

#[test]
fn schema_grant_preserves_access_after_object_revoke_until_schema_revoke() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-schema-object-precedence-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    for (request_id, sql) in [
        (
            "create-users",
            "create table users (id bigint not null primary key)",
        ),
        (
            "create-orders",
            "create table orders (id bigint not null primary key)",
        ),
    ] {
        let create_table = ConnectorRequest {
            request_id: request_id.to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        };
        let response = app.handle_connector_request_for_session(&create_table, "root-session");
        assert_eq!(response.status, ResponseStatus::Applied);
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let show_users_columns = ConnectorRequest {
        request_id: "show-users-columns".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let before_grant = app.handle_connector_request_for_session(&show_users_columns, "alice-session");
    assert_eq!(before_grant.status, ResponseStatus::Rejected);

    let grant_schema = ConnectorRequest {
        request_id: "grant-schema-main-select".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema main to alice".to_string(),
            },
        },
    };

    let grant_schema_response = app.handle_connector_request_for_session(&grant_schema, "root-session");
    assert_eq!(grant_schema_response.status, ResponseStatus::Applied);

    let after_schema_grant = app.handle_connector_request_for_session(&show_users_columns, "alice-session");
    assert_eq!(after_schema_grant.status, ResponseStatus::Applied);

    let revoke_users_object = ConnectorRequest {
        request_id: "revoke-users-select-object".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on users from alice".to_string(),
            },
        },
    };

    let revoke_users_response =
        app.handle_connector_request_for_session(&revoke_users_object, "root-session");
    assert_eq!(revoke_users_response.status, ResponseStatus::Applied);

    let after_object_revoke = app.handle_connector_request_for_session(&show_users_columns, "alice-session");
    assert_eq!(after_object_revoke.status, ResponseStatus::Applied);

    let revoke_schema = ConnectorRequest {
        request_id: "revoke-schema-main-select".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on schema main from alice".to_string(),
            },
        },
    };

    let revoke_schema_response = app.handle_connector_request_for_session(&revoke_schema, "root-session");
    assert_eq!(revoke_schema_response.status, ResponseStatus::Applied);

    let after_schema_revoke = app.handle_connector_request_for_session(&show_users_columns, "alice-session");
    assert_eq!(after_schema_revoke.status, ResponseStatus::Rejected);

}

#[test]
fn object_grant_survives_schema_revoke_and_remains_object_scoped() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-schema-object-survival-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    for (request_id, sql) in [
        (
            "create-users",
            "create table users (id bigint not null primary key)",
        ),
        (
            "create-orders",
            "create table orders (id bigint not null primary key)",
        ),
    ] {
        let create_table = ConnectorRequest {
            request_id: request_id.to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        };
        let response = app.handle_connector_request_for_session(&create_table, "root-session");
        assert_eq!(response.status, ResponseStatus::Applied);
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let show_users_columns = ConnectorRequest {
        request_id: "show-users-columns".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_orders_columns = ConnectorRequest {
        request_id: "show-orders-columns".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    let grant_users_object = ConnectorRequest {
        request_id: "grant-users-select-object".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to alice".to_string(),
            },
        },
    };

    let grant_schema = ConnectorRequest {
        request_id: "grant-schema-main-select".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema main to alice".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&grant_users_object, "root-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&grant_schema, "root-session").status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request_for_session(&show_users_columns, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders_columns, "alice-session").status,
        ResponseStatus::Applied
    );

    let revoke_schema = ConnectorRequest {
        request_id: "revoke-schema-main-select".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on schema main from alice".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&revoke_schema, "root-session").status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request_for_session(&show_users_columns, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders_columns, "alice-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn qualified_object_grant_uses_explicit_database_not_query_hint() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-qualified-object-target-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        let response = app.handle_connector_request_for_session(&create_db, "root-session");
        assert_eq!(response.status, ResponseStatus::Applied);

        let create_users = ConnectorRequest {
            request_id: format!("create-users-{database_name}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: database_name.to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };
        let create_users_response =
            app.handle_connector_request_for_session(&create_users, "root-session");
        assert_eq!(create_users_response.status, ResponseStatus::Applied);
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let grant_qualified_object = ConnectorRequest {
        request_id: "grant-qualified-object-analytics-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on analytics.users to alice".to_string(),
            },
        },
    };

    let grant_response =
        app.handle_connector_request_for_session(&grant_qualified_object, "root-session");
    assert_eq!(grant_response.status, ResponseStatus::Applied);

    let show_analytics_users = ConnectorRequest {
        request_id: "show-users-analytics".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_main_users = ConnectorRequest {
        request_id: "show-users-main".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let analytics_allowed =
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session");
    assert_eq!(analytics_allowed.status, ResponseStatus::Applied);

    let main_denied = app.handle_connector_request_for_session(&show_main_users, "alice-session");
    assert_eq!(main_denied.status, ResponseStatus::Rejected);

}

#[test]
fn schema_grant_uses_explicit_schema_not_query_hint_database() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-schema-target-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        let response = app.handle_connector_request_for_session(&create_db, "root-session");
        assert_eq!(response.status, ResponseStatus::Applied);

        let create_users = ConnectorRequest {
            request_id: format!("create-users-{database_name}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: database_name.to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };
        let create_users_response =
            app.handle_connector_request_for_session(&create_users, "root-session");
        assert_eq!(create_users_response.status, ResponseStatus::Applied);
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let grant_schema = ConnectorRequest {
        request_id: "grant-schema-analytics-select".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema analytics to alice".to_string(),
            },
        },
    };

    let grant_response = app.handle_connector_request_for_session(&grant_schema, "root-session");
    assert_eq!(grant_response.status, ResponseStatus::Applied);

    let show_analytics_users = ConnectorRequest {
        request_id: "show-users-analytics".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_main_users = ConnectorRequest {
        request_id: "show-users-main".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let analytics_allowed =
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session");
    assert_eq!(analytics_allowed.status, ResponseStatus::Applied);

    let main_denied = app.handle_connector_request_for_session(&show_main_users, "alice-session");
    assert_eq!(main_denied.status, ResponseStatus::Rejected);

}

#[test]
fn qualified_object_revoke_only_affects_targeted_database_object() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-qualified-object-revoke-target-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        let response = app.handle_connector_request_for_session(&create_db, "root-session");
        assert_eq!(response.status, ResponseStatus::Applied);

        let create_users = ConnectorRequest {
            request_id: format!("create-users-{database_name}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: database_name.to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };
        let create_users_response =
            app.handle_connector_request_for_session(&create_users, "root-session");
        assert_eq!(create_users_response.status, ResponseStatus::Applied);
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let grant_main_users = ConnectorRequest {
        request_id: "grant-main-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to alice".to_string(),
            },
        },
    };

    let grant_analytics_users = ConnectorRequest {
        request_id: "grant-analytics-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on analytics.users to alice".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&grant_main_users, "root-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&grant_analytics_users, "root-session").status,
        ResponseStatus::Applied
    );

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-before-revoke".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_analytics_users = ConnectorRequest {
        request_id: "show-analytics-users-before-revoke".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Applied
    );

    let revoke_analytics_users = ConnectorRequest {
        request_id: "revoke-analytics-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on analytics.users from alice".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&revoke_analytics_users, "root-session").status,
        ResponseStatus::Applied
    );

    let show_main_after_revoke = ConnectorRequest {
        request_id: "show-main-users-after-revoke".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_analytics_after_revoke = ConnectorRequest {
        request_id: "show-analytics-users-after-revoke".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_after_revoke, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_after_revoke, "alice-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn schema_revoke_only_affects_targeted_database_not_other_schema_grants() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-schema-revoke-target-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        let response = app.handle_connector_request_for_session(&create_db, "root-session");
        assert_eq!(response.status, ResponseStatus::Applied);

        let create_users = ConnectorRequest {
            request_id: format!("create-users-{database_name}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: database_name.to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };
        let create_users_response =
            app.handle_connector_request_for_session(&create_users, "root-session");
        assert_eq!(create_users_response.status, ResponseStatus::Applied);
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let grant_main_schema = ConnectorRequest {
        request_id: "grant-main-schema".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema main to alice".to_string(),
            },
        },
    };

    let grant_analytics_schema = ConnectorRequest {
        request_id: "grant-analytics-schema".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema analytics to alice".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&grant_main_schema, "root-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&grant_analytics_schema, "root-session").status,
        ResponseStatus::Applied
    );

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-before-schema-revoke".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_analytics_users = ConnectorRequest {
        request_id: "show-analytics-users-before-schema-revoke".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Applied
    );

    let revoke_analytics_schema = ConnectorRequest {
        request_id: "revoke-analytics-schema".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on schema analytics from alice".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&revoke_analytics_schema, "root-session").status,
        ResponseStatus::Applied
    );

    let show_main_after_revoke = ConnectorRequest {
        request_id: "show-main-users-after-schema-revoke".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_analytics_after_revoke = ConnectorRequest {
        request_id: "show-analytics-users-after-schema-revoke".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_after_revoke, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_after_revoke, "alice-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn mixed_cross_database_revoke_order_keeps_scope_isolated_per_step() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-mixed-revoke-order-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        let response = app.handle_connector_request_for_session(&create_db, "root-session");
        assert_eq!(response.status, ResponseStatus::Applied);

        let create_users = ConnectorRequest {
            request_id: format!("create-users-{database_name}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: database_name.to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };
        let create_users_response =
            app.handle_connector_request_for_session(&create_users, "root-session");
        assert_eq!(create_users_response.status, ResponseStatus::Applied);
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let grant_main_users = ConnectorRequest {
        request_id: "grant-main-users-object".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to alice".to_string(),
            },
        },
    };

    let grant_analytics_schema = ConnectorRequest {
        request_id: "grant-analytics-schema".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema analytics to alice".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&grant_main_users, "root-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&grant_analytics_schema, "root-session").status,
        ResponseStatus::Applied
    );

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-mixed-revoke-order".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_analytics_users = ConnectorRequest {
        request_id: "show-analytics-users-mixed-revoke-order".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Applied
    );

    let revoke_analytics_schema = ConnectorRequest {
        request_id: "revoke-analytics-schema-mixed".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on schema analytics from alice".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&revoke_analytics_schema, "root-session").status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Rejected
    );

    let revoke_main_users = ConnectorRequest {
        request_id: "revoke-main-users-object-mixed".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on users from alice".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&revoke_main_users, "root-session").status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn malformed_qualified_acl_target_is_rejected() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-malformed-qualified-target-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let malformed_grant = ConnectorRequest {
        request_id: "grant-malformed-qualified-target".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on analytics. to alice".to_string(),
            },
        },
    };

    let response = app.handle_connector_request_for_session(&malformed_grant, "root-session");
    assert_eq!(response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected parser rejection for malformed qualified ACL target");
    };

    assert!(message.to_lowercase().contains("parse") || message.to_lowercase().contains("syntax"));

}

#[test]
fn schema_grant_with_grant_option_tracks_grant_acl_and_revokes_cleanly() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-grant-option-schema-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let grant_with_option = ConnectorRequest {
        request_id: "grant-schema-main-select-with-option".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema main to alice with grant option".to_string(),
            },
        },
    };

    let grant_response = app.handle_connector_request_for_session(&grant_with_option, "root-session");
    assert_eq!(grant_response.status, ResponseStatus::Applied);

    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;

    let catalog = app
        .catalogs
        .get(&main_id)
        .expect("main catalog should exist");

    let granted_acl = catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL should exist after grant with option");

    assert!(granted_acl.acl.contains("SELECT"));
    assert!(granted_acl.grant_acl.contains("SELECT"));

    let revoke_schema = ConnectorRequest {
        request_id: "revoke-schema-main-select".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on schema main from alice".to_string(),
            },
        },
    };

    let revoke_response = app.handle_connector_request_for_session(&revoke_schema, "root-session");
    assert_eq!(revoke_response.status, ResponseStatus::Applied);

    let catalog = app
        .catalogs
        .get(&main_id)
        .expect("main catalog should exist after revoke");

    let revoked_acl = catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL should still exist after revoke");

    assert!(!revoked_acl.acl.contains("SELECT"));
    assert!(!revoked_acl.grant_acl.contains("SELECT"));

}

#[test]
fn acl_and_non_acl_batch_is_rejected_without_applying_acl_side_effect() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-acl-batch-reject-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_main, "root-session").status,
        ResponseStatus::Applied
    );

    let create_users = ConnectorRequest {
        request_id: "create-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_users, "root-session").status,
        ResponseStatus::Applied
    );

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let mixed_batch = ConnectorRequest {
        request_id: "mixed-acl-non-acl-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to alice; show columns from users".to_string(),
            },
        },
    };

    let mixed_response = app.handle_connector_request_for_session(&mixed_batch, "root-session");
    assert_eq!(mixed_response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(message) = mixed_response.result else {
        panic!("expected mixed ACL/non-ACL batch rejection error");
    };

    assert!(message.contains("GRANT/REVOKE cannot be combined with non-ACL statements"));

    let show_users = ConnectorRequest {
        request_id: "show-users-after-mixed-batch-reject".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let alice_response = app.handle_connector_request_for_session(&show_users, "alice-session");
    assert_eq!(alice_response.status, ResponseStatus::Rejected);

}

#[test]
fn non_root_acl_batch_is_rejected_without_acl_side_effects() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-non-root-acl-batch-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_main, "root-session").status,
        ResponseStatus::Applied
    );

    for (request_id, sql) in [
        (
            "create-users",
            "create table users (id bigint not null primary key)",
        ),
        (
            "create-orders",
            "create table orders (id bigint not null primary key)",
        ),
    ] {
        let create_table = ConnectorRequest {
            request_id: request_id.to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_table, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("bob-session".to_string(), 1, "bob".to_string());
    app.init_session("alice-session".to_string(), 2, "alice".to_string());

    let non_root_acl_batch = ConnectorRequest {
        request_id: "non-root-acl-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob; grant select on orders to bob".to_string(),
            },
        },
    };

    let response = app.handle_connector_request_for_session(&non_root_acl_batch, "alice-session");
    assert_eq!(response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(_) = response.result else {
        panic!("expected non-root ACL batch rejection error");
    };

    let show_users = ConnectorRequest {
        request_id: "show-users-bob-after-non-root-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_orders = ConnectorRequest {
        request_id: "show-orders-bob-after-non-root-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn root_acl_only_batch_applies_multiple_acl_mutations() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-root-acl-batch-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_main, "root-session").status,
        ResponseStatus::Applied
    );

    for (request_id, sql) in [
        (
            "create-users",
            "create table users (id bigint not null primary key)",
        ),
        (
            "create-orders",
            "create table orders (id bigint not null primary key)",
        ),
    ] {
        let create_table = ConnectorRequest {
            request_id: request_id.to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_table, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("bob-session".to_string(), 1, "bob".to_string());

    let root_acl_batch = ConnectorRequest {
        request_id: "root-acl-only-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob; grant select on orders to bob".to_string(),
            },
        },
    };

    let batch_response = app.handle_connector_request_for_session(&root_acl_batch, "root-session");
    assert_eq!(batch_response.status, ResponseStatus::Applied);

    let show_users = ConnectorRequest {
        request_id: "show-users-bob-after-root-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_orders = ConnectorRequest {
        request_id: "show-orders-bob-after-root-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Applied
    );

}

#[test]
fn non_root_cross_database_acl_batch_is_rejected_without_side_effects() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-non-root-cross-db-acl-batch-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_db, "root-session").status,
            ResponseStatus::Applied
        );

        let create_users = ConnectorRequest {
            request_id: format!("create-users-{database_name}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: database_name.to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_users, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());
    app.init_session("bob-session".to_string(), 2, "bob".to_string());

    let non_root_acl_batch = ConnectorRequest {
        request_id: "non-root-cross-db-acl-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob; grant select on analytics.users to bob".to_string(),
            },
        },
    };

    let response = app.handle_connector_request_for_session(&non_root_acl_batch, "alice-session");
    assert_eq!(response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(_) = response.result else {
        panic!("expected non-root cross-database ACL batch rejection error");
    };

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-bob-after-non-root-cross-db-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_analytics_users = ConnectorRequest {
        request_id: "show-analytics-users-bob-after-non-root-cross-db-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "bob-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "bob-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn non_root_delegated_grant_attempt_is_rejected_even_after_access_grant() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-non-root-delegated-grant-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_main, "root-session").status,
        ResponseStatus::Applied
    );

    let create_users = ConnectorRequest {
        request_id: "create-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_users, "root-session").status,
        ResponseStatus::Applied
    );

    app.init_session("alice-session".to_string(), 1, "alice".to_string());
    app.init_session("bob-session".to_string(), 2, "bob".to_string());

    let root_grants_alice_access = ConnectorRequest {
        request_id: "root-grant-select-users-to-alice".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to alice".to_string(),
            },
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&root_grants_alice_access, "root-session").status,
        ResponseStatus::Applied
    );

    let non_root_delegation_attempt = ConnectorRequest {
        request_id: "alice-delegated-grant-attempt".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob".to_string(),
            },
        },
    };

    let response = app.handle_connector_request_for_session(&non_root_delegation_attempt, "alice-session");
    assert_eq!(response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(_) = response.result else {
        panic!("expected non-root delegated grant attempt rejection error");
    };

    let show_users_as_alice = ConnectorRequest {
        request_id: "show-users-as-alice-after-delegation-attempt".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_users_as_bob = ConnectorRequest {
        request_id: "show-users-as-bob-after-delegation-attempt".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_users_as_alice, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_users_as_bob, "bob-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn non_root_cross_database_delegated_grant_attempt_is_rejected_without_side_effects() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-non-root-cross-db-delegated-grant-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_db, "root-session").status,
            ResponseStatus::Applied
        );

        let create_users = ConnectorRequest {
            request_id: format!("create-users-{database_name}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: database_name.to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_users, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());
    app.init_session("bob-session".to_string(), 2, "bob".to_string());

    let root_grants_alice_schema_access = ConnectorRequest {
        request_id: "root-grant-main-schema-to-alice".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema main to alice".to_string(),
            },
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&root_grants_alice_schema_access, "root-session").status,
        ResponseStatus::Applied
    );

    let non_root_cross_db_delegation_attempt = ConnectorRequest {
        request_id: "alice-cross-db-delegated-grant-attempt".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on analytics.users to bob".to_string(),
            },
        },
    };

    let response = app.handle_connector_request_for_session(
        &non_root_cross_db_delegation_attempt,
        "alice-session",
    );
    assert_eq!(response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(_) = response.result else {
        panic!("expected non-root cross-database delegated grant rejection error");
    };

    let show_main_users_as_alice = ConnectorRequest {
        request_id: "show-main-users-as-alice-after-cross-db-delegation-attempt".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_analytics_users_as_bob = ConnectorRequest {
        request_id: "show-analytics-users-as-bob-after-cross-db-delegation-attempt".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users_as_alice, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users_as_bob, "bob-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn root_acl_batch_with_malformed_statement_has_no_partial_side_effects() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-root-acl-batch-malformed-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_main, "root-session").status,
        ResponseStatus::Applied
    );

    let create_users = ConnectorRequest {
        request_id: "create-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_users, "root-session").status,
        ResponseStatus::Applied
    );

    app.init_session("bob-session".to_string(), 1, "bob".to_string());

    let malformed_acl_batch = ConnectorRequest {
        request_id: "root-acl-batch-malformed".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob; grant select on analytics. to bob".to_string(),
            },
        },
    };

    let response = app.handle_connector_request_for_session(&malformed_acl_batch, "root-session");
    assert_eq!(response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(_) = response.result else {
        panic!("expected malformed ACL batch rejection error");
    };

    let show_users = ConnectorRequest {
        request_id: "show-users-bob-after-malformed-root-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn cross_database_grant_option_is_scoped_to_target_database_only() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-cross-db-grant-option-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_db, "root-session").status,
            ResponseStatus::Applied
        );

        let create_users = ConnectorRequest {
            request_id: format!("create-users-{database_name}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: database_name.to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_users, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let grant_option_analytics = ConnectorRequest {
        request_id: "grant-schema-analytics-select-with-option".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema analytics to alice with grant option".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&grant_option_analytics, "root-session").status,
        ResponseStatus::Applied
    );

    let analytics_id = serverlib::DatabaseId::from_database_name("analytics")
        .expect("analytics database id should normalize")
        .0;
    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;

    let analytics_catalog = app
        .catalogs
        .get(&analytics_id)
        .expect("analytics catalog should exist");
    let analytics_acl = analytics_catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL should exist in analytics");
    assert!(analytics_acl.acl.contains("SELECT"));
    assert!(analytics_acl.grant_acl.contains("SELECT"));

    if let Some(main_catalog) = app.catalogs.get(&main_id)
        && let Some(main_acl) = main_catalog.effective_account_acl_entry("alice")
    {
        assert!(!main_acl.acl.contains("SELECT"));
        assert!(!main_acl.grant_acl.contains("SELECT"));
    }

    let show_analytics_users = ConnectorRequest {
        request_id: "show-analytics-users-grant-option-scope".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-grant-option-scope".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn root_multi_target_acl_batch_with_late_malformed_statement_has_no_partial_side_effects() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-root-multi-target-malformed-batch-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_db, "root-session").status,
            ResponseStatus::Applied
        );

        for table in ["users", "orders"] {
            let create_table = ConnectorRequest {
                request_id: format!("create-{table}-{database_name}"),
                command: ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: database_name.to_string(),
                        sql: format!("create table {table} (id bigint not null primary key)"),
                    },
                },
            };
            assert_eq!(
                app.handle_connector_request_for_session(&create_table, "root-session").status,
                ResponseStatus::Applied
            );
        }
    }

    app.init_session("bob-session".to_string(), 1, "bob".to_string());

    let malformed_acl_batch = ConnectorRequest {
        request_id: "root-multi-target-acl-batch-malformed-late".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob; grant select on analytics.orders to bob; grant select on analytics. to bob".to_string(),
            },
        },
    };

    let response = app.handle_connector_request_for_session(&malformed_acl_batch, "root-session");
    assert_eq!(response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(_) = response.result else {
        panic!("expected malformed multi-target ACL batch rejection error");
    };

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-after-multi-target-malformed-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_analytics_orders = ConnectorRequest {
        request_id: "show-analytics-orders-after-multi-target-malformed-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "bob-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_orders, "bob-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn cross_database_revoke_cleans_grant_option_only_for_target_database() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-cross-db-revoke-grant-option-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_db, "root-session").status,
            ResponseStatus::Applied
        );

        let create_users = ConnectorRequest {
            request_id: format!("create-users-{database_name}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: database_name.to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_users, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    let grant_main_with_option = ConnectorRequest {
        request_id: "grant-main-schema-select-with-option".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema main to alice with grant option".to_string(),
            },
        },
    };

    let grant_analytics_with_option = ConnectorRequest {
        request_id: "grant-analytics-schema-select-with-option".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema analytics to alice with grant option".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&grant_main_with_option, "root-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&grant_analytics_with_option, "root-session").status,
        ResponseStatus::Applied
    );

    let revoke_analytics = ConnectorRequest {
        request_id: "revoke-analytics-schema-select".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on schema analytics from alice".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&revoke_analytics, "root-session").status,
        ResponseStatus::Applied
    );

    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;
    let analytics_id = serverlib::DatabaseId::from_database_name("analytics")
        .expect("analytics database id should normalize")
        .0;

    let main_catalog = app
        .catalogs
        .get(&main_id)
        .expect("main catalog should exist");
    let main_acl = main_catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL should exist in main");
    assert!(main_acl.acl.contains("SELECT"));
    assert!(main_acl.grant_acl.contains("SELECT"));

    let analytics_catalog = app
        .catalogs
        .get(&analytics_id)
        .expect("analytics catalog should exist");
    let analytics_acl = analytics_catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL should exist in analytics");
    assert!(!analytics_acl.acl.contains("SELECT"));
    assert!(!analytics_acl.grant_acl.contains("SELECT"));

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-after-analytics-revoke".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_analytics_users = ConnectorRequest {
        request_id: "show-analytics-users-after-analytics-revoke".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn acl_batch_recovery_after_malformed_request_applies_next_valid_batch_deterministically() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-acl-batch-recovery-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_main, "root-session").status,
        ResponseStatus::Applied
    );

    for table in ["users", "orders"] {
        let create_table = ConnectorRequest {
            request_id: format!("create-{table}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: format!("create table {table} (id bigint not null primary key)"),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_table, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("bob-session".to_string(), 1, "bob".to_string());

    let malformed_batch = ConnectorRequest {
        request_id: "malformed-acl-batch-first".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob; grant select on analytics. to bob".to_string(),
            },
        },
    };

    let malformed_response = app.handle_connector_request_for_session(&malformed_batch, "root-session");
    assert_eq!(malformed_response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(_) = malformed_response.result else {
        panic!("expected malformed ACL batch rejection error");
    };

    let show_users = ConnectorRequest {
        request_id: "show-users-after-malformed-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_orders = ConnectorRequest {
        request_id: "show-orders-after-malformed-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Rejected
    );

    let valid_batch = ConnectorRequest {
        request_id: "valid-acl-batch-after-malformed".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob; grant select on orders to bob".to_string(),
            },
        },
    };

    let valid_response = app.handle_connector_request_for_session(&valid_batch, "root-session");
    assert_eq!(valid_response.status, ResponseStatus::Applied);

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Applied
    );

}

#[test]
fn cross_database_grant_option_transition_chain_remains_scope_isolated() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-cross-db-grant-option-chain-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_db, "root-session").status,
            ResponseStatus::Applied
        );

        let create_users = ConnectorRequest {
            request_id: format!("create-users-{database_name}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: database_name.to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_users, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    for (request_id, sql) in [
        (
            "grant-main-select-with-option",
            "grant select on schema main to alice with grant option",
        ),
        (
            "grant-analytics-select-with-option",
            "grant select on schema analytics to alice with grant option",
        ),
        (
            "revoke-analytics-select",
            "revoke select on schema analytics from alice",
        ),
        (
            "regrant-analytics-select-no-option",
            "grant select on schema analytics to alice",
        ),
        (
            "regrant-analytics-select-with-option",
            "grant select on schema analytics to alice with grant option",
        ),
        ("revoke-main-select", "revoke select on schema main from alice"),
    ] {
        let request = ConnectorRequest {
            request_id: request_id.to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&request, "root-session").status,
            ResponseStatus::Applied
        );
    }

    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;
    let analytics_id = serverlib::DatabaseId::from_database_name("analytics")
        .expect("analytics database id should normalize")
        .0;

    let main_catalog = app
        .catalogs
        .get(&main_id)
        .expect("main catalog should exist");
    let main_acl = main_catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL should exist in main");
    assert!(!main_acl.acl.contains("SELECT"));
    assert!(!main_acl.grant_acl.contains("SELECT"));

    let analytics_catalog = app
        .catalogs
        .get(&analytics_id)
        .expect("analytics catalog should exist");
    let analytics_acl = analytics_catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL should exist in analytics");
    assert!(analytics_acl.acl.contains("SELECT"));
    assert!(analytics_acl.grant_acl.contains("SELECT"));

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-after-transition-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let show_analytics_users = ConnectorRequest {
        request_id: "show-analytics-users-after-transition-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Applied
    );

}

#[test]
fn repeated_malformed_acl_batches_at_different_positions_preserve_acl_state_until_valid_batch() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-malformed-batch-positions-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_main, "root-session").status,
        ResponseStatus::Applied
    );

    for table in ["users", "orders", "invoices"] {
        let create_table = ConnectorRequest {
            request_id: format!("create-{table}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: format!("create table {table} (id bigint not null primary key)"),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_table, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("bob-session".to_string(), 1, "bob".to_string());

    let initial_valid_grant = ConnectorRequest {
        request_id: "initial-valid-grant-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob".to_string(),
            },
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&initial_valid_grant, "root-session").status,
        ResponseStatus::Applied
    );

    let show_users = ConnectorRequest {
        request_id: "show-users-malformed-position-test".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_orders = ConnectorRequest {
        request_id: "show-orders-malformed-position-test".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };
    let show_invoices = ConnectorRequest {
        request_id: "show-invoices-malformed-position-test".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from invoices".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_invoices, "bob-session").status,
        ResponseStatus::Rejected
    );

    for (request_id, sql) in [
        (
            "malformed-batch-head",
            "grant select on analytics. to bob; grant select on orders to bob",
        ),
        (
            "malformed-batch-middle",
            "grant select on orders to bob; grant select on analytics. to bob; grant select on invoices to bob",
        ),
        (
            "malformed-batch-tail",
            "grant select on orders to bob; grant select on invoices to bob; grant select on analytics. to bob",
        ),
    ] {
        let malformed_batch = ConnectorRequest {
            request_id: request_id.to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        };

        let response = app.handle_connector_request_for_session(&malformed_batch, "root-session");
        assert_eq!(response.status, ResponseStatus::Rejected);
        let ConnectorResult::Error(_) = response.result else {
            panic!("expected malformed ACL batch rejection error");
        };

        assert_eq!(
            app.handle_connector_request_for_session(&show_users, "bob-session").status,
            ResponseStatus::Applied
        );
        assert_eq!(
            app.handle_connector_request_for_session(&show_orders, "bob-session").status,
            ResponseStatus::Rejected
        );
        assert_eq!(
            app.handle_connector_request_for_session(&show_invoices, "bob-session").status,
            ResponseStatus::Rejected
        );
    }

    let final_valid_batch = ConnectorRequest {
        request_id: "final-valid-batch-after-malformed-position-series".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on orders to bob; grant select on invoices to bob".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&final_valid_batch, "root-session").status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_invoices, "bob-session").status,
        ResponseStatus::Applied
    );

}

#[test]
fn cross_database_object_acl_transition_chain_remains_target_isolated() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-cross-db-object-chain-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_db, "root-session").status,
            ResponseStatus::Applied
        );

        for table in ["users", "orders"] {
            let create_table = ConnectorRequest {
                request_id: format!("create-{database_name}-{table}"),
                command: ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: database_name.to_string(),
                        sql: format!("create table {table} (id bigint not null primary key)"),
                    },
                },
            };
            assert_eq!(
                app.handle_connector_request_for_session(&create_table, "root-session").status,
                ResponseStatus::Applied
            );
        }
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    for (request_id, sql) in [
        ("grant-main-users", "grant select on users to alice"),
        (
            "grant-analytics-orders",
            "grant select on analytics.orders to alice",
        ),
        (
            "revoke-analytics-orders",
            "revoke select on analytics.orders from alice",
        ),
        (
            "grant-analytics-users",
            "grant select on analytics.users to alice",
        ),
        ("revoke-main-users", "revoke select on users from alice"),
    ] {
        let request = ConnectorRequest {
            request_id: request_id.to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&request, "root-session").status,
            ResponseStatus::Applied
        );
    }

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-after-object-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_main_orders = ConnectorRequest {
        request_id: "show-main-orders-after-object-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };
    let show_analytics_users = ConnectorRequest {
        request_id: "show-analytics-users-after-object-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_analytics_orders = ConnectorRequest {
        request_id: "show-analytics-orders-after-object-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_main_orders, "alice-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_orders, "alice-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn alternating_malformed_grant_revoke_batches_preserve_state_until_valid_reconciliation() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-malformed-grant-revoke-alternating-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_main, "root-session").status,
        ResponseStatus::Applied
    );

    for table in ["users", "orders"] {
        let create_table = ConnectorRequest {
            request_id: format!("create-{table}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: format!("create table {table} (id bigint not null primary key)"),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_table, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("bob-session".to_string(), 1, "bob".to_string());

    let initial_grant = ConnectorRequest {
        request_id: "initial-grant-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob".to_string(),
            },
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&initial_grant, "root-session").status,
        ResponseStatus::Applied
    );

    let show_users = ConnectorRequest {
        request_id: "show-users-after-alternating-malformed".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_orders = ConnectorRequest {
        request_id: "show-orders-after-alternating-malformed".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Rejected
    );

    for (request_id, sql) in [
        (
            "malformed-grant-head",
            "grant select on analytics. to bob; revoke select on users from bob",
        ),
        (
            "malformed-revoke-tail",
            "grant select on orders to bob; revoke select on analytics. from bob",
        ),
        (
            "malformed-grant-tail",
            "revoke select on users from bob; grant select on analytics. to bob",
        ),
        (
            "malformed-revoke-head",
            "revoke select on analytics. from bob; grant select on orders to bob",
        ),
    ] {
        let malformed_batch = ConnectorRequest {
            request_id: request_id.to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        };

        let response = app.handle_connector_request_for_session(&malformed_batch, "root-session");
        assert_eq!(response.status, ResponseStatus::Rejected);
        let ConnectorResult::Error(_) = response.result else {
            panic!("expected malformed alternating ACL batch rejection error");
        };

        assert_eq!(
            app.handle_connector_request_for_session(&show_users, "bob-session").status,
            ResponseStatus::Applied
        );
        assert_eq!(
            app.handle_connector_request_for_session(&show_orders, "bob-session").status,
            ResponseStatus::Rejected
        );
    }

    let valid_reconciliation_batch = ConnectorRequest {
        request_id: "valid-reconciliation-after-alternating-malformed".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on orders to bob; revoke select on users from bob".to_string(),
            },
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&valid_reconciliation_batch, "root-session").status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Applied
    );

}

#[test]
fn cross_database_schema_grant_with_object_revoke_chain_preserves_scope_precedence() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-schema-object-chain-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_db, "root-session").status,
            ResponseStatus::Applied
        );

        for table in ["users", "orders"] {
            let create_table = ConnectorRequest {
                request_id: format!("create-{database_name}-{table}"),
                command: ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: database_name.to_string(),
                        sql: format!("create table {table} (id bigint not null primary key)"),
                    },
                },
            };
            assert_eq!(
                app.handle_connector_request_for_session(&create_table, "root-session").status,
                ResponseStatus::Applied
            );
        }
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    for (request_id, sql) in [
        ("grant-main-schema", "grant select on schema main to alice"),
        (
            "grant-analytics-schema",
            "grant select on schema analytics to alice",
        ),
        ("revoke-main-users", "revoke select on main.users from alice"),
        (
            "revoke-analytics-orders",
            "revoke select on analytics.orders from alice",
        ),
        (
            "grant-analytics-users-object",
            "grant select on analytics.users to alice",
        ),
        ("revoke-analytics-schema", "revoke select on schema analytics from alice"),
    ] {
        let request = ConnectorRequest {
            request_id: request_id.to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&request, "root-session").status,
            ResponseStatus::Applied
        );
    }

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-after-schema-object-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_main_orders = ConnectorRequest {
        request_id: "show-main-orders-after-schema-object-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };
    let show_analytics_users = ConnectorRequest {
        request_id: "show-analytics-users-after-schema-object-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_analytics_orders = ConnectorRequest {
        request_id: "show-analytics-orders-after-schema-object-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_main_orders, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_orders, "alice-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn mixed_schema_object_malformed_batch_rejects_without_partial_side_effects() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-schema-object-malformed-batch-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_main, "root-session").status,
        ResponseStatus::Applied
    );

    for table in ["users", "orders"] {
        let create_table = ConnectorRequest {
            request_id: format!("create-{table}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: format!("create table {table} (id bigint not null primary key)"),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_table, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("bob-session".to_string(), 1, "bob".to_string());

    let malformed_mixed_batch = ConnectorRequest {
        request_id: "malformed-schema-object-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on analytics. to bob; grant select on schema main to bob; revoke select on users from bob".to_string(),
            },
        },
    };

    let malformed_response = app.handle_connector_request_for_session(&malformed_mixed_batch, "root-session");
    assert_eq!(malformed_response.status, ResponseStatus::Rejected);
    let ConnectorResult::Error(_) = malformed_response.result else {
        panic!("expected malformed mixed schema/object batch rejection error");
    };

    let show_users = ConnectorRequest {
        request_id: "show-users-after-malformed-schema-object-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_orders = ConnectorRequest {
        request_id: "show-orders-after-malformed-schema-object-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Rejected
    );

    let valid_reconciliation_batch = ConnectorRequest {
        request_id: "valid-schema-object-reconciliation-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to bob; revoke select on users from bob; grant select on orders to bob".to_string(),
            },
        },
    };

    let valid_response = app.handle_connector_request_for_session(&valid_reconciliation_batch, "root-session");
    assert_eq!(valid_response.status, ResponseStatus::Applied);

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Applied
    );

}

#[test]
fn cross_database_mixed_schema_object_grant_option_chain_scopes_grant_acl_and_access() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-mixed-schema-object-grant-option-chain-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    for database_name in ["main", "analytics"] {
        let create_db = ConnectorRequest {
            request_id: format!("create-db-{database_name}"),
            command: ConnectorCommand::CreateDatabase {
                database_name: database_name.to_string(),
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_db, "root-session").status,
            ResponseStatus::Applied
        );

        for table in ["users", "orders"] {
            let create_table = ConnectorRequest {
                request_id: format!("create-{database_name}-{table}"),
                command: ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: database_name.to_string(),
                        sql: format!("create table {table} (id bigint not null primary key)"),
                    },
                },
            };
            assert_eq!(
                app.handle_connector_request_for_session(&create_table, "root-session").status,
                ResponseStatus::Applied
            );
        }
    }

    app.init_session("alice-session".to_string(), 1, "alice".to_string());

    for (request_id, sql) in [
        (
            "grant-main-schema-with-option",
            "grant select on schema main to alice with grant option",
        ),
        (
            "grant-analytics-users-object",
            "grant select on analytics.users to alice",
        ),
        (
            "grant-analytics-schema-with-option",
            "grant select on schema analytics to alice with grant option",
        ),
        (
            "revoke-analytics-schema",
            "revoke select on schema analytics from alice",
        ),
        ("revoke-main-orders", "revoke select on main.orders from alice"),
    ] {
        let request = ConnectorRequest {
            request_id: request_id.to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&request, "root-session").status,
            ResponseStatus::Applied
        );
    }

    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;
    let analytics_id = serverlib::DatabaseId::from_database_name("analytics")
        .expect("analytics database id should normalize")
        .0;

    let main_catalog = app
        .catalogs
        .get(&main_id)
        .expect("main catalog should exist");
    let main_acl = main_catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL should exist in main");
    assert!(main_acl.acl.contains("SELECT"));
    assert!(main_acl.grant_acl.contains("SELECT"));

    let analytics_catalog = app
        .catalogs
        .get(&analytics_id)
        .expect("analytics catalog should exist");
    let analytics_acl = analytics_catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL should exist in analytics");
    assert!(!analytics_acl.grant_acl.contains("SELECT"));

    let show_main_users = ConnectorRequest {
        request_id: "show-main-users-after-mixed-grant-option-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_main_orders = ConnectorRequest {
        request_id: "show-main-orders-after-mixed-grant-option-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };
    let show_analytics_users = ConnectorRequest {
        request_id: "show-analytics-users-after-mixed-grant-option-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_analytics_orders = ConnectorRequest {
        request_id: "show-analytics-orders-after-mixed-grant-option-chain".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "analytics".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_main_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_main_orders, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_users, "alice-session").status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_analytics_orders, "alice-session").status,
        ResponseStatus::Rejected
    );

}

#[test]
fn mixed_schema_object_grant_option_malformed_batch_has_no_side_effects_then_recovers() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-mixed-grant-option-malformed-batch-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };
    assert_eq!(
        app.handle_connector_request_for_session(&create_main, "root-session").status,
        ResponseStatus::Applied
    );

    for table in ["users", "orders"] {
        let create_table = ConnectorRequest {
            request_id: format!("create-{table}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: format!("create table {table} (id bigint not null primary key)"),
                },
            },
        };
        assert_eq!(
            app.handle_connector_request_for_session(&create_table, "root-session").status,
            ResponseStatus::Applied
        );
    }

    app.init_session("bob-session".to_string(), 1, "bob".to_string());

    let malformed_batch = ConnectorRequest {
        request_id: "malformed-mixed-grant-option-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema main to bob with grant option; grant select on analytics. to bob; revoke select on users from bob".to_string(),
            },
        },
    };

    let malformed_response = app.handle_connector_request_for_session(&malformed_batch, "root-session");
    assert_eq!(malformed_response.status, ResponseStatus::Rejected);
    let ConnectorResult::Error(_) = malformed_response.result else {
        panic!("expected malformed mixed grant-option batch rejection error");
    };

    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;
    if let Some(main_catalog) = app.catalogs.get(&main_id)
        && let Some(acl) = main_catalog.effective_account_acl_entry("bob")
    {
        assert!(!acl.acl.contains("SELECT"));
        assert!(!acl.grant_acl.contains("SELECT"));
    }

    let show_users = ConnectorRequest {
        request_id: "show-users-after-malformed-mixed-grant-option-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };
    let show_orders = ConnectorRequest {
        request_id: "show-orders-after-malformed-mixed-grant-option-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from orders".to_string(),
            },
        },
    };

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Rejected
    );

    let valid_reconciliation_batch = ConnectorRequest {
        request_id: "valid-mixed-grant-option-reconciliation-batch".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on schema main to bob with grant option; revoke select on schema main from bob; grant select on orders to bob".to_string(),
            },
        },
    };

    let valid_response = app.handle_connector_request_for_session(&valid_reconciliation_batch, "root-session");
    assert_eq!(valid_response.status, ResponseStatus::Applied);

    let main_catalog = app
        .catalogs
        .get(&main_id)
        .expect("main catalog should exist");
    let bob_acl = main_catalog
        .effective_account_acl_entry("bob")
        .expect("bob ACL should exist in main after reconciliation");
    assert!(!bob_acl.grant_acl.contains("SELECT"));

    assert_eq!(
        app.handle_connector_request_for_session(&show_users, "bob-session").status,
        ResponseStatus::Rejected
    );
    assert_eq!(
        app.handle_connector_request_for_session(&show_orders, "bob-session").status,
        ResponseStatus::Applied
    );

}

#[test]
fn authorization_interleaving_grant_revoke_cycles_enforce_post_revoke_denial() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-interleaving-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let create_users_table = ConnectorRequest {
        request_id: "create-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    };

    let create_users_table_response =
        app.handle_connector_request_for_session(&create_users_table, "root-session");
    assert_eq!(create_users_table_response.status, ResponseStatus::Applied);

    app.init_session("alice-session-a".to_string(), 1, "alice".to_string());
    app.init_session("alice-session-b".to_string(), 2, "alice".to_string());

    let show_users_columns = |request_id: &str| ConnectorRequest {
        request_id: request_id.to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let grant = |request_id: &str| ConnectorRequest {
        request_id: request_id.to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to alice".to_string(),
            },
        },
    };

    let revoke = |request_id: &str| ConnectorRequest {
        request_id: request_id.to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on users from alice".to_string(),
            },
        },
    };

    for cycle in 0..5 {

        let grant_response = app.handle_connector_request_for_session(
            &grant(&format!("grant-users-select-{cycle}")),
            "root-session",
        );
        assert_eq!(grant_response.status, ResponseStatus::Applied);

        let allow_a = app.handle_connector_request_for_session(
            &show_users_columns(&format!("show-users-columns-allow-a-{cycle}")),
            "alice-session-a",
        );
        assert_eq!(allow_a.status, ResponseStatus::Applied);

        let allow_b = app.handle_connector_request_for_session(
            &show_users_columns(&format!("show-users-columns-allow-b-{cycle}")),
            "alice-session-b",
        );
        assert_eq!(allow_b.status, ResponseStatus::Applied);

        let revoke_response = app.handle_connector_request_for_session(
            &revoke(&format!("revoke-users-select-{cycle}")),
            "root-session",
        );
        assert_eq!(revoke_response.status, ResponseStatus::Applied);

        let deny_a = app.handle_connector_request_for_session(
            &show_users_columns(&format!("show-users-columns-deny-a-{cycle}")),
            "alice-session-a",
        );
        assert_eq!(deny_a.status, ResponseStatus::Rejected);

        let deny_b = app.handle_connector_request_for_session(
            &show_users_columns(&format!("show-users-columns-deny-b-{cycle}")),
            "alice-session-b",
        );
        assert_eq!(deny_b.status, ResponseStatus::Rejected);

    }

}

#[test]
fn authorization_interleaving_high_contention_multi_session_revokes_stay_effective() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-high-contention-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let create_users_table = ConnectorRequest {
        request_id: "create-users".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    };

    let create_users_table_response =
        app.handle_connector_request_for_session(&create_users_table, "root-session");
    assert_eq!(create_users_table_response.status, ResponseStatus::Applied);

    let session_ids = [
        "alice-session-01",
        "alice-session-02",
        "alice-session-03",
        "alice-session-04",
        "alice-session-05",
        "alice-session-06",
    ];

    for (idx, session_id) in session_ids.iter().enumerate() {
        app.init_session((*session_id).to_string(), idx + 1, "alice".to_string());
    }

    let show_users_columns = |request_id: &str| ConnectorRequest {
        request_id: request_id.to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show columns from users".to_string(),
            },
        },
    };

    let grant = |request_id: &str| ConnectorRequest {
        request_id: request_id.to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "grant select on users to alice".to_string(),
            },
        },
    };

    let revoke = |request_id: &str| ConnectorRequest {
        request_id: request_id.to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "revoke select on users from alice".to_string(),
            },
        },
    };

    for cycle in 0..8 {

        let grant_response = app.handle_connector_request_for_session(
            &grant(&format!("grant-users-select-contention-{cycle}")),
            "root-session",
        );
        assert_eq!(grant_response.status, ResponseStatus::Applied);

        for burst in 0..2 {
            for session_id in session_ids.iter() {
                let allow = app.handle_connector_request_for_session(
                    &show_users_columns(&format!("show-users-allow-{cycle}-{burst}-{session_id}")),
                    session_id,
                );
                assert_eq!(allow.status, ResponseStatus::Applied);
            }
        }

        let revoke_response = app.handle_connector_request_for_session(
            &revoke(&format!("revoke-users-select-contention-{cycle}")),
            "root-session",
        );
        assert_eq!(revoke_response.status, ResponseStatus::Applied);

        for burst in 0..3 {
            for session_id in session_ids.iter() {
                let deny = app.handle_connector_request_for_session(
                    &show_users_columns(&format!("show-users-deny-{cycle}-{burst}-{session_id}")),
                    session_id,
                );
                assert_eq!(deny.status, ResponseStatus::Rejected);
            }
        }

    }

}

#[test]
fn authorization_parallel_reader_writer_contention_preserves_revoke_effectiveness() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-authz-parallel-contention-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let app = Arc::new(Mutex::new(
        ServerApp::new(config).expect("server app should initialize"),
    ));

    {
        let mut guard = app.lock().expect("app lock should be available");

        let create_main = ConnectorRequest {
            request_id: "create-main".to_string(),
            command: ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        };

        let create_main_response = guard.handle_connector_request_for_session(&create_main, "root-session");
        assert_eq!(create_main_response.status, ResponseStatus::Applied);

        let create_users_table = ConnectorRequest {
            request_id: "create-users".to_string(),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key)".to_string(),
                },
            },
        };

        let create_users_table_response =
            guard.handle_connector_request_for_session(&create_users_table, "root-session");
        assert_eq!(create_users_table_response.status, ResponseStatus::Applied);

        for idx in 1..=8 {
            guard.init_session(
                format!("alice-session-parallel-{idx}"),
                idx,
                "alice".to_string(),
            );
        }
    }

    let session_ids: Vec<String> = (1..=8)
        .map(|idx| format!("alice-session-parallel-{idx}"))
        .collect();

    let barrier = Arc::new(Barrier::new(session_ids.len() + 1));
    let applied_total = Arc::new(AtomicUsize::new(0));
    let rejected_total = Arc::new(AtomicUsize::new(0));

    let mut readers = Vec::new();
    for session_id in session_ids.clone() {
        let app_reader = Arc::clone(&app);
        let barrier_reader = Arc::clone(&barrier);
        let applied_reader = Arc::clone(&applied_total);
        let rejected_reader = Arc::clone(&rejected_total);

        readers.push(thread::spawn(move || {
            barrier_reader.wait();
            for iteration in 0..120 {
                let request = ConnectorRequest {
                    request_id: format!("parallel-read-{session_id}-{iteration}"),
                    command: ConnectorCommand::Query {
                        query: connector::DataQuery {
                            database_id: "main".to_string(),
                            sql: "show columns from users".to_string(),
                        },
                    },
                };

                let status = {
                    let mut guard = app_reader.lock().expect("app lock should be available");
                    guard
                        .handle_connector_request_for_session(&request, &session_id)
                        .status
                };

                match status {
                    ResponseStatus::Applied => {
                        applied_reader.fetch_add(1, Ordering::Relaxed);
                    }
                    ResponseStatus::Rejected => {
                        rejected_reader.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }

                if iteration % 8 == 0 {
                    thread::yield_now();
                }
            }
        }));
    }

    let app_writer = Arc::clone(&app);
    let barrier_writer = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        barrier_writer.wait();
        for cycle in 0..45 {
            let grant = ConnectorRequest {
                request_id: format!("parallel-grant-{cycle}"),
                command: ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: "grant select on users to alice".to_string(),
                    },
                },
            };

            let revoke = ConnectorRequest {
                request_id: format!("parallel-revoke-{cycle}"),
                command: ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: "revoke select on users from alice".to_string(),
                    },
                },
            };

            let grant_status = {
                let mut guard = app_writer.lock().expect("app lock should be available");
                guard.handle_connector_request_for_session(&grant, "root-session").status
            };
            assert_eq!(grant_status, ResponseStatus::Applied);

            thread::sleep(Duration::from_millis(1));

            let revoke_status = {
                let mut guard = app_writer.lock().expect("app lock should be available");
                guard.handle_connector_request_for_session(&revoke, "root-session").status
            };
            assert_eq!(revoke_status, ResponseStatus::Applied);

            thread::sleep(Duration::from_millis(1));
        }
    });

    writer.join().expect("writer thread should finish");
    for reader in readers {
        reader.join().expect("reader thread should finish");
    }

    assert!(
        applied_total.load(Ordering::Relaxed) > 0,
        "at least one read should be allowed during grant windows"
    );
    assert!(
        rejected_total.load(Ordering::Relaxed) > 0,
        "at least one read should be rejected during revoke windows"
    );

    let mut guard = app.lock().expect("app lock should be available");
    for (idx, session_id) in session_ids.iter().enumerate() {
        let final_check = ConnectorRequest {
            request_id: format!("parallel-final-deny-{idx}"),
            command: ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "show columns from users".to_string(),
                },
            },
        };

        let response = guard.handle_connector_request_for_session(&final_check, session_id);
        assert_eq!(response.status, ResponseStatus::Rejected);
    }

}

#[test]
fn create_user_creates_acl_entry_and_wal_snapshot() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-create-user-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let create_user = ConnectorRequest {
        request_id: "create-user-alice".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create user 'alice' identified by 'secret'".to_string(),
            },
        },
    };

    let create_user_response = app.handle_connector_request_for_session(&create_user, "root-session");
    assert_eq!(create_user_response.status, ResponseStatus::Applied);

    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;

    let catalog = app
        .catalogs
        .get(&main_id)
        .expect("main catalog should exist");

    let acl_entry = catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL entry should exist");

    assert!(acl_entry.acl.is_empty());
    assert!(acl_entry.grant_acl.is_empty());
    assert!(acl_entry.object_acl.is_empty());

    let credential = catalog
        .effective_user_credential("alice")
        .expect("alice credential should exist");

    assert!(credential.verify_password("secret", app.node_id()));

    let wal_id = app.resolve_catalog_wal_stream_for_database("main");
    let security_records = app
        .wal
        .since_kinds(&wal_id, None, &[TransactionKind::SecurityChange]);

    let latest_credential_payload = security_records
        .iter()
        .rev()
        .find_map(|record| {
            record.payload_logical().and_then(|payload| {
                std::panic::catch_unwind(|| ServerApp::decode_user_credential_wal_payload(payload))
                    .ok()
                    .and_then(Result::ok)
            })
        })
        .expect("latest security payload should include user credential snapshot");

    assert_eq!(latest_credential_payload.user_id.0, "alice");
    assert!(latest_credential_payload.verify_password("secret", app.node_id()));

    let latest_payload = security_records
        .last()
        .and_then(|record| record.payload_logical())
        .expect("latest security payload should exist");

    let latest_acl_entry = ServerApp::decode_account_acl_wal_payload(latest_payload)
        .expect("latest security payload should decode as ACL snapshot");

    assert_eq!(latest_acl_entry.user_id.0, "alice");
    assert!(latest_acl_entry.acl.is_empty());

}

#[test]
fn create_user_duplicate_requires_if_not_exists() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-create-user-duplicate-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let first_create = ConnectorRequest {
        request_id: "create-user-first".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create user 'alice' identified by 'secret'".to_string(),
            },
        },
    };

    let first_create_response = app.handle_connector_request_for_session(&first_create, "root-session");
    assert_eq!(first_create_response.status, ResponseStatus::Applied);

    let duplicate_create = ConnectorRequest {
        request_id: "create-user-duplicate".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create user 'alice' identified by 'secret'".to_string(),
            },
        },
    };

    let duplicate_response = app.handle_connector_request_for_session(&duplicate_create, "root-session");
    assert_eq!(duplicate_response.status, ResponseStatus::Rejected);

    let if_not_exists_create = ConnectorRequest {
        request_id: "create-user-if-not-exists".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create user if not exists 'alice' identified by 'secret'".to_string(),
            },
        },
    };

    let if_not_exists_response =
        app.handle_connector_request_for_session(&if_not_exists_create, "root-session");
    assert_eq!(if_not_exists_response.status, ResponseStatus::Applied);

    let ConnectorResult::Mutation(mutation) = if_not_exists_response.result else {
        panic!("expected mutation result");
    };

    assert_eq!(mutation.affected_rows, 0);

}

#[test]
fn create_user_requires_identified_by_password_clause() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-create-user-syntax-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let create_main = ConnectorRequest {
        request_id: "create-main".to_string(),
        command: ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    };

    let create_main_response = app.handle_connector_request_for_session(&create_main, "root-session");
    assert_eq!(create_main_response.status, ResponseStatus::Applied);

    let invalid_create_user = ConnectorRequest {
        request_id: "create-user-missing-password".to_string(),
        command: ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create user 'alice'".to_string(),
            },
        },
    };

    let response = app.handle_connector_request_for_session(&invalid_create_user, "root-session");
    assert_eq!(response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result");
    };

    assert!(message.contains("CREATE USER requires syntax"));

}

#[test]
fn bootstrap_replays_security_change_password_from_wal() {

    reset_bootstrap_password_for_tests();

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-bootstrap-security-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root.clone());
    let mut app = ServerApp::new(config).expect("server app should initialize");

    configure_bootstrap_crypto_context(app.node_id().to_string(), None);

    let payload = encode_set_password_wal_payload("root", "sam")
        .expect("security payload should encode");

    let wal_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;

    app.wal
        .append(
            &wal_id,
            TransactionRecord::with_payload(
                TransactionId(1),
                None,
                None,
                1,
                UserId::from_username("bootstrap-security-tester"),
                TransactionKind::SecurityChange,
                payload,
            ),
        )
        .expect("security transaction should append");

    app.bootstrap()
        .expect("bootstrap should replay security changes");

    let mut session = ServerConnectionSession::new("127.0.0.1:4001".to_string(), 1);
    assert!(session.authenticate_if_valid_token(&md5_hash("sam")));

    let mut old_password_session =
        ServerConnectionSession::new("127.0.0.1:4001".to_string(), 2);
    assert!(!old_password_session.authenticate_if_valid_token(&md5_hash("root")));

    reset_bootstrap_password_for_tests();
}

#[test]
fn bootstrap_acl_replay_prefers_latest_wal_snapshot_for_user() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-bootstrap-acl-latest-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root.clone());
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut old_acl = serverlib::AccountAclEntry::new(serverlib::UserId("alice".to_string()), "main");
    old_acl.append_object_privilege("users", serverlib::engine::security::AccountPrivilege::Select);

    let mut latest_acl = serverlib::AccountAclEntry::new(serverlib::UserId("alice".to_string()), "main");
    latest_acl.append_privilege(serverlib::engine::security::AccountPrivilege::Select);
    latest_acl.append_grant_option_for_privilege(serverlib::engine::security::AccountPrivilege::Select);

    let old_payload = ServerApp::encode_account_acl_wal_payload(&old_acl)
        .expect("old ACL payload should encode");
    let latest_payload = ServerApp::encode_account_acl_wal_payload(&latest_acl)
        .expect("latest ACL payload should encode");

    let wal_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;

    app.wal
        .append(
            &wal_id,
            TransactionRecord::with_payload(
                TransactionId(1),
                None,
                None,
                1,
                UserId::from_username("acl-tester"),
                TransactionKind::SecurityChange,
                old_payload,
            ),
        )
        .expect("older ACL WAL record should append");

    app.wal
        .append(
            &wal_id,
            TransactionRecord::with_payload(
                TransactionId(2),
                None,
                Some(TransactionId(1)),
                2,
                UserId::from_username("acl-tester"),
                TransactionKind::SecurityChange,
                latest_payload,
            ),
        )
        .expect("latest ACL WAL record should append");

    app.bootstrap()
        .expect("bootstrap should replay latest ACL snapshot");

    let main_id = serverlib::DatabaseId::from_database_name("main")
        .expect("main database id should normalize")
        .0;

    let catalog = app
        .catalogs
        .get(&main_id)
        .expect("main catalog should exist");

    let effective_acl = catalog
        .effective_account_acl_entry("alice")
        .expect("alice ACL should exist after replay");

    assert!(effective_acl.acl.contains("SELECT"));
    assert!(effective_acl.grant_acl.contains("SELECT"));
    assert!(effective_acl.object_acl.is_empty());

}

#[test]
fn bootstrap_replays_latest_schema_from_wal() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-bootstrap-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root.clone());
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let database_name = "schema_bootstrap";
    let mut catalog = DatabaseCatalog::create_empty_from_name(database_name)
        .expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "name".to_string(),
                field_type: FieldType::Text,
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("base table should register");

    catalog
        .save_in_directory(&app.node_data_dir)
        .expect("catalog should be persisted");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "email".to_string(),
        field_type: FieldType::Text,
        nullable: false,
        indexed: FieldIndex::Indexed,
        default_value: None,
        metadata: None,
    }]);

    let payload = SchemaChangePayload {
        table_id: "users".to_string(),
        schema_revision: 2,
        schema_epoch: 2,
        entity_id: None,
        schema: schema.clone(),
    };

    app.wal
        .append(
            &catalog.database_id.0,
            TransactionRecord::with_payload(
                TransactionId(1),
                None,
                None,
                1,
                UserId::from_username("bootstrap-tester"),
                TransactionKind::SchemaChange,
                payload.encode().expect("schema payload should encode"),
            ),
        )
        .expect("schema transaction should append");

    app.bootstrap().expect("bootstrap should replay schemas");

    let loaded = app
        .catalogs()
        .get(&catalog.database_id.0)
        .expect("catalog should be loaded");

    assert_eq!(loaded.table_schema("users"), Some(schema.clone()));
    assert_eq!(loaded.table_schema_revision("users"), Some(2));
    let email_index_id = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string()],
    )
    .index_id
    .0;
    assert!(loaded.index(&email_index_id).is_some());

}

#[test]
fn bootstrap_replays_sql_definition_and_metadata_from_wal() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-bootstrap-sql-definition-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root.clone());
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let database_name = "sql_definition_bootstrap";
    let mut catalog = DatabaseCatalog::create_empty_from_name(database_name)
        .expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("base table should register");

    catalog
        .save_in_directory(&app.node_data_dir)
        .expect("catalog should be persisted");

    let wal_id = catalog.database_id.0.clone();
    let actor = UserId::from_username("bootstrap-object-replay");

    let trigger_payload = SqlDefinitionPayload {
        object_id: "trg_users_bi".to_string(),
        object_kind: SqlObjectKind::Trigger,
        action: SqlDefinitionAction::Upsert,
        schema_epoch: 1,
        sql: "create trigger trg_users_bi before insert on users for each row begin end"
            .to_string(),
        dependencies: vec!["users".to_string()],
    };

    app.wal
        .append(
            &wal_id,
            TransactionRecord::with_payload(
                TransactionId(1),
                None,
                None,
                1,
                actor.clone(),
                TransactionKind::SqlDefinitionChange,
                trigger_payload
                    .encode()
                    .expect("trigger sql payload should encode"),
            ),
        )
        .expect("trigger sql definition append should succeed");

    let trigger_metadata_payload = EntityMetadataPayload {
        entity_id: "trg_users_bi".to_string(),
        metadata: EntityMetadata::default()
            .with_creator("bootstrap-object-replay")
            .with_created_at(2),
    };

    app.wal
        .append(
            &wal_id,
            TransactionRecord::with_payload(
                TransactionId(2),
                None,
                Some(TransactionId(1)),
                2,
                actor.clone(),
                TransactionKind::MetadataChange,
                trigger_metadata_payload
                    .encode()
                    .expect("trigger metadata payload should encode"),
            ),
        )
        .expect("trigger metadata append should succeed");

    let view_upsert_payload = SqlDefinitionPayload {
        object_id: "users_v".to_string(),
        object_kind: SqlObjectKind::View,
        action: SqlDefinitionAction::Upsert,
        schema_epoch: 1,
        sql: "create view users_v as select * from users".to_string(),
        dependencies: vec!["users".to_string()],
    };

    app.wal
        .append(
            &wal_id,
            TransactionRecord::with_payload(
                TransactionId(3),
                None,
                Some(TransactionId(2)),
                3,
                actor.clone(),
                TransactionKind::SqlDefinitionChange,
                view_upsert_payload
                    .encode()
                    .expect("view upsert payload should encode"),
            ),
        )
        .expect("view upsert append should succeed");

    let view_drop_payload = SqlDefinitionPayload {
        object_id: "users_v".to_string(),
        object_kind: SqlObjectKind::View,
        action: SqlDefinitionAction::Drop,
        schema_epoch: 2,
        sql: String::new(),
        dependencies: Vec::new(),
    };

    app.wal
        .append(
            &wal_id,
            TransactionRecord::with_payload(
                TransactionId(4),
                None,
                Some(TransactionId(3)),
                4,
                actor,
                TransactionKind::SqlDefinitionChange,
                view_drop_payload
                    .encode()
                    .expect("view drop payload should encode"),
            ),
        )
        .expect("view drop append should succeed");

    app.bootstrap()
        .expect("bootstrap should replay entity construction records");

    let loaded = app
        .catalogs()
        .get(&wal_id)
        .expect("catalog should be loaded");

    assert!(loaded.trigger("trg_users_bi").is_some());
    
    assert_eq!(
        loaded
            .entity_metadata("trg_users_bi")
            .and_then(|metadata| metadata.created_by),
        Some("bootstrap-object-replay".to_string())
    );
    
    assert!(loaded.view("users_v").is_none());

}

#[test]
fn select_query_returns_table_schema_columns() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-query-routing-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![
                FieldDef {
                    seqno: 1,
                    field_name: "id".to_string(),
                    field_type: FieldType::Int(64),
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 2,
                    field_name: "email".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::Indexed,
                    default_value: None,
                    metadata: None,
                },
            ]),
        )
        .expect("table should register");

    app.catalogs.insert("main".to_string(), catalog);

    let request = ConnectorRequest::new(
        "req-query-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select * from users".to_string(),
            },
        },
    );

    let response = app.handle_connector_request(&request);

    assert_eq!(response.request_id, "req-query-1");
    assert_eq!(response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query result");
    };

    let column_names = result
        .columns
        .iter()
        .map(|field| field.field_name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(column_names, vec!["id", "email"]);
    assert!(result.rows.is_empty());

}

#[test]
fn show_tables_query_returns_table_name_rows() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-show-tables-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("users table should register");

    catalog
        .register_table(
            "accounts",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("accounts table should register");

    app.catalogs.insert("main".to_string(), catalog);

    let request = ConnectorRequest::new(
        "req-show-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show tables".to_string(),
            },
        },
    );

    let response = app.handle_connector_request(&request);

    assert_eq!(response.request_id, "req-show-1");
    assert_eq!(response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query result");
    };

    let column_names = result
        .columns
        .iter()
        .map(|field| field.field_name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(column_names, vec!["table_name", "store_kind"]);
    assert_eq!(result.rows.len(), 2);

    let row_values = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8_lossy(&row[0]).to_string(),
                String::from_utf8_lossy(&row[1]).to_string(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        row_values,
        vec![
            ("accounts".to_string(), "permanent".to_string()),
            ("users".to_string(), "permanent".to_string()),
        ]
    );

}

#[test]
fn schema_command_create_table_executes_via_query_routing() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-schema-command-routing-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let database_name = "schema_route_db";
    let database_id = serverlib::DatabaseId::from_database_name(database_name)
        .expect("database id should normalize")
        .0;

    let create_db = ConnectorRequest::new(
        "req-create-db",
        ConnectorCommand::CreateDatabase {
            database_name: database_name.to_string(),
        },
    );

    let create_db_response = app.handle_connector_request(&create_db);
    assert_eq!(create_db_response.status, ResponseStatus::Applied);

    let create_table = ConnectorRequest::new(
        "req-schema-create-table",
        ConnectorCommand::Schema {
            database_id: database_id.clone(),
            command: connector::SchemaCommand::CreateTable {
                table_id: "users".to_string(),
                fields: vec![
                    connector::FieldSpec::new("id", common::schema::FieldKind::UInt(64))
                        .primary_key(),
                    connector::FieldSpec::new("email", common::schema::FieldKind::Text),
                ],
            },
        },
    );

    let create_table_response = app.handle_connector_request(&create_table);
    assert_eq!(create_table_response.status, ResponseStatus::Applied);

    let catalog = app
        .catalogs()
        .get(&database_id)
        .expect("target catalog should exist");
    assert!(
        catalog.table_schema("users").is_some(),
        "expected users table to be registered via schema command"
    );

}

#[test]
fn mutation_command_insert_executes_via_query_routing() {

    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-mutation-command-routing-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let database_name = "mutation_route_db";
    let database_id = serverlib::DatabaseId::from_database_name(database_name)
        .expect("database id should normalize")
        .0;

    let create_db = ConnectorRequest::new(
        "req-create-db",
        ConnectorCommand::CreateDatabase {
            database_name: database_name.to_string(),
        },
    );

    let create_db_response = app.handle_connector_request(&create_db);
    assert_eq!(create_db_response.status, ResponseStatus::Applied);

    let create_table = ConnectorRequest::new(
        "req-query-create-table",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: database_id.clone(),
                sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                    .to_string(),
            },
        },
    );

    let create_table_response = app.handle_connector_request(&create_table);
    assert_eq!(create_table_response.status, ResponseStatus::Applied);

    let insert = ConnectorRequest::new(
        "req-mutation-insert",
        ConnectorCommand::Mutation {
            database_id: database_id.clone(),
            mutation: connector::DataMutation::Insert {
                table_id: "users".to_string(),
                values: vec![
                    connector::FieldValue {
                        name: "id".to_string(),
                        value: b"1".to_vec(),
                    },
                    connector::FieldValue {
                        name: "email".to_string(),
                        value: b"sam@example.com".to_vec(),
                    },
                ],
            },
        },
    );

    let insert_response = app.handle_connector_request(&insert);
    assert_eq!(insert_response.status, ResponseStatus::Applied);

    let verify = ConnectorRequest::new(
        "req-mutation-verify",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: database_id,
                sql: "select count(*) as c_all from users".to_string(),
            },
        },
    );

    let verify_response = app.handle_connector_request(&verify);
    assert_eq!(verify_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = verify_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], b"1".to_vec());

}

#[test]
fn create_table_query_registers_table_with_schema() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-create-table-query-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let request = ConnectorRequest::new(
        "req-create-table-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255))"
                    .to_string(),
            },
        },
    );

    let response = app.handle_connector_request(&request);
    assert_eq!(response.status, ResponseStatus::Applied);

    let catalog = app.catalogs.get("main").expect("main catalog should exist");
    let schema = catalog
        .table_schema("users")
        .expect("users schema should exist");
    
    assert_eq!(schema.fields.len(), 2);
    assert_eq!(schema.fields[0].field_name, "id");
    assert_eq!(schema.fields[0].indexed, FieldIndex::PrimaryKey);
    assert_eq!(schema.fields[1].field_name, "email");

}

#[test]
fn insert_query_appends_insert_record_to_table_wal() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-insert-query-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    let create_request = ConnectorRequest::new(
        "req-create-table-insert-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                    .to_string(),
            },
        },
    );

    let create_response = app.handle_connector_request(&create_request);
    assert_eq!(create_response.status, ResponseStatus::Applied);

    let insert_request = ConnectorRequest::new(
        "req-insert-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
            },
        },
    );

    let insert_response = app.handle_connector_request(&insert_request);
    assert_eq!(insert_response.status, ResponseStatus::Applied);

    let ConnectorResult::Mutation(mutation) = insert_response.result else {
        panic!("expected mutation result");
    };
    assert_eq!(mutation.affected_rows, 1);

    let users_stream_id = table_stream_id(&app, "main", "users");
    let records = app.wal.since(&users_stream_id, None);
    let insert_record = records
        .iter()
        .find(|record| record.kind == TransactionKind::Insert)
        .expect("insert transaction should be present in table WAL");

    let schema = app
        .catalogs
        .get("main")
        .and_then(|catalog| catalog.table_schema("users"))
        .expect("users schema should exist");

    let payload = decode_row_payload(
        &schema,
        insert_record.payload().expect("insert payload should be present"),
    )
        .expect("insert payload should deserialize");

    assert_eq!(
        payload.get("id").map(|value| render_stored_field_value(value)),
        Some(b"1".to_vec())
    );
    assert_eq!(payload.get("email"), Some(&b"sam@example.com".to_vec()));

}

#[test]
fn update_query_updates_live_row() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-update-query-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    let create_request = ConnectorRequest::new(
        "req-create-table-update-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                    .to_string(),
            },
        },
    );

    let create_response = app.handle_connector_request(&create_request);
    assert_eq!(create_response.status, ResponseStatus::Applied);

    let insert_request = ConnectorRequest::new(
        "req-insert-update-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
            },
        },
    );

    let insert_response = app.handle_connector_request(&insert_request);
    assert_eq!(insert_response.status, ResponseStatus::Applied);

    let update_request = ConnectorRequest::new(
        "req-update-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "update users set email='sam+updated@example.com' where id=1".to_string(),
            },
        },
    );

    let update_response = app.handle_connector_request(&update_request);
    assert_eq!(update_response.status, ResponseStatus::Applied);

    let ConnectorResult::Mutation(update_mutation) = update_response.result else {
        panic!("expected mutation result");
    };
    assert_eq!(update_mutation.affected_rows, 1);

    let select_request = ConnectorRequest::new(
        "req-select-update-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id, email from users where id=1".to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], b"1".to_vec());
    assert_eq!(result.rows[0][1], b"sam+updated@example.com".to_vec());

    assert_eq!(
        app.catalogs
            .get("main")
            .and_then(|catalog| catalog.table_status("users")),
        Some(ObjectStatus::Ready)
    );

}

#[test]
fn rejected_insert_releases_table_write_lock() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-insert-abort-lock-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    let create_response = app.handle_connector_request(&ConnectorRequest::new(
        "req-create-table-lock-abort",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                    .to_string(),
            },
        },
    ));
    assert_eq!(create_response.status, ResponseStatus::Applied);

    let first_insert = app.handle_connector_request(&ConnectorRequest::new(
        "req-insert-lock-abort-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
            },
        },
    ));
    assert_eq!(first_insert.status, ResponseStatus::Applied);

    let duplicate_insert = app.handle_connector_request(&ConnectorRequest::new(
        "req-insert-lock-abort-2",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
            },
        },
    ));
    assert_eq!(duplicate_insert.status, ResponseStatus::Rejected);

    assert_eq!(
        app.catalogs
            .get("main")
            .and_then(|catalog| catalog.table_status("users")),
        Some(ObjectStatus::Ready)
    );

    let second_insert = app.handle_connector_request(&ConnectorRequest::new(
        "req-insert-lock-abort-3",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (2, 'alex@example.com')".to_string(),
            },
        },
    ));
    assert_eq!(second_insert.status, ResponseStatus::Applied);

}

#[test]
fn affinity_schema_sync_failure_returns_database_to_ready() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-affinity-schema-sync-lock-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("load->ready should be valid");
    app.catalogs.insert("main".to_string(), catalog);

    let result = app.apply_affinity_schema_definitions(
        "main",
        &["this is not valid sql".to_string()],
    );

    assert!(result.is_err());
    assert_eq!(
        app.catalogs.get("main").map(|catalog| catalog.status()),
        Some(ObjectStatus::Ready)
    );

}

#[test]
fn affinity_wal_import_ignores_stale_schema_revision_and_returns_database_to_ready() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-affinity-wal-sync-lock-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::Indexed,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("users table should register");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("load->ready should be valid");
    let wal_stream_id = catalog.database_id.0.clone();
    app.catalogs.insert("main".to_string(), catalog);

    // Stale schema revisions can arrive again during replicated catchup and
    // should be treated as idempotent.
    let stale_schema_payload = SchemaChangePayload {
        table_id: "users".to_string(),
        schema_revision: 0,
        schema_epoch: 1,
        entity_id: None,
        schema: TableSchema::new(vec![FieldDef {
            seqno: 1,
            field_name: "id".to_string(),
            field_type: FieldType::Int(64),
            nullable: false,
            indexed: FieldIndex::Indexed,
            default_value: None,
            metadata: None,
        }]),
    };

    let malformed = vec![(
        wal_stream_id,
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            UserId::from_username("affinity-sync-test"),
            TransactionKind::SchemaChange,
            stale_schema_payload
                .encode()
                .expect("schema payload should encode"),
        ),
    )];

    let result = app.import_wal_records("main", malformed);

    assert!(result.is_ok());
    assert_eq!(
        app.catalogs.get("main").map(|catalog| catalog.status()),
        Some(ObjectStatus::Ready)
    );

}

#[test]
fn wal_export_with_stream_cursors_does_not_skip_unseen_streams() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-wal-export-stream-cursors-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::Indexed,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("users table should register");
    catalog
        .register_table(
            "accounts",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::Indexed,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("accounts table should register");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("load->ready should be valid");
    app.catalogs.insert("main".to_string(), catalog);

    let users_stream_id = table_stream_id(&app, "main", "users");
    let accounts_stream_id = table_stream_id(&app, "main", "accounts");

    app.wal
        .append(
            &users_stream_id,
            TransactionRecord::without_payload(
                TransactionId(1),
                None,
                None,
                1,
                UserId::from_username("export-test"),
                TransactionKind::Ignore,
            ),
        )
        .expect("users WAL append should succeed");

    app.wal
        .append(
            &accounts_stream_id,
            TransactionRecord::without_payload(
                TransactionId(2),
                None,
                None,
                2,
                UserId::from_username("export-test"),
                TransactionKind::Ignore,
            ),
        )
        .expect("accounts WAL append should succeed");

    let mut stream_cursors = std::collections::HashMap::new();
    stream_cursors.insert(users_stream_id.clone(), TransactionId(1));

    let exported = app
        .export_wal_records_for_database(
            "main",
            Some(TransactionId(1000)),
            Some(&stream_cursors),
        )
        .expect("WAL export should succeed");

    assert!(
        exported
            .iter()
            .any(|(stream_id, record)| stream_id == &accounts_stream_id && record.id == TransactionId(2)),
        "accounts stream records should not be filtered by global cursor"
    );
    assert!(
        exported
            .iter()
            .all(|(stream_id, record)| {
                !(stream_id == &users_stream_id && record.id <= TransactionId(1))
            }),
        "users stream should respect its own cursor"
    );

}

#[test]
fn session_transaction_control_is_scoped_by_session_id() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-tx-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let begin = ConnectorRequest::new(
        "req-begin-session-a",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "begin".to_string(),
            },
        },
    );

    let begin_response = app.handle_connector_request_for_session(&begin, "session-a");
    assert_eq!(begin_response.status, ResponseStatus::Applied);

    let commit_other_session = ConnectorRequest::new(
        "req-commit-session-b",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );

    let commit_other_response =
        app.handle_connector_request_for_session(&commit_other_session, "session-b");
    assert_eq!(commit_other_response.status, ResponseStatus::Rejected);

    let commit_same_session = ConnectorRequest::new(
        "req-commit-session-a",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );

    let commit_same_response =
        app.handle_connector_request_for_session(&commit_same_session, "session-a");
    
    assert_eq!(commit_same_response.status, ResponseStatus::Applied);

}

#[test]
fn active_session_transaction_stages_queries_until_commit() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-tx-block-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_table = ConnectorRequest::new(
        "req-create-table-tx-commit",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    );

    let create_table_response = app.handle_connector_request(&create_table);
    assert_eq!(create_table_response.status, ResponseStatus::Applied);

    let begin = ConnectorRequest::new(
        "req-begin-tx",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "start transaction".to_string(),
            },
        },
    );

    let begin_response = app.handle_connector_request_for_session(&begin, "session-a");
    assert_eq!(begin_response.status, ResponseStatus::Applied);

    let staged_query = ConnectorRequest::new(
        "req-staged",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id) values (1)".to_string(),
            },
        },
    );

    let staged_response =
        app.handle_connector_request_for_session(&staged_query, "session-a");
    assert_eq!(staged_response.status, ResponseStatus::Applied);

    let read_before_commit = ConnectorRequest::new(
        "req-select-before-commit",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id from users where id=1".to_string(),
            },
        },
    );

    let read_before_commit_response =
        app.handle_connector_request_for_session(&read_before_commit, "session-b");
    assert_eq!(read_before_commit_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(read_before_commit_result) = read_before_commit_response.result else {
        panic!("expected query result");
    };
    assert!(read_before_commit_result.rows.is_empty());

    let commit_request = ConnectorRequest::new(
        "req-commit",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );

    let commit_response =
        app.handle_connector_request_for_session(&commit_request, "session-a");
    assert_eq!(commit_response.status, ResponseStatus::Applied);

    let ConnectorResult::Mutation(commit_mutation) = commit_response.result else {
        panic!("expected mutation result");
    };
    assert_eq!(commit_mutation.affected_rows, 1);

    let read_committed_row = ConnectorRequest::new(
        "req-select-after-commit",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id from users where id=1".to_string(),
            },
        },
    );

    let read_response = app.handle_connector_request_for_session(&read_committed_row, "session-b");
    assert_eq!(read_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(read_result) = read_response.result else {
        panic!("expected query result");
    };
    assert_eq!(read_result.rows.len(), 1);
    
}

#[test]
fn transaction_commit_rejects_duplicate_insert_during_validation() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-tx-duplicate-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_table = ConnectorRequest::new(
        "req-create-table-tx-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    );

    let create_table_response = app.handle_connector_request(&create_table);
    assert_eq!(create_table_response.status, ResponseStatus::Applied);

    let seed_insert = ConnectorRequest::new(
        "req-seed-user",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id) values (1)".to_string(),
            },
        },
    );

    let seed_insert_response = app.handle_connector_request(&seed_insert);
    assert_eq!(seed_insert_response.status, ResponseStatus::Applied);

    let begin = ConnectorRequest::new(
        "req-begin-tx-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "start transaction".to_string(),
            },
        },
    );

    let begin_response = app.handle_connector_request_for_session(&begin, "session-a");
    assert_eq!(begin_response.status, ResponseStatus::Applied);

    let staged_query = ConnectorRequest::new(
        "req-staged-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id) values (1)".to_string(),
            },
        },
    );

    let staged_response = app.handle_connector_request_for_session(&staged_query, "session-a");
    assert_eq!(staged_response.status, ResponseStatus::Applied);

    let commit_request = ConnectorRequest::new(
        "req-commit-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );

    let commit_response = app.handle_connector_request_for_session(&commit_request, "session-a");
    assert_eq!(commit_response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(message) = commit_response.result else {
        panic!("expected duplicate insert validation error");
    };

    assert!(message.contains("transaction validation failed at staged statement 1"));
    assert!(message.contains("duplicate primary key"));

}

#[test]
fn lightweight_import_commit_failure_clears_transaction_state_for_followup_reads() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-import-duplicate-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_table = ConnectorRequest::new(
        "req-create-table-import-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    );
    assert_eq!(app.handle_connector_request(&create_table).status, ResponseStatus::Applied);

    let seed_insert = ConnectorRequest::new(
        "req-seed-user-import-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id) values (1)".to_string(),
            },
        },
    );
    assert_eq!(app.handle_connector_request(&seed_insert).status, ResponseStatus::Applied);

    let begin = ConnectorRequest::new(
        "req-begin-import-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "begin /*distdb_import*/".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&begin, "session-a").status,
        ResponseStatus::Applied
    );

    let staged_query = ConnectorRequest::new(
        "req-staged-import-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id) values (1)".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&staged_query, "session-a").status,
        ResponseStatus::Applied
    );

    let commit_request = ConnectorRequest::new(
        "req-commit-import-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );

    let commit_response = app.handle_connector_request_for_session(&commit_request, "session-a");
    assert_eq!(commit_response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(commit_error) = &commit_response.result else {
        panic!("expected lightweight import commit failure message");
    };

    assert!(commit_error.contains("transaction validation failed at staged statement 1"));
    assert!(commit_error.contains("duplicate primary key"));

    let followup_read = ConnectorRequest::new(
        "req-followup-read-import-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id from users where id=1".to_string(),
            },
        },
    );

    let followup_response = app.handle_connector_request_for_session(&followup_read, "session-a");
    assert_eq!(followup_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = followup_response.result else {
        panic!("expected query result after failed lightweight import commit");
    };

    let _ = result;

    let rollback_request = ConnectorRequest::new(
        "req-rollback-import-duplicate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "rollback".to_string(),
            },
        },
    );

    let rollback_response = app.handle_connector_request_for_session(&rollback_request, "session-a");
    assert_eq!(rollback_response.status, ResponseStatus::Rejected);
}

#[test]
fn explicit_transaction_rejects_non_dml_schema_statements() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-tx-isolation-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let begin = ConnectorRequest::new(
        "req-begin-isolation",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "begin".to_string(),
            },
        },
    );

    let begin_response = app.handle_connector_request_for_session(&begin, "session-a");
    assert_eq!(begin_response.status, ResponseStatus::Applied);

    let create_table_in_tx = ConnectorRequest::new(
        "req-create-table-in-tx",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    );

    let create_table_response =
        app.handle_connector_request_for_session(&create_table_in_tx, "session-a");
    assert_eq!(create_table_response.status, ResponseStatus::Rejected);

    let ConnectorResult::Error(message) = create_table_response.result else {
        panic!("expected error result");
    };

    assert!(
        message.contains("only single-statement insert/update/delete queries are allowed inside explicit transactions")
    );

}

#[test]
fn snapshot_isolation_rejects_concurrent_write_write_conflicts() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-snapshot-conflict-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_table = ConnectorRequest::new(
        "req-create-table-snapshot-conflict",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255) not null)".to_string(),
            },
        },
    );

    assert_eq!(app.handle_connector_request(&create_table).status, ResponseStatus::Applied);

    let seed_row = ConnectorRequest::new(
        "req-seed-snapshot-conflict",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (1, 'seed@example.com')".to_string(),
            },
        },
    );
    assert_eq!(app.handle_connector_request(&seed_row).status, ResponseStatus::Applied);

    for session_id in ["session-a", "session-b"] {
        let begin = ConnectorRequest::new(
            format!("req-begin-snapshot-conflict-{session_id}"),
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "begin".to_string(),
                },
            },
        );
        assert_eq!(
            app.handle_connector_request_for_session(&begin, session_id)
                .status,
            ResponseStatus::Applied
        );
    }

    let stage_a = ConnectorRequest::new(
        "req-stage-update-snapshot-conflict-a",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "update users set email='from-a@example.com' where id=1".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&stage_a, "session-a")
            .status,
        ResponseStatus::Applied
    );

    let stage_b = ConnectorRequest::new(
        "req-stage-update-snapshot-conflict-b",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "update users set email='from-b@example.com' where id=1".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&stage_b, "session-b")
            .status,
        ResponseStatus::Applied
    );

    let commit_a = ConnectorRequest::new(
        "req-commit-snapshot-conflict-a",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&commit_a, "session-a")
            .status,
        ResponseStatus::Applied
    );

    let commit_b = ConnectorRequest::new(
        "req-commit-snapshot-conflict-b",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );

    let commit_b_response = app.handle_connector_request_for_session(&commit_b, "session-b");
    assert_eq!(commit_b_response.status, ResponseStatus::Rejected);

}

#[test]
fn snapshot_isolation_keeps_repeatable_reads_within_transaction() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-snapshot-repeatable-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_table = ConnectorRequest::new(
        "req-create-table-snapshot-repeatable",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255) not null)".to_string(),
            },
        },
    );
    assert_eq!(app.handle_connector_request(&create_table).status, ResponseStatus::Applied);

    let seed_row = ConnectorRequest::new(
        "req-seed-snapshot-repeatable",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (1, 'seed@example.com')".to_string(),
            },
        },
    );
    assert_eq!(app.handle_connector_request(&seed_row).status, ResponseStatus::Applied);

    let begin = ConnectorRequest::new(
        "req-begin-snapshot-repeatable",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "begin".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&begin, "session-a")
            .status,
        ResponseStatus::Applied
    );

    let tx_read = ConnectorRequest::new(
        "req-read-snapshot-repeatable-tx",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select email from users where id=1".to_string(),
            },
        },
    );

    let concurrent_update = ConnectorRequest::new(
        "req-concurrent-update-snapshot-repeatable",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "update users set email='updated@example.com' where id=1".to_string(),
            },
        },
    );

    let first_read = app.handle_connector_request_for_session(&tx_read, "session-a");
    assert_eq!(first_read.status, ResponseStatus::Applied);

    let concurrent_update_response = app.handle_connector_request(&concurrent_update);
    assert_eq!(concurrent_update_response.status, ResponseStatus::Applied);

    let second_read = app.handle_connector_request_for_session(&tx_read, "session-a");
    assert_eq!(second_read.status, ResponseStatus::Applied);

    let ConnectorResult::Query(first_rows) = first_read.result else {
        panic!("expected first query result");
    };
    let ConnectorResult::Query(second_rows) = second_read.result else {
        panic!("expected second query result");
    };

    assert_eq!(first_rows.rows, second_rows.rows);
    
}

#[test]
fn snapshot_isolation_transactional_reads_see_own_staged_writes() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-snapshot-own-writes-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_table = ConnectorRequest::new(
        "req-create-table-snapshot-own-writes",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255) not null)".to_string(),
            },
        },
    );
    assert_eq!(app.handle_connector_request(&create_table).status, ResponseStatus::Applied);

    let begin = ConnectorRequest::new(
        "req-begin-snapshot-own-writes",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "begin".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&begin, "session-a")
            .status,
        ResponseStatus::Applied
    );

    let stage_insert = ConnectorRequest::new(
        "req-stage-insert-snapshot-own-writes",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (7, 'tx@example.com')".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&stage_insert, "session-a")
            .status,
        ResponseStatus::Applied
    );

    let tx_read = ConnectorRequest::new(
        "req-read-snapshot-own-writes",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select email from users where id=7".to_string(),
            },
        },
    );

    let tx_read_response = app.handle_connector_request_for_session(&tx_read, "session-a");
    assert_eq!(tx_read_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(tx_read_result) = tx_read_response.result else {
        panic!("expected query result");
    };
    assert_eq!(tx_read_result.rows.len(), 1);
    assert_eq!(tx_read_result.rows[0][0], b"tx@example.com".to_vec());

    let outside_read = ConnectorRequest::new(
        "req-outside-read-snapshot-own-writes",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select email from users where id=7".to_string(),
            },
        },
    );

    let outside_read_response =
        app.handle_connector_request_for_session(&outside_read, "session-b");
    assert_eq!(outside_read_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(outside_read_result) = outside_read_response.result else {
        panic!("expected query result");
    };
    assert!(outside_read_result.rows.is_empty());

}

#[test]
fn serializable_rejects_write_skew_across_disjoint_rows() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-serializable-write-skew-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_table = ConnectorRequest::new(
        "req-create-table-serializable-write-skew",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table oncall (id bigint not null primary key, on_call bigint not null)".to_string(),
            },
        },
    );
    assert_eq!(app.handle_connector_request(&create_table).status, ResponseStatus::Applied);

    for (request_id, sql) in [
        (
            "req-seed-serializable-write-skew-1",
            "insert into oncall (id, on_call) values (1, 1)",
        ),
        (
            "req-seed-serializable-write-skew-2",
            "insert into oncall (id, on_call) values (2, 1)",
        ),
    ] {
        let seed = ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        );
        assert_eq!(app.handle_connector_request(&seed).status, ResponseStatus::Applied);
    }

    for session_id in ["session-a", "session-b"] {
        let begin = ConnectorRequest::new(
            format!("req-begin-serializable-write-skew-{session_id}"),
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "begin".to_string(),
                },
            },
        );
        assert_eq!(
            app.handle_connector_request_for_session(&begin, session_id)
                .status,
            ResponseStatus::Applied
        );
    }

    let tx_read = ConnectorRequest::new(
        "req-read-serializable-write-skew",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id from oncall where on_call=1".to_string(),
            },
        },
    );

    let tx_read_a = app.handle_connector_request_for_session(&tx_read, "session-a");
    let tx_read_b = app.handle_connector_request_for_session(&tx_read, "session-b");
    assert_eq!(tx_read_a.status, ResponseStatus::Applied);
    assert_eq!(tx_read_b.status, ResponseStatus::Applied);

    let ConnectorResult::Query(rows_a) = tx_read_a.result else {
        panic!("expected query result");
    };
    let ConnectorResult::Query(rows_b) = tx_read_b.result else {
        panic!("expected query result");
    };
    assert_eq!(rows_a.rows.len(), 2);
    assert_eq!(rows_b.rows.len(), 2);

    let stage_a = ConnectorRequest::new(
        "req-stage-serializable-write-skew-a",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "update oncall set on_call=0 where id=1".to_string(),
            },
        },
    );
    let stage_b = ConnectorRequest::new(
        "req-stage-serializable-write-skew-b",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "update oncall set on_call=0 where id=2".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&stage_a, "session-a")
            .status,
        ResponseStatus::Applied
    );
    assert_eq!(
        app.handle_connector_request_for_session(&stage_b, "session-b")
            .status,
        ResponseStatus::Applied
    );

    let commit_a = ConnectorRequest::new(
        "req-commit-serializable-write-skew-a",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&commit_a, "session-a")
            .status,
        ResponseStatus::Applied
    );

    let commit_b = ConnectorRequest::new(
        "req-commit-serializable-write-skew-b",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );
    
    let commit_b_response = app.handle_connector_request_for_session(&commit_b, "session-b");
    assert_eq!(commit_b_response.status, ResponseStatus::Rejected);

}

#[test]
fn commit_groups_staged_dml_into_one_write_batch() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-tx-group-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_table = ConnectorRequest::new(
        "req-create-table-tx-group",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    );

    let create_table_response = app.handle_connector_request(&create_table);
    assert_eq!(create_table_response.status, ResponseStatus::Applied);

    let begin = ConnectorRequest::new(
        "req-begin-tx-group",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "start transaction".to_string(),
            },
        },
    );

    let begin_response = app.handle_connector_request_for_session(&begin, "session-a");
    assert_eq!(begin_response.status, ResponseStatus::Applied);

    for (request_id, sql) in [
        ("req-staged-1", "insert into users (id) values (1)"),
        ("req-staged-2", "insert into users (id) values (2)"),
    ] {
        let staged_request = ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        );

        let staged_response =
            app.handle_connector_request_for_session(&staged_request, "session-a");
        assert_eq!(staged_response.status, ResponseStatus::Applied);
    }

    let commit_request = ConnectorRequest::new(
        "req-commit-group",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );

    let commit_response =
        app.handle_connector_request_for_session(&commit_request, "session-a");
    assert_eq!(commit_response.status, ResponseStatus::Applied);

    let users_stream_id = table_stream_id(&app, "main", "users");
    let records = app.wal.since(&users_stream_id, None);

    let write_begin = records
        .iter()
        .filter(|record| record.kind == TransactionKind::WriteBegin)
        .collect::<Vec<_>>();
    assert_eq!(write_begin.len(), 1);

    let write_commit = records
        .iter()
        .filter(|record| record.kind == TransactionKind::WriteCommit)
        .collect::<Vec<_>>();
    assert_eq!(write_commit.len(), 1);

    let write_abort = records
        .iter()
        .filter(|record| record.kind == TransactionKind::WriteAbort)
        .count();
    assert_eq!(write_abort, 0);

    let group_id = write_begin[0]
        .groupid
        .expect("write begin should carry the transaction group id");
    assert_eq!(write_commit[0].groupid, Some(group_id));

    let inserts = records
        .iter()
        .filter(|record| record.kind == TransactionKind::Insert)
        .collect::<Vec<_>>();
    assert_eq!(inserts.len(), 2);
    
    assert!(
        inserts
            .iter()
            .all(|record| record.groupid == Some(group_id))
    );

}

#[expect(clippy::single_element_loop, reason = "loop allows use of `break` to fail test immediately when condition is not met")]
#[test]
fn failed_commit_validation_leaves_real_wal_and_indexes_clean() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-tx-abort-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [(
        "req-create-table-tx-abort",
        "create table users (id bigint not null primary key)",
    )] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));
        assert_eq!(response.status, ResponseStatus::Applied);
    }

    let begin = ConnectorRequest::new(
        "req-begin-tx-abort",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "start transaction".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&begin, "session-a")
            .status,
        ResponseStatus::Applied
    );

    for (request_id, sql) in [
        ("req-staged-abort-1", "insert into users (id) values (1)"),
        ("req-staged-abort-2", "insert into users (id) values (1)"),
    ] {
        let staged_request = ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        );

        let staged_response =
            app.handle_connector_request_for_session(&staged_request, "session-a");
        assert_eq!(staged_response.status, ResponseStatus::Applied);
    }

    let commit_request = ConnectorRequest::new(
        "req-commit-abort",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );

    let commit_response =
        app.handle_connector_request_for_session(&commit_request, "session-a");
    assert_eq!(commit_response.status, ResponseStatus::Rejected);

    let users_stream_id = table_stream_id(&app, "main", "users");
    let records_after_failed_commit = app.wal.since(&users_stream_id, None);
    assert!(!records_after_failed_commit.iter().any(|record| {
        matches!(
            record.kind,
            TransactionKind::Insert
                | TransactionKind::Delete
                | TransactionKind::Update
                | TransactionKind::WriteBegin
                | TransactionKind::WriteCommit
                | TransactionKind::WriteAbort
        )
    }));

    let read_request = ConnectorRequest::new(
        "req-select-after-abort",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id from users where id=1".to_string(),
            },
        },
    );

    let read_response = app.handle_connector_request_for_session(&read_request, "session-b");
    assert_eq!(read_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(read_result) = read_response.result else {
        panic!("expected query result");
    };
    assert!(read_result.rows.is_empty());

    let retry_insert = ConnectorRequest::new(
        "req-insert-after-abort",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id) values (1)".to_string(),
            },
        },
    );

    let retry_insert_response = app.handle_connector_request(&retry_insert);
    assert_eq!(retry_insert_response.status, ResponseStatus::Applied);
}

#[test]
fn commit_shares_one_group_id_across_touched_tables() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-tx-multitable-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-multitable",
            "create table users (id bigint not null primary key)",
        ),
        (
            "req-create-profiles-multitable",
            "create table profiles (id bigint not null primary key)",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));
        assert_eq!(response.status, ResponseStatus::Applied);
    }

    let begin = ConnectorRequest::new(
        "req-begin-tx-multitable",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "start transaction".to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request_for_session(&begin, "session-a")
            .status,
        ResponseStatus::Applied
    );

    for (request_id, sql) in [
        ("req-staged-users-multitable", "insert into users (id) values (1)"),
        (
            "req-staged-profiles-multitable",
            "insert into profiles (id) values (10)",
        ),
    ] {
        let staged_request = ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        );

        let staged_response =
            app.handle_connector_request_for_session(&staged_request, "session-a");
        assert_eq!(staged_response.status, ResponseStatus::Applied);
    }

    let commit_request = ConnectorRequest::new(
        "req-commit-multitable",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );

    let commit_response =
        app.handle_connector_request_for_session(&commit_request, "session-a");
    assert_eq!(commit_response.status, ResponseStatus::Applied);

    let users_stream_id = table_stream_id(&app, "main", "users");
    let profiles_stream_id = table_stream_id(&app, "main", "profiles");
    let users_records = app.wal.since(&users_stream_id, None);
    let profiles_records = app.wal.since(&profiles_stream_id, None);

    let users_group_id = users_records
        .iter()
        .find(|record| record.kind == TransactionKind::WriteBegin)
        .and_then(|record| record.groupid)
        .expect("users write begin should have a group id");
    let profiles_group_id = profiles_records
        .iter()
        .find(|record| record.kind == TransactionKind::WriteBegin)
        .and_then(|record| record.groupid)
        .expect("profiles write begin should have a group id");

    assert_eq!(users_group_id, profiles_group_id);
    
    assert!(users_records.iter().any(|record| {
        record.kind == TransactionKind::WriteCommit && record.groupid == Some(users_group_id)
    }));
    
    assert!(profiles_records.iter().any(|record| {
        record.kind == TransactionKind::WriteCommit && record.groupid == Some(users_group_id)
    }));
    
    assert!(users_records.iter().any(|record| {
        record.kind == TransactionKind::Insert && record.groupid == Some(users_group_id)
    }));
    
    assert!(profiles_records.iter().any(|record| {
        record.kind == TransactionKind::Insert && record.groupid == Some(users_group_id)
    }));

}

#[test]
fn rollback_discards_staged_queries_for_session() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-tx-rollback-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_table = ConnectorRequest::new(
        "req-create-table-tx-rollback",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key)".to_string(),
            },
        },
    );

    let create_table_response = app.handle_connector_request(&create_table);
    assert_eq!(create_table_response.status, ResponseStatus::Applied);

    let begin = ConnectorRequest::new(
        "req-begin",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "begin".to_string(),
            },
        },
    );

    let begin_response = app.handle_connector_request_for_session(&begin, "session-a");
    assert_eq!(begin_response.status, ResponseStatus::Applied);

    let staged_create = ConnectorRequest::new(
        "req-stage-insert-rollback",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id) values (1)".to_string(),
            },
        },
    );

    let stage_response =
        app.handle_connector_request_for_session(&staged_create, "session-a");
    assert_eq!(stage_response.status, ResponseStatus::Applied);

    let rollback = ConnectorRequest::new(
        "req-rollback",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "rollback".to_string(),
            },
        },
    );

    let rollback_response = app.handle_connector_request_for_session(&rollback, "session-a");
    assert_eq!(rollback_response.status, ResponseStatus::Applied);

    let verify_absent = ConnectorRequest::new(
        "req-verify-rollback",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id from users where id=1".to_string(),
            },
        },
    );

    let verify_response = app.handle_connector_request_for_session(&verify_absent, "session-b");
    assert_eq!(verify_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(verify_result) = verify_response.result else {
        panic!("expected query result");
    };
    assert_eq!(verify_result.rows.len(), 0);
}

#[test]
fn disconnect_rollback_clears_active_session_transaction() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-session-disconnect-rollback-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let begin = ConnectorRequest::new(
        "req-begin-disconnect",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "begin".to_string(),
            },
        },
    );

    let begin_response = app.handle_connector_request_for_session(&begin, "session-a");
    assert_eq!(begin_response.status, ResponseStatus::Applied);

    let staged_insert = ConnectorRequest::new(
        "req-stage-insert-disconnect",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id) values (1)".to_string(),
            },
        },
    );

    let staged_response = app.handle_connector_request_for_session(&staged_insert, "session-a");
    assert_eq!(staged_response.status, ResponseStatus::Applied);

    assert!(app.rollback_session_transaction("session-a"));

    let commit = ConnectorRequest::new(
        "req-commit-after-disconnect",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "commit".to_string(),
            },
        },
    );

    let commit_response = app.handle_connector_request_for_session(&commit, "session-a");
    assert_eq!(commit_response.status, ResponseStatus::Rejected);
}

#[test]
fn delete_query_removes_live_row() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-delete-query-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    let create_request = ConnectorRequest::new(
        "req-create-table-delete-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                    .to_string(),
            },
        },
    );

    let create_response = app.handle_connector_request(&create_request);
    assert_eq!(create_response.status, ResponseStatus::Applied);

    let insert_request = ConnectorRequest::new(
        "req-insert-delete-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
            },
        },
    );

    let insert_response = app.handle_connector_request(&insert_request);
    assert_eq!(insert_response.status, ResponseStatus::Applied);

    let delete_request = ConnectorRequest::new(
        "req-delete-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "delete from users where id=1".to_string(),
            },
        },
    );

    let delete_response = app.handle_connector_request(&delete_request);
    assert_eq!(delete_response.status, ResponseStatus::Applied);

    let ConnectorResult::Mutation(delete_mutation) = delete_response.result else {
        panic!("expected mutation result");
    };
    assert_eq!(delete_mutation.affected_rows, 1);

    let select_request = ConnectorRequest::new(
        "req-select-delete-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id, email from users where id=1".to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert!(result.rows.is_empty());
}

#[test]
fn update_query_with_join_updates_matching_target_row() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-update-join-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-update-join-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-update-join-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-insert-users-update-join-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-users-update-join-2",
            "insert into users (id, email) values (2, 'alex@example.com')",
        ),
        (
            "req-insert-profiles-update-join-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(
            response.status,
            ResponseStatus::Applied,
            "request '{}' failed with result {:?}",
            request_id,
            response.result,
        );
    }

    let update_request = ConnectorRequest::new(
        "req-update-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "update users u join profiles p on u.id = p.user_id set email='sam+updated@example.com' where p.name = 'Sam'"
                    .to_string(),
            },
        },
    );

    let update_response = app.handle_connector_request(&update_request);
    assert_eq!(update_response.status, ResponseStatus::Applied);

    let ConnectorResult::Mutation(mutation) = update_response.result else {
        panic!("expected mutation result");
    };
    assert_eq!(mutation.affected_rows, 1);

    let select_request = ConnectorRequest::new(
        "req-select-update-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id, email from users".to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 2);

    let mut emails = result
        .rows
        .iter()
        .map(|row| String::from_utf8(row[1].clone()).expect("email should be valid utf8"))
        .collect::<Vec<_>>();

    emails.sort();

    assert_eq!(
        emails,
        vec![
            "alex@example.com".to_string(),
            "sam+updated@example.com".to_string(),
        ]
    );
}

#[test]
fn delete_query_with_left_outer_join_removes_unmatched_target_row() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-delete-left-join-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-delete-join-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-delete-join-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-insert-users-delete-join-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-users-delete-join-2",
            "insert into users (id, email) values (2, 'alex@example.com')",
        ),
        (
            "req-insert-profiles-delete-join-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(
            response.status,
            ResponseStatus::Applied,
            "request '{}' failed with result {:?}",
            request_id,
            response.result,
        );
    }

    let delete_request = ConnectorRequest::new(
        "req-delete-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "delete u from users u left join profiles p on u.id = p.user_id where p.name is null"
                    .to_string(),
            },
        },
    );

    let delete_response = app.handle_connector_request(&delete_request);
    assert_eq!(delete_response.status, ResponseStatus::Applied);

    let ConnectorResult::Mutation(mutation) = delete_response.result else {
        panic!("expected mutation result");
    };
    assert_eq!(mutation.affected_rows, 1);

    let select_request = ConnectorRequest::new(
        "req-select-delete-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id, email from users".to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], b"1".to_vec());
}

#[test]
fn select_inner_join_returns_matching_rows() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-select-join-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-join-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-join-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-insert-users-join-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-profiles-join-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(
            response.status,
            ResponseStatus::Applied,
            "request '{}' failed with result {:?}",
            request_id,
            response.result,
        );
    }

    let select_request = ConnectorRequest::new(
        "req-select-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select u.email, p.name from users u inner join profiles p on u.id = p.user_id where u.id = 1"
                    .to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], b"sam@example.com".to_vec());
    assert_eq!(result.rows[0][1], b"Sam".to_vec());
}

#[test]
fn select_inner_join_preserves_one_to_many_matches() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-select-join-many-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-join-many-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-join-many-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-insert-users-join-many-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-profiles-join-many-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
        (
            "req-insert-profiles-join-many-2",
            "insert into profiles (id, user_id, name) values (11, 1, 'Samuel')",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(
            response.status,
            ResponseStatus::Applied,
            "request '{}' failed with result {:?}",
            request_id,
            response.result,
        );
    }

    let select_request = ConnectorRequest::new(
        "req-select-join-many-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select u.email, p.name from users u inner join profiles p on u.id = p.user_id where u.id = 1"
                    .to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 2);

    let mut names = result
        .rows
        .iter()
        .map(|row| String::from_utf8(row[1].clone()).expect("name should be valid utf8"))
        .collect::<Vec<_>>();

    names.sort();

    assert_eq!(names, vec!["Sam".to_string(), "Samuel".to_string()]);

    for row in &result.rows {
        assert_eq!(row[0], b"sam@example.com".to_vec());
    }
}

#[test]
fn select_left_join_returns_unmatched_left_rows_with_nulls() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-select-left-join-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-left-join-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-left-join-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-insert-users-left-join-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-users-left-join-2",
            "insert into users (id, email) values (2, 'alex@example.com')",
        ),
        (
            "req-insert-profiles-left-join-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(
            response.status,
            ResponseStatus::Applied,
            "request '{}' failed with result {:?}",
            request_id,
            response.result,
        );
    }

    let select_request = ConnectorRequest::new(
        "req-select-left-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select u.email, p.name from users u left join profiles p on u.id = p.user_id"
                    .to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 2);

    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("email should be valid utf8"),
                String::from_utf8(row[1].clone()).expect("name should be valid utf8"),
            )
        })
        .collect::<Vec<_>>();

    rows.sort();

    assert_eq!(
        rows,
        vec![
            ("alex@example.com".to_string(), "NULL".to_string()),
            ("sam@example.com".to_string(), "Sam".to_string()),
        ]
    );
}

#[test]
fn select_left_join_where_right_field_is_null_filters_after_tuple_formation() {
    
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-select-left-join-where-null-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-left-join-null-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-left-join-null-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-insert-users-left-join-null-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-users-left-join-null-2",
            "insert into users (id, email) values (2, 'alex@example.com')",
        ),
        (
            "req-insert-profiles-left-join-null-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(
            response.status,
            ResponseStatus::Applied,
            "request '{}' failed with result {:?}",
            request_id,
            response.result,
        );
    }

    let select_request = ConnectorRequest::new(
        "req-select-left-join-null-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select u.email from users u left join profiles p on u.id = p.user_id where p.name is null"
                    .to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], b"alex@example.com".to_vec());

}

#[test]
fn select_left_outer_join_null_extends_unmatched_rows() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-select-left-outer-join-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-left-outer-join-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-left-outer-join-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-insert-users-left-outer-join-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-users-left-outer-join-2",
            "insert into users (id, email) values (2, 'alex@example.com')",
        ),
        (
            "req-insert-profiles-left-outer-join-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(response.status, ResponseStatus::Applied);
    }

    let select_request = ConnectorRequest::new(
        "req-select-left-outer-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select u.email, p.name from users u left outer join profiles p on u.id = p.user_id"
                    .to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 2);

    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("email should be valid utf8"),
                String::from_utf8(row[1].clone()).expect("name should be valid utf8"),
            )
        })
        .collect::<Vec<_>>();

    rows.sort();

    assert_eq!(
        rows,
        vec![
            ("alex@example.com".to_string(), "NULL".to_string()),
            ("sam@example.com".to_string(), "Sam".to_string()),
        ]
    );
}

#[test]
fn select_right_outer_join_null_extends_unmatched_rows() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-select-right-outer-join-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-right-outer-join-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-right-outer-join-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-insert-users-right-outer-join-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-profiles-right-outer-join-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
        (
            "req-insert-profiles-right-outer-join-2",
            "insert into profiles (id, user_id, name) values (11, 2, 'Orphan')",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(response.status, ResponseStatus::Applied);
    }

    let select_request = ConnectorRequest::new(
        "req-select-right-outer-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select u.email, p.name from users u right outer join profiles p on u.id = p.user_id"
                    .to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 2);

    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("email should be valid utf8"),
                String::from_utf8(row[1].clone()).expect("name should be valid utf8"),
            )
        })
        .collect::<Vec<_>>();

    rows.sort();

    assert_eq!(
        rows,
        vec![
            ("NULL".to_string(), "Orphan".to_string()),
            ("sam@example.com".to_string(), "Sam".to_string()),
        ]
    );
}

#[test]
fn select_full_outer_join_null_extends_both_sides() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-select-full-outer-join-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-full-outer-join-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-full-outer-join-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-insert-users-full-outer-join-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-users-full-outer-join-2",
            "insert into users (id, email) values (2, 'alex@example.com')",
        ),
        (
            "req-insert-profiles-full-outer-join-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
        (
            "req-insert-profiles-full-outer-join-2",
            "insert into profiles (id, user_id, name) values (11, 3, 'Orphan')",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(response.status, ResponseStatus::Applied);
    }

    let select_request = ConnectorRequest::new(
        "req-select-full-outer-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select u.email, p.name from users u full outer join profiles p on u.id = p.user_id"
                    .to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 3);

    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("email should be valid utf8"),
                String::from_utf8(row[1].clone()).expect("name should be valid utf8"),
            )
        })
        .collect::<Vec<_>>();

    rows.sort();

    assert_eq!(
        rows,
        vec![
            ("NULL".to_string(), "Orphan".to_string()),
            ("alex@example.com".to_string(), "NULL".to_string()),
            ("sam@example.com".to_string(), "Sam".to_string()),
        ]
    );
}

#[test]
fn explain_select_with_multiple_joins_returns_join_steps() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-explain-multi-join-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-explain-multi-join-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-explain-multi-join-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-create-teams-explain-multi-join-1",
            "create table teams (id bigint not null primary key, profile_id bigint not null, label varchar(255) not null)",
        ),
        (
            "req-insert-users-explain-multi-join-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-profiles-explain-multi-join-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
        (
            "req-insert-teams-explain-multi-join-1",
            "insert into teams (id, profile_id, label) values (100, 10, 'core')",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(response.status, ResponseStatus::Applied);
    }

    let explain_request = ConnectorRequest::new(
        "req-explain-multi-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "explain select u.email, p.name, t.label from users u inner join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id where u.id = 1"
                    .to_string(),
            },
        },
    );

    let explain_response = app.handle_connector_request(&explain_request);
    assert_eq!(explain_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = explain_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][1], b"base".to_vec());
    assert_eq!(result.rows[1][1], b"inner".to_vec());
    assert_eq!(result.rows[2][1], b"left".to_vec());
}

#[test]
fn explain_insert_update_delete_return_plan_details() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-explain-mutations-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    let explain_insert = ConnectorRequest::new(
        "req-explain-insert-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "explain insert into users (id, email) values (1, 'sam@example.com')"
                    .to_string(),
            },
        },
    );

    let insert_response = app.handle_connector_request(&explain_insert);
    assert_eq!(insert_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(insert_result) = insert_response.result else {
        panic!("expected query result");
    };

    assert!(
        insert_result
            .rows
            .iter()
            .any(|row| row == &vec![b"operation".to_vec(), b"insert".to_vec()])
    );

    let explain_update = ConnectorRequest::new(
        "req-explain-update-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "explain update users u join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id set u.email = 'sam+updated@example.com' where t.label = 'core'"
                    .to_string(),
            },
        },
    );

    let update_response = app.handle_connector_request(&explain_update);
    assert_eq!(update_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(update_result) = update_response.result else {
        panic!("expected query result");
    };

    assert!(
        update_result
            .rows
            .iter()
            .any(|row| row == &vec![b"join_count".to_vec(), b"2".to_vec()])
    );
    assert!(
        update_result
            .rows
            .iter()
            .any(|row| row == &vec![b"join[0].kind".to_vec(), b"inner".to_vec()])
    );
    assert!(
        update_result
            .rows
            .iter()
            .any(|row| row == &vec![b"join[1].kind".to_vec(), b"left".to_vec()])
    );

    let explain_delete = ConnectorRequest::new(
        "req-explain-delete-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "explain delete u from users u join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id where t.label is null"
                    .to_string(),
            },
        },
    );

    let delete_response = app.handle_connector_request(&explain_delete);
    assert_eq!(delete_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(delete_result) = delete_response.result else {
        panic!("expected query result");
    };

    assert!(
        delete_result
            .rows
            .iter()
            .any(|row| row == &vec![b"operation".to_vec(), b"delete".to_vec()])
    );
    assert!(
        delete_result
            .rows
            .iter()
            .any(|row| row == &vec![b"join_count".to_vec(), b"2".to_vec()])
    );
}

#[test]
fn insert_select_copies_rows_into_target_table() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-insert-select-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-insert-select-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-users-archive-insert-select-1",
            "create table users_archive (id bigint not null, email varchar(255) not null)",
        ),
        (
            "req-insert-users-insert-select-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-users-insert-select-2",
            "insert into users (id, email) values (2, 'alex@example.com')",
        ),
        (
            "req-insert-select-run-1",
            "insert into users_archive (id, email) select id, email from users where id = 1",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(
            response.status,
            ResponseStatus::Applied,
            "request '{}' failed with result {:?}",
            request_id,
            response.result,
        );
    }

    let select_request = ConnectorRequest::new(
        "req-select-users-archive-insert-select-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id, email from users_archive".to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], b"1".to_vec());
    assert_eq!(result.rows[0][1], b"sam@example.com".to_vec());
}

#[test]
fn insert_select_with_join_materializes_joined_source_rows() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-insert-select-join-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    app.catalogs.insert("main".to_string(), catalog);

    for (request_id, sql) in [
        (
            "req-create-users-insert-select-join-1",
            "create table users (id bigint not null primary key, email varchar(255) not null)",
        ),
        (
            "req-create-profiles-insert-select-join-1",
            "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
        ),
        (
            "req-create-flat-insert-select-join-1",
            "create table user_profile_flat (email varchar(255) not null, profile_name varchar(255) not null)",
        ),
        (
            "req-insert-users-insert-select-join-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-users-insert-select-join-2",
            "insert into users (id, email) values (2, 'alex@example.com')",
        ),
        (
            "req-insert-profiles-insert-select-join-1",
            "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
        ),
        (
            "req-insert-select-join-run-1",
            "insert into user_profile_flat (email, profile_name) select u.email, p.name from users u inner join profiles p on u.id = p.user_id",
        ),
    ] {
        let response = app.handle_connector_request(&ConnectorRequest::new(
            request_id,
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        ));

        assert_eq!(response.status, ResponseStatus::Applied);
    }

    let select_request = ConnectorRequest::new(
        "req-select-flat-insert-select-join-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select email, profile_name from user_profile_flat".to_string(),
            },
        },
    );

    let select_response = app.handle_connector_request(&select_request);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = select_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], b"sam@example.com".to_vec());
    assert_eq!(result.rows[0][1], b"Sam".to_vec());
}

#[test]
fn select_alias_where_pk_returns_empty_when_runtime_index_is_empty() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-select-empty-index-fallback-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_request = ConnectorRequest::new(
        "req-create-table-empty-index-fallback-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                    .to_string(),
            },
        },
    );

    let create_response = app.handle_connector_request(&create_request);
    assert_eq!(create_response.status, ResponseStatus::Applied);

    let insert_request = ConnectorRequest::new(
        "req-insert-empty-index-fallback-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
            },
        },
    );

    let insert_response = app.handle_connector_request(&insert_request);
    assert_eq!(insert_response.status, ResponseStatus::Applied);

    // Simulate stale runtime index state: index registered but no entries.
    app.runtime_indexes = RuntimeIndexStore::new();
    let index_defs = {
        let catalog = app.catalogs.get("main").expect("main catalog should exist");
        let table = catalog.table("users").expect("users table should exist");
        table.indexes.values().cloned().collect::<Vec<_>>()
    };

    let table_stream_id = {
        let catalog = app.catalogs.get("main").expect("main catalog should exist");
        catalog
            .entity_wal_stream_id("users")
            .expect("users stream id should exist")
    };

    let primary_index_id = index_defs
        .iter()
        .find(|index| index.is_primary_key())
        .map(|index| index.index_id.0.clone())
        .expect("primary key index should exist");

    for index in index_defs {
        app.runtime_indexes
            .register_index_for_table(&table_stream_id, &index);
    }

    assert_eq!(
        app.runtime_indexes
            .cardinality_for_table(&table_stream_id, &primary_index_id),
        Some(0)
    );

    let query_request = ConnectorRequest::new(
        "req-select-empty-index-fallback-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select u.* from users u where u.id = '1'".to_string(),
            },
        },
    );

    let query_response = app.handle_connector_request(&query_request);
    assert_eq!(query_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = query_response.result else {
        panic!("expected query result");
    };

    assert_eq!(result.rows.len(), 0);
}

#[test]
fn describe_table_query_returns_schema_rows() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-describe-table-query-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_request = ConnectorRequest::new(
        "req-create-table-2",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255))"
                    .to_string(),
            },
        },
    );

    let create_response = app.handle_connector_request(&create_request);
    assert_eq!(create_response.status, ResponseStatus::Applied);

    let describe_request = ConnectorRequest::new(
        "req-describe-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "describe users".to_string(),
            },
        },
    );

    let describe_response = app.handle_connector_request(&describe_request);
    assert_eq!(describe_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = describe_response.result else {
        panic!("expected query result");
    };

    let column_names = result
        .columns
        .iter()
        .map(|field| field.field_name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        column_names,
        vec!["object_type", "field", "type", "null", "key", "default"]
    );
    assert_eq!(result.rows.len(), 2);

    let first_row = result
        .rows
        .first()
        .expect("describe should return first row");
    assert_eq!(String::from_utf8_lossy(&first_row[0]), "table");
    assert_eq!(String::from_utf8_lossy(&first_row[1]), "id");
    assert_eq!(String::from_utf8_lossy(&first_row[4]), "PRI");

    let second_row = result
        .rows
        .get(1)
        .expect("describe should return second row");

    assert_eq!(String::from_utf8_lossy(&second_row[1]), "email");
}

#[test]
fn describe_sql_backed_objects_returns_original_sql_and_object_type() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-describe-sql-object-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("users table should register");

    let trigger_sql =
        "create trigger trg_users_bi before insert on users for each row begin end";
    let procedure_sql = "create procedure p_sync() begin select 1; end";

    catalog
        .register_trigger("trg_users_bi", trigger_sql, vec!["users".to_string()])
        .expect("trigger should register");
    catalog
        .register_stored_procedure("p_sync", procedure_sql, Vec::new())
        .expect("procedure should register");

    app.catalogs.insert("main".to_string(), catalog);

    let describe_trigger = ConnectorRequest::new(
        "req-describe-trigger-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "describe trg_users_bi".to_string(),
            },
        },
    );

    let describe_procedure = ConnectorRequest::new(
        "req-describe-procedure-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "describe p_sync".to_string(),
            },
        },
    );

    let trigger_response = app.handle_connector_request(&describe_trigger);
    let procedure_response = app.handle_connector_request(&describe_procedure);

    assert_eq!(trigger_response.status, ResponseStatus::Applied);
    assert_eq!(procedure_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(trigger_result) = trigger_response.result else {
        panic!("expected query result for trigger describe");
    };
    let ConnectorResult::Query(procedure_result) = procedure_response.result else {
        panic!("expected query result for procedure describe");
    };

    assert_eq!(String::from_utf8_lossy(&trigger_result.rows[0][0]), "trigger");
    assert_eq!(String::from_utf8_lossy(&trigger_result.rows[0][1]), "trg_users_bi");
    assert_eq!(String::from_utf8_lossy(&trigger_result.rows[0][2]), trigger_sql);

    assert_eq!(
        String::from_utf8_lossy(&procedure_result.rows[0][0]),
        "stored_procedure"
    );
    assert_eq!(String::from_utf8_lossy(&procedure_result.rows[0][1]), "p_sync");
    assert_eq!(String::from_utf8_lossy(&procedure_result.rows[0][2]), procedure_sql);
}

#[test]
fn describe_table_marks_composite_unique_columns_as_mul() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-describe-composite-unique-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_request = ConnectorRequest::new(
        "req-create-places-composite-unique",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table places (uid bigint not null primary key, uni_id bigint not null, form varchar(3) not null default '', unique key uq_uni_id_form (uni_id, form))".to_string(),
            },
        },
    );

    let create_response = app.handle_connector_request(&create_request);
    assert_eq!(create_response.status, ResponseStatus::Applied);

    let describe_request = ConnectorRequest::new(
        "req-describe-places-composite-unique",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "describe places".to_string(),
            },
        },
    );

    let describe_response = app.handle_connector_request(&describe_request);
    assert_eq!(describe_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = describe_response.result else {
        panic!("expected query result");
    };

    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8_lossy(&cell).to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let uni_id_row = rows
        .iter()
        .find(|row| row.get(1).map(|field| field.as_str()) == Some("uni_id"))
        .expect("describe should include uni_id field");
    assert_eq!(uni_id_row[4], "MUL");

    let form_row = rows
        .iter()
        .find(|row| row.get(1).map(|field| field.as_str()) == Some("form"))
        .expect("describe should include form field");
    assert_eq!(form_row[4], "MUL");
}

#[test]
fn debug_procedure_returns_cached_artifact_details() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-debug-procedure-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let procedure_sql = "create procedure p_sync() begin select 1; end";

    catalog
        .register_stored_procedure("p_sync", procedure_sql, Vec::new())
        .expect("procedure should register");

    app.catalogs.insert("main".to_string(), catalog);

    let request = ConnectorRequest::new(
        "req-debug-procedure-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "debug procedure p_sync".to_string(),
            },
        },
    );

    let response = app.handle_connector_request(&request);
    assert_eq!(response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query result for debug procedure");
    };

    let attr_value = |attribute: &str| {
        result.rows.iter().find_map(|row| {
            if String::from_utf8_lossy(&row[0]) == attribute {
                Some(String::from_utf8_lossy(&row[1]).to_string())
            } else {
                None
            }
        })
    };

    assert_eq!(attr_value("entity_type").as_deref(), Some("stored_procedure"));
    assert_eq!(attr_value("entity_name").as_deref(), Some("p_sync"));
    assert_eq!(attr_value("cache_present").as_deref(), Some("true"));
    assert_eq!(attr_value("sql").as_deref(), Some(procedure_sql));

    assert!(
        attr_value("resources").is_some(),
        "debug output should include resources attribute"
    );
    assert!(
        attr_value("result_set_count").is_some(),
        "debug output should include result_set_count attribute"
    );
    assert!(
        attr_value("procedure_dependencies").is_some(),
        "debug output should include procedure_dependencies attribute"
    );
    assert!(
        attr_value("procedure_variables").is_some(),
        "debug output should include procedure_variables attribute"
    );
    assert!(
        attr_value("procedure_outputs").is_some(),
        "debug output should include procedure_outputs attribute"
    );
}

#[test]
fn drop_if_exists_for_sql_backed_objects_is_idempotent() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-drop-if-exists-sql-objects-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table("users", TableSchema::new(Vec::new()))
        .expect("users table should register");
    app.catalogs.insert("main".to_string(), catalog);

    let drop_missing_trigger = ConnectorRequest::new(
        "req-drop-missing-trigger-if-exists",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "drop trigger if exists trg_missing".to_string(),
            },
        },
    );

    let drop_missing_procedure = ConnectorRequest::new(
        "req-drop-missing-procedure-if-exists",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "drop procedure if exists p_missing".to_string(),
            },
        },
    );

    let trigger_response = app.handle_connector_request(&drop_missing_trigger);
    let procedure_response = app.handle_connector_request(&drop_missing_procedure);

    assert_eq!(trigger_response.status, ResponseStatus::Applied);
    assert_eq!(procedure_response.status, ResponseStatus::Applied);

    let ConnectorResult::Mutation(trigger_mutation) = trigger_response.result else {
        panic!("expected mutation result for trigger drop if exists");
    };
    let ConnectorResult::Mutation(procedure_mutation) = procedure_response.result else {
        panic!("expected mutation result for procedure drop if exists");
    };

    assert_eq!(trigger_mutation.affected_rows, 0);
    assert_eq!(procedure_mutation.affected_rows, 0);
}

#[test]
fn drop_table_query_removes_table() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-drop-table-query-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table("users", TableSchema::new(Vec::new()))
        .expect("users table should register");
    app.catalogs.insert("main".to_string(), catalog);

    let normalized_table_id = common::normalize_identifier!("users");
    let legacy_table_stream_file = app
        .node_data_dir
        .join(FileKind::Data.file_name(&normalized_table_id));
    let hashed_table_stream_file = app
        .node_data_dir
        .join(FileKind::Data.file_name(common::helpers::stable_id(&[&normalized_table_id])));

    std::fs::write(&legacy_table_stream_file, b"legacy stream")
        .expect("legacy table stream file should be created");
    std::fs::write(&hashed_table_stream_file, b"hashed stream")
        .expect("hashed table stream file should be created");

    let request = ConnectorRequest::new(
        "req-drop-table-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "drop table users".to_string(),
            },
        },
    );

    let response = app.handle_connector_request(&request);
    assert_eq!(response.status, ResponseStatus::Applied);

    let catalog = app.catalogs.get("main").expect("main catalog should exist");
    assert!(catalog.table("users").is_none());
    assert!(!legacy_table_stream_file.exists());
    assert!(!hashed_table_stream_file.exists());
}

#[test]
fn alter_table_query_updates_schema() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-alter-table-query-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    app.catalogs.insert("main".to_string(), catalog);

    let create_request = ConnectorRequest::new(
        "req-create-table-alter-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create table users (id bigint not null primary key, email varchar(255))"
                    .to_string(),
            },
        },
    );

    let create_response = app.handle_connector_request(&create_request);
    assert_eq!(create_response.status, ResponseStatus::Applied);

    let alter_request = ConnectorRequest::new(
        "req-alter-table-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "alter table users add column status varchar(20) not null default 'active', rename column email to login_email"
                    .to_string(),
            },
        },
    );

    let alter_response = app.handle_connector_request(&alter_request);
    assert_eq!(alter_response.status, ResponseStatus::Applied);

    let catalog = app.catalogs.get("main").expect("main catalog should exist");
    let schema = catalog
        .table_schema("users")
        .expect("users schema should exist");

    assert!(schema.field("status").is_some());
    assert!(schema.field("login_email").is_some());
    assert!(schema.field("email").is_none());
}

#[test]
fn schema_command_alter_table_update_field_uses_modify_column_path() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-schema-alter-modify-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let database_name = "alter_modify_db";
    let database_id = serverlib::DatabaseId::from_database_name(database_name)
        .expect("database id should normalize")
        .0;

    let create_db = ConnectorRequest::new(
        "req-create-db-modify",
        ConnectorCommand::CreateDatabase {
            database_name: database_name.to_string(),
        },
    );
    assert_eq!(
        app.handle_connector_request(&create_db).status,
        ResponseStatus::Applied
    );

    let create_table = ConnectorRequest::new(
        "req-create-table-modify",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: database_id.clone(),
                sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                    .to_string(),
            },
        },
    );
    assert_eq!(
        app.handle_connector_request(&create_table).status,
        ResponseStatus::Applied
    );

    let alter = ConnectorRequest::new(
        "req-schema-modify",
        ConnectorCommand::Schema {
            database_id: database_id.clone(),
            command: connector::SchemaCommand::AlterTable {
                change: connector::SchemaChangeRequest::new("users").update_field(
                    connector::FieldSpec::new("email", common::schema::FieldKind::StringFixed(512)),
                ),
            },
        },
    );

    let alter_response = app.handle_connector_request(&alter);
    assert_eq!(
        alter_response.status,
        ResponseStatus::Applied,
        "alter modify rejected: {:?}",
        alter_response.result
    );

    let catalog = app
        .catalogs()
        .get(&database_id)
        .expect("target catalog should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let email = schema.field("email").expect("email field should exist");

    assert_eq!(email.field_type, serverlib::FieldType::StringFixed(512));

}

#[test]
fn create_database_query_creates_catalog() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-create-db-query-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let request = ConnectorRequest::new(
        "req-create-db-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create database analytics".to_string(),
            },
        },
    );

    let response = app.handle_connector_request(&request);
    assert_eq!(response.status, ResponseStatus::Applied);
    assert!(!app.catalogs().is_empty());

}

#[test]
fn drop_database_query_removes_catalog() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-drop-db-query-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root.clone());

    let mut app = ServerApp::new(config).expect("server app should initialize");

    let catalog = DatabaseCatalog::create_empty_from_name("analytics")
        .expect("catalog should be created");

    catalog
        .save_in_directory(&app.node_data_dir)
        .expect("catalog should be persisted");

    app.catalogs
        .insert(catalog.database_id.0.clone(), catalog.clone());

    let catalog_file = app.node_data_dir.join(catalog.file_name());
    assert!(catalog_file.exists());

    let request = ConnectorRequest::new(
        "req-drop-db-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "drop database analytics".to_string(),
            },
        },
    );

    let response = app.handle_connector_request(&request);

    assert_eq!(response.status, ResponseStatus::Applied);
    assert!(app.catalogs().get("analytics").is_none());
    assert!(!catalog_file.exists());

}

#[test]
fn create_and_drop_sql_backed_objects_are_wired() {

    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-sql-backed-objects-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("users table should register");

    app.catalogs.insert("main".to_string(), catalog);

    let create_view = ConnectorRequest::new(
        "req-create-view",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create view users_v as select * from users".to_string(),
            },
        },
    );

    let create_trigger = ConnectorRequest::new(
        "req-create-trigger",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql:
                    "create trigger trg_users_bi before insert on users for each row begin end"
                        .to_string(),
            },
        },
    );

    let create_procedure = ConnectorRequest::new(
        "req-create-procedure",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create procedure p_sync() begin end".to_string(),
            },
        },
    );

    let create_view_response = app.handle_connector_request(&create_view);
    let create_trigger_response = app.handle_connector_request(&create_trigger);
    let create_procedure_response = app.handle_connector_request(&create_procedure);

    assert_eq!(create_view_response.status, ResponseStatus::Applied);
    assert_eq!(create_trigger_response.status, ResponseStatus::Applied);
    assert_eq!(create_procedure_response.status, ResponseStatus::Applied);

    let catalog = app.catalogs.get("main").expect("main catalog should exist");

    let view_snapshot = app
        .node_data_dir
        .join(FileKind::Entity.file_name(common::helpers::stable_id(&["users_v"])));
    
    let trigger_snapshot = app
        .node_data_dir
        .join(FileKind::Entity.file_name(common::helpers::stable_id(&["trg_users_bi"])));
    
    let procedure_snapshot = app
        .node_data_dir
        .join(FileKind::Entity.file_name(common::helpers::stable_id(&["p_sync"])));

    let view_wal = app
        .node_data_dir
        .join(FileKind::Data.file_name(
            common::helpers::stable_id(&[
                &catalog
                    .entity_wal_stream_id("users_v")
                    .expect("view WAL stream should exist"),
            ]),
        ));

    let trigger_wal = app
        .node_data_dir
        .join(FileKind::Data.file_name(
            common::helpers::stable_id(&[
                &catalog
                    .entity_wal_stream_id("trg_users_bi")
                    .expect("trigger WAL stream should exist"),
            ]),
        ));
        
    let procedure_wal = app
        .node_data_dir
        .join(FileKind::Data.file_name(
            common::helpers::stable_id(&[
                &catalog
                    .entity_wal_stream_id("p_sync")
                    .expect("procedure WAL stream should exist"),
            ]),
        ));

    assert!(view_snapshot.exists());
    assert!(trigger_snapshot.exists());
    assert!(procedure_snapshot.exists());
    assert!(view_wal.exists());
    assert!(trigger_wal.exists());
    assert!(procedure_wal.exists());

    assert!(catalog.view("users_v").is_some());
    assert!(catalog.trigger("trg_users_bi").is_some());
    assert!(catalog.stored_procedure("p_sync").is_some());

    let drop_view = ConnectorRequest::new(
        "req-drop-view",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "drop view users_v".to_string(),
            },
        },
    );

    let drop_trigger = ConnectorRequest::new(
        "req-drop-trigger",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "drop trigger trg_users_bi on users".to_string(),
            },
        },
    );

    let drop_procedure = ConnectorRequest::new(
        "req-drop-procedure",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "drop procedure p_sync".to_string(),
            },
        },
    );

    let drop_view_response = app.handle_connector_request(&drop_view);
    let drop_trigger_response = app.handle_connector_request(&drop_trigger);
    let drop_procedure_response = app.handle_connector_request(&drop_procedure);

    assert_eq!(drop_view_response.status, ResponseStatus::Applied);
    assert_eq!(drop_trigger_response.status, ResponseStatus::Applied);
    assert_eq!(drop_procedure_response.status, ResponseStatus::Applied);

    let catalog = app.catalogs.get("main").expect("main catalog should exist");
    assert!(catalog.view("users_v").is_none());
    assert!(catalog.trigger("trg_users_bi").is_none());
    assert!(catalog.stored_procedure("p_sync").is_none());
    assert!(!view_snapshot.exists());
    assert!(!trigger_snapshot.exists());
    assert!(!procedure_snapshot.exists());
    assert!(!view_wal.exists());
    assert!(!trigger_wal.exists());
    assert!(!procedure_wal.exists());
}

#[test]
fn select_from_view_uses_scoped_materialization_without_catalog_residue() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-select-view-scoped-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![
                FieldDef {
                    seqno: 1,
                    field_name: "id".to_string(),
                    field_type: FieldType::Int(64),
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 2,
                    field_name: "email".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
            ]),
        )
        .expect("users table should register");

    app.catalogs.insert("main".to_string(), catalog);

    let insert_row = ConnectorRequest::new(
        "req-insert-users",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
            },
        },
    );

    let create_view = ConnectorRequest::new(
        "req-create-view-select",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create view users_v as select id, email from users".to_string(),
            },
        },
    );

    let select_view = ConnectorRequest::new(
        "req-select-view",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id, email from users_v where id = 1".to_string(),
            },
        },
    );

    let insert_response = app.handle_connector_request(&insert_row);
    let create_view_response = app.handle_connector_request(&create_view);
    let select_response = app.handle_connector_request(&select_view);

    assert_eq!(insert_response.status, ResponseStatus::Applied);
    assert_eq!(create_view_response.status, ResponseStatus::Applied);
    assert_eq!(select_response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(query_result) = select_response.result else {
        panic!("select response should be a query result");
    };

    assert_eq!(query_result.rows.len(), 1);
    assert_eq!(query_result.rows[0][0], b"1".to_vec());
    assert_eq!(query_result.rows[0][1], b"sam@example.com".to_vec());

    let catalog = app.catalogs.get("main").expect("main catalog should exist");
    assert!(
        catalog
            .table_ids()
            .iter()
            .all(|table_id| !table_id.starts_with("__scoped_view_"))
    );
}

#[test]
fn alter_view_updates_stored_definition() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-alter-view-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");
    let database_id = serverlib::DatabaseId::from_database_name("main")
        .expect("database id should normalize")
        .0;

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-db-alter-view",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-table-alter-view",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email text not null)"
                        .to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-view-before-alter",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create view users_v as select id from users".to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    let alter_view_response = app.handle_connector_request(&ConnectorRequest::new(
        "req-alter-view",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "alter view users_v as select email from users".to_string(),
            },
        },
    ));

    assert_eq!(alter_view_response.status, ResponseStatus::Applied);

    let catalog = app
        .catalogs
        .get(&database_id)
        .expect("main catalog should exist");
    let view = catalog.view("users_v").expect("view should exist");

    assert!(view.sql.to_ascii_lowercase().contains("select email from users"));
}

#[test]
fn create_and_drop_index_via_query_dispatch() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-index-ops-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");
    let database_id = serverlib::DatabaseId::from_database_name("main")
        .expect("database id should normalize")
        .0;

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-db-index",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-table-index",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email text not null)"
                        .to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    let create_index_response = app.handle_connector_request(&ConnectorRequest::new(
        "req-create-index",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "create index idx_users_email on users(email)".to_string(),
            },
        },
    ));

    assert_eq!(
        create_index_response.status,
        ResponseStatus::Applied,
        "create index rejected: {:?}",
        create_index_response.result
    );

    let catalog = app
        .catalogs
        .get(&database_id)
        .expect("main catalog should exist");
    let users = catalog.table("users").expect("users table should exist");
    assert!(users.indexes.contains_key("idx_users_email"));

    let users_stream_id = table_stream_id(&app, &database_id, "users");
    assert!(app
        .runtime_indexes
        .index_for_table(&users_stream_id, "idx_users_email")
        .is_some());

    let drop_index_response = app.handle_connector_request(&ConnectorRequest::new(
        "req-drop-index",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "drop index idx_users_email".to_string(),
            },
        },
    ));

    assert_eq!(
        drop_index_response.status,
        ResponseStatus::Applied,
        "drop index rejected: {:?}",
        drop_index_response.result
    );

    let catalog = app
        .catalogs
        .get(&database_id)
        .expect("main catalog should exist");
    let users = catalog.table("users").expect("users table should exist");
    assert!(!users.indexes.contains_key("idx_users_email"));
    assert!(app
        .runtime_indexes
        .index_for_table(&users_stream_id, "idx_users_email")
        .is_none());
}

#[test]
fn drop_table_removes_associated_runtime_indexes() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-drop-table-runtime-indexes-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");
    let database_id = serverlib::DatabaseId::from_database_name("main")
        .expect("database id should normalize")
        .0;

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-db-drop-table-indexes",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-table-drop-table-indexes",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email text not null)"
                        .to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-index-drop-table-indexes",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create index idx_users_email on users(email)".to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-insert-drop-table-indexes",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "insert into users (id, email) values (1, 'a@x.com')".to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    let users_stream_id = table_stream_id(&app, &database_id, "users");

    let index_ids = app
        .catalogs
        .get(&database_id)
        .and_then(|catalog| catalog.table("users"))
        .map(|table| table.indexes.keys().cloned().collect::<Vec<_>>())
        .expect("users table and indexes should exist");

    let tracked_index_ids = index_ids
        .into_iter()
        .filter(|index_id| {
            app.runtime_indexes
                .index_for_table(&users_stream_id, index_id)
                .is_some()
        })
        .collect::<Vec<_>>();

    assert!(!tracked_index_ids.is_empty());

    let drop_table_response = app.handle_connector_request(&ConnectorRequest::new(
        "req-drop-table-indexes",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "drop table users".to_string(),
            },
        },
    ));

    assert_eq!(drop_table_response.status, ResponseStatus::Applied);

    for index_id in &tracked_index_ids {
        assert!(app
            .runtime_indexes
            .index_for_table(&users_stream_id, index_id)
            .is_none());
    }
}

#[test]
fn truncate_table_compacts_wal_and_clears_live_rows() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-truncate-table-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");
    let database_id = serverlib::DatabaseId::from_database_name("main")
        .expect("database id should normalize")
        .0;

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-db-truncate",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-table-truncate",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email text not null)"
                        .to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-insert-user-1-truncate",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "insert into users (id, email) values (1, 'a@x.com')".to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-insert-user-2-truncate",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "insert into users (id, email) values (2, 'b@x.com')".to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    let users_stream_id = table_stream_id(&app, &database_id, "users");
    let before = app.wal.since(&users_stream_id, None);
    assert!(before.iter().any(|record| record.kind == TransactionKind::Insert));

    let truncate_response = app.handle_connector_request(&ConnectorRequest::new(
        "req-truncate-users",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "truncate table users".to_string(),
            },
        },
    ));

    assert_eq!(truncate_response.status, ResponseStatus::Applied);

    let select_after = app.handle_connector_request(&ConnectorRequest::new(
        "req-select-after-truncate",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select id, email from users".to_string(),
            },
        },
    ));

    let ConnectorResult::Query(result) = select_after.result else {
        panic!("expected query result after truncate");
    };

    assert_eq!(result.rows.len(), 0);

    let after = app.wal.since(&users_stream_id, None);
    assert!(after.iter().any(|record| record.kind == TransactionKind::Truncate));
    assert!(after.iter().all(|record| {
        !matches!(
            record.kind,
            TransactionKind::Insert | TransactionKind::Update | TransactionKind::Delete
        )
    }));
}

#[test]
fn show_indexes_reports_user_defined_index_after_restart() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-show-indexes-restart-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root.clone());
    let mut app = ServerApp::new(config).expect("server app should initialize");

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-db-show-indexes",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-table-show-indexes",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email text not null)"
                        .to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    assert_eq!(
        app.handle_connector_request(&ConnectorRequest::new(
            "req-create-index-show-indexes",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create index idx_users_email on users(email)".to_string(),
                },
            },
        ))
        .status,
        ResponseStatus::Applied
    );

    drop(app);

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should reinitialize");
    app.bootstrap().expect("bootstrap should replay index lifecycle");

    let response = app.handle_connector_request(&ConnectorRequest::new(
        "req-show-indexes-after-restart",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show indexes from users".to_string(),
            },
        },
    ));

    assert_eq!(response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query result");
    };

    let names = result
        .rows
        .iter()
        .map(|row| String::from_utf8(row[1].clone()).expect("index name should be utf8"))
        .collect::<Vec<_>>();

    assert!(
        names.iter().any(|name| name == "idx_users_email"),
        "show indexes should include user-defined index after restart"
    );
}

#[test]
fn connector_client_path_can_query_show_tables_without_simulation() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-client-path-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("users table should register");

    catalog
        .register_table(
            "accounts",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("accounts table should register");

    app.catalogs.insert("main".to_string(), catalog);

    let transport = InProcessServerTransport {
        app: RefCell::new(app),
    };
    let client = ConnectorClient::new(transport);

    let request = ConnectorRequest::new(
        "req-client-show-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "show tables".to_string(),
            },
        },
    );

    let response = client
        .execute(&request)
        .expect("connector client should receive applied response");

    assert_eq!(response.request_id, "req-client-show-1");
    assert_eq!(response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query result");
    };

    let row_values = result
        .rows
        .iter()
        .map(|row| String::from_utf8_lossy(&row[0]).to_string())
        .collect::<Vec<_>>();

    assert_eq!(row_values, vec!["accounts", "users"]);
}

#[test]
fn connector_client_path_can_show_tables_for_explicit_database_token() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-client-show-explicit-db-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut main_catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("main catalog should be created");
    main_catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(64),
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("users table should register");

    let locations_catalog =
        DatabaseCatalog::create_empty_from_name("locations").expect("locations catalog should be created");

    app.catalogs.insert("main".to_string(), main_catalog);
    app.catalogs.insert("locations".to_string(), locations_catalog);

    let transport = InProcessServerTransport {
        app: RefCell::new(app),
    };
    let client = ConnectorClient::new(transport);

    let request = ConnectorRequest::new(
        "req-client-show-explicit-db-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "locations".to_string(),
                sql: "show main.tables".to_string(),
            },
        },
    );

    let response = client
        .execute(&request)
        .expect("connector client should receive applied response");

    assert_eq!(response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query result");
    };

    let row_values = result
        .rows
        .iter()
        .map(|row| String::from_utf8_lossy(&row[0]).to_string())
        .collect::<Vec<_>>();

    assert_eq!(row_values, vec!["users"]);
}

#[test]
fn connector_client_path_can_query_select_without_simulation() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-client-select-path-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![
                FieldDef {
                    seqno: 1,
                    field_name: "id".to_string(),
                    field_type: FieldType::Int(64),
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 2,
                    field_name: "email".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::Indexed,
                    default_value: None,
                    metadata: None,
                },
            ]),
        )
        .expect("users table should register");

    app.catalogs.insert("main".to_string(), catalog);

    let transport = InProcessServerTransport {
        app: RefCell::new(app),
    };

    let client = ConnectorClient::new(transport);

    let request = ConnectorRequest::new(
        "req-client-select-1",
        ConnectorCommand::Query {
            query: connector::DataQuery {
                database_id: "main".to_string(),
                sql: "select * from users".to_string(),
            },
        },
    );

    let response = client
        .execute(&request)
        .expect("connector client should receive applied response");

    assert_eq!(response.request_id, "req-client-select-1");
    assert_eq!(response.status, ResponseStatus::Applied);

    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query result");
    };

    let column_names = result
        .columns
        .iter()
        .map(|field| field.field_name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(column_names, vec!["id", "email"]);
    assert!(result.rows.is_empty());
}

#[test]
fn query_path_stress_respects_timing_thresholds() {
    let unique_suffix = common::epoch_nanos!();

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-server-query-stress-{}-{}",
        std::process::id(),
        unique_suffix
    ));

    let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
    let mut app = ServerApp::new(config).expect("server app should initialize");

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![
                FieldDef {
                    seqno: 1,
                    field_name: "id".to_string(),
                    field_type: FieldType::Int(64),
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 2,
                    field_name: "email".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::Indexed,
                    default_value: None,
                    metadata: None,
                },
            ]),
        )
        .expect("users table should register");

    app.catalogs.insert("main".to_string(), catalog);

    let thresholds = QueryTimingThresholds::from_env();
    let mut durations_ms = Vec::with_capacity(thresholds.stress_iterations);

    let batch_start = std::time::Instant::now();

    for idx in 0..thresholds.stress_iterations {
        let sql = if idx % 2 == 0 {
            "select * from users"
        } else {
            "show tables"
        };

        let request = ConnectorRequest::new(
            format!("stress-req-{idx}"),
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: sql.to_string(),
                },
            },
        );

        let start = std::time::Instant::now();
        let response = app.handle_connector_request(&request);
        let elapsed_ms = start.elapsed().as_millis();

        assert_eq!(response.status, ResponseStatus::Applied);
        durations_ms.push(elapsed_ms);
    }

    let batch_elapsed_ms = batch_start.elapsed().as_millis();
    durations_ms.sort_unstable();

    let p95 = percentile(&durations_ms, 95);
    let p99 = percentile(&durations_ms, 99);

    assert!(
        p95 <= thresholds.p95_max_ms,
        "p95 latency exceeded threshold: p95={}ms threshold={}ms",
        p95,
        thresholds.p95_max_ms
    );
    assert!(
        p99 <= thresholds.p99_max_ms,
        "p99 latency exceeded threshold: p99={}ms threshold={}ms",
        p99,
        thresholds.p99_max_ms
    );
    assert!(
        batch_elapsed_ms <= thresholds.batch_max_ms,
        "batch duration exceeded threshold: batch={}ms threshold={}ms",
        batch_elapsed_ms,
        thresholds.batch_max_ms
    );
}

#[expect(clippy::manual_div_ceil, reason = "clarity of rank calculation")]
fn percentile(sorted_values: &[u128], pct: usize) -> u128 {

    if sorted_values.is_empty() {
        return 0;
    }

    let rank = ((pct * sorted_values.len()) + 99) / 100;
    let idx = rank.saturating_sub(1).min(sorted_values.len() - 1);
    sorted_values[idx]
}
