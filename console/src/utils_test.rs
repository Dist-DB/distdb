use super::*;
use connector::{ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, DataQuery};
use std::sync::{Mutex, OnceLock};

const TIMEOUT_ENV: &str = "DISTDB_CONSOLE_SHOW_PEERS_TIMEOUT_SECS";
const SQL_TIMEOUT_ENV: &str = "DISTDB_CONSOLE_SQL_TIMEOUT_SECS";

fn timeout_env_guard() -> &'static Mutex<()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(()))
}

#[test]
fn show_peers_timeout_uses_default_when_env_missing() {
    let _lock = timeout_env_guard()
        .lock()
        .expect("timeout env test mutex should lock");
    unsafe {
        std::env::remove_var(TIMEOUT_ENV);
    }

    assert_eq!(show_peers_request_timeout_secs(), 1);
}

#[test]
fn show_peers_timeout_clamps_env_value() {
    let _lock = timeout_env_guard()
        .lock()
        .expect("timeout env test mutex should lock");

    unsafe {
        std::env::set_var(TIMEOUT_ENV, "0");
    }
    assert_eq!(show_peers_request_timeout_secs(), 1);

    unsafe {
        std::env::set_var(TIMEOUT_ENV, "200");
    }
    assert_eq!(show_peers_request_timeout_secs(), 30);

    unsafe {
        std::env::remove_var(TIMEOUT_ENV);
    }
}

#[test]
fn sql_timeout_uses_default_when_env_missing() {
    let _lock = timeout_env_guard()
        .lock()
        .expect("timeout env test mutex should lock");
    unsafe {
        std::env::remove_var(SQL_TIMEOUT_ENV);
    }

    assert_eq!(sql_request_timeout_secs(), 120);
}

#[test]
fn sql_timeout_clamps_env_value() {
    let _lock = timeout_env_guard()
        .lock()
        .expect("timeout env test mutex should lock");

    unsafe {
        std::env::set_var(SQL_TIMEOUT_ENV, "10");
    }
    assert_eq!(sql_request_timeout_secs(), 30);

    unsafe {
        std::env::set_var(SQL_TIMEOUT_ENV, "5000");
    }
    assert_eq!(sql_request_timeout_secs(), 3600);

    unsafe {
        std::env::remove_var(SQL_TIMEOUT_ENV);
    }
}

#[test]
fn validate_database_probe_accepts_non_rejected_status() {
    let response = ConnectorResponse::applied("req-1", ConnectorResult::Error("ignored".to_string()));

    assert!(validate_use_database_probe_response("main", &response).is_ok());
}

#[test]
fn validate_database_probe_includes_rejection_message() {
    let response = ConnectorResponse::rejected("req-2", "database not found");

    let error = validate_use_database_probe_response("analytics", &response)
        .expect_err("rejected probe must return an error");

    assert!(error.contains("analytics"));
    assert!(error.contains("database not found"));
}

#[test]
fn query_database_id_extension_reads_query_target() {
    let request = ConnectorRequest::new(
        "req-3",
        ConnectorCommand::Query {
            query: DataQuery {
                database_id: "tenant1".to_string(),
                sql: "show tables".to_string(),
            },
        },
    );

    assert_eq!(request.query_database_id(), "tenant1");
}
