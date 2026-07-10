use super::*;

#[test]
fn normalize_bootstrap_addr_accepts_host_port() {
    assert_eq!(
        normalize_bootstrap_addr("127.0.0.1:9400"),
        Some("/ip4/127.0.0.1/tcp/9400".to_string())
    );
}

#[test]
fn normalize_bootstrap_addr_accepts_dns_host() {
    assert_eq!(
        normalize_bootstrap_addr("node.local:9400"),
        Some("/dns/node.local/tcp/9400".to_string())
    );
}

#[test]
fn resolve_database_uses_current_database_when_present() {
    let db = resolve_database_for_sql(Some("main"), "select * from t")
        .expect("database resolution should succeed");
    assert_eq!(db, "main");
}

#[test]
fn resolve_database_allows_show_databases_without_database() {
    let db = resolve_database_for_sql(None, "show databases")
        .expect("global query should use fallback database");
    assert_eq!(db, DEFAULT_DATABASE);
}

#[test]
fn resolve_database_rejects_non_global_sql_without_database() {
    let error = resolve_database_for_sql(None, "select * from users")
        .expect_err("query should fail when no active database exists");
    assert!(matches!(error, ClientError::Config(_)));
}

#[test]
fn parse_options_from_cli_args_supports_servers_list() {
    let args = vec![
        "servers=127.0.0.1:9400,node.local:9401".to_string(),
        "tls=required".to_string(),
        "database=main".to_string(),
    ];

    let options =
        ClientOptions::from_cli_args(&args).expect("options parsing should succeed");

    assert_eq!(options.servers.len(), 2);
    assert_eq!(options.tls_mode, TlsMode::Required);
    assert_eq!(options.database.as_deref(), Some("main"));
}
