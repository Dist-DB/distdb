use super::*;
use peerlib::ConnectorTlsConfig;

#[test]
fn execute_import_file_requires_active_database_selection() {
    let mut session = ConsoleSession::new(
        vec!["127.0.0.1:4001".to_string()],
        ConnectorTlsConfig::default(),
    )
    .expect("session should initialize");

    let err = execute_import_file(&mut session, "dummy.sql")
        .expect_err("import should fail when no database is selected");

    assert!(
        err.to_string()
            .contains("no active database selected; run `use <database>;` first")
    );
}
