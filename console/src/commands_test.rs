use super::*;
use crate::{ConsoleCommand, TEMP_CONNECT_USER};

#[test]
fn parse_command_requires_semicolon() {
	assert!(matches!(
		parse_console_command("show peers", TEMP_CONNECT_USER),
		Ok(None)
	));
	assert!(matches!(
		parse_console_command("show peers;", TEMP_CONNECT_USER),
		Ok(Some(ConsoleCommand::ShowPeers))
	));
}

#[test]
fn parse_delimiter_directive_updates_delimiter() {
	assert!(matches!(
		parse_console_command_with_delimiter("delimiter //", TEMP_CONNECT_USER, ";"),
		Ok(Some(ConsoleCommand::SetDelimiter(delimiter))) if delimiter == "//"
	));
}

#[test]
fn parse_custom_delimiter_executes_sql_with_suffix() {
	assert!(matches!(
		parse_console_command_with_delimiter("select 1//", TEMP_CONNECT_USER, "//"),
		Ok(Some(ConsoleCommand::Sql(sql))) if sql == "select 1"
	));

	assert!(matches!(
		parse_console_command_with_delimiter("select 1;", TEMP_CONNECT_USER, "//"),
		Ok(None)
	));
}

#[test]
fn parse_delimiter_requires_token() {
	assert!(parse_console_command_with_delimiter("delimiter", TEMP_CONNECT_USER, ";").is_err());
	assert!(parse_console_command_with_delimiter("delimiter ;", TEMP_CONNECT_USER, ";").is_ok());
	assert!(matches!(
		parse_console_command_with_delimiter("delimiter //;", TEMP_CONNECT_USER, ";"),
		Ok(Some(ConsoleCommand::SetDelimiter(delimiter))) if delimiter == "//"
	));
}

#[test]
fn parse_command_recognises_keywords() {
	assert!(matches!(
		parse_console_command("help;", TEMP_CONNECT_USER),
		Ok(Some(ConsoleCommand::Help))
	));
	assert!(matches!(
		parse_console_command("exit;", TEMP_CONNECT_USER),
		Ok(Some(ConsoleCommand::Exit))
	));
	assert!(matches!(
		parse_console_command("disconnect;", TEMP_CONNECT_USER),
		Ok(Some(ConsoleCommand::Disconnect))
	));
	assert!(matches!(
		parse_console_command("show p2p;", TEMP_CONNECT_USER),
		Ok(Some(ConsoleCommand::ShowP2p))
	));
	assert!(matches!(
		parse_console_command("show log;", TEMP_CONNECT_USER),
		Ok(Some(ConsoleCommand::ShowLog))
	));
}

#[test]
fn parse_connect_requires_user_at_peer() {
	assert!(parse_console_command("connect server-node-01;", TEMP_CONNECT_USER).is_err());
	assert!(parse_console_command("connect @server-node-01;", TEMP_CONNECT_USER).is_err());
	assert!(
		parse_console_command("connect other@server-node-01;", TEMP_CONNECT_USER).is_err()
	);
	assert!(matches!(
		parse_console_command("connect root@server-node-01;", TEMP_CONNECT_USER),
		Ok(Some(ConsoleCommand::ConnectPeer { .. }))
	));
}

#[test]
fn parse_use_database_extracts_name() {
	match parse_console_command("use mydb;", TEMP_CONNECT_USER) {
		Ok(Some(ConsoleCommand::UseDatabase(name))) => assert_eq!(name, "mydb"),
		other => panic!("unexpected: {:?}", other.is_ok()),
	}
}

#[test]
fn parse_import_extracts_file_name() {
	match parse_console_command("import data/locations.sql;", TEMP_CONNECT_USER) {
		Ok(Some(ConsoleCommand::ImportFile(file_name))) => {
			assert_eq!(file_name, "data/locations.sql")
		}
		other => panic!("unexpected: {:?}", other.is_ok()),
	}
}

#[test]
fn parse_import_requires_file_name() {
	assert!(parse_console_command("import ;", TEMP_CONNECT_USER).is_err());
}

#[test]
fn parse_sql_falls_through() {
	assert!(matches!(
		parse_console_command("select 1;", TEMP_CONNECT_USER),
		Ok(Some(ConsoleCommand::Sql(_)))
	));
}

#[test]
fn parse_rejects_new_command_when_previous_missing_semicolon() {
	let result = parse_console_command(
		"show peers\nconnect root@server-node-01;",
		TEMP_CONNECT_USER,
	);
	assert!(matches!(result, Err(message) if message.contains("missing ';'")));
}

#[test]
fn ctrl_d_on_empty_does_not_abort() {
	assert!(matches!(parse_console_command("", TEMP_CONNECT_USER), Ok(None)));
}

#[test]
fn extract_password_token_detects_password_command() {
	assert_eq!(extract_password_token_input("password secret;"), Some("secret"));
	assert_eq!(extract_password_token_input("PASSWORD secret"), Some("secret"));
	assert_eq!(
		extract_password_token_input("SET PASSWORD FOR 'root' = PASSWORD('secret');"),
		None
	);
	assert_eq!(
		extract_password_token_input("SET PASSWORD FOR root = PASSWORD(secret);"),
		None
	);
	assert_eq!(extract_password_token_input("select 1"), None);
}

#[test]
fn auth_password_input_detects_set_password_for_syntax() {
	assert_eq!(
		auth_password_input("SET PASSWORD FOR 'root' = PASSWORD('secret');"),
		Some("secret")
	);
	assert_eq!(
		auth_password_input("SET PASSWORD FOR root = PASSWORD(secret);"),
		None
	);
}

#[test]
fn resolve_database_for_auth_without_selection_uses_fallback() {
	let database = resolve_database_for_sql(None, true, "password secret;", "main")
		.expect("auth should allow fallback");
	assert_eq!(database, "main");
}

#[test]
fn resolve_database_without_selection_rejects_non_auth() {
	let result = resolve_database_for_sql(None, false, "select 1;", "main");
	assert!(matches!(
		result,
		Err("no active database selected; run `use <database>;` first")
	));
}

#[test]
fn resolve_database_without_selection_allows_qualified_select() {
	let database = resolve_database_for_sql(
		None,
		false,
		"select * from locations.places where display_name='Amsterdam';",
		"main",
	)
	.expect("qualified select should resolve database");

	assert_eq!(database, "locations");
}

#[test]
fn resolve_database_without_selection_allows_qualified_show_tables() {
	let database = resolve_database_for_sql(None, false, "show locations.tables;", "main")
		.expect("qualified show tables should resolve database");

	assert_eq!(database, "locations");
}

#[test]
fn resolve_database_without_selection_allows_show_databases() {
	let database = resolve_database_for_sql(None, false, "show databases;", "main")
		.expect("show databases should not require explicit selection");
	assert_eq!(database, "main");
}

#[test]
fn resolve_database_without_selection_allows_status_commands() {
	let entities_db = resolve_database_for_sql(None, false, "show entities;", "main")
		.expect("show entities should not require explicit selection");
	assert_eq!(entities_db, "main");

	let variables_db = resolve_database_for_sql(None, false, "show variables;", "main")
		.expect("show variables should not require explicit selection");
	assert_eq!(variables_db, "main");

	let variable_db =
		resolve_database_for_sql(None, false, "show variable cte.timeout_ms;", "main")
			.expect("show variable should not require explicit selection");
	assert_eq!(variable_db, "main");

	let bootstrap_db =
		resolve_database_for_sql(None, false, "show bootstrap status;", "main")
			.expect("show bootstrap status should not require explicit selection");
	assert_eq!(bootstrap_db, "main");

	let peers_db = resolve_database_for_sql(None, false, "show server peers;", "main")
		.expect("show server peers should not require explicit selection");
	assert_eq!(peers_db, "main");

	let workers_db =
		resolve_database_for_sql(None, false, "show catalog workers;", "main")
			.expect("show catalog workers should not require explicit selection");
	assert_eq!(workers_db, "main");
}
