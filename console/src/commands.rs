use crate::ConsoleCommand;

pub fn extract_password_token_input(sql: &str) -> Option<&str> {

    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mut parts = trimmed.split_whitespace();
    let command = parts.next()?;
    let password = parts.next()?;

    if command.eq_ignore_ascii_case("password") {
        return Some(password);
    }

    None

}

pub fn resolve_database_for_sql(
    current_database: Option<&str>,
    is_auth_request: bool,
    sql: &str,
    auth_fallback_database: &str,
) -> Result<String, &'static str> {

    if let Some(database) = current_database {
        return Ok(database.to_string());
    }

    if is_auth_request || is_global_sql_without_database(sql) {
        return Ok(auth_fallback_database.to_string());
    }

    if let Some(database_name) = database_from_qualified_select_sql(sql) {
        return Ok(database_name.to_string());
    }

    Err("no active database selected; run `use <database>;` first")

}

pub fn parse_console_command(
    input: &str,
    temp_connect_user: &str,
) -> Result<Option<ConsoleCommand>, String> {

    parse_console_command_with_delimiter(input, temp_connect_user, ";")

}

pub fn parse_console_command_with_delimiter(
    input: &str,
    temp_connect_user: &str,
    delimiter: &str,
) -> Result<Option<ConsoleCommand>, String> {

    if delimiter.is_empty() {
        return Err("active delimiter cannot be empty".to_string());
    }

    let trimmed = input.trim();

    if trimmed.is_empty() {
        return Ok(None);
    }

    if let Some(next_delimiter) = parse_delimiter_directive(trimmed, delimiter)? {
        return Ok(Some(ConsoleCommand::SetDelimiter(next_delimiter)));
    }

    if !trimmed.ends_with(delimiter) {
        return Ok(None);
    }

    let Some(command_text) = trimmed.strip_suffix(delimiter) else {
        return Ok(None);
    };

    let command_text = command_text.trim();
    if command_text.contains('\n') {
        let lines: Vec<&str> = command_text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect();

        if lines.len() > 1
            && lines[..lines.len() - 1]
                .iter()
                .any(|line| is_console_command_fragment(line))
        {
            return Err(
                "previous console command is missing ';' before starting a new command"
                    .to_string(),
            );
        }
    }

    let lowered = command_text.to_lowercase();

    if lowered == "help" || lowered == ".help" {
        return Ok(Some(ConsoleCommand::Help));
    }

    if lowered == "exit" || lowered == "quit" || lowered == "\\q" {
        return Ok(Some(ConsoleCommand::Exit));
    }

    if lowered == "show p2p" {
        return Ok(Some(ConsoleCommand::ShowP2p));
    }

    if lowered == "show log" {
        return Ok(Some(ConsoleCommand::ShowLog));
    }

    if lowered == "show peers" {
        return Ok(Some(ConsoleCommand::ShowPeers));
    }

    if lowered == "disconnect" {
        return Ok(Some(ConsoleCommand::Disconnect));
    }

    if let Some(database_name) = command_text.strip_prefix("use ") {
        let database_name = database_name.trim();
        if database_name.is_empty() {
            return Err("use requires a database name".to_string());
        }
        return Ok(Some(ConsoleCommand::UseDatabase(database_name.to_string())));
    }

    let import_prefix = "import";

    if lowered.starts_with(import_prefix)
        && command_text
            .chars()
            .nth(import_prefix.len())
            .map(|ch| ch.is_whitespace())
            .unwrap_or(true)
    {
        let file_name = command_text[import_prefix.len()..].trim();

        if file_name.is_empty() {
            return Err("import requires a file name".to_string());
        }

        let file_name = file_name
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();

        if file_name.is_empty() {
            return Err("import requires a file name".to_string());
        }

        return Ok(Some(ConsoleCommand::ImportFile(file_name)));
    }

    if let Some(target) = command_text.strip_prefix("connect ") {

        let target = target.trim();

        if target.is_empty() {
            return Err("connect requires a peer id".to_string());
        }

        let (user, peer_id) = parse_connect_target(target, temp_connect_user)?;

        return Ok(Some(ConsoleCommand::ConnectPeer { user, peer_id }));

    }

    let sql = command_text.trim();
    if sql.is_empty() {
        return Ok(None);
    }

    Ok(Some(ConsoleCommand::Sql(sql.to_string())))

}

pub fn print_help() {
    println!("distdb console commands:");
    println!("  help | .help              show this message");
    println!("  exit | quit | \\q          leave console");
    println!("  use <database>;           switch active database");
    println!("  show p2p;                 display connector/server p2p stack status");
    println!("  show log;                 display in-console command/response log");
    println!("  show peers;               list discovered p2p peers (* = active)");
    println!("  connect <user@peer-id>;   switch session to a discovered peer");
    println!("  disconnect;               close the active peer session connection");
    println!("  import <file.sql>;        stream SQL file into active database");
    println!("  delimiter <token>         change SQL terminator for this console session");
    println!("  <sql>;                    run SQL statements (multi-line supported)");
    println!();
    println!("Note: default delimiter is ';' (override with `delimiter <token>`)");
}

pub fn parse_connect_target(
    target: &str,
    temp_connect_user: &str,
) -> Result<(String, String), String> {

    let Some((user, peer_id)) = target.split_once('@') else {
        return Err("connect requires format user@peer-id".to_string());
    };

    let user = user.trim();
    let peer_id = peer_id.trim();

    if user.is_empty() || peer_id.is_empty() {
        return Err("connect requires format user@peer-id".to_string());
    }

    if user != temp_connect_user {
        return Err("invalid user".to_string());
    }

    Ok((user.to_string(), peer_id.to_string()))

}

fn database_from_qualified_select_sql(sql: &str) -> Option<&str> {

    let trimmed = sql.trim().trim_end_matches(';');
    let lowered = trimmed.to_ascii_lowercase();
    let from_index = lowered.find(" from ")?;
    let after_from = trimmed[from_index + " from ".len()..].trim_start();
    let table_token = after_from
        .split_whitespace()
        .next()?
        .trim_end_matches(',')
        .trim_end_matches(';');

    let (database_name, table_name) = table_token.rsplit_once('.')?;
    if database_name.trim().is_empty() || table_name.trim().is_empty() {
        return None;
    }

    Some(database_name)

}

fn is_global_sql_without_database(sql: &str) -> bool {

    let tokens = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect::<Vec<_>>();

    if tokens.len() < 2 {
        return false;
    }

    if tokens[0] == "show" && tokens[1] == "bootstrap" {
        return tokens.get(2).is_some_and(|token| token == "status");
    }

    if tokens[0] == "show" && tokens[1] == "catalog" {
        return tokens.get(2).is_some_and(|token| token == "workers");
    }

    matches!(
        (tokens[0].as_str(), tokens[1].as_str()),
        ("show", "databases")
            | ("show", "entities")
            | ("show", "server")
            | ("create", "database")
            | ("drop", "database")
    )

}

fn is_console_command_fragment(line: &str) -> bool {

    let lowered = line.to_lowercase();

    matches!(
        lowered.as_str(),
        "help" | ".help" | "exit" | "quit" | "\\q" | "show p2p" | "show log" | "show peers" | "disconnect"
    ) || lowered.starts_with("use ")
        || lowered.starts_with("import ")
        || lowered.starts_with("connect ")
        || lowered.starts_with("delimiter ")

}

fn parse_delimiter_directive(
    trimmed_input: &str,
    active_delimiter: &str,
) -> Result<Option<String>, String> {

    let normalized = trimmed_input.trim();

    let mut parts = normalized.split_whitespace();
    let Some(first) = parts.next() else {
        return Ok(None);
    };

    if !first.eq_ignore_ascii_case("delimiter") {
        return Ok(None);
    }

    let mut next_delimiter = parts
        .next()
        .ok_or_else(|| "delimiter requires a token".to_string())?
        .to_string();

    if parts.next().is_some() {
        return Err("delimiter accepts exactly one token".to_string());
    }

    if active_delimiter != ";" && next_delimiter.ends_with(active_delimiter) {
        if let Some(without_suffix) = next_delimiter.strip_suffix(active_delimiter) {
            next_delimiter = without_suffix.to_string();
        }
    }

    if active_delimiter == ";" && next_delimiter.ends_with(';') && next_delimiter != ";" {
        next_delimiter = next_delimiter.trim_end_matches(';').to_string();
    }

    if next_delimiter.is_empty() {
        return Err("delimiter requires a token".to_string());
    };

    Ok(Some(next_delimiter))

}

#[cfg(test)]
mod tests {

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
        assert_eq!(extract_password_token_input("select 1"), None);
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

}
