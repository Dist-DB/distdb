
mod bootstrap;
mod commands;
mod import;
mod output;
mod session;
mod utils;

pub const TEMP_CONNECT_USER: &str = "root";
const SERVER_PEER_DISCOVERY_SQL: &str = "__distdb_show_server_peers__";
const IMPORT_TRANSPORT_RETRY_LIMIT: usize = 3;
const SQL_TRANSPORT_RETRY_LIMIT: usize = 3;
const IMPORT_TRANSACTION_BATCH_SIZE: usize = 500;
const IMPORT_TRANSACTION_BATCH_MAX_AGE_MS: u128 = 500;
const IMPORT_BEGIN_STATEMENT: &str = "begin /*distdb_import*/";
const DEFAULT_CONNECTOR_IO_TIMEOUT_SECS: u64 = 120;
const IMPORT_LARGE_STATEMENT_BYTES: usize = 256_000;
const IMPORT_INSERT_CHUNK_TARGET_BYTES: usize = 256_000;
const IMPORT_INSERT_CHUNK_MAX_TUPLES: usize = 512;

pub enum ConsoleCommand {
    Help,
    Exit,
    SetDelimiter(String),
    ShowP2p,
    ShowLog,
    ShowPeers,
    ConnectPeer { user: String, peer_id: String },
    Disconnect,
    UseDatabase(String),
    ImportFile(String),
    Sql(String),
}

pub use session::ConsoleSession;

pub use utils::{
    auth_password_input, bootstrap_peers_from_cli_args, connector_tls_config_from_cli_args,
    extract_password_token_input,
    normalize_bootstrap_addr, normalize_bootstrap_peers, parse_connect_target,
    parse_console_command, parse_console_command_with_delimiter, print_help,
    resolve_database_for_sql,
};
