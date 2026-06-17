use common::helpers::utils::md5_hash;
use common::helpers::{aes_decrypt, aes_encrypt};
use common::{PeerSession, SessionLog, SessionLogEventType};
use connector::{ConnectorCommand, ConnectorRequest};

const SERVER_TEMP_PASSWORD: &str = "password";
const SERVER_TEMP_USER: &str = "root";
const SERVER_TEMP_TOKEN_SALT: &[u8; 8] = b"distdbv1";

#[derive(Debug)]
pub struct ServerConnectionSession {
    peer_addr: String,
    challenge_id: String,
    pub session_id: String,
    session: PeerSession,
    log: SessionLog,
    pub authenticated: bool,
    encrypted_password_md5_token: String,
}

impl ServerConnectionSession {
    pub fn new(peer_addr: String, connection_id: usize) -> Self {
        let challenge_id = format!("challenge-{}-{connection_id}", now_millis());

        let session_id = md5_hash(
            format!("{}:{}:{}", SERVER_TEMP_USER, peer_addr, challenge_id).as_str(),
        );

        let session = PeerSession::new()
            .with_user_id(SERVER_TEMP_USER)
            .with_session_id(session_id.clone());

        let expected_md5_token = md5_hash(SERVER_TEMP_PASSWORD);
        let security_secret = security_context_secret(SERVER_TEMP_USER, "bootstrap");
        let encrypted_password_md5_token = aes_encrypt(
            &expected_md5_token,
            &security_secret,
            SERVER_TEMP_TOKEN_SALT,
        );

        let mut log = SessionLog::new();

        log.add_entry(
            SessionLogEventType::Connect,
            format!("connector peer connected from {peer_addr}"),
            true,
        );

        log.add_entry(
            SessionLogEventType::Authenticate,
            format!(
                "password challenge issued id={challenge_id} user={}",
                SERVER_TEMP_USER
            ),
            true,
        );

        Self {
            peer_addr,
            challenge_id,
            session_id,
            session,
            log,
            authenticated: false,
            encrypted_password_md5_token,
        }
    }

    pub fn challenge_message(&self) -> String {
        format!(
            "password challenge required challenge_id={} session_id={} peer={}",
            self.challenge_id, self.session_id, self.peer_addr
        )
    }

    pub fn record_request(&mut self, request: &ConnectorRequest) {
        let event_type = match &request.command {
            ConnectorCommand::Query { query } => {
                self.session.current_database = Some(query.database_id.clone());
                SessionLogEventType::QueryExecute
            }
            ConnectorCommand::Schema { database_id, .. } => {
                self.session.current_database = Some(database_id.clone());
                SessionLogEventType::SchemaChange
            }
            ConnectorCommand::Mutation { database_id, .. } => {
                self.session.current_database = Some(database_id.clone());
                SessionLogEventType::Other
            }
            ConnectorCommand::CreateDatabase { database_name } => {
                self.session.current_database = Some(database_name.clone());
                SessionLogEventType::Other
            }
        };

        self.log.add_entry(
            event_type,
            format!(
                "request_id={} routed by server session db={}",
                request.request_id,
                self.session.current_database.as_deref().unwrap_or("<none>")
            ),
            true,
        );
    }

    pub fn mark_disconnect(&mut self) {
        self.session.clear_connection_state();

        self.log.add_entry(
            SessionLogEventType::Disconnect,
            "connector peer disconnected",
            true,
        );
    }

    pub fn authenticate_if_valid_token(&mut self, candidate_password_md5_token: &str) -> bool {
        let security_secret = security_context_secret(SERVER_TEMP_USER, "bootstrap");
        let expected_password_md5_token =
            aes_decrypt(&self.encrypted_password_md5_token, &security_secret);

        if candidate_password_md5_token == expected_password_md5_token {
            self.authenticated = true;
            self.session.auth_token = Some(format!("{}-authenticated", SERVER_TEMP_USER));

            self.log.add_entry(
                SessionLogEventType::Authenticate,
                format!(
                    "temporary password accepted user={} token={}",
                    SERVER_TEMP_USER, candidate_password_md5_token
                ),
                true,
            );

            true
        } else {
            self.log.add_entry(
                SessionLogEventType::Authenticate,
                format!(
                    "temporary password rejected user={} token={}",
                    SERVER_TEMP_USER, candidate_password_md5_token
                ),
                false,
            );

            false
        }
    }
}

pub fn extract_auth_token(sql: &str) -> Option<&str> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mut parts = trimmed.split_whitespace();
    let command = parts.next()?;
    let token = parts.next()?;
    if command.eq_ignore_ascii_case("password_token") {
        return Some(token);
    }
    None
}

fn security_context_secret(user_id: &str, database_id: &str) -> String {
    format!("distdb-security:{}:{}", user_id, database_id)
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
