use common::helpers::utils::md5_hash;
use common::helpers::{aes_decrypt, aes_encrypt, stable_id};
use common::{PeerSession, SessionLog, SessionLogEventType};
use connector::{ConnectorCommand, ConnectorRequest};
use std::sync::{OnceLock, RwLock};

const SERVER_TEMP_PASSWORD: &str = "root";
const SERVER_TEMP_USER: &str = "root";
const SERVER_TEMP_TOKEN_SALT: &[u8; 8] = b"distdbv1";

static BOOTSTRAP_PASSWORD_STATE: OnceLock<RwLock<BootstrapPasswordState>> = OnceLock::new();
static BOOTSTRAP_CRYPTO_CONTEXT: OnceLock<RwLock<BootstrapCryptoContext>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetPasswordDirective<'a> {
    pub user_id: &'a str,
    pub password: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BootstrapPasswordWalPayload {
    user_id: String,
    password_nonce: String,
    encrypted_password: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BootstrapPasswordState {
    password_nonce: String,
    encrypted_password: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BootstrapCryptoContext {
    server_identifier: String,
    first_schema_wal_timestamp_ms: Option<u64>,
}

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

pub fn configure_bootstrap_crypto_context(
    server_identifier: impl Into<String>,
    first_schema_wal_timestamp_ms: Option<u64>,
) {
    let store = bootstrap_crypto_context_store();
    let mut guard = store.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = BootstrapCryptoContext {
        server_identifier: server_identifier.into(),
        first_schema_wal_timestamp_ms,
    };
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

        let expected_md5_token = md5_hash(&current_bootstrap_password_plaintext());
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
            },

            ConnectorCommand::Schema { database_id, .. } => {
                self.session.current_database = Some(database_id.clone());
                SessionLogEventType::SchemaChange
            },

            ConnectorCommand::Mutation { database_id, .. } => {
                self.session.current_database = Some(database_id.clone());
                SessionLogEventType::Other
            },

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

        if constant_time_eq(
            candidate_password_md5_token.as_bytes(),
            expected_password_md5_token.as_bytes(),
        ) {
            self.authenticated = true;
            self.session.auth_token = Some(format!("{}-authenticated", SERVER_TEMP_USER));

            self.log.add_entry(
                SessionLogEventType::Authenticate,
                format!("bootstrap password accepted user={}", SERVER_TEMP_USER),
                true,
            );

            true
        } else {
            self.log.add_entry(
                SessionLogEventType::Authenticate,
                format!("bootstrap password rejected user={}", SERVER_TEMP_USER),
                false,
            );

            false
        }

    }

    pub fn user_id(&self) -> &str {

        self.session
            .user_id
            .as_deref()
            .unwrap_or(SERVER_TEMP_USER)

    }
}

pub fn extract_auth_token(sql: &str) -> Option<&str> {

    let trimmed = sql.trim().trim_end_matches(';').trim();

    let mut parts = trimmed.split_whitespace();
    let command = parts.next()?;
    let token = parts.next()?;
    
    if parts.next().is_some() {
        return None;
    }
    
    if command.eq_ignore_ascii_case("password_token") || command.eq_ignore_ascii_case("password") {
        return Some(token);
    }
    
    None

}

pub fn extract_set_password_directive(sql: &str) -> Option<SetPasswordDirective<'_>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let (user_id, password) = extract_set_password_literals(trimmed)?;
    Some(SetPasswordDirective { user_id, password })
}

pub fn set_bootstrap_password(user_id: &str, password: &str) -> Result<(), String> {
    set_bootstrap_password_state(user_id, encrypt_bootstrap_password(password))
}

pub fn encode_set_password_wal_payload(user_id: &str, password: &str) -> Result<Vec<u8>, String> {

    validate_set_password_input(user_id, password)?;

    let state = encrypt_bootstrap_password(password);

    let payload = BootstrapPasswordWalPayload {
        user_id: SERVER_TEMP_USER.to_string(),
        password_nonce: state.password_nonce,
        encrypted_password: state.encrypted_password,
    };

    Ok(format!(
        "{}\n{}\n{}",
        payload.user_id,
        payload.password_nonce,
        payload.encrypted_password,
    )
    .into_bytes())

}

pub fn apply_bootstrap_password_wal_payload(payload: &[u8]) -> Result<(), String> {

    let text = String::from_utf8(payload.to_vec())
        .map_err(|_| "set password failed: cannot decode wal payload".to_string())?;

    let mut parts = text.splitn(3, '\n');
    
    let Some(user_id) = parts.next() else {
        return Err("set password failed: wal payload is malformed".to_string());
    };
    
    let Some(password_nonce) = parts.next() else {
        return Err("set password failed: wal payload is malformed".to_string());
    };
    
    let Some(encrypted_password) = parts.next() else {
        return Err("set password failed: wal payload is malformed".to_string());
    };

    let decoded = BootstrapPasswordWalPayload {
        user_id: user_id.to_string(),
        password_nonce: password_nonce.to_string(),
        encrypted_password: encrypted_password.to_string(),
    };

    set_bootstrap_password_state(
        &decoded.user_id,
        BootstrapPasswordState {
            password_nonce: decoded.password_nonce,
            encrypted_password: decoded.encrypted_password,
        },
    )

}

fn validate_set_password_input(user_id: &str, password: &str) -> Result<(), String> {

    if !user_id.eq_ignore_ascii_case(SERVER_TEMP_USER) {
        return Err(format!(
            "set password failed: only '{}' is supported currently",
            SERVER_TEMP_USER
        ));
    }

    if password.trim().is_empty() {
        return Err("set password failed: password cannot be empty".to_string());
    }

    Ok(())

}

fn set_bootstrap_password_state(
    user_id: &str,
    state: BootstrapPasswordState,
) -> Result<(), String> {

    if !user_id.eq_ignore_ascii_case(SERVER_TEMP_USER) {
        return Err(format!(
            "set password failed: only '{}' is supported currently",
            SERVER_TEMP_USER
        ));
    }

    if state.encrypted_password.trim().is_empty() {
        return Err("set password failed: encrypted password cannot be empty".to_string());
    }

    if state.password_nonce.trim().is_empty() {
        return Err("set password failed: password nonce cannot be empty".to_string());
    }

    let _ = decrypt_bootstrap_password(&state)?;

    let store = bootstrap_password_state_store();
    let mut guard = store.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = state;

    Ok(())

}

fn extract_set_password_literals(sql: &str) -> Option<(&str, &str)> {

    let mut rest = sql;

    rest = strip_prefix_ci(rest, "set")?.trim_start();
    rest = strip_prefix_ci(rest, "password")?.trim_start();
    rest = strip_prefix_ci(rest, "for")?.trim_start();

    let (user_id, next) = parse_single_quoted_literal(rest)?;
    rest = next.trim_start();

    if !rest.starts_with('=') {
        return None;
    }

    rest = rest[1..].trim_start();
    rest = strip_prefix_ci(rest, "password")?.trim_start();

    if !rest.starts_with('(') {
        return None;
    }

    rest = rest[1..].trim_start();

    let (password, next) = parse_single_quoted_literal(rest)?;
    rest = next.trim_start();

    if !rest.starts_with(')') {
        return None;
    }

    if !rest[1..].trim().is_empty() {
        return None;
    }

    Some((user_id, password))

}

fn bootstrap_password_state_store() -> &'static RwLock<BootstrapPasswordState> {
    BOOTSTRAP_PASSWORD_STATE
        .get_or_init(|| RwLock::new(encrypt_bootstrap_password(SERVER_TEMP_PASSWORD)))
}

fn current_bootstrap_password_state() -> BootstrapPasswordState {
    let store = bootstrap_password_state_store();
    let guard = store.read().unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.clone()
}

fn current_bootstrap_password_plaintext() -> String {
    let state = current_bootstrap_password_state();
    decrypt_bootstrap_password(&state).unwrap_or_else(|_| SERVER_TEMP_PASSWORD.to_string())
}

fn encrypt_bootstrap_password(password: &str) -> BootstrapPasswordState {

    let context = current_bootstrap_crypto_context();
    let nonce = build_bootstrap_password_nonce(
        SERVER_TEMP_USER,
        &context.server_identifier,
        context.first_schema_wal_timestamp_ms,
    );
    let secret = build_bootstrap_password_secret(&nonce, &context.server_identifier);
    let salt = salt_from_nonce(&nonce);

    BootstrapPasswordState {
        password_nonce: nonce,
        encrypted_password: aes_encrypt(password, &secret, &salt),
    }

}

fn decrypt_bootstrap_password(state: &BootstrapPasswordState) -> Result<String, String> {

    let context = current_bootstrap_crypto_context();
    let secret = build_bootstrap_password_secret(&state.password_nonce, &context.server_identifier);
    let plaintext = std::panic::catch_unwind(|| aes_decrypt(&state.encrypted_password, &secret))
        .map_err(|_| "set password failed: encrypted password is invalid".to_string())?;

    if plaintext.is_empty() {
        Err("set password failed: encrypted password is invalid".to_string())
    } else {
        Ok(plaintext)
    }

}

fn bootstrap_crypto_context_store() -> &'static RwLock<BootstrapCryptoContext> {

    BOOTSTRAP_CRYPTO_CONTEXT.get_or_init(|| {
        RwLock::new(BootstrapCryptoContext {
            server_identifier: "distdb-bootstrap".to_string(),
            first_schema_wal_timestamp_ms: None,
        })
    })

}

fn current_bootstrap_crypto_context() -> BootstrapCryptoContext {

    let store = bootstrap_crypto_context_store();
    let guard = store.read().unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.clone()

}

fn build_bootstrap_password_nonce(
    user_name: &str,
    server_identifier: &str,
    first_schema_wal_timestamp_ms: Option<u64>,
) -> String {

    let wal_seed = first_schema_wal_timestamp_ms
        .map(|value| value.to_string())
        .unwrap_or_else(|| "wal-ts-unset".to_string());

    stable_id(&[
        "distdb-password-nonce",
        "bootstrap",
        &user_name.trim().to_ascii_lowercase(),
        server_identifier,
        &wal_seed,
    ])

}

fn build_bootstrap_password_secret(password_nonce: &str, server_identifier: &str) -> String {

    stable_id(&[
        "distdb-password-secret",
        password_nonce,
        server_identifier,
    ])

}

fn salt_from_nonce(password_nonce: &str) -> [u8; 8] {

    let mut salt = [0u8; 8];
    let bytes = password_nonce.as_bytes();
    
    for idx in 0..8 {
        salt[idx] = if idx < bytes.len() { bytes[idx] } else { b'0' };
    }
    
    salt

}

#[cfg(test)]
pub(crate) fn reset_bootstrap_password_for_tests() {
    let store = bootstrap_password_state_store();
    let mut guard = store.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    *guard = encrypt_bootstrap_password(SERVER_TEMP_PASSWORD);
}

fn strip_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .filter(|candidate| candidate.eq_ignore_ascii_case(prefix))
        .map(|_| &value[prefix.len()..])
}

fn parse_single_quoted_literal(value: &str) -> Option<(&str, &str)> {
    let remainder = value.strip_prefix('\'')?;
    let end_idx = remainder.find('\'')?;
    let (literal, tail) = remainder.split_at(end_idx);
    Some((literal, &tail[1..]))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut diff = 0u8;
    for (l, r) in left.iter().zip(right.iter()) {
        diff |= l ^ r;
    }

    diff == 0
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


#[cfg(test)]
#[path = "session_test.rs"]
mod tests;
