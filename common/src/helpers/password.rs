use crate::helpers::hash::stable_id;

pub fn derive_password_nonce(database_name: &str, server_identifier: &str, first_schema_wal_timestamp_ms: Option<u64>) -> String {
    let wal_seed = first_schema_wal_timestamp_ms
        .map(|value| value.to_string())
        .unwrap_or_else(|| "wal-ts-unset".to_string());

    stable_id(&[
        "distdb-password-nonce",
        database_name,
        server_identifier,
        &wal_seed,
    ])
}

pub fn derive_password_secret(password_nonce: &str, server_identifier: &str) -> String {
    stable_id(&[
        "distdb-password-secret",
        password_nonce,
        server_identifier,
    ])
}

pub fn salt_from_nonce(password_nonce: &str) -> [u8; 8] {
    let mut salt = [0u8; 8];
    let bytes = password_nonce.as_bytes();
    for idx in 0..8 {
        salt[idx] = if idx < bytes.len() { bytes[idx] } else { b'0' };
    }
    salt
}
