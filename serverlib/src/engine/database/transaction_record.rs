
use crate::core::identity::UserId;

use super::transaction_id::TransactionId;
use super::transaction_kind::TransactionKind;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionRecord {
    pub id: TransactionId,
    #[serde(default)]
    pub groupid: Option<TransactionId>,
    pub refid: Option<TransactionId>,
    pub timestamp_epoch_ms: u64,
    pub actor: UserId,
    pub kind: TransactionKind,
    pub payload: Vec<u8>,
}

/// Hex-encode a `(stream_id, TransactionRecord)` frame for wire/replication transport.
pub fn encode_wal_frame(frame: &(String, TransactionRecord)) -> Result<String, String> {
    
    let bytes = bincode::serialize(frame)
        .map_err(|err| format!("failed to serialize WAL frame: {}", err))?;
    
    let mut encoded = String::with_capacity(bytes.len() * 2);
    
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{:02x}", b);
    }
    
    Ok(encoded)

}

/// Decode a hex-encoded WAL frame back to `(stream_id, TransactionRecord)`.
pub fn decode_wal_frame(encoded: &str) -> Result<(String, TransactionRecord), String> {

    if !encoded.len().is_multiple_of(2) {
        return Err("invalid WAL frame encoding length".to_string());
    }

    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    let chars = encoded.as_bytes();
    let mut i = 0usize;

    while i < chars.len() {
        let chunk = std::str::from_utf8(&chars[i..i + 2])
            .map_err(|err| format!("invalid WAL frame utf8: {}", err))?;
        let value = u8::from_str_radix(chunk, 16)
            .map_err(|err| format!("invalid WAL frame hex '{}': {}", chunk, err))?;
        bytes.push(value);
        i += 2;
    }

    bincode::deserialize::<(String, TransactionRecord)>(&bytes)
        .map_err(|err| format!("failed to deserialize WAL frame: {}", err))

}
