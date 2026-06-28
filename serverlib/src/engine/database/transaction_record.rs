
use crate::core::identity::UserId;
use common::helpers::base64::{b64_decode, b64_encode_bytes};

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

/// Base64-encode a `(stream_id, TransactionRecord)` frame for wire/replication transport only.
/// Persisted WAL/table payload bytes remain binary (bincode payload in file records).
pub fn encode_wal_frame(frame: &(String, TransactionRecord)) -> Result<String, String> {

    let bytes = bincode::serialize(frame)
        .map_err(|err| format!("failed to serialize WAL frame: {}", err))?;

    Ok(b64_encode_bytes(&bytes))
    
}

/// Decode a WAL frame back to `(stream_id, TransactionRecord)`.
///
/// Accepts new base64 frames and legacy hex frames for backward compatibility.
pub fn decode_wal_frame(encoded: &str) -> Result<(String, TransactionRecord), String> {

    let base64_bytes = b64_decode(encoded);
    
    if !base64_bytes.is_empty() {
        if let Ok(frame) = bincode::deserialize::<(String, TransactionRecord)>(&base64_bytes) {
            return Ok(frame);
        }
    }

    if !encoded.len().is_multiple_of(2) {
        return Err("invalid WAL frame encoding; expected base64 or hex".to_string());
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_frame() -> (String, TransactionRecord) {
        (
            "stream1".to_string(),
            TransactionRecord {
                id: TransactionId(1),
                groupid: None,
                refid: None,
                timestamp_epoch_ms: 1,
                actor: UserId("system".to_string()),
                kind: TransactionKind::Ignore,
                payload: vec![1, 2, 3, 4],
            },
        )
    }

    #[test]
    fn wal_frame_roundtrip_uses_base64() {
        let frame = sample_frame();
        let encoded = encode_wal_frame(&frame).expect("encode should succeed");
        let decoded = decode_wal_frame(&encoded).expect("decode should succeed");

        assert_eq!(decoded, frame);
    }

    #[test]
    fn wal_frame_decode_accepts_legacy_hex() {
        let frame = sample_frame();
        let bytes = bincode::serialize(&frame).expect("serialize should succeed");
        let legacy_hex = bytes
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();

        let decoded = decode_wal_frame(&legacy_hex).expect("legacy hex should decode");
        assert_eq!(decoded, frame);
    }
}
