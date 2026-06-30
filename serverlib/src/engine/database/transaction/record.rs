
use std::borrow::Cow;

use crate::core::identity::UserId;
use common::helpers::base64::{b64_decode, b64_encode_bytes};

use super::id::TransactionId;
use super::kind::TransactionKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadTransformError {
    UnsupportedFormat,
    InvalidCompressedPayload,
    InvalidEncryptedPayload,
    DecryptFailed,
    EncryptionNotConfigured,
    IntegrityCheckFailed,
    InternalTransformError(String),
}

impl std::fmt::Display for PayloadTransformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedFormat => f.write_str("unsupported payload format"),
            Self::InvalidCompressedPayload => f.write_str("invalid compressed payload"),
            Self::InvalidEncryptedPayload => f.write_str("invalid encrypted payload"),
            Self::DecryptFailed => f.write_str("payload decrypt failed"),
            Self::EncryptionNotConfigured => f.write_str("payload encryption not configured"),
            Self::IntegrityCheckFailed => f.write_str("payload integrity check failed"),
            Self::InternalTransformError(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for PayloadTransformError {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransactionPayloadContext {
    stream_id: Option<String>,
    database_id: Option<String>,
    table_id: Option<String>,
    encryption_key_ref: Option<String>,
    encryption_key_version: Option<u32>,
}

impl TransactionPayloadContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stream_id(mut self, stream_id: impl Into<String>) -> Self {
        self.stream_id = Some(stream_id.into());
        self
    }

    pub fn with_database_id(mut self, database_id: impl Into<String>) -> Self {
        self.database_id = Some(database_id.into());
        self
    }

    pub fn with_table_id(mut self, table_id: impl Into<String>) -> Self {
        self.table_id = Some(table_id.into());
        self
    }

    pub fn with_at_rest_encryption(
        mut self,
        key_ref: impl Into<String>,
        key_version: u32,
    ) -> Self {
        self.encryption_key_ref = Some(key_ref.into());
        self.encryption_key_version = Some(key_version);
        self
    }

    pub fn stream_id(&self) -> Option<&str> {
        self.stream_id.as_deref()
    }

    pub fn database_id(&self) -> Option<&str> {
        self.database_id.as_deref()
    }

    pub fn table_id(&self) -> Option<&str> {
        self.table_id.as_deref()
    }

    pub fn at_rest_encryption_key_ref(&self) -> Option<&str> {
        self.encryption_key_ref.as_deref()
    }

    pub fn at_rest_encryption_key_version(&self) -> Option<u32> {
        self.encryption_key_version
    }

    pub fn at_rest_encryption_enabled(&self) -> bool {
        self.encryption_key_ref.is_some()
    }
}

pub trait TransactionPayloadResolver {
    fn resolve_payload(
        &self,
        raw_payload: Option<&[u8]>,
        context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError>;
}

pub trait TransactionPayloadTransform: Send + Sync {
    fn transform_payload(
        &self,
        payload: &[u8],
        context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError>;
}

pub trait TransactionPayloadWriteTransform: Send + Sync {
    fn transform_payload_for_write(
        &self,
        record: &TransactionRecord,
        payload: &[u8],
        context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError>;
}

#[derive(Default)]
pub struct ChainedTransactionPayloadResolver {
    transforms: Vec<Box<dyn TransactionPayloadTransform>>,
}

impl ChainedTransactionPayloadResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_transform<T: TransactionPayloadTransform + 'static>(mut self, transform: T) -> Self {
        self.transforms.push(Box::new(transform));
        self
    }

    pub fn push_transform<T: TransactionPayloadTransform + 'static>(&mut self, transform: T) {
        self.transforms.push(Box::new(transform));
    }
}

impl TransactionPayloadResolver for ChainedTransactionPayloadResolver {
    fn resolve_payload(
        &self,
        raw_payload: Option<&[u8]>,
        context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
        let Some(payload) = raw_payload else {
            return Ok(None);
        };

        let mut current = Cow::Borrowed(payload);

        for transform in &self.transforms {
            if let Some(transformed) = transform.transform_payload(current.as_ref(), context)? {
                current = Cow::Owned(transformed);
            }
        }

        match current {
            Cow::Borrowed(_) => Ok(Some(payload.to_vec())),
            Cow::Owned(bytes) => Ok(Some(bytes)),
        }
    }
}

#[derive(Default)]
pub struct ChainedTransactionPayloadWriter {
    transforms: Vec<Box<dyn TransactionPayloadWriteTransform>>,
}

impl ChainedTransactionPayloadWriter {

    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_transform<T: TransactionPayloadWriteTransform + 'static>(mut self, transform: T) -> Self {
        self.transforms.push(Box::new(transform));
        self
    }

    pub fn push_transform<T: TransactionPayloadWriteTransform + 'static>(&mut self, transform: T) {
        self.transforms.push(Box::new(transform));
    }

    pub fn write_payload(
        &self,
        record: &TransactionRecord,
        raw_payload: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
        
        let context = TransactionPayloadContext::default();
        self.write_payload_with_context(record, raw_payload, &context)

    }

    pub fn write_payload_with_context(
        &self,
        record: &TransactionRecord,
        raw_payload: Option<&[u8]>,
        context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError> {

        let Some(payload) = raw_payload else {
            return Ok(None);
        };

        let mut current = Cow::Borrowed(payload);

        for transform in &self.transforms {
            if let Some(transformed) =
                transform.transform_payload_for_write(record, current.as_ref(), context)?
            {
                current = Cow::Owned(transformed);
            }
        }

        match current {
            Cow::Borrowed(_) => Ok(Some(payload.to_vec())),
            Cow::Owned(bytes) => Ok(Some(bytes)),
        }

    }

}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlainTransactionPayloadResolver;

impl TransactionPayloadResolver for PlainTransactionPayloadResolver {

    fn resolve_payload(
        &self,
        raw_payload: Option<&[u8]>,
        _context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError> {

        Ok(raw_payload.map(|payload| payload.to_vec()))
    
    }

}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TransactionPayloadShadow {
    resolved_payload: Option<Vec<u8>>,
    resolved_context: Option<TransactionPayloadContext>,
    is_resolved: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TransactionRecord {
    pub id: TransactionId,
    #[serde(default)]
    pub groupid: Option<TransactionId>,
    pub refid: Option<TransactionId>,
    pub timestamp_epoch_ms: u64,
    pub actor: UserId,
    pub kind: TransactionKind,
    #[serde(default)]
    payload: Option<Vec<u8>>,
    #[serde(skip, default)]
    payload_shadow: TransactionPayloadShadow,
}

impl PartialEq for TransactionRecord {
    
    fn eq(&self, other: &Self) -> bool {

        self.id == other.id
            && self.groupid == other.groupid
            && self.refid == other.refid
            && self.timestamp_epoch_ms == other.timestamp_epoch_ms
            && self.actor == other.actor
            && self.kind == other.kind
            && self.payload() == other.payload()

    }

}

impl Eq for TransactionRecord {}

impl TransactionRecord {

    pub fn new(
        id: TransactionId,
        groupid: Option<TransactionId>,
        refid: Option<TransactionId>,
        timestamp_epoch_ms: u64,
        actor: UserId,
        kind: TransactionKind,
        payload: Option<Vec<u8>>,
    ) -> Self {
        
        Self {
            id,
            groupid,
            refid,
            timestamp_epoch_ms,
            actor,
            kind,
            payload,
            payload_shadow: TransactionPayloadShadow::default(),
        }

    }

    pub fn with_payload(
        id: TransactionId,
        groupid: Option<TransactionId>,
        refid: Option<TransactionId>,
        timestamp_epoch_ms: u64,
        actor: UserId,
        kind: TransactionKind,
        payload: Vec<u8>,
    ) -> Self {
        
        Self::new(
            id,
            groupid,
            refid,
            timestamp_epoch_ms,
            actor,
            kind,
            Some(payload),
        )

    }

    pub fn without_payload(
        id: TransactionId,
        groupid: Option<TransactionId>,
        refid: Option<TransactionId>,
        timestamp_epoch_ms: u64,
        actor: UserId,
        kind: TransactionKind,
    ) -> Self {

        Self::new(id, groupid, refid, timestamp_epoch_ms, actor, kind, None)

    }

    pub fn payload(&self) -> Option<&[u8]> {
        self.payload_logical()
    }

    pub fn payload_logical(&self) -> Option<&[u8]> {
        self.resolved_payload().or_else(|| self.payload_raw())
    }

    pub fn payload_raw(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    pub fn payload_bytes(&self) -> Option<&[u8]> {
        self.payload_logical()
    }

    pub fn resolved_payload(&self) -> Option<&[u8]> {
        if self.payload_shadow.is_resolved {
            self.payload_shadow.resolved_payload.as_deref()
        } else {
            None
        }
    }

    pub fn resolve_payload_with<R: TransactionPayloadResolver>(
        &mut self,
        resolver: &R,
    ) -> Result<Option<&[u8]>, PayloadTransformError> {
        
        let context = TransactionPayloadContext::default();
        self.resolve_payload_with_context(resolver, &context)

    }

    pub fn resolve_payload_with_context<R: TransactionPayloadResolver>(
        &mut self,
        resolver: &R,
        context: &TransactionPayloadContext,
    ) -> Result<Option<&[u8]>, PayloadTransformError> {
        
        if self.payload_shadow.is_resolved
            && self.payload_shadow.resolved_context.as_ref() == Some(context)
        {
            return Ok(self.payload_shadow.resolved_payload.as_deref());
        }

        let resolved = resolver.resolve_payload(self.payload_raw(), context)?;
        self.payload_shadow.resolved_payload = resolved;
        self.payload_shadow.resolved_context = Some(context.clone());
        self.payload_shadow.is_resolved = true;

        Ok(self.payload_shadow.resolved_payload.as_deref())
        
    }

    pub fn set_payload(&mut self, payload: Option<Vec<u8>>) {
        self.payload = payload;
        self.payload_shadow = TransactionPayloadShadow::default();
    }

    pub fn payload_mut(&mut self) -> Option<&mut Vec<u8>> {
        self.payload_shadow = TransactionPayloadShadow::default();
        self.payload.as_mut()
    }

    pub fn clear_payload(&mut self) {
        self.payload = None;
        self.payload_shadow = TransactionPayloadShadow::default();
    }

    pub fn has_payload(&self) -> bool {
        self.payload.is_some()
    }
    
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
    use std::cell::Cell;

    fn sample_frame() -> (String, TransactionRecord) {
        (
            "stream1".to_string(),
            TransactionRecord::with_payload(
                TransactionId(1),
                None,
                None,
                1,
                UserId("system".to_string()),
                TransactionKind::Ignore,
                vec![1, 2, 3, 4],
            ),
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

    #[test]
    fn plain_payload_resolver_returns_raw_bytes_and_caches_once() {
        struct CountingResolver<'a> {
            calls: &'a Cell<usize>,
        }

        impl TransactionPayloadResolver for CountingResolver<'_> {
            fn resolve_payload(
                &self,
                raw_payload: Option<&[u8]>,
                _context: &TransactionPayloadContext,
            ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
                self.calls.set(self.calls.get() + 1);
                Ok(raw_payload.map(|payload| payload.to_vec()))
            }
        }

        let calls = Cell::new(0usize);
        let resolver = CountingResolver { calls: &calls };
        let mut record = TransactionRecord::with_payload(
            TransactionId(7),
            None,
            None,
            7,
            UserId("system".to_string()),
            TransactionKind::Insert,
            vec![9, 8, 7],
        );

        assert_eq!(record.resolve_payload_with(&resolver).unwrap(), Some(&[9, 8, 7][..]));
        assert_eq!(record.resolved_payload(), Some(&[9, 8, 7][..]));
        assert_eq!(record.resolve_payload_with(&resolver).unwrap(), Some(&[9, 8, 7][..]));
        assert_eq!(calls.get(), 1);

        record.set_payload(Some(vec![1, 2, 3]));
        assert!(record.resolved_payload().is_none());
        assert_eq!(record.resolve_payload_with(&resolver).unwrap(), Some(&[1, 2, 3][..]));
        assert_eq!(calls.get(), 2);
    }

    #[test]
    fn plain_payload_resolver_preserves_none_payload() {
        let resolver = PlainTransactionPayloadResolver;
        let mut record = TransactionRecord::without_payload(
            TransactionId(11),
            None,
            None,
            11,
            UserId("system".to_string()),
            TransactionKind::Ignore,
        );

        assert_eq!(record.resolve_payload_with(&resolver).unwrap(), None);
        assert_eq!(record.resolved_payload(), None);
    }

    #[test]
    fn chained_payload_resolver_applies_transforms_in_order() {
        struct PrefixTransform;

        impl TransactionPayloadTransform for PrefixTransform {
            fn transform_payload(
                &self,
                payload: &[u8],
                _context: &TransactionPayloadContext,
            ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
                let mut transformed = b"prefix:".to_vec();
                transformed.extend_from_slice(payload);
                Ok(Some(transformed))
            }
        }

        struct SuffixTransform;

        impl TransactionPayloadTransform for SuffixTransform {
            fn transform_payload(
                &self,
                payload: &[u8],
                _context: &TransactionPayloadContext,
            ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
                let mut transformed = payload.to_vec();
                transformed.extend_from_slice(b":suffix");
                Ok(Some(transformed))
            }
        }

        let resolver = ChainedTransactionPayloadResolver::new()
            .with_transform(PrefixTransform)
            .with_transform(SuffixTransform);

        let mut record = TransactionRecord::with_payload(
            TransactionId(12),
            None,
            None,
            12,
            UserId("system".to_string()),
            TransactionKind::Insert,
            b"payload".to_vec(),
        );

        assert_eq!(
            record.resolve_payload_with(&resolver).unwrap(),
            Some(&b"prefix:payload:suffix"[..])
        );
        assert_eq!(record.payload_raw(), Some(&b"payload"[..]));
        assert_eq!(record.payload_logical(), Some(&b"prefix:payload:suffix"[..]));
    }

    #[test]
    fn chained_payload_resolver_leaves_payload_unchanged_when_no_transform_matches() {
        struct NoopTransform;

        impl TransactionPayloadTransform for NoopTransform {
            fn transform_payload(
                &self,
                _payload: &[u8],
                _context: &TransactionPayloadContext,
            ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
                Ok(None)
            }
        }

        let resolver = ChainedTransactionPayloadResolver::new().with_transform(NoopTransform);
        let mut record = TransactionRecord::with_payload(
            TransactionId(13),
            None,
            None,
            13,
            UserId("system".to_string()),
            TransactionKind::Insert,
            b"payload".to_vec(),
        );

        assert_eq!(record.resolve_payload_with(&resolver).unwrap(), Some(&b"payload"[..]));
        assert_eq!(record.payload_raw(), Some(&b"payload"[..]));
    }

    #[test]
    fn chained_payload_writer_applies_transforms_in_order() {
        struct PrefixWriteTransform;

        impl TransactionPayloadWriteTransform for PrefixWriteTransform {
            fn transform_payload_for_write(
                &self,
                _record: &TransactionRecord,
                payload: &[u8],
                _context: &TransactionPayloadContext,
            ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
                let mut transformed = b"prefix:".to_vec();
                transformed.extend_from_slice(payload);
                Ok(Some(transformed))
            }
        }

        struct SuffixWriteTransform;

        impl TransactionPayloadWriteTransform for SuffixWriteTransform {
            fn transform_payload_for_write(
                &self,
                _record: &TransactionRecord,
                payload: &[u8],
                _context: &TransactionPayloadContext,
            ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
                let mut transformed = payload.to_vec();
                transformed.extend_from_slice(b":suffix");
                Ok(Some(transformed))
            }
        }

        let writer = ChainedTransactionPayloadWriter::new()
            .with_transform(PrefixWriteTransform)
            .with_transform(SuffixWriteTransform);

        let record = TransactionRecord::with_payload(
            TransactionId(14),
            None,
            None,
            14,
            UserId("system".to_string()),
            TransactionKind::Insert,
            b"payload".to_vec(),
        );

        assert_eq!(
            writer.write_payload(&record, record.payload_raw()).unwrap(),
            Some(b"prefix:payload:suffix".to_vec())
        );
    }

    #[test]
    fn chained_payload_writer_leaves_payload_unchanged_when_no_transform_matches() {
        struct NoopWriteTransform;

        impl TransactionPayloadWriteTransform for NoopWriteTransform {
            fn transform_payload_for_write(
                &self,
                _record: &TransactionRecord,
                _payload: &[u8],
                _context: &TransactionPayloadContext,
            ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
                Ok(None)
            }
        }

        let writer = ChainedTransactionPayloadWriter::new().with_transform(NoopWriteTransform);
        let record = TransactionRecord::with_payload(
            TransactionId(15),
            None,
            None,
            15,
            UserId("system".to_string()),
            TransactionKind::Insert,
            b"payload".to_vec(),
        );

        assert_eq!(
            writer.write_payload(&record, record.payload_raw()).unwrap(),
            Some(b"payload".to_vec())
        );
    }

    #[test]
    fn chained_payload_writer_passes_context_to_transforms() {
        struct ContextAwareWriteTransform;

        impl TransactionPayloadWriteTransform for ContextAwareWriteTransform {
            fn transform_payload_for_write(
                &self,
                _record: &TransactionRecord,
                payload: &[u8],
                context: &TransactionPayloadContext,
            ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
                let mut transformed = payload.to_vec();
                transformed.extend_from_slice(context.table_id().unwrap_or("none").as_bytes());
                Ok(Some(transformed))
            }
        }

        let writer = ChainedTransactionPayloadWriter::new().with_transform(ContextAwareWriteTransform);
        let context = TransactionPayloadContext::new().with_table_id("users");
        let record = TransactionRecord::with_payload(
            TransactionId(16),
            None,
            None,
            16,
            UserId("system".to_string()),
            TransactionKind::Insert,
            b"payload:".to_vec(),
        );

        assert_eq!(
            writer
                .write_payload_with_context(&record, record.payload_raw(), &context)
                .unwrap(),
            Some(b"payload:users".to_vec())
        );
    }

    #[test]
    fn resolve_payload_with_context_recomputes_when_context_changes() {
        struct ContextAwareResolver;

        impl TransactionPayloadResolver for ContextAwareResolver {
            fn resolve_payload(
                &self,
                raw_payload: Option<&[u8]>,
                context: &TransactionPayloadContext,
            ) -> Result<Option<Vec<u8>>, PayloadTransformError> {
                let Some(payload) = raw_payload else {
                    return Ok(None);
                };

                let mut transformed = payload.to_vec();
                transformed.extend_from_slice(
                    context.table_id().unwrap_or("none").as_bytes(),
                );
                Ok(Some(transformed))
            }
        }

        let resolver = ContextAwareResolver;
        let mut record = TransactionRecord::with_payload(
            TransactionId(17),
            None,
            None,
            17,
            UserId("system".to_string()),
            TransactionKind::Insert,
            b"payload:".to_vec(),
        );
        let users_context = TransactionPayloadContext::new().with_table_id("users");
        let orders_context = TransactionPayloadContext::new().with_table_id("orders");

        assert_eq!(
            record
                .resolve_payload_with_context(&resolver, &users_context)
                .unwrap(),
            Some(&b"payload:users"[..])
        );
        assert_eq!(
            record
                .resolve_payload_with_context(&resolver, &orders_context)
                .unwrap(),
            Some(&b"payload:orders"[..])
        );
    }
}
