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
