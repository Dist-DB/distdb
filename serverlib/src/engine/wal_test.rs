
use std::hint::black_box;
use std::time::Instant;

use super::*;

fn make_record(id: u64, kind: TransactionKind, actor: &UserId) -> TransactionRecord {
    TransactionRecord::with_payload(
        TransactionId(id),
        None,
        None,
        id,
        actor.clone(),
        kind,
        vec![id as u8],
    )
}

#[test]
fn since_offset_for_monotonic_transaction_ids_returns_suffix_start() {
    let actor = UserId::from_username("tester");
    let entries = vec![
        make_record(1, TransactionKind::Insert, &actor),
        make_record(2, TransactionKind::Insert, &actor),
        make_record(3, TransactionKind::Insert, &actor),
        make_record(4, TransactionKind::Insert, &actor),
        make_record(5, TransactionKind::Insert, &actor),
    ];

    let offset = first_record_index_after_id(&entries, TransactionId(3));

    assert_eq!(offset, 3);
    assert_eq!(entries[offset].id, TransactionId(4));
}

#[test]
fn since_from_transaction_id_benchmark_matches_linear_scan() {
    let actor = UserId::from_username("tester");
    let mut entries = Vec::with_capacity(50_000);

    for id in 1..=50_000u64 {
        entries.push(make_record(id, TransactionKind::Insert, &actor));
    }

    let cutoff = TransactionId(49_900);

    let legacy_start = Instant::now();
    let legacy = black_box(
        entries
            .iter()
            .filter(|entry| entry.id.0 > cutoff.0)
            .cloned()
            .collect::<Vec<_>>(),
    );
    let legacy_elapsed = legacy_start.elapsed();

    let optimized_start = Instant::now();
    let offset = first_record_index_after_id(&entries, cutoff);
    let optimized = black_box(entries[offset..].to_vec());
    let optimized_elapsed = optimized_start.elapsed();

    assert_eq!(legacy.len(), optimized.len());
    assert_eq!(legacy.first().map(|record| record.id), optimized.first().map(|record| record.id));
    assert_eq!(legacy.last().map(|record| record.id), optimized.last().map(|record| record.id));

    println!(
        "legacy_elapsed_ns={} optimized_elapsed_ns={}",
        legacy_elapsed.as_nanos(),
        optimized_elapsed.as_nanos(),
    );
}

#[test]
fn wal_hydration_benchmark_style_avoids_reloading_disk_state() {
    let temp_root = std::env::temp_dir().join(format!(
        "distdb-wal-hydration-benchmark-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));

    std::fs::create_dir_all(&temp_root).expect("temp wal dir should be created");

    let wal = ConcurrentWalManager::with_data_dir(temp_root.clone());
    let actor = UserId::from_username("tester");

    for id in 1..=10_000u64 {
        wal.append("users", make_record(id, TransactionKind::Insert, &actor))
            .expect("append should succeed");
    }

    let stream_key = super::obfuscated_stream_key("users").expect("stream key should resolve");
    let wal_file = temp_root.join(FileKind::Data.file_name(&stream_key));
    assert!(wal_file.exists(), "wal file should exist after appends");

    let baseline_start = Instant::now();
    let baseline = black_box((0..100).fold(0u64, |acc, _| {
        let records = load_records_from_file(&wal_file);
        acc + records.len() as u64
    }));
    let baseline_elapsed = baseline_start.elapsed();

    let optimized_start = Instant::now();
    let _optimized = black_box((0..100).fold(0u64, |acc, _| {
        acc + u64::from(wal.has_write_after("users", 0))
    }));
    let optimized_elapsed = optimized_start.elapsed();

    assert!(baseline > 0);

    println!(
        "wal_hydration_benchmark_style baseline_elapsed_ns={} optimized_elapsed_ns={}",
        baseline_elapsed.as_nanos(),
        optimized_elapsed.as_nanos(),
    );

    let _ = std::fs::remove_dir_all(temp_root);
}

#[test]
fn wal_decode_slice_vs_chunk_copy_benchmark_style() {
    let actor = UserId::from_username("tester");
    let total_records = 40_000u64;
    let measure_runs = 5u32;

    let mut wal_bytes = make_header(FileKind::Data).to_vec();
    for id in 1..=total_records {
        let record = TransactionRecord::with_payload(
            TransactionId(id),
            None,
            None,
            id,
            actor.clone(),
            TransactionKind::Insert,
            vec![id as u8; 64],
        );
        let frame = frame_record(&record).expect("frame encode should succeed");
        wal_bytes.extend_from_slice(&frame);
    }

    fn decode_all_no_copy(bytes: &[u8]) -> usize {
        let mut pos = HEADER_SIZE;
        let mut decoded = 0usize;

        while pos + 8 <= bytes.len() {
            let len = u64::from_le_bytes(
                bytes[pos..pos + 8]
                    .try_into()
                    .expect("slice is exactly 8 bytes"),
            ) as usize;
            pos += 8;

            if pos + len > bytes.len() {
                break;
            }

            let record = decode_record_from_storage(&bytes[pos..pos + len])
                .expect("slice decode should succeed");
            black_box(record);
            decoded += 1;
            pos += len;
        }

        decoded
    }

    fn decode_all_with_copy(bytes: &[u8]) -> usize {
        let mut pos = HEADER_SIZE;
        let mut decoded = 0usize;

        while pos + 8 <= bytes.len() {
            let len = u64::from_le_bytes(
                bytes[pos..pos + 8]
                    .try_into()
                    .expect("slice is exactly 8 bytes"),
            ) as usize;
            pos += 8;

            if pos + len > bytes.len() {
                break;
            }

            let copied = bytes[pos..pos + len].to_vec();
            let record = decode_record_from_storage(&copied)
                .expect("copied-frame decode should succeed");
            black_box(record);
            decoded += 1;
            pos += len;
        }

        decoded
    }

    assert_eq!(decode_all_no_copy(&wal_bytes), total_records as usize);
    assert_eq!(decode_all_with_copy(&wal_bytes), total_records as usize);

    let mut slice_ns = 0u128;
    let mut copy_ns = 0u128;

    for _ in 0..measure_runs {
        let start = Instant::now();
        let decoded = decode_all_no_copy(&wal_bytes);
        slice_ns += start.elapsed().as_nanos();
        black_box(decoded);
    }

    for _ in 0..measure_runs {
        let start = Instant::now();
        let decoded = decode_all_with_copy(&wal_bytes);
        copy_ns += start.elapsed().as_nanos();
        black_box(decoded);
    }

    let avg_slice_ns = slice_ns / measure_runs as u128;
    let avg_copy_ns = copy_ns / measure_runs as u128;

    let slice_per_record_ns = avg_slice_ns as f64 / total_records as f64;
    let copy_per_record_ns = avg_copy_ns as f64 / total_records as f64;
    let delta_pct = ((avg_copy_ns as f64 - avg_slice_ns as f64) / avg_slice_ns as f64) * 100.0;

    println!(
        "wal_decode_slice_vs_chunk_copy records={} runs={} avg_slice_ns={} avg_copy_ns={} slice_per_record_ns={:.2} copy_per_record_ns={:.2} copy_vs_slice_delta_pct={:.2}",
        total_records,
        measure_runs,
        avg_slice_ns,
        avg_copy_ns,
        slice_per_record_ns,
        copy_per_record_ns,
        delta_pct,
    );
}

#[test]
fn wal_decode_default_context_inline_vs_local_benchmark_style() {
    let actor = UserId::from_username("tester");
    let record = TransactionRecord::with_payload(
        TransactionId(1),
        None,
        None,
        1,
        actor,
        TransactionKind::Insert,
        vec![42; 256],
    );

    let encoded = encode_record_for_storage(&record).expect("record should encode");
    let iterations = 200_000usize;
    let runs = 8u32;

    fn decode_inline_default(encoded: &[u8]) -> TransactionRecord {
        decode_record_from_storage_with_context(encoded, &TransactionPayloadContext::default())
            .expect("inline default decode should succeed")
    }

    fn decode_local_default(encoded: &[u8]) -> TransactionRecord {
        let context = TransactionPayloadContext::default();
        decode_record_from_storage_with_context(encoded, &context)
            .expect("local default decode should succeed")
    }

    assert_eq!(decode_inline_default(&encoded), decode_local_default(&encoded));

    let mut inline_ns = 0u128;
    let mut local_ns = 0u128;

    for _ in 0..runs {
        let start = Instant::now();
        for _ in 0..iterations {
            black_box(decode_inline_default(&encoded));
        }
        inline_ns += start.elapsed().as_nanos();
    }

    for _ in 0..runs {
        let start = Instant::now();
        for _ in 0..iterations {
            black_box(decode_local_default(&encoded));
        }
        local_ns += start.elapsed().as_nanos();
    }

    let avg_inline_ns = inline_ns / runs as u128;
    let avg_local_ns = local_ns / runs as u128;
    let inline_per_op_ns = avg_inline_ns as f64 / iterations as f64;
    let local_per_op_ns = avg_local_ns as f64 / iterations as f64;
    let delta_pct = ((avg_inline_ns as f64 - avg_local_ns as f64) / avg_local_ns as f64) * 100.0;

    println!(
        "wal_decode_default_context_inline_vs_local iterations={} runs={} avg_inline_ns={} avg_local_ns={} inline_per_op_ns={:.2} local_per_op_ns={:.2} inline_vs_local_delta_pct={:.2}",
        iterations,
        runs,
        avg_inline_ns,
        avg_local_ns,
        inline_per_op_ns,
        local_per_op_ns,
        delta_pct,
    );
}

#[test]
fn compact_keeps_latest_schema_metadata_and_appends_truncate_marker() {
    let wal = ConcurrentWalManager::new();
    let actor = UserId::from_username("tester");

    wal.append("users", make_record(1, TransactionKind::Insert, &actor))
        .expect("append should succeed");
    wal.append(
        "users",
        make_record(2, TransactionKind::SchemaChange, &actor),
    )
    .expect("append should succeed");
    wal.append("users", make_record(3, TransactionKind::Update, &actor))
        .expect("append should succeed");
    wal.append(
        "users",
        make_record(4, TransactionKind::SecurityChange, &actor),
    )
    .expect("append should succeed");
    wal.append("users", make_record(5, TransactionKind::Delete, &actor))
        .expect("append should succeed");

    wal.compact_stream_to_latest_schema_and_metadata("users", actor, 99)
        .expect("compact should succeed");

    let records = wal.since("users", None);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].kind, TransactionKind::SchemaChange);
    assert_eq!(records[0].id, TransactionId(2));
    assert_eq!(records[1].kind, TransactionKind::SecurityChange);
    assert_eq!(records[1].id, TransactionId(4));
    assert_eq!(records[2].kind, TransactionKind::Truncate);
    assert_eq!(records[2].id, TransactionId(6));
    assert_eq!(records[2].refid, None);
    assert_eq!(records[2].timestamp_epoch_ms, 99);
}

#[test]
fn compact_clears_refids_to_removed_records() {
    let wal = ConcurrentWalManager::new();
    let actor = UserId::from_username("tester");

    wal.append(
        "users",
        make_record(1, TransactionKind::SchemaChange, &actor),
    )
    .expect("append should succeed");
    wal.append(
        "users",
        make_record(2, TransactionKind::MetadataChange, &actor),
    )
    .expect("append should succeed");
    wal.append(
        "users",
        TransactionRecord::with_payload(
            TransactionId(3),
            None,
            Some(TransactionId(1)),
            3,
            actor.clone(),
            TransactionKind::SchemaChange,
            vec![3],
        ),
    )
    .expect("append should succeed");

    wal.compact_stream_to_latest_schema_and_metadata("users", actor, 100)
        .expect("compact should succeed");

    let records = wal.since("users", None);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].id, TransactionId(2));
    assert_eq!(records[0].refid, None);
    assert_eq!(records[1].id, TransactionId(3));
    assert_eq!(records[1].refid, None);
    assert_eq!(records[2].kind, TransactionKind::Truncate);
    assert_eq!(records[2].id, TransactionId(4));
    assert_eq!(records[2].refid, Some(TransactionId(3)));
}

#[test]
fn compact_prefers_latest_metadata_change_record_when_present() {
    let wal = ConcurrentWalManager::new();
    let actor = UserId::from_username("tester");

    wal.append(
        "users",
        make_record(1, TransactionKind::SchemaChange, &actor),
    )
    .expect("append should succeed");
    wal.append(
        "users",
        make_record(2, TransactionKind::SecurityChange, &actor),
    )
    .expect("append should succeed");
    wal.append(
        "users",
        make_record(3, TransactionKind::MetadataChange, &actor),
    )
    .expect("append should succeed");

    wal.compact_stream_to_latest_schema_and_metadata("users", actor, 101)
        .expect("compact should succeed");

    let records = wal.since("users", None);
    assert_eq!(records.len(), 3);
    assert_eq!(records[1].kind, TransactionKind::MetadataChange);
}

#[test]
fn delete_stream_removes_in_memory_and_disk_state() {
    let temp_root = std::env::temp_dir().join(format!(
        "distdb-wal-delete-stream-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));

    std::fs::create_dir_all(&temp_root).expect("temp wal dir should be created");

    let wal = ConcurrentWalManager::with_data_dir(temp_root.clone());
    let actor = UserId::from_username("tester");
    wal.append("users", make_record(1, TransactionKind::Insert, &actor))
        .expect("append should succeed");

    let stream_key = super::obfuscated_stream_key("users").expect("stream key should resolve");
    let wal_file = temp_root.join(FileKind::Data.file_name(&stream_key));
    assert!(wal_file.exists());

    wal.delete_stream("users")
        .expect("delete stream should succeed");

    assert!(wal.since("users", None).is_empty());
    assert!(!wal_file.exists());

    let _ = std::fs::remove_dir_all(temp_root);
}

#[test]
fn clear_stream_records_clears_durable_disk_state() {
    let temp_root = std::env::temp_dir().join(format!(
        "distdb-wal-clear-stream-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));

    std::fs::create_dir_all(&temp_root).expect("temp wal dir should be created");

    let wal = ConcurrentWalManager::with_data_dir(temp_root.clone());
    let actor = UserId::from_username("tester");

    wal.append("users", make_record(1, TransactionKind::Insert, &actor))
        .expect("append should succeed");

    assert_eq!(wal.since("users", None).len(), 1);

    wal.clear_stream_records("users")
        .expect("clear stream should succeed");

    // The stream should remain empty after re-hydration from disk.
    assert!(wal.since("users", None).is_empty());

    wal.append("users", make_record(2, TransactionKind::Insert, &actor))
        .expect("append after clear should succeed");

    let records = wal.since("users", None);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, TransactionId(2));

    let _ = std::fs::remove_dir_all(temp_root);
}

#[test]
fn in_memory_mode_appends_without_filesystem_backing() {
    let wal = ConcurrentWalManager::in_memory();
    let actor = UserId::from_username("tester");

    wal.append("users", make_record(1, TransactionKind::Insert, &actor))
        .expect("append should succeed");

    assert!(wal.data_dir.is_none());

    let records = wal.since("users", None);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, TransactionKind::Insert);
}

#[test]
fn stream_mode_defaults_to_durable_and_can_be_set_ephemeral() {
    let wal = ConcurrentWalManager::new();

    assert_eq!(wal.stream_mode("users"), WalStreamMode::Durable);
    assert!(wal.is_stream_replicable("users"));

    wal.set_stream_mode("users", WalStreamMode::Ephemeral)
        .expect("setting stream mode should succeed");

    assert_eq!(wal.stream_mode("users"), WalStreamMode::Ephemeral);
    assert!(!wal.is_stream_replicable("users"));
}

#[test]
fn stream_mode_flip_after_activity_updates_replication_state() {
    let wal = ConcurrentWalManager::new();
    let actor = UserId::from_username("tester");

    wal.append("users", make_record(1, TransactionKind::Insert, &actor))
        .expect("append should succeed");

    assert!(wal.is_stream_replicable("users"));

    wal.set_stream_mode("users", WalStreamMode::Ephemeral)
        .expect("setting stream mode should succeed");

    assert_eq!(wal.stream_mode("users"), WalStreamMode::Ephemeral);
    assert!(!wal.is_stream_replicable("users"));

    wal.set_stream_mode("users", WalStreamMode::Durable)
        .expect("setting stream mode should succeed");

    assert_eq!(wal.stream_mode("users"), WalStreamMode::Durable);
    assert!(wal.is_stream_replicable("users"));
}

#[test]
fn append_batch_accepts_strictly_increasing_non_contiguous_ids() {
    let wal = ConcurrentWalManager::new();
    let actor = UserId::from_username("tester");

    wal.append_batch(
        "users",
        vec![
            make_record(10, TransactionKind::Insert, &actor),
            make_record(12, TransactionKind::Update, &actor),
            make_record(20, TransactionKind::Delete, &actor),
        ],
    )
    .expect("append batch should succeed for strictly increasing ids");

    let records = wal.since("users", None);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].id, TransactionId(10));
    assert_eq!(records[1].id, TransactionId(12));
    assert_eq!(records[2].id, TransactionId(20));
}

#[test]
fn ephemeral_stream_in_file_mode_keeps_data_in_memory_only() {
    let temp_root = std::env::temp_dir().join(format!(
        "distdb-wal-ephemeral-stream-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));

    std::fs::create_dir_all(&temp_root).expect("temp wal dir should be created");

    let wal = ConcurrentWalManager::with_data_dir(temp_root.clone());
    let actor = UserId::from_username("tester");

    wal.set_stream_mode("temp_users", WalStreamMode::Ephemeral)
        .expect("setting stream mode should succeed");

    wal.append("temp_users", make_record(1, TransactionKind::Insert, &actor))
        .expect("append should succeed");

    let stream_key = super::obfuscated_stream_key("temp_users")
        .expect("stream key should resolve");
    let wal_file = temp_root.join(FileKind::Data.file_name(&stream_key));

    assert!(!wal_file.exists());
    assert_eq!(wal.stream_mode("temp_users"), WalStreamMode::Ephemeral);
    assert!(!wal.is_stream_replicable("temp_users"));
    assert_eq!(wal.since("temp_users", None).len(), 1);

    let _ = std::fs::remove_dir_all(temp_root);
}

#[test]
fn encoded_storage_record_roundtrip_handles_large_payloads() {
    let actor = UserId::from_username("tester");
    let record = TransactionRecord::with_payload(
        TransactionId(1),
        None,
        None,
        1,
        actor,
        TransactionKind::Insert,
        vec![b'x'; 8192],
    );

    let stored = super::encode_record_for_storage(&record).expect("record should encode");
    let decoded = super::decode_record_from_storage(&stored).expect("record should decode");
    let raw = common::helpers::bincode_compat::serialize(&record).expect("record should serialize");

    assert_eq!(decoded, record);
    assert!(stored.len() < raw.len());
}

#[test]
fn decode_storage_record_accepts_legacy_uncompressed_bytes() {
    let actor = UserId::from_username("tester");
    let record = TransactionRecord::with_payload(
        TransactionId(7),
        None,
        None,
        7,
        actor,
        TransactionKind::Update,
        vec![1, 2, 3],
    );

    let legacy_raw = common::helpers::bincode_compat::serialize(&record).expect("legacy record should serialize");
    let decoded =
        super::decode_record_from_storage(&legacy_raw).expect("legacy record should decode");

    assert_eq!(decoded, record);
}

#[test]
fn encoded_storage_record_compresses_small_non_encrypted_payloads() {
    let actor = UserId::from_username("tester");
    let record = TransactionRecord::with_payload(
        TransactionId(8),
        None,
        None,
        8,
        actor,
        TransactionKind::Insert,
        b"ip_lookup:UNITED STATES".to_vec(),
    );

    let stored = super::encode_record_for_storage(&record).expect("record should encode");
    let decoded = super::decode_record_from_storage(&stored).expect("record should decode");
    let stored_record: TransactionRecord =
        common::helpers::bincode_compat::deserialize(&stored).expect("stored record should deserialize");

    assert_ne!(stored, common::helpers::bincode_compat::serialize(&record).expect("record should serialize"));
    assert!(
        stored_record
            .payload()
            .expect("payload should be present")
            .starts_with(&[0x78])
    );
    assert_eq!(decoded, record);
}

#[test]
fn decoded_storage_record_collapses_to_logical_payload_on_default_decode() {
    let actor = UserId::from_username("tester");
    let record = TransactionRecord::with_payload(
        TransactionId(9),
        None,
        None,
        9,
        actor,
        TransactionKind::Insert,
        b"ip_lookup:CANADA".to_vec(),
    );

    let stored = super::encode_record_for_storage(&record).expect("record should encode");
    let decoded = super::decode_record_from_storage(&stored).expect("record should decode");

    assert_eq!(decoded.payload(), Some(&b"ip_lookup:CANADA"[..]));
    assert_eq!(decoded.payload_raw(), Some(&b"ip_lookup:CANADA"[..]));
}

#[test]
fn encoded_storage_record_skips_compression_for_encrypted_mutation_payloads() {
    let actor = UserId::from_username("tester");
    let encrypted_payload = crate::engine::database::row_payload::
        encode_encrypted_row_payload_envelope(
            1,
            vec![7; 12],
            vec![9; 16],
            std::iter::repeat_n(b'x', 16384).collect(),
        )
        .expect("encrypted payload should encode");

    let record = TransactionRecord::with_payload(
        TransactionId(9),
        None,
        None,
        9,
        actor,
        TransactionKind::Insert,
        encrypted_payload,
    );

    let raw = common::helpers::bincode_compat::serialize(&record).expect("record should serialize");
    let stored = super::encode_record_for_storage(&record).expect("record should encode");
    let decoded = super::decode_record_from_storage(&stored).expect("record should decode");

    assert_eq!(stored, raw);
    assert_eq!(decoded, record);
}

#[test]
fn storage_write_and_read_chains_roundtrip_plaintext_payload() {
    let actor = UserId::from_username("tester");
    let record = TransactionRecord::with_payload(
        TransactionId(10),
        None,
        None,
        10,
        actor,
        TransactionKind::Insert,
        b"roundtrip-chain-payload".to_vec(),
    );

    let stored = super::encode_record_for_storage(&record).expect("record should encode");
    let decoded = super::decode_record_from_storage(&stored).expect("record should decode");
    let stored_record: TransactionRecord =
        common::helpers::bincode_compat::deserialize(&stored).expect("stored record should deserialize");

    assert_eq!(decoded.payload_logical(), record.payload_raw());
    assert_ne!(stored_record.payload_raw(), record.payload_raw());
}

#[test]
fn storage_encode_with_encryption_context_encrypts_payload() {
    let actor = UserId::from_username("tester");
    let context = crate::TransactionPayloadContext::new()
        .with_database_id("main")
        .with_table_id("users")
        .with_at_rest_encryption("enc:node-main:db-main", 1);
    let record = TransactionRecord::with_payload(
        TransactionId(11),
        None,
        None,
        11,
        actor,
        TransactionKind::Insert,
        b"needs-encryption".to_vec(),
    );

    let stored = super::encode_record_for_storage_with_context(&record, &context)
        .expect("encryption should succeed with configured provider");

    let stored_record: TransactionRecord =
        common::helpers::bincode_compat::deserialize(&stored).expect("stored record should deserialize");

    let stored_payload = stored_record
        .payload_raw()
        .expect("stored payload should be present");

    assert!(
        crate::engine::database::row_payload::looks_like_encrypted_row_payload(stored_payload),
        "stored payload should be encrypted envelope"
    );

    let decoded = super::decode_record_from_storage_with_context(&stored, &context)
        .expect("decode with context should succeed");

    assert_eq!(decoded.payload_logical(), Some(&b"needs-encryption"[..]));
}

#[test]
fn storage_decode_with_encryption_context_rejects_mismatched_key_material() {
    let actor = UserId::from_username("tester");
    let encrypted_payload = crate::engine::database::row_payload::
        encode_encrypted_row_payload_envelope(
            1,
            vec![7; 12],
            vec![9; 16],
            b"ciphertext".to_vec(),
        )
        .expect("encrypted payload should encode");
    let write_context = crate::TransactionPayloadContext::new()
        .with_database_id("main")
        .with_table_id("users")
        .with_at_rest_encryption("enc:node-main:db-main", 1);
    let read_context = crate::TransactionPayloadContext::new()
        .with_database_id("main")
        .with_table_id("users")
        .with_at_rest_encryption("enc:node-main:db-other", 1);
    let record = TransactionRecord::with_payload(
        TransactionId(12),
        None,
        None,
        12,
        actor,
        TransactionKind::Insert,
        encrypted_payload,
    );

    let encrypted_stored = super::encode_record_for_storage_with_context(&record, &write_context)
        .expect("encryption should succeed");

    let err = super::decode_record_from_storage_with_context(&encrypted_stored, &read_context)
        .expect_err("mismatched key material should fail decrypt");

    assert_eq!(err, "failed to deserialize WAL record");
}

