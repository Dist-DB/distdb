use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use super::*;

fn make_temp_data_dir() -> PathBuf {
    let unique = format!(
        "rtix-snapshot-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0),
    );
    let dir = std::env::temp_dir().join(unique);
    fs::create_dir_all(dir.join("runtime-index")).expect("create runtime-index test dir");
    dir
}

#[test]
fn chunked_loader_restores_declared_empty_indexes() {
    let data_dir = make_temp_data_dir();
    let manifest = RuntimeIndexTableSnapshotChunkedManifest {
        table_id: "users".to_string(),
        latest_tx_id: 10,
        schema_fingerprint: "schema-v1".to_string(),
        live_row_count: 0,
        wal_size_bytes: 1,
        wal_modified_epoch_ms: 1,
        empty_index_ids: vec!["ind:users:display_name".to_string()],
        chunk_refs: Vec::new(),
    };
    let snapshot = RuntimeIndexSnapshotService::load_runtime_index_snapshot_chunks(
        &data_dir, "users", &manifest, None,
    )
    .expect("load chunked snapshot");
    assert_eq!(snapshot.indexes.len(), 1);
    assert_eq!(snapshot.indexes[0].index_id, "ind:users:display_name");
    assert!(snapshot.indexes[0].entries.is_empty());
    let _ = fs::remove_dir_all(data_dir);
}

#[test]
fn chunked_loader_merges_empty_and_chunked_indexes() {
    let data_dir = make_temp_data_dir();
    let index_id = "ind:users:email".to_string();
    let table_stream_id = "users";
    let file_name = RuntimeIndexSnapshotService::runtime_index_snapshot_chunk_file_name(
        table_stream_id, &index_id, 0,
    );
    let chunk_path = RuntimeIndexSnapshotService::runtime_index_snapshot_chunk_path(
        &data_dir, &file_name,
    );
    let payload = RuntimeIndexSnapshotChunkPayload {
        index_id: index_id.clone(),
        entries: vec![vec![b"alice@example.com".to_vec()]],
        row_refs_by_entry: vec![42],
        postings_by_entry: vec![Vec::new()],
        row_refs: Vec::new(),
    };
    let mut encoded_chunk = make_header(FileKind::Entity).to_vec();
    encoded_chunk.extend_from_slice(
        &encode_snapshot_payload(&payload).expect("encode chunk payload"),
    );
    write_bytes_atomic(&chunk_path, &encoded_chunk).expect("write chunk payload");
    let manifest = RuntimeIndexTableSnapshotChunkedManifest {
        table_id: "users".to_string(),
        latest_tx_id: 11,
        schema_fingerprint: "schema-v1".to_string(),
        live_row_count: 1,
        wal_size_bytes: 2,
        wal_modified_epoch_ms: 2,
        empty_index_ids: vec!["ind:users:display_name".to_string()],
        chunk_refs: vec![RuntimeIndexSnapshotChunkRef {
            index_id: index_id.clone(),
            chunk_seq: 0,
            file_name,
        }],
    };
    let snapshot = RuntimeIndexSnapshotService::load_runtime_index_snapshot_chunks(
        &data_dir, table_stream_id, &manifest, None,
    )
    .expect("load chunked snapshot");
    assert_eq!(snapshot.indexes.len(), 2);
    let email_index = snapshot
        .indexes
        .iter()
        .find(|index| index.index_id == index_id)
        .expect("email index present");
    assert_eq!(email_index.entries.len(), 1);
    assert_eq!(email_index.row_refs_by_entry, vec![42]);
    let empty_index = snapshot
        .indexes
        .iter()
        .find(|index| index.index_id == "ind:users:display_name")
        .expect("empty index present");
    assert!(empty_index.entries.is_empty());
    let _ = fs::remove_dir_all(data_dir);
}

#[test]
fn save_and_load_snapshot_preserves_all_row_refs_for_duplicate_non_unique_key() {
    let data_dir = make_temp_data_dir();
    let table_stream_id = "places";
    let wal_path = RuntimeIndexSnapshotService::wal_stream_path(&data_dir, table_stream_id);
    fs::create_dir_all(wal_path.parent().expect("wal path has parent"))
        .expect("create wal parent dir");
    fs::write(&wal_path, b"fake-wal-bytes").expect("write fake wal file");
    let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(&data_dir, table_stream_id);
    let schema = TableSchema::new(vec![
        crate::FieldDef {
            field_name: "id".to_string(), seqno: 1, field_type: crate::FieldType::UInt(64),
            indexed: crate::FieldIndex::PrimaryKey, nullable: false, default_value: None, metadata: None,
        },
        crate::FieldDef {
            field_name: "display_name".to_string(), seqno: 2, field_type: crate::FieldType::Text,
            indexed: crate::FieldIndex::Indexed, nullable: false, default_value: None, metadata: None,
        },
    ]);
    let index = DatabaseIndex::from_table_fields(
        table_stream_id, crate::DatabaseIndexKind::Indexed, vec!["display_name".to_string()],
    );
    let mut indexes = HashMap::new();
    indexes.insert(index.index_id.0.clone(), index.clone());
    let table = DatabaseTable::new(table_stream_id.to_string(), schema, indexes);
    let cologne_key = vec![b"Cologne".to_vec()];
    let snapshot_index = RuntimeIndexSnapshotIndex {
        index_id: index.index_id.0.clone(), entries: vec![cologne_key.clone()],
        row_refs_by_entry: Vec::new(),
        postings_by_entry: vec![(1..=10u64).collect()],
        row_refs: Vec::new(),
    };
    RuntimeIndexSnapshotService::save_runtime_index_snapshot(
        &data_dir, &table, table_stream_id, 100, 10, wal_fingerprint, vec![snapshot_index],
    ).expect("save runtime index snapshot");
    let loaded = RuntimeIndexSnapshotService::load_runtime_index_snapshot(
        &data_dir, &table, table_stream_id, std::slice::from_ref(&index), wal_fingerprint,
    ).expect("load runtime index snapshot");
    let restored_index = loaded.snapshot.indexes.iter()
        .find(|snapshot_index| snapshot_index.index_id == index.index_id.0)
        .expect("display_name index present in restored snapshot");
    assert_eq!(
        restored_index.postings_by_entry,
        vec![(1..=10u64).collect::<Vec<_>>()],
    );
    let _ = fs::remove_dir_all(data_dir);
}
