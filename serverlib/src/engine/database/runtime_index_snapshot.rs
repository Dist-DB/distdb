use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use common::helpers::format::{make_header, verify_header, FileKind, HEADER_SIZE};
use common::helpers::hash::stable_id;
use common::helpers::io::{read_bytes, write_bytes_atomic};
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;

use super::index::DatabaseIndex;
use super::table::DatabaseTable;
use crate::{
    EqualityTableCacheSnapshot,
    TableSchema,
    snapshot_equality_cache,
};

const RUNTIME_INDEX_SNAPSHOT_FILE_STEM_PREFIX: &str = "rtix";
const LIVE_ROW_CHECKPOINT_FILE_STEM_PREFIX: &str = "lrows";
const ACCESSOR_CACHE_SNAPSHOT_FILE_STEM_PREFIX: &str = "acix";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct RuntimeIndexTableSnapshot {
    pub(crate) table_id: String,
    pub(crate) latest_tx_id: u64,
    pub(crate) schema_fingerprint: String,
    pub(crate) live_row_count: usize,
    #[serde(default)]
    pub(crate) wal_size_bytes: u64,
    #[serde(default)]
    pub(crate) wal_modified_epoch_ms: u64,
    pub(crate) indexes: Vec<RuntimeIndexSnapshotIndex>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct RuntimeIndexSnapshotIndex {
    pub(crate) index_id: String,
    pub(crate) entries: Vec<Vec<Vec<u8>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedRuntimeIndexSnapshot {
    pub(crate) snapshot: RuntimeIndexTableSnapshot,
    pub(crate) legacy_plain_encoding: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TableLiveRowCheckpoint {
    pub(crate) table_id: String,
    pub(crate) latest_tx_id: u64,
    pub(crate) schema_fingerprint: String,
    pub(crate) wal_size_bytes: u64,
    pub(crate) wal_modified_epoch_ms: u64,
    pub(crate) live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TableAccessorCacheSnapshot {
    pub(crate) table_id: String,
    pub(crate) latest_tx_id: u64,
    pub(crate) schema_fingerprint: String,
    pub(crate) wal_size_bytes: u64,
    pub(crate) wal_modified_epoch_ms: u64,
    pub(crate) live_row_count: usize,
    pub(crate) warm_fields: Vec<String>,
    pub(crate) cache: EqualityTableCacheSnapshot,
}

pub(crate) struct RuntimeIndexSnapshotService;

impl RuntimeIndexSnapshotService {

    pub(crate) fn wal_stream_fingerprint(data_dir: &Path, table_stream_id: &str) -> Option<(u64, u64)> {

        let path = Self::wal_stream_path(data_dir, table_stream_id);
        let metadata = fs::metadata(path).ok()?;

        let modified_epoch_ms = metadata
            .modified()
            .ok()?
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis() as u64;

        Some((metadata.len(), modified_epoch_ms))

    }

    #[expect(clippy::type_complexity, reason="returning a tuple of (latest_tx_id, live_rows)")]
    pub(crate) fn load_live_row_checkpoint_rows(
        data_dir: &Path,
        table_stream_id: &str,
        table_id: &str,
        schema: &TableSchema,
    ) -> Option<(u64, Vec<(u64, HashMap<String, Vec<u8>>)>)> {

        let checkpoint_path = Self::live_row_checkpoint_path(data_dir, table_stream_id);
        let bytes = read_bytes(&checkpoint_path).ok()?;

        if verify_header(FileKind::Entity, &bytes).is_err() || bytes.len() <= HEADER_SIZE {
            return None;
        }

        let (checkpoint, _legacy_plain_encoding): (TableLiveRowCheckpoint, bool) =
            decode_snapshot_payload(&bytes[HEADER_SIZE..])?;

        let schema_fingerprint = table_schema_fingerprint_for_parts(table_id, schema)?;

        if checkpoint.table_id != table_id || checkpoint.schema_fingerprint != schema_fingerprint {
            return None;
        }

        let (wal_size_bytes, wal_modified_epoch_ms) = Self::wal_stream_fingerprint(data_dir, table_stream_id)?;
        if checkpoint.wal_size_bytes != wal_size_bytes
            || checkpoint.wal_modified_epoch_ms != wal_modified_epoch_ms
        {
            return None;
        }

        Some((checkpoint.latest_tx_id, checkpoint.live_rows))

    }

    pub(crate) fn load_runtime_index_snapshot(
        data_dir: &Path,
        table: &DatabaseTable,
        table_stream_id: &str,
        tracked_indexes: &[DatabaseIndex],
        wal_fingerprint: Option<(u64, u64)>,
    ) -> Option<LoadedRuntimeIndexSnapshot> {

        let snapshot_path = Self::runtime_index_snapshot_path(data_dir, table_stream_id);
        let bytes = read_bytes(&snapshot_path).ok()?;

        if verify_header(FileKind::Entity, &bytes).is_err() || bytes.len() <= HEADER_SIZE {
            return None;
        }

        let (snapshot, legacy_plain_encoding): (RuntimeIndexTableSnapshot, bool) =
            decode_snapshot_payload(&bytes[HEADER_SIZE..])?;

        let schema_fingerprint = table_schema_fingerprint(table)?;

        if snapshot.table_id != table.table_id
            || snapshot.schema_fingerprint != schema_fingerprint
        {
            return None;
        }

        #[expect(clippy::question_mark, reason="we want to return None if the wal fingerprint is unavailable")]
        let Some((wal_size_bytes, wal_modified_epoch_ms)) = wal_fingerprint else {
            return None;
        };

        if snapshot.wal_size_bytes != wal_size_bytes
            || snapshot.wal_modified_epoch_ms != wal_modified_epoch_ms
        {
            return None;
        }

        let snapshot_index_ids = snapshot
            .indexes
            .iter()
            .map(|index| index.index_id.as_str())
            .collect::<HashSet<_>>();

        if tracked_indexes
            .iter()
            .any(|index| !snapshot_index_ids.contains(index.index_id.0.as_str()))
        {
            return None;
        }

        Some(LoadedRuntimeIndexSnapshot {
            snapshot,
            legacy_plain_encoding,
        })

    }

    pub(crate) fn load_live_row_checkpoint(
        data_dir: &Path,
        table: &DatabaseTable,
        table_stream_id: &str,
        wal_fingerprint: Option<(u64, u64)>,
    ) -> Option<TableLiveRowCheckpoint> {

        let checkpoint_path = Self::live_row_checkpoint_path(data_dir, table_stream_id);
        let bytes = read_bytes(&checkpoint_path).ok()?;

        if verify_header(FileKind::Entity, &bytes).is_err() || bytes.len() <= HEADER_SIZE {
            return None;
        }

        let (checkpoint, _legacy_plain_encoding): (TableLiveRowCheckpoint, bool) =
            decode_snapshot_payload(&bytes[HEADER_SIZE..])?;

        let schema_fingerprint = table_schema_fingerprint(table)?;

        if checkpoint.table_id != table.table_id || checkpoint.schema_fingerprint != schema_fingerprint {
            return None;
        }

        #[expect(clippy::question_mark, reason="we want to return None if the wal fingerprint is unavailable")]
        let Some((wal_size_bytes, wal_modified_epoch_ms)) = wal_fingerprint else {
            return None;
        };

        if checkpoint.wal_size_bytes != wal_size_bytes
            || checkpoint.wal_modified_epoch_ms != wal_modified_epoch_ms
        {
            return None;
        }

        Some(checkpoint)

    }

    pub(crate) fn load_accessor_cache_snapshot(
        data_dir: &Path,
        table: &DatabaseTable,
        table_stream_id: &str,
        wal_fingerprint: Option<(u64, u64)>,
        warm_fields: &[String],
    ) -> Option<TableAccessorCacheSnapshot> {

        let snapshot_path = Self::accessor_cache_snapshot_path(data_dir, table_stream_id);
        let bytes = read_bytes(&snapshot_path).ok()?;

        if verify_header(FileKind::Entity, &bytes).is_err() || bytes.len() <= HEADER_SIZE {
            return None;
        }

        let (snapshot, _legacy_plain_encoding): (TableAccessorCacheSnapshot, bool) =
            decode_snapshot_payload(&bytes[HEADER_SIZE..])?;

        let schema_fingerprint = table_schema_fingerprint(table)?;

        if snapshot.table_id != table.table_id || snapshot.schema_fingerprint != schema_fingerprint {
            return None;
        }

        #[expect(clippy::question_mark, reason="we want to return None if the wal fingerprint is unavailable")]
        let Some((wal_size_bytes, wal_modified_epoch_ms)) = wal_fingerprint else {
            return None;
        };

        if snapshot.wal_size_bytes != wal_size_bytes
            || snapshot.wal_modified_epoch_ms != wal_modified_epoch_ms
        {
            return None;
        }

        if !warm_fields
            .iter()
            .all(|field_name| snapshot.warm_fields.iter().any(|saved| saved == field_name))
        {
            return None;
        }

        Some(snapshot)

    }

    pub(crate) fn save_runtime_index_snapshot(
        data_dir: &Path,
        table: &DatabaseTable,
        table_stream_id: &str,
        latest_tx_id: u64,
        live_row_count: usize,
        wal_fingerprint: Option<(u64, u64)>,
        indexes: Vec<RuntimeIndexSnapshotIndex>,
    ) -> Result<(), String> {

        let (wal_size_bytes, wal_modified_epoch_ms) = wal_fingerprint
            .ok_or_else(|| "wal fingerprint unavailable".to_string())?;

        let expected_wal_fingerprint = (wal_size_bytes, wal_modified_epoch_ms);

        if Self::wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
            return Err("wal fingerprint changed before snapshot write".to_string());
        }

        let schema_fingerprint = table_schema_fingerprint(table)
            .ok_or_else(|| "schema fingerprint serialization failed".to_string())?;

        let snapshot = RuntimeIndexTableSnapshot {
            table_id: table.table_id.clone(),
            latest_tx_id,
            schema_fingerprint,
            live_row_count,
            wal_size_bytes,
            wal_modified_epoch_ms,
            indexes,
        };

        let mut content = make_header(FileKind::Entity).to_vec();
        let payload = encode_snapshot_payload(&snapshot)?;
        content.extend_from_slice(&payload);

        let snapshot_path = Self::runtime_index_snapshot_path(data_dir, table_stream_id);
        write_bytes_atomic(&snapshot_path, &content)
            .map_err(|err| format!("snapshot write failed: {err}"))?;

        if Self::wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
            let _ = fs::remove_file(&snapshot_path);
            return Err("wal fingerprint changed after snapshot write".to_string());
        }

        Ok(())

    }

    pub(crate) fn save_live_row_checkpoint(
        data_dir: &Path,
        table: &DatabaseTable,
        table_stream_id: &str,
        latest_tx_id: u64,
        wal_fingerprint: Option<(u64, u64)>,
        live_rows: &[(u64, HashMap<String, Vec<u8>>) ],
    ) -> Result<(), String> {

        let (wal_size_bytes, wal_modified_epoch_ms) = wal_fingerprint
            .ok_or_else(|| "wal fingerprint unavailable".to_string())?;

        let expected_wal_fingerprint = (wal_size_bytes, wal_modified_epoch_ms);

        if Self::wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
            return Err("wal fingerprint changed before live-row checkpoint write".to_string());
        }

        let schema_fingerprint = table_schema_fingerprint(table)
            .ok_or_else(|| "schema fingerprint serialization failed".to_string())?;

        let checkpoint = TableLiveRowCheckpoint {
            table_id: table.table_id.clone(),
            latest_tx_id,
            schema_fingerprint,
            wal_size_bytes,
            wal_modified_epoch_ms,
            live_rows: live_rows.to_vec(),
        };

        let mut content = make_header(FileKind::Entity).to_vec();
        let payload = encode_snapshot_payload(&checkpoint)?;
        content.extend_from_slice(&payload);

        let checkpoint_path = Self::live_row_checkpoint_path(data_dir, table_stream_id);
        write_bytes_atomic(&checkpoint_path, &content)
            .map_err(|err| format!("live-row checkpoint write failed: {err}"))?;

        if Self::wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
            let _ = fs::remove_file(&checkpoint_path);
            return Err("wal fingerprint changed after live-row checkpoint write".to_string());
        }

        Ok(())

    }

    pub(crate) fn save_accessor_cache_snapshot(
        data_dir: &Path,
        table: &DatabaseTable,
        table_stream_id: &str,
        latest_tx_id: u64,
        wal_fingerprint: Option<(u64, u64)>,
        warm_fields: &[String],
        cache_scope_id: usize,
    ) -> Result<(), String> {

        let (wal_size_bytes, wal_modified_epoch_ms) = wal_fingerprint
            .ok_or_else(|| "wal fingerprint unavailable".to_string())?;

        let expected_wal_fingerprint = (wal_size_bytes, wal_modified_epoch_ms);

        if Self::wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
            return Err("wal fingerprint changed before accessor cache snapshot write".to_string());
        }

        let schema_fingerprint = table_schema_fingerprint(table)
            .ok_or_else(|| "schema fingerprint serialization failed".to_string())?;

        let cache = snapshot_equality_cache(cache_scope_id, table_stream_id)
            .ok_or_else(|| "equality cache snapshot missing".to_string())?;

        let snapshot = TableAccessorCacheSnapshot {
            table_id: table.table_id.clone(),
            latest_tx_id,
            schema_fingerprint,
            wal_size_bytes,
            wal_modified_epoch_ms,
            live_row_count: cache.rows_by_id.len(),
            warm_fields: warm_fields.to_vec(),
            cache,
        };

        let mut content = make_header(FileKind::Entity).to_vec();
        let payload = encode_snapshot_payload(&snapshot)?;
        content.extend_from_slice(&payload);

        let snapshot_path = Self::accessor_cache_snapshot_path(data_dir, table_stream_id);
        write_bytes_atomic(&snapshot_path, &content)
            .map_err(|err| format!("accessor cache snapshot write failed: {err}"))?;

        if Self::wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
            let _ = fs::remove_file(&snapshot_path);
            return Err("wal fingerprint changed after accessor cache snapshot write".to_string());
        }

        Ok(())

    }

    fn runtime_index_snapshot_path(data_dir: &Path, table_stream_id: &str) -> PathBuf {
        let table_key = stable_id(&[table_stream_id]);
        let stem = format!("{}_{}", RUNTIME_INDEX_SNAPSHOT_FILE_STEM_PREFIX, table_key);

        data_dir
            .join("runtime-index")
            .join(FileKind::Entity.file_name(stem))
    }

    fn accessor_cache_snapshot_path(data_dir: &Path, table_stream_id: &str) -> PathBuf {
        let table_key = stable_id(&[table_stream_id]);
        let stem = format!("{}_{}", ACCESSOR_CACHE_SNAPSHOT_FILE_STEM_PREFIX, table_key);

        data_dir
            .join("accessor-cache")
            .join(FileKind::Entity.file_name(stem))
    }

    fn live_row_checkpoint_path(data_dir: &Path, table_stream_id: &str) -> PathBuf {
        let table_key = stable_id(&[table_stream_id]);
        let stem = format!("{}_{}", LIVE_ROW_CHECKPOINT_FILE_STEM_PREFIX, table_key);

        data_dir
            .join("live-rows")
            .join(FileKind::Entity.file_name(stem))
    }

    fn wal_stream_path(data_dir: &Path, table_stream_id: &str) -> PathBuf {
        let stream_key = stable_id(&[table_stream_id]);
        data_dir.join(FileKind::Data.file_name(stream_key))
    }
}

fn table_schema_fingerprint(table: &DatabaseTable) -> Option<String> {
    table_schema_fingerprint_for_parts(&table.table_id, table.schema())
}

fn table_schema_fingerprint_for_parts(
    table_id: &str,
    schema: &TableSchema,
) -> Option<String> {
    let encoded = bincode::serialize(schema).ok()?;

    let hex = encoded
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>();

    Some(stable_id(&[table_id, &hex]))
}

fn encode_snapshot_payload<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let raw = bincode::serialize(value)
        .map_err(|_| "snapshot serialization failed".to_string())?;

    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());

    encoder
        .write_all(&raw)
        .map_err(|_| "snapshot compression failed".to_string())?;

    encoder
        .finish()
        .map_err(|_| "snapshot compression finish failed".to_string())
}

fn decode_snapshot_payload<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Option<(T, bool)> {
    if let Ok(decoded) = bincode::deserialize::<T>(payload) {
        return Some((decoded, true));
    }

    let decoder = ZlibDecoder::new(payload);
    let mut reader = BufReader::new(decoder);

    bincode::deserialize_from::<_, T>(&mut reader)
        .ok()
        .map(|decoded| (decoded, false))
}