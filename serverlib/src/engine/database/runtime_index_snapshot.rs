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

use super::databaseindex::DatabaseIndex;
use super::table::DatabaseTable;
use crate::{
    EqualityTableCacheSnapshot,
    TableSchema,
    snapshot_equality_cache,
};

const RUNTIME_INDEX_SNAPSHOT_FILE_STEM_PREFIX: &str = "rtix";
const LIVE_ROW_CHECKPOINT_FILE_STEM_PREFIX: &str = "lrows";
const LIVE_ROW_COUNT_CHECKPOINT_FILE_STEM_PREFIX: &str = "lrcnt";
const ACCESSOR_CACHE_SNAPSHOT_FILE_STEM_PREFIX: &str = "acix";
const LIVE_ROW_CHECKPOINT_COMPRESS_MAX_ROWS: usize = 150_000;
const LIVE_ROW_CHECKPOINT_MAX_BYTES_DEFAULT: u64 = 256 * 1024 * 1024;
const ACCESSOR_SNAPSHOT_PERSIST_ROWS_DEFAULT: bool = false;

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
    #[serde(default)]
    pub(crate) row_refs_by_entry: Vec<u64>,
    #[serde(default)]
    pub(crate) row_refs: Vec<(Vec<Vec<u8>>, u64)>,
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
pub(crate) struct TableLiveRowCountCheckpoint {
    pub(crate) table_id: String,
    pub(crate) latest_tx_id: u64,
    pub(crate) schema_fingerprint: String,
    pub(crate) wal_size_bytes: u64,
    pub(crate) wal_modified_epoch_ms: u64,
    pub(crate) live_row_count: usize,
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

    fn accessor_snapshot_persist_rows() -> bool {

        std::env::var("DISTDB_ACCESSOR_SNAPSHOT_PERSIST_ROWS")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(ACCESSOR_SNAPSHOT_PERSIST_ROWS_DEFAULT)

    }

    fn live_row_checkpoint_compress_max_rows() -> usize {

        std::env::var("DISTDB_LIVE_ROW_CHECKPOINT_COMPRESS_MAX_ROWS")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(LIVE_ROW_CHECKPOINT_COMPRESS_MAX_ROWS)

    }

    fn live_row_checkpoint_max_bytes() -> u64 {

        std::env::var("DISTDB_LIVE_ROW_CHECKPOINT_MAX_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(LIVE_ROW_CHECKPOINT_MAX_BYTES_DEFAULT)

    }

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
        let metadata = fs::metadata(&checkpoint_path).ok()?;

        if metadata.len() > Self::live_row_checkpoint_max_bytes() {
            let _ = fs::remove_file(&checkpoint_path);

            log::debug!(
                "live-row checkpoint restore skipped table={} stream={} reason=oversized_checkpoint file_bytes={} max_bytes={}",
                table_id,
                table_stream_id,
                metadata.len(),
                Self::live_row_checkpoint_max_bytes(),
            );

            return None;
        }

        let bytes = read_bytes(&checkpoint_path).ok()?;

        if verify_header(FileKind::Entity, &bytes).is_err() || bytes.len() <= HEADER_SIZE {
            return None;
        }

        let (checkpoint, legacy_plain_encoding): (TableLiveRowCheckpoint, bool) =
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

        if !legacy_plain_encoding
            && checkpoint.live_rows.len() > Self::live_row_checkpoint_compress_max_rows()
        {
            let mut rewritten = make_header(FileKind::Entity).to_vec();
            if let Ok(payload) = common::helpers::bincode_compat::serialize(&checkpoint) {
                rewritten.extend_from_slice(&payload);
                if let Err(err) = write_bytes_atomic(&checkpoint_path, &rewritten) {
                    log::debug!(
                        "live-row checkpoint plain rewrite skipped table={} stream={} reason={}",
                        table_id,
                        table_stream_id,
                        err,
                    );
                }
            }
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
            log::debug!(
                "runtime index snapshot restore miss table={} stream={} reason=invalid_header_or_empty",
                table.table_id,
                table_stream_id,
            );
            return None;
        }

        let (snapshot, legacy_plain_encoding): (RuntimeIndexTableSnapshot, bool) =
            match decode_snapshot_payload_with_reason(&bytes[HEADER_SIZE..]) {
                Ok(decoded) => decoded,
                Err(reason) => {
                    let _ = fs::remove_file(&snapshot_path);

                    log::warn!(
                        "runtime index snapshot file removed after decode failure table={} stream={} path={} reason={}",
                        table.table_id,
                        table_stream_id,
                        snapshot_path.display(),
                        reason,
                    );

                    log::debug!(
                        "runtime index snapshot restore miss table={} stream={} reason=decode_failed",
                        table.table_id,
                        table_stream_id,
                    );
                    return None;
                }
            };

        let schema_fingerprint = match table_schema_fingerprint(table) {
            Some(fingerprint) => fingerprint,
            None => {
                log::debug!(
                    "runtime index snapshot restore miss table={} stream={} reason=schema_fingerprint_unavailable",
                    table.table_id,
                    table_stream_id,
                );
                return None;
            }
        };

        if snapshot.table_id != table.table_id
            || snapshot.schema_fingerprint != schema_fingerprint
        {
            log::debug!(
                "runtime index snapshot restore miss table={} stream={} reason=table_or_schema_mismatch snapshot_table={} snapshot_schema={} current_schema={}",
                table.table_id,
                table_stream_id,
                snapshot.table_id,
                snapshot.schema_fingerprint,
                schema_fingerprint,
            );
            return None;
        }

        let Some((wal_size_bytes, wal_modified_epoch_ms)) = wal_fingerprint else {
            log::debug!(
                "runtime index snapshot restore miss table={} stream={} reason=wal_fingerprint_unavailable",
                table.table_id,
                table_stream_id,
            );
            return None;
        };

        if snapshot.wal_size_bytes != wal_size_bytes
            || snapshot.wal_modified_epoch_ms != wal_modified_epoch_ms
        {
            log::debug!(
                "runtime index snapshot restore miss table={} stream={} reason=wal_fingerprint_mismatch snapshot=({}, {}) current=({}, {})",
                table.table_id,
                table_stream_id,
                snapshot.wal_size_bytes,
                snapshot.wal_modified_epoch_ms,
                wal_size_bytes,
                wal_modified_epoch_ms,
            );
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
            let missing = tracked_indexes
                .iter()
                .filter(|index| !snapshot_index_ids.contains(index.index_id.0.as_str()))
                .map(|index| index.index_id.0.clone())
                .collect::<Vec<_>>()
                .join(",");

            log::debug!(
                "runtime index snapshot restore miss table={} stream={} reason=tracked_index_missing missing_indexes={}",
                table.table_id,
                table_stream_id,
                missing,
            );
            return None;
        }

        let (index_count, entry_count, key_bytes, row_refs_legacy_count, row_refs_compact_count) =
            snapshot_memory_shape(&snapshot);

        log::info!(
            "runtime index snapshot restore shape table={} stream={} file_bytes={} indexes={} entries={} key_bytes={} row_refs_legacy={} row_refs_compact={} legacy_plain_encoding={}",
            table.table_id,
            table_stream_id,
            bytes.len(),
            index_count,
            entry_count,
            key_bytes,
            row_refs_legacy_count,
            row_refs_compact_count,
            legacy_plain_encoding,
        );

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

    pub(crate) fn load_live_row_count_checkpoint(
        data_dir: &Path,
        table_stream_id: &str,
        table_id: &str,
        schema: &TableSchema,
    ) -> Option<(u64, usize)> {

        let checkpoint_path = Self::live_row_count_checkpoint_path(data_dir, table_stream_id);
        let bytes = read_bytes(&checkpoint_path).ok()?;

        if verify_header(FileKind::Entity, &bytes).is_err() || bytes.len() <= HEADER_SIZE {
            return None;
        }

        let (checkpoint, _legacy_plain_encoding): (TableLiveRowCountCheckpoint, bool) =
            decode_snapshot_payload(&bytes[HEADER_SIZE..])?;

        let schema_fingerprint = table_schema_fingerprint_for_parts(table_id, schema)?;

        if checkpoint.table_id != table_id || checkpoint.schema_fingerprint != schema_fingerprint {
            return None;
        }

        let (wal_size_bytes, wal_modified_epoch_ms) =
            Self::wal_stream_fingerprint(data_dir, table_stream_id)?;

        if checkpoint.wal_size_bytes != wal_size_bytes
            || checkpoint.wal_modified_epoch_ms != wal_modified_epoch_ms
        {
            return None;
        }

        Some((checkpoint.latest_tx_id, checkpoint.live_row_count))

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

        if snapshot.live_row_count > 0 && snapshot.cache.rows_by_id.is_empty() {
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

        if live_rows.len() > Self::live_row_checkpoint_compress_max_rows() {
            log::debug!(
                "live-row checkpoint save skipped table={} stream={} live_rows={} max_rows={}",
                table.table_id,
                table_stream_id,
                live_rows.len(),
                Self::live_row_checkpoint_compress_max_rows(),
            );

            return Ok(());
        }

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
        let compress_threshold = Self::live_row_checkpoint_compress_max_rows();
        let payload = if live_rows.len() > compress_threshold {
            common::helpers::bincode_compat::serialize(&checkpoint)
                .map_err(|_| "snapshot serialization failed".to_string())?
        } else {
            encode_snapshot_payload(&checkpoint)?
        };
        content.extend_from_slice(&payload);

        let checkpoint_path = Self::live_row_checkpoint_path(data_dir, table_stream_id);
        write_bytes_atomic(&checkpoint_path, &content)
            .map_err(|err| format!("live-row checkpoint write failed: {err}"))?;

        if Self::wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
            let _ = fs::remove_file(&checkpoint_path);
            return Err("wal fingerprint changed after live-row checkpoint write".to_string());
        }

        Self::save_live_row_count_checkpoint(
            data_dir,
            table,
            table_stream_id,
            latest_tx_id,
            wal_fingerprint,
            live_rows.len(),
        )?;

        Ok(())

    }

    pub(crate) fn save_live_row_count_checkpoint(
        data_dir: &Path,
        table: &DatabaseTable,
        table_stream_id: &str,
        latest_tx_id: u64,
        wal_fingerprint: Option<(u64, u64)>,
        live_row_count: usize,
    ) -> Result<(), String> {

        let (wal_size_bytes, wal_modified_epoch_ms) = wal_fingerprint
            .ok_or_else(|| "wal fingerprint unavailable".to_string())?;

        let expected_wal_fingerprint = (wal_size_bytes, wal_modified_epoch_ms);

        if Self::wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
            return Err("wal fingerprint changed before live-row count checkpoint write".to_string());
        }

        let schema_fingerprint = table_schema_fingerprint(table)
            .ok_or_else(|| "schema fingerprint serialization failed".to_string())?;

        let checkpoint = TableLiveRowCountCheckpoint {
            table_id: table.table_id.clone(),
            latest_tx_id,
            schema_fingerprint,
            wal_size_bytes,
            wal_modified_epoch_ms,
            live_row_count,
        };

        let mut content = make_header(FileKind::Entity).to_vec();
        let payload = encode_snapshot_payload(&checkpoint)?;
        content.extend_from_slice(&payload);

        let checkpoint_path = Self::live_row_count_checkpoint_path(data_dir, table_stream_id);
        write_bytes_atomic(&checkpoint_path, &content)
            .map_err(|err| format!("live-row count checkpoint write failed: {err}"))?;

        if Self::wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
            let _ = fs::remove_file(&checkpoint_path);
            return Err("wal fingerprint changed after live-row count checkpoint write".to_string());
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

        if !Self::accessor_snapshot_persist_rows() {
            return Ok(());
        }

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

    pub(crate) fn runtime_index_snapshot_path(data_dir: &Path, table_stream_id: &str) -> PathBuf {
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

    fn live_row_count_checkpoint_path(data_dir: &Path, table_stream_id: &str) -> PathBuf {
        let table_key = stable_id(&[table_stream_id]);
        let stem = format!("{}_{}", LIVE_ROW_COUNT_CHECKPOINT_FILE_STEM_PREFIX, table_key);

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
    let encoded = common::helpers::bincode_compat::serialize(schema).ok()?;

    let hex = encoded
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>();

    Some(stable_id(&[table_id, &hex]))
}

fn encode_snapshot_payload<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let raw = common::helpers::bincode_compat::serialize(value)
        .map_err(|_| "snapshot serialization failed".to_string())?;

    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());

    encoder
        .write_all(&raw)
        .map_err(|_| "snapshot compression failed".to_string())?;

    encoder
        .finish()
        .map_err(|_| "snapshot compression finish failed".to_string())
}

fn decode_snapshot_payload_with_reason<T: serde::de::DeserializeOwned>(
    payload: &[u8],
) -> Result<(T, bool), String> {
    let decoder = ZlibDecoder::new(payload);
    let mut reader = BufReader::new(decoder);

    let compressed_decode = common::helpers::bincode_compat::deserialize_from::<_, T>(&mut reader)
        .map(|decoded| (decoded, false));

    if let Ok(decoded) = compressed_decode {
        return Ok(decoded);
    }

    let compressed_error = compressed_decode
        .err()
        .map(|err| err.to_string())
        .unwrap_or_else(|| "unknown compressed decode error".to_string());

    let plain_decode = common::helpers::bincode_compat::deserialize::<T>(payload)
        .map(|decoded| (decoded, true));

    if let Ok(decoded) = plain_decode {
        return Ok(decoded);
    }

    let plain_error = plain_decode
        .err()
        .map(|err| err.to_string())
        .unwrap_or_else(|| "unknown plain decode error".to_string());

    Err(format!(
        "compressed_decode_error={} plain_decode_error={}",
        compressed_error,
        plain_error,
    ))
}

fn decode_snapshot_payload<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Option<(T, bool)> {
    decode_snapshot_payload_with_reason(payload).ok()
}

fn snapshot_memory_shape(snapshot: &RuntimeIndexTableSnapshot) -> (usize, usize, usize, usize, usize) {
    let index_count = snapshot.indexes.len();

    let entry_count = snapshot
        .indexes
        .iter()
        .map(|index| index.entries.len())
        .sum::<usize>();

    let key_bytes = snapshot
        .indexes
        .iter()
        .flat_map(|index| index.entries.iter())
        .flat_map(|key_tuple| key_tuple.iter())
        .map(|part| part.len())
        .sum::<usize>();

    let row_refs_legacy_count = snapshot
        .indexes
        .iter()
        .map(|index| index.row_refs.len())
        .sum::<usize>();

    let row_refs_compact_count = snapshot
        .indexes
        .iter()
        .map(|index| index.row_refs_by_entry.iter().filter(|item| **item != 0).count())
        .sum::<usize>();

    (
        index_count,
        entry_count,
        key_bytes,
        row_refs_legacy_count,
        row_refs_compact_count,
    )
}