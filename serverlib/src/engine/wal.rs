use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use common::helpers::format::{make_header, verify_header, FileKind, HEADER_SIZE};
use common::helpers::{append_bytes, read_bytes, stable_id, write_bytes};

use crate::core::identity::UserId;
use crate::engine::database::transaction::{TransactionId, TransactionLog, TransactionRecord};
use crate::TransactionKind;

#[derive(Debug)]
enum WalCommand {
    Append {
        record: TransactionRecord,
        ack: Sender<Result<(), &'static str>>,
    },
    CompactToLatestSchemaAndMetadata {
        actor: UserId,
        timestamp_epoch_ms: u64,
        ack: Sender<Result<(), &'static str>>,
    },
    Shutdown,
}

#[derive(Debug)]
pub struct ConcurrentWalManager {
    workers: Mutex<HashMap<String, Sender<WalCommand>>>,
    storage: Arc<Mutex<HashMap<String, Vec<TransactionRecord>>>>,
    data_dir: Option<Arc<PathBuf>>,
}

impl Default for ConcurrentWalManager {
    fn default() -> Self {
        Self {
            workers: Mutex::new(HashMap::new()),
            storage: Arc::new(Mutex::new(HashMap::new())),
            data_dir: None,
        }
    }
}

impl ConcurrentWalManager {

    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        Self {
            workers: Mutex::new(HashMap::new()),
            storage: Arc::new(Mutex::new(HashMap::new())),
            data_dir: Some(Arc::new(data_dir)),
        }
    }

    pub fn active_worker_count(&self) -> usize {
        self.workers
            .lock()
            .map(|workers| workers.len())
            .unwrap_or(0)
    }

    pub fn shutdown_all(&self) -> Result<(), &'static str> {
        let workers = self
            .workers
            .lock()
            .map_err(|_| "failed to lock WAL workers")?;
        for sender in workers.values() {
            let _ = sender.send(WalCommand::Shutdown);
        }
        Ok(())
    }

    pub fn compact_stream_to_latest_schema_and_metadata(
        &self,
        wal_id: &str,
        actor: UserId,
        timestamp_epoch_ms: u64,
    ) -> Result<(), &'static str> {
        let sender = self.get_or_spawn_worker(wal_id)?;
        let (ack_tx, ack_rx) = mpsc::channel::<Result<(), &'static str>>();
        sender
            .send(WalCommand::CompactToLatestSchemaAndMetadata {
                actor,
                timestamp_epoch_ms,
                ack: ack_tx,
            })
            .map_err(|_| "failed to send WAL compact command")?;

        ack_rx
            .recv()
            .map_err(|_| "failed to receive WAL compact acknowledgement")?
    }

    fn get_or_spawn_worker(&self, wal_id: &str) -> Result<Sender<WalCommand>, &'static str> {
        let stream_key = obfuscated_stream_key(wal_id)?;
        let mut workers = self
            .workers
            .lock()
            .map_err(|_| "failed to lock WAL workers")?;

        if let Some(existing) = workers.get(&stream_key) {
            return Ok(existing.clone());
        }

        let wal_path = self
            .data_dir
            .as_ref()
            .map(|dir| dir.join(FileKind::Data.file_name(&stream_key)));

        let sender = spawn_worker(stream_key.clone(), Arc::clone(&self.storage), wal_path);
        workers.insert(stream_key, sender.clone());
        Ok(sender)
    }

}

impl TransactionLog for ConcurrentWalManager {

    fn append(&self, wal_id: &str, record: TransactionRecord) -> Result<(), &'static str> {
        let sender = self.get_or_spawn_worker(wal_id)?;
        let (ack_tx, ack_rx) = mpsc::channel::<Result<(), &'static str>>();
        sender
            .send(WalCommand::Append {
                record,
                ack: ack_tx,
            })
            .map_err(|_| "failed to send WAL append command")?;

        ack_rx
            .recv()
            .map_err(|_| "failed to receive WAL append acknowledgement")?
    }

    fn since(&self, wal_id: &str, from: Option<TransactionId>) -> Vec<TransactionRecord> {
        let stream_key = match obfuscated_stream_key(wal_id) {
            Ok(k) => k,
            Err(_) => return Vec::new(),
        };

        let store = match self.storage.lock() {
            Ok(store) => store,
            Err(_) => return Vec::new(),
        };

        store
            .get(&stream_key)
            .map(|entries| {
                match from {
                    Some(min_id) => entries
                        .iter()
                        .filter(|entry| entry.id.0 > min_id.0)
                        .cloned()
                        .collect(),
                    None => entries.clone(),
                }
            })
            .unwrap_or_default()

    }
    
}

fn frame_record(record: &TransactionRecord) -> Result<Vec<u8>, &'static str> {
    let encoded =
        bincode::serialize(record).map_err(|_| "failed to serialize WAL record")?;
    let len = encoded.len() as u64;
    let mut frame = Vec::with_capacity(8 + encoded.len());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&encoded);
    Ok(frame)
}

fn obfuscated_stream_key(wal_id: &str) -> Result<String, &'static str> {
    let normalized = wal_id.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err("wal_id must not be empty");
    }
    Ok(stable_id(&[&normalized]))
}

fn load_records_from_file(path: &Path) -> Vec<TransactionRecord> {
    let bytes = match read_bytes(path) {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };

    if let Err(e) = verify_header(FileKind::Data, &bytes) {
        log::error!("invalid WAL header in '{}': {}", path.display(), e);
        return Vec::new();
    }

    let mut records = Vec::new();
    let mut pos = HEADER_SIZE;
    while pos + 8 <= bytes.len() {
        let len = u64::from_le_bytes(
            bytes[pos..pos + 8]
                .try_into()
                .expect("slice is exactly 8 bytes"),
        ) as usize;
        pos += 8;
        if pos + len > bytes.len() {
            log::warn!("truncated WAL frame at byte offset {}, stopping replay", pos);
            break;
        }
        match bincode::deserialize::<TransactionRecord>(&bytes[pos..pos + len]) {
            Ok(record) => records.push(record),
            Err(e) => {
                log::error!("failed to deserialize WAL frame at byte {}: {}", pos, e);
                break;
            }
        }
        pos += len;
    }
    records
}

fn ensure_wal_file(path: &Path) -> Result<(), &'static str> {
    match read_bytes(path) {
        Ok(existing) => {
            verify_header(FileKind::Data, &existing).map_err(|_| "invalid WAL file header/version")?;
            Ok(())
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            write_bytes(path, &make_header(FileKind::Data))
                .map_err(|_| "failed to initialize WAL file header")
        }
        Err(_) => Err("failed to inspect WAL file"),
    }
}

fn rewrite_wal_file(path: &Path, records: &[TransactionRecord]) -> Result<(), &'static str> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&make_header(FileKind::Data));
    for record in records {
        let frame = frame_record(record)?;
        bytes.extend_from_slice(&frame);
    }
    write_bytes(path, &bytes).map_err(|_| "failed to rewrite compacted WAL file")
}

fn compact_entries_to_latest_schema_and_metadata(
    entries: &mut Vec<TransactionRecord>,
    actor: UserId,
    timestamp_epoch_ms: u64,
) {
    let last_id = entries.last().map(|record| record.id).unwrap_or(TransactionId(0));

    let latest_schema = entries
        .iter()
        .rev()
        .find(|record| record.kind == TransactionKind::SchemaChange)
        .cloned();
    let latest_metadata = entries
        .iter()
        .rev()
        .find(|record| {
            record.kind == TransactionKind::MetadataChange
                || record.kind == TransactionKind::SecurityChange
        })
        .cloned();

    let mut retained = Vec::new();
    if let Some(schema) = latest_schema {
        retained.push(schema);
    }
    if let Some(metadata) = latest_metadata {
        retained.push(metadata);
    }
    retained.sort_by_key(|record| record.id.0);

    retained.push(TransactionRecord {
        id: TransactionId(last_id.0 + 1),
        refid: Some(last_id),
        timestamp_epoch_ms,
        actor,
        kind: TransactionKind::Truncate,
        payload: Vec::new(),
    });

    *entries = retained;
}

fn spawn_worker(
    stream_key: String,
    storage: Arc<Mutex<HashMap<String, Vec<TransactionRecord>>>>,
    wal_path: Option<PathBuf>,
) -> Sender<WalCommand> {
    let (tx, rx) = mpsc::channel::<WalCommand>();
    thread::spawn(move || {
        if let Some(ref path) = wal_path {
            if let Err(e) = ensure_wal_file(path) {
                log::error!("failed to initialize WAL file '{}': {}", path.display(), e);
            }
            let existing = load_records_from_file(path);
            let count = existing.len();
            if let Ok(mut state) = storage.lock() {
                state.entry(stream_key.clone()).or_default().extend(existing);
            }
            log::info!(
                "WAL worker started for stream={} (replayed {} record(s) from disk)",
                stream_key,
                count
            );
        } else {
            log::info!("WAL worker started for stream={} (in-memory only)", stream_key);
        }

        while let Ok(command) = rx.recv() {
            match command {
                WalCommand::Append { record, ack } => {
                    if let Ok(mut state) = storage.lock() {
                        let entries = state.entry(stream_key.clone()).or_default();
                        let is_ordered = entries
                            .last()
                            .map(|last| record.id.0 > last.id.0)
                            .unwrap_or(true);

                        if is_ordered {
                            if let Some(ref path) = wal_path {
                                match frame_record(&record) {
                                    Ok(frame) => {
                                        if let Err(e) = append_bytes(path, &frame) {
                                            log::error!(
                                                "failed to persist WAL record for stream={}: {}",
                                                stream_key,
                                                e
                                            );
                                            let _ = ack.send(Err(
                                                "failed to persist WAL record to disk",
                                            ));
                                            continue;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = ack.send(Err(e));
                                        continue;
                                    }
                                }
                            }
                            entries.push(record);
                            let _ = ack.send(Ok(()));
                        } else {
                            log::warn!(
                                "out-of-order transaction rejected for stream={}",
                                stream_key
                            );
                            let _ = ack.send(Err(
                                "out-of-order transaction id for table WAL stream",
                            ));
                        }
                    } else {
                        log::error!(
                            "failed to acquire WAL storage lock for stream={}",
                            stream_key
                        );
                        let _ = ack.send(Err("failed to lock WAL storage"));
                        break;
                    }
                }
                WalCommand::CompactToLatestSchemaAndMetadata {
                    actor,
                    timestamp_epoch_ms,
                    ack,
                } => {
                    if let Ok(mut state) = storage.lock() {
                        let entries = state.entry(stream_key.clone()).or_default();
                        compact_entries_to_latest_schema_and_metadata(
                            entries,
                            actor,
                            timestamp_epoch_ms,
                        );

                        if let Some(ref path) = wal_path {
                            if let Err(e) = rewrite_wal_file(path, entries) {
                                log::error!(
                                    "failed to rewrite compacted WAL for stream={}: {}",
                                    stream_key,
                                    e
                                );
                                let _ = ack.send(Err(e));
                                continue;
                            }
                        }

                        let _ = ack.send(Ok(()));
                    } else {
                        log::error!(
                            "failed to acquire WAL storage lock during compact for stream={}",
                            stream_key
                        );
                        let _ = ack.send(Err("failed to lock WAL storage"));
                        break;
                    }
                }
                WalCommand::Shutdown => {
                    log::info!("WAL worker shutting down for stream={}", stream_key);
                    break;
                }
            }
        }
    });
    tx
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(id: u64, kind: TransactionKind, actor: &UserId) -> TransactionRecord {
        TransactionRecord {
            id: TransactionId(id),
            refid: None,
            timestamp_epoch_ms: id,
            actor: actor.clone(),
            kind,
            payload: vec![id as u8],
        }
    }

    #[test]
    fn compact_keeps_latest_schema_metadata_and_appends_truncate_marker() {
        let wal = ConcurrentWalManager::new();
        let actor = UserId::from_username("tester");

        wal.append("users", make_record(1, TransactionKind::Insert, &actor))
            .expect("append should succeed");
        wal.append("users", make_record(2, TransactionKind::SchemaChange, &actor))
            .expect("append should succeed");
        wal.append("users", make_record(3, TransactionKind::Update, &actor))
            .expect("append should succeed");
        wal.append("users", make_record(4, TransactionKind::SecurityChange, &actor))
            .expect("append should succeed");
        wal.append("users", make_record(5, TransactionKind::Delete, &actor))
            .expect("append should succeed");

        wal.compact_stream_to_latest_schema_and_metadata(
            "users",
            actor.clone(),
            99,
        )
        .expect("compact should succeed");

        let records = wal.since("users", None);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].kind, TransactionKind::SchemaChange);
        assert_eq!(records[0].id, TransactionId(2));
        assert_eq!(records[1].kind, TransactionKind::SecurityChange);
        assert_eq!(records[1].id, TransactionId(4));
        assert_eq!(records[2].kind, TransactionKind::Truncate);
        assert_eq!(records[2].id, TransactionId(6));
        assert_eq!(records[2].refid, Some(TransactionId(5)));
        assert_eq!(records[2].timestamp_epoch_ms, 99);
    }

    #[test]
    fn compact_prefers_latest_metadata_change_record_when_present() {
        let wal = ConcurrentWalManager::new();
        let actor = UserId::from_username("tester");

        wal.append("users", make_record(1, TransactionKind::SchemaChange, &actor))
            .expect("append should succeed");
        wal.append("users", make_record(2, TransactionKind::SecurityChange, &actor))
            .expect("append should succeed");
        wal.append("users", make_record(3, TransactionKind::MetadataChange, &actor))
            .expect("append should succeed");

        wal.compact_stream_to_latest_schema_and_metadata("users", actor, 101)
            .expect("compact should succeed");

        let records = wal.since("users", None);
        assert_eq!(records.len(), 3);
        assert_eq!(records[1].kind, TransactionKind::MetadataChange);
    }
}