use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use common::helpers::{append_bytes, read_bytes};

use crate::engine::transaction::{TransactionId, TransactionLog, TransactionRecord};

#[derive(Debug)]
enum WalCommand {
    Append {
        record: TransactionRecord,
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

    fn get_or_spawn_worker(&self, table_id: &str) -> Result<Sender<WalCommand>, &'static str> {
        let mut workers = self
            .workers
            .lock()
            .map_err(|_| "failed to lock WAL workers")?;

        if let Some(existing) = workers.get(table_id) {
            return Ok(existing.clone());
        }

        let wal_path = self
            .data_dir
            .as_ref()
            .map(|dir| dir.join("wal").join(format!("{}.wal", table_id)));

        let sender = spawn_worker(table_id.to_string(), Arc::clone(&self.storage), wal_path);
        workers.insert(table_id.to_string(), sender.clone());
        Ok(sender)
    }

}

impl TransactionLog for ConcurrentWalManager {

    fn append(&self, record: TransactionRecord) -> Result<(), &'static str> {
        let table_id = record.table_id.clone();
        let sender = self.get_or_spawn_worker(&table_id)?;
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

    fn since(&self, table_id: &str, from: Option<TransactionId>) -> Vec<TransactionRecord> {

        let store = match self.storage.lock() {
            Ok(store) => store,
            Err(_) => return Vec::new(),
        };

        store
            .get(table_id)
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

fn load_records_from_file(path: &Path) -> Vec<TransactionRecord> {
    let bytes = match read_bytes(path) {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };
    let mut records = Vec::new();
    let mut pos = 0;
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

fn spawn_worker(
    table_id: String,
    storage: Arc<Mutex<HashMap<String, Vec<TransactionRecord>>>>,
    wal_path: Option<PathBuf>,
) -> Sender<WalCommand> {
    let (tx, rx) = mpsc::channel::<WalCommand>();
    thread::spawn(move || {
        if let Some(ref path) = wal_path {
            let existing = load_records_from_file(path);
            let count = existing.len();
            if let Ok(mut state) = storage.lock() {
                state.entry(table_id.clone()).or_default().extend(existing);
            }
            log::info!(
                "WAL worker started for table={} (replayed {} record(s) from disk)",
                table_id,
                count
            );
        } else {
            log::info!("WAL worker started for table={} (in-memory only)", table_id);
        }

        while let Ok(command) = rx.recv() {
            match command {
                WalCommand::Append { record, ack } => {
                    if let Ok(mut state) = storage.lock() {
                        let entries = state.entry(table_id.clone()).or_default();
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
                                                "failed to persist WAL record for table={}: {}",
                                                table_id,
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
                                "out-of-order transaction rejected for table={}",
                                table_id
                            );
                            let _ = ack.send(Err(
                                "out-of-order transaction id for table WAL stream",
                            ));
                        }
                    } else {
                        log::error!(
                            "failed to acquire WAL storage lock for table={}",
                            table_id
                        );
                        let _ = ack.send(Err("failed to lock WAL storage"));
                        break;
                    }
                }
                WalCommand::Shutdown => {
                    log::info!("WAL worker shutting down for table={}", table_id);
                    break;
                }
            }
        }
    });
    tx
}