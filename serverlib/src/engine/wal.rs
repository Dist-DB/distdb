use std::collections::HashMap;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::engine::transaction::{TransactionId, TransactionLog, TransactionRecord};

#[derive(Debug)]
enum WalCommand {
    Append {
        record: TransactionRecord,
        ack: Sender<Result<(), &'static str>>,
    },
    Shutdown,
}

#[derive(Debug, Default)]
pub struct ConcurrentWalManager {
    workers: Mutex<HashMap<String, Sender<WalCommand>>>,
    storage: Arc<Mutex<HashMap<String, Vec<TransactionRecord>>>>,
}

impl ConcurrentWalManager {

    pub fn new() -> Self {
        Self::default()
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

        let sender = spawn_worker(table_id.to_string(), Arc::clone(&self.storage));
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

fn spawn_worker(
    table_id: String,
    storage: Arc<Mutex<HashMap<String, Vec<TransactionRecord>>>>,
) -> Sender<WalCommand> {
    let (tx, rx) = mpsc::channel::<WalCommand>();
    thread::spawn(move || {
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
                            entries.push(record);
                            let _ = ack.send(Ok(()));
                        } else {
                            let _ = ack.send(Err(
                                "out-of-order transaction id for table WAL stream",
                            ));
                        }
                    } else {
                        let _ = ack.send(Err("failed to lock WAL storage"));
                        break;
                    }
                }
                WalCommand::Shutdown => break,
            }
        }
    });
    tx
}