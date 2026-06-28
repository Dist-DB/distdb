use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;

use common::helpers::format::{make_header, verify_header, FileKind, HEADER_SIZE};
use common::helpers::{read_bytes, stable_id, write_bytes};

use crate::core::identity::UserId;
use crate::engine::database::row_payload::looks_like_encrypted_row_payload;
use crate::engine::database::transaction::{TransactionId, TransactionLog, TransactionRecord};
use crate::TransactionKind;

const WAL_PAYLOAD_COMPRESSION_MAGIC: [u8; 4] = *b"wpc1";
const WAL_PAYLOAD_COMPRESSION_VERSION: u8 = 1;
const WAL_PAYLOAD_COMPRESSION_CODEC_ZLIB: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct LegacyCompressedWalPayloadEnvelope {
    magic: [u8; 4],
    version: u8,
    codec: u8,
    original_len: u32,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalStreamMode {
    Durable,
    Ephemeral,
}

#[derive(Debug)]
enum WalCommand {

    Append {
        record: TransactionRecord,
        ack: Sender<Result<(), &'static str>>,
    },

    AppendBatch {
        records: Vec<TransactionRecord>,
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
    write_high_water_by_stream: Mutex<HashMap<String, u64>>,
    stream_modes: Mutex<HashMap<String, WalStreamMode>>,
    data_dir: Option<Arc<PathBuf>>,
}

impl Default for ConcurrentWalManager {

    fn default() -> Self {
        Self {
            workers: Mutex::new(HashMap::new()),
            storage: Arc::new(Mutex::new(HashMap::new())),
            write_high_water_by_stream: Mutex::new(HashMap::new()),
            stream_modes: Mutex::new(HashMap::new()),
            data_dir: None,
        }
    }

}

impl ConcurrentWalManager {

    pub fn cache_scope_id(&self) -> usize {
        Arc::as_ptr(&self.storage) as usize
    }

    fn hydrate_stream_if_needed(&self, wal_id: &str, stream_key: &str) {
        if !matches!(self.stream_mode(wal_id), WalStreamMode::Durable) {
            return;
        }

        let Some(data_dir) = &self.data_dir else {
            return;
        };

        let needs_hydration = match self.storage.lock() {
            Ok(store) => !store.contains_key(stream_key),
            Err(_) => return,
        };

        if !needs_hydration {
            return;
        }

        let wal_path = data_dir.join(FileKind::Data.file_name(stream_key));
        if !wal_path.exists() {
            return;
        }

        let existing = load_records_from_file(&wal_path);
        if let Ok(mut store) = self.storage.lock() {
            store.entry(stream_key.to_string()).or_insert(existing);
        }

        if let (Ok(store), Ok(mut high_water)) = (
            self.storage.lock(),
            self.write_high_water_by_stream.lock(),
        ) {
            let max_ts = store
                .get(stream_key)
                .and_then(|entries| latest_write_timestamp(entries));

            match max_ts {
                Some(ts) => {
                    high_water.insert(stream_key.to_string(), ts);
                }
                None => {
                    high_water.remove(stream_key);
                }
            }
        }
    }

    pub fn has_write_after(&self, wal_id: &str, timestamp_epoch_ms: u64) -> bool {
        let stream_key = match obfuscated_stream_key(wal_id) {
            Ok(k) => k,
            Err(_) => return false,
        };

        self.hydrate_stream_if_needed(wal_id, &stream_key);

        if let Ok(high_water) = self.write_high_water_by_stream.lock()
            && let Some(max_ts) = high_water.get(&stream_key) {
                return *max_ts > timestamp_epoch_ms;
            }

        let max_ts = {
            let store = match self.storage.lock() {
                Ok(store) => store,
                Err(_) => return false,
            };

            store
                .get(&stream_key)
                .and_then(|entries| latest_write_timestamp(entries))
        };

        if let Ok(mut high_water) = self.write_high_water_by_stream.lock() {
            match max_ts {
                Some(ts) => {
                    high_water.insert(stream_key.clone(), ts);
                }
                None => {
                    high_water.remove(&stream_key);
                }
            }
        }

        max_ts.is_some_and(|max_ts| max_ts > timestamp_epoch_ms)
    }

    /* Build a memory-resident WAL manager.
    /
    / This mode never persists records to `.dtbl` files and is suitable
    / for tests, ephemeral nodes, or high-speed pipelines where durability
    / is handled elsewhere.
    / 
    */ 

    pub fn in_memory() -> Self {
        Self::new()
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        Self {
            workers: Mutex::new(HashMap::new()),
            storage: Arc::new(Mutex::new(HashMap::new())),
            write_high_water_by_stream: Mutex::new(HashMap::new()),
            stream_modes: Mutex::new(HashMap::new()),
            data_dir: Some(Arc::new(data_dir)),
        }
    }

    pub fn set_stream_mode(
        &self,
        wal_id: &str,
        mode: WalStreamMode,
    ) -> Result<(), &'static str> {
        let stream_key = obfuscated_stream_key(wal_id)?;

        let mut modes = self
            .stream_modes
            .lock()
            .map_err(|_| "failed to lock WAL stream mode registry")?;

        modes.insert(stream_key, mode);

        Ok(())
    }

    pub fn stream_mode(&self, wal_id: &str) -> WalStreamMode {
        let Ok(stream_key) = obfuscated_stream_key(wal_id) else {
            return WalStreamMode::Durable;
        };

        let Ok(modes) = self.stream_modes.lock() else {
            return WalStreamMode::Durable;
        };

        modes
            .get(&stream_key)
            .copied()
            .unwrap_or(WalStreamMode::Durable)
    }

    pub fn is_stream_replicable(&self, wal_id: &str) -> bool {
        matches!(self.stream_mode(wal_id), WalStreamMode::Durable)
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
            .map_err(|_| "failed to receive WAL compact acknowledgement")??;

        let stream_key = obfuscated_stream_key(wal_id)?;
        if let (Ok(store), Ok(mut high_water)) = (
            self.storage.lock(),
            self.write_high_water_by_stream.lock(),
        ) {
            let max_ts = store
                .get(&stream_key)
                .and_then(|entries| latest_write_timestamp(entries));

            match max_ts {
                Some(ts) => {
                    high_water.insert(stream_key.clone(), ts);
                }
                None => {
                    high_water.remove(&stream_key);
                }
            }
        }

        Ok(())

    }

    pub fn delete_stream(&self, wal_id: &str) -> Result<(), &'static str> {

        let stream_key = obfuscated_stream_key(wal_id)?;

        let sender = {
            let mut workers = self
                .workers
                .lock()
                .map_err(|_| "failed to lock WAL workers")?;
            workers.remove(&stream_key)
        };

        if let Some(sender) = sender {
            let _ = sender.send(WalCommand::Shutdown);
        }

        {
            let mut storage = self
                .storage
                .lock()
                .map_err(|_| "failed to lock WAL storage")?;
            storage.remove(&stream_key);
        }

        {
            let mut modes = self
                .stream_modes
                .lock()
                .map_err(|_| "failed to lock WAL stream mode registry")?;
            modes.remove(&stream_key);
        }

        {
            let mut high_water = self
                .write_high_water_by_stream
                .lock()
                .map_err(|_| "failed to lock WAL write high-water map")?;
            high_water.remove(&stream_key);
        }

        if let Some(data_dir) = &self.data_dir {
            let wal_path = data_dir.join(FileKind::Data.file_name(&stream_key));
            if let Err(err) = fs::remove_file(wal_path)
                && err.kind() != ErrorKind::NotFound {
                    return Err("failed to delete WAL file");
                }
        }

        Ok(())

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

        let stream_mode = self
            .stream_modes
            .lock()
            .ok()
            .and_then(|modes| modes.get(&stream_key).copied())
            .unwrap_or(WalStreamMode::Durable);

        let wal_path = match stream_mode {
            WalStreamMode::Durable => self
                .data_dir
                .as_ref()
                .map(|dir| dir.join(FileKind::Data.file_name(&stream_key))),
            WalStreamMode::Ephemeral => None,
        };

        let (sender, ready_rx) = spawn_worker(stream_key.clone(), Arc::clone(&self.storage), wal_path);

        ready_rx
            .recv()
            .map_err(|_| "failed to receive WAL worker startup acknowledgement")?;
        
        workers.insert(stream_key, sender.clone());
        
        Ok(sender)

    }

    pub fn latest_transaction_id(&self, wal_id: &str) -> Option<TransactionId> {

        let stream_key = obfuscated_stream_key(wal_id).ok()?;

        self.hydrate_stream_if_needed(wal_id, &stream_key);

        let store = self.storage.lock().ok()?;
        store
            .get(&stream_key)
            .and_then(|entries| entries.last().map(|entry| entry.id))
    }

    pub fn append_batch(
        &self,
        wal_id: &str,
        records: Vec<TransactionRecord>,
    ) -> Result<(), &'static str> {
        if records.is_empty() {
            return Ok(());
        }

        let batch_max_write_ts = records
            .iter()
            .filter_map(write_timestamp_if_data_write)
            .max();

        let sender = self.get_or_spawn_worker(wal_id)?;
        let (ack_tx, ack_rx) = mpsc::channel::<Result<(), &'static str>>();

        sender
            .send(WalCommand::AppendBatch {
                records,
                ack: ack_tx,
            })
            .map_err(|_| "failed to send WAL append-batch command")?;

        ack_rx
            .recv()
            .map_err(|_| "failed to receive WAL append-batch acknowledgement")??;

        if let Some(batch_max_write_ts) = batch_max_write_ts
            && let Ok(stream_key) = obfuscated_stream_key(wal_id)
            && let Ok(mut high_water) = self.write_high_water_by_stream.lock() {
                high_water
                    .entry(stream_key)
                    .and_modify(|current| {
                        if batch_max_write_ts > *current {
                            *current = batch_max_write_ts;
                        }
                    })
                    .or_insert(batch_max_write_ts);
            }

        Ok(())
    }

}

impl TransactionLog for ConcurrentWalManager {

    fn append(&self, wal_id: &str, record: TransactionRecord) -> Result<(), &'static str> {
        let write_ts = write_timestamp_if_data_write(&record);
        
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
            .map_err(|_| "failed to receive WAL append acknowledgement")??;

        if let Some(write_ts) = write_ts
            && let Ok(stream_key) = obfuscated_stream_key(wal_id)
            && let Ok(mut high_water) = self.write_high_water_by_stream.lock() {
                high_water
                    .entry(stream_key)
                    .and_modify(|current| {
                        if write_ts > *current {
                            *current = write_ts;
                        }
                    })
                    .or_insert(write_ts);
            }

        Ok(())

    }

    fn since(&self, wal_id: &str, from: Option<TransactionId>) -> Vec<TransactionRecord> {

        let stream_key = match obfuscated_stream_key(wal_id) {
            Ok(k) => k,
            Err(_) => return Vec::new(),
        };

        self.hydrate_stream_if_needed(wal_id, &stream_key);

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

    let encoded = encode_record_for_storage(record)?;
    let len = encoded.len() as u64;
    let mut frame = Vec::with_capacity(8 + encoded.len());

    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&encoded);

    Ok(frame)

}

pub(crate) fn encode_record_for_storage(
    record: &TransactionRecord,
) -> Result<Vec<u8>, &'static str> {
    let mut record_for_storage = record.clone();
    record_for_storage.payload = maybe_compress_record_payload_bytes(
        &record_for_storage.payload,
        should_skip_payload_compression(record),
    )?;

    bincode::serialize(&record_for_storage).map_err(|_| "failed to serialize WAL record")
}

pub(crate) fn decode_record_from_storage(
    encoded: &[u8],
) -> Result<TransactionRecord, &'static str> {
    let record = bincode::deserialize::<TransactionRecord>(encoded)
        .map_err(|_| "failed to deserialize WAL record")?;

    decode_record_payload_if_needed(record)
}

fn should_skip_payload_compression(record: &TransactionRecord) -> bool {
    matches!(
        record.kind,
        TransactionKind::Insert | TransactionKind::Update | TransactionKind::Delete
    ) && looks_like_encrypted_row_payload(&record.payload)
}

fn maybe_compress_record_payload_bytes(
    payload: &[u8],
    skip_compression: bool,
) -> Result<Vec<u8>, &'static str> {

    if skip_compression
        || looks_like_compressed_payload_envelope(payload)
    {
        return Ok(payload.to_vec());
    }

    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(payload)
        .map_err(|_| "failed to compress WAL payload")?;

    let compressed = encoder
        .finish()
        .map_err(|_| "failed to finish WAL payload compression")?;

    Ok(compressed)
    
}

fn decode_record_payload_if_needed(
    mut record: TransactionRecord,
) -> Result<TransactionRecord, &'static str> {
    if let Some(decoded_payload) = maybe_decode_compressed_payload_bytes(&record.payload)? {
        record.payload = decoded_payload;
    }

    Ok(record)
}

fn maybe_decode_compressed_payload_bytes(
    payload: &[u8],
) -> Result<Option<Vec<u8>>, &'static str> {
    if looks_like_zlib_payload(payload) {
        if let Some(decoded) = try_zlib_decode_payload(payload) {
            return Ok(Some(decoded));
        }

        return Err("failed to decompress WAL payload");
    }

    if looks_like_compressed_payload_envelope(payload) {
        let compressed = &payload[WAL_PAYLOAD_COMPRESSION_MAGIC.len()..];
        if let Some(decoded) = try_zlib_decode_payload(compressed) {
            return Ok(Some(decoded));
        }

        return Err("failed to decompress WAL payload");
    }

    if let Some(decoded_legacy) = try_decode_legacy_enveloped_payload(payload)? {
        return Ok(Some(decoded_legacy));
    }

    Ok(None)
}

fn try_zlib_decode_payload(compressed: &[u8]) -> Option<Vec<u8>> {
    let mut decoder = ZlibDecoder::new(compressed);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).ok()?;
    Some(decompressed)
}

fn looks_like_zlib_payload(payload: &[u8]) -> bool {
    if payload.len() < 2 || payload[0] != 0x78 {
        return false;
    }

    let header = u16::from(payload[0]) << 8 | u16::from(payload[1]);
    header % 31 == 0
}

fn try_decode_legacy_enveloped_payload(
    payload: &[u8],
) -> Result<Option<Vec<u8>>, &'static str> {
    let Ok(envelope) = bincode::deserialize::<LegacyCompressedWalPayloadEnvelope>(payload) else {
        return Ok(None);
    };

    if envelope.magic != WAL_PAYLOAD_COMPRESSION_MAGIC {
        return Ok(None);
    }

    if envelope.version != WAL_PAYLOAD_COMPRESSION_VERSION {
        return Err("unsupported WAL payload envelope version");
    }

    if envelope.codec != WAL_PAYLOAD_COMPRESSION_CODEC_ZLIB {
        return Err("unsupported WAL payload envelope codec");
    }

    let Some(decompressed) = try_zlib_decode_payload(envelope.payload.as_slice()) else {
        return Err("failed to decompress WAL payload");
    };

    if decompressed.len() != envelope.original_len as usize {
        return Err("decompressed WAL payload length mismatch");
    }

    Ok(Some(decompressed))
}

fn looks_like_compressed_payload_envelope(payload: &[u8]) -> bool {
    payload.starts_with(&WAL_PAYLOAD_COMPRESSION_MAGIC)
}

fn write_timestamp_if_data_write(record: &TransactionRecord) -> Option<u64> {
    if matches!(
        record.kind,
        TransactionKind::Insert | TransactionKind::Update | TransactionKind::Delete
    ) {
        Some(record.timestamp_epoch_ms)
    } else {
        None
    }
}

fn latest_write_timestamp(entries: &[TransactionRecord]) -> Option<u64> {
    entries
        .iter()
        .filter_map(write_timestamp_if_data_write)
        .max()
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
        
        match decode_record_from_storage(&bytes[pos..pos + len]) {
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
        },

        Err(e) if e.kind() == ErrorKind::NotFound => {
            write_bytes(path, &make_header(FileKind::Data))
                .map_err(|_| "failed to initialize WAL file header")
        },

        Err(_) => Err("failed to inspect WAL file"),

    }

}

fn open_wal_append_file(path: &Path) -> Result<fs::File, &'static str> {
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|_| "failed to open WAL append file")
}

fn append_wal_bytes(
    append_file: &mut Option<fs::File>,
    path: &Path,
    bytes: &[u8],
) -> Result<(), &'static str> {
    if append_file.is_none() {
        *append_file = Some(open_wal_append_file(path)?);
    }

    if let Some(file) = append_file.as_mut()
        && file.write_all(bytes).is_ok() {
            return Ok(());
        }

    // Recover once by reopening the append handle in case the previous fd
    // became invalid or encountered transient I/O errors.
    *append_file = Some(open_wal_append_file(path)?);

    if let Some(file) = append_file.as_mut() {
        file.write_all(bytes)
            .map_err(|_| "failed to persist WAL bytes to disk")?;
    }

    Ok(())
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

    let mut retained_ids = std::collections::HashSet::new();
    if let Some(schema) = latest_schema.clone() {
        retained_ids.insert(schema.id.0);
    }

    if let Some(metadata) = latest_metadata.clone() {
        retained_ids.insert(metadata.id.0);
    }

    for record in entries.iter_mut() {
        if !retained_ids.contains(&record.id.0) {
            record.kind = TransactionKind::Ignore;
            record.refid = None;
            record.payload.clear();
        }
    }

    let mut retained = Vec::new();
    if let Some(mut schema) = latest_schema {
        if schema.refid.is_some_and(|refid| !retained_ids.contains(&refid.0)) {
            schema.refid = None;
        }
        retained.push(schema);
    }

    if let Some(mut metadata) = latest_metadata {
        if metadata.refid.is_some_and(|refid| !retained_ids.contains(&refid.0)) {
            metadata.refid = None;
        }
        retained.push(metadata);
    }

    retained.sort_by_key(|record| record.id.0);

    let truncate_refid = entries
        .last()
        .map(|record| record.id)
        .filter(|refid| retained_ids.contains(&refid.0));

    retained.push(TransactionRecord {
        id: TransactionId(last_id.0 + 1),
        groupid: None,
        refid: truncate_refid,
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
) -> (Sender<WalCommand>, mpsc::Receiver<()>) {
    
    let (tx, rx) = mpsc::channel::<WalCommand>();
    let (ready_tx, ready_rx) = mpsc::channel::<()>();

    thread::spawn(move || {

        let mut append_file: Option<fs::File> = None;

        if let Some(ref path) = wal_path {

            if let Err(e) = ensure_wal_file(path) {
                log::error!("failed to initialize WAL file '{}': {}", path.display(), e);
            }

            append_file = open_wal_append_file(path).ok();

            let existing = load_records_from_file(path);
            let mut count = 0usize;

            if let Ok(mut state) = storage.lock() {
                let entries = if let Some(entries) = state.get_mut(&stream_key) {
                    entries
                } else {
                    state.insert(stream_key.clone(), Vec::new());
                    state.get_mut(&stream_key).expect("WAL stream entry should exist")
                };
                if entries.is_empty() {
                    count = existing.len();
                    entries.extend(existing);
                } else {
                    count = entries.len();
                }
            }

            log::info!(
                "WAL worker started for stream={} (replayed {} record(s) from disk)",
                stream_key,
                count
            );

        } else {
            log::debug!("WAL worker started for stream={} (in-memory only)", stream_key);
        }

        let _ = ready_tx.send(());

        while let Ok(command) = rx.recv() {

            match command {

                WalCommand::Append { record, ack } => {

                    if let Ok(mut state) = storage.lock() {

                        let entries = if let Some(entries) = state.get_mut(&stream_key) {
                            entries
                        } else {
                            state.insert(stream_key.clone(), Vec::new());
                            // inserted above, so mutable access is guaranteed
                            state.get_mut(&stream_key).expect("WAL stream entry should exist")
                        };
                        let is_ordered = entries
                            .last()
                            .map(|last| record.id.0 > last.id.0)
                            .unwrap_or(true);

                        if is_ordered {

                            if let Some(ref path) = wal_path {

                                match frame_record(&record) {

                                    Ok(frame) => {
                                        if let Err(e) = append_wal_bytes(&mut append_file, path, &frame) {
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
                                    },

                                    Err(e) => {
                                        let _ = ack.send(Err(e));
                                        continue;
                                    }

                                }

                            }

                            entries.push(record);
                            let _ = ack.send(Ok(()));
                        
                        } else {
                            
                            if entries.contains(&record) {
                                // Exact duplicate already present; treat as idempotent success.
                                let _ = ack.send(Ok(()));
                                continue;
                            }

                            // Insert older records into sorted position so affinity imports from
                            // peers with divergent local id ranges are still preserved.
                            let mut insert_pos = entries
                                .binary_search_by_key(&record.id.0, |existing| existing.id.0)
                                .unwrap_or_else(|idx| idx);
                            while insert_pos < entries.len()
                                && entries[insert_pos].id.0 <= record.id.0
                            {
                                insert_pos += 1;
                            }

                            entries.insert(insert_pos, record);

                            if let Some(ref path) = wal_path
                                && let Err(e) = rewrite_wal_file(path, entries) {
                                    log::error!(
                                        "failed to rewrite WAL file for out-of-order insert stream={}: {}",
                                        stream_key,
                                        e
                                    );
                                    let _ = ack.send(Err(e));
                                    continue;
                                }

                            if let Some(ref path) = wal_path {
                                append_file = open_wal_append_file(path).ok();
                            }

                            log::warn!(
                                "out-of-order transaction accepted and merged for stream={}",
                                stream_key
                            );
                            let _ = ack.send(Ok(()));
                        }

                    } else {

                        log::error!(
                            "failed to acquire WAL storage lock for stream={}",
                            stream_key
                        );
                        let _ = ack.send(Err("failed to lock WAL storage"));
                        break;
                    }
                },

                WalCommand::AppendBatch { records, ack } => {

                    if records.is_empty() {
                        let _ = ack.send(Ok(()));
                        continue;
                    }

                    if let Ok(mut state) = storage.lock() {

                        let entries = if let Some(entries) = state.get_mut(&stream_key) {
                            entries
                        } else {
                            state.insert(stream_key.clone(), Vec::new());
                            state.get_mut(&stream_key).expect("WAL stream entry should exist")
                        };
                        let mut expected_next_id = entries
                            .last()
                            .map(|last| last.id.0.saturating_add(1))
                            .unwrap_or(1);

                        let ordered = records
                            .iter()
                            .all(|record| {
                                let is_next = record.id.0 == expected_next_id;
                                if is_next {
                                    expected_next_id = expected_next_id.saturating_add(1);
                                }
                                is_next
                            });

                        if ordered {

                            if let Some(ref path) = wal_path {
                                let mut frames = Vec::new();
                                let mut frame_error: Option<&'static str> = None;

                                for record in &records {
                                    match frame_record(record) {
                                        Ok(frame) => frames.extend_from_slice(&frame),
                                        Err(e) => {
                                            frame_error = Some(e);
                                            break;
                                        }
                                    }
                                }

                                if let Some(err) = frame_error {
                                    let _ = ack.send(Err(err));
                                    continue;
                                }

                                if let Err(e) = append_wal_bytes(&mut append_file, path, &frames) {
                                    log::error!(
                                        "failed to persist WAL record batch for stream={}: {}",
                                        stream_key,
                                        e
                                    );
                                    let _ = ack.send(Err("failed to persist WAL record batch to disk"));
                                    continue;
                                }
                            }

                            let reserve_hint = records.len().saturating_add(records.len() / 2);
                            entries.reserve(reserve_hint);
                            entries.extend(records);
                            let _ = ack.send(Ok(()));
                            continue;

                        }

                        let mut batch_error: Option<&'static str> = None;
                        let reserve_hint = records.len().saturating_add(records.len() / 2);
                        entries.reserve(reserve_hint);

                        for record in records {

                            let is_ordered = entries
                                .last()
                                .map(|last| record.id.0 > last.id.0)
                                .unwrap_or(true);

                            if is_ordered {

                                if let Some(ref path) = wal_path {
                                    match frame_record(&record) {
                                        Ok(frame) => {
                                            if let Err(e) = append_wal_bytes(&mut append_file, path, &frame) {
                                                log::error!(
                                                    "failed to persist WAL record for stream={}: {}",
                                                    stream_key,
                                                    e
                                                );
                                                batch_error = Some("failed to persist WAL record to disk");
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            batch_error = Some(e);
                                            break;
                                        }
                                    }
                                }

                                if batch_error.is_some() {
                                    break;
                                }

                                entries.push(record);
                                continue;
                            }

                            if entries.contains(&record) {
                                continue;
                            }

                            let mut insert_pos = entries
                                .binary_search_by_key(&record.id.0, |existing| existing.id.0)
                                .unwrap_or_else(|idx| idx);
                            while insert_pos < entries.len() && entries[insert_pos].id.0 <= record.id.0 {
                                insert_pos += 1;
                            }

                            entries.insert(insert_pos, record);

                            if let Some(ref path) = wal_path
                                && let Err(e) = rewrite_wal_file(path, entries) {
                                    log::error!(
                                        "failed to rewrite WAL file for out-of-order insert stream={}: {}",
                                        stream_key,
                                        e
                                    );
                                    batch_error = Some(e);
                                    break;
                                }

                            if let Some(ref path) = wal_path {
                                append_file = open_wal_append_file(path).ok();
                            }
                        }

                        if let Some(err) = batch_error {
                            let _ = ack.send(Err(err));
                            continue;
                        }

                        log::warn!(
                            "out-of-order transaction batch accepted and merged for stream={}",
                            stream_key
                        );
                        let _ = ack.send(Ok(()));

                    } else {

                        log::error!(
                            "failed to acquire WAL storage lock for batch stream={}",
                            stream_key
                        );
                        let _ = ack.send(Err("failed to lock WAL storage"));
                        break;
                    }
                },

                WalCommand::CompactToLatestSchemaAndMetadata {
                    actor,
                    timestamp_epoch_ms,
                    ack,
                } => {
                    if let Ok(mut state) = storage.lock() {
                        let entries = if let Some(entries) = state.get_mut(&stream_key) {
                            entries
                        } else {
                            state.insert(stream_key.clone(), Vec::new());
                            state.get_mut(&stream_key).expect("WAL stream entry should exist")
                        };
                        compact_entries_to_latest_schema_and_metadata(
                            entries,
                            actor,
                            timestamp_epoch_ms,
                        );

                        if let Some(ref path) = wal_path
                            && let Err(e) = rewrite_wal_file(path, entries) {
                                log::error!(
                                    "failed to rewrite compacted WAL for stream={}: {}",
                                    stream_key,
                                    e
                                );
                                let _ = ack.send(Err(e));
                                continue;
                            }

                        if let Some(ref path) = wal_path {
                            append_file = open_wal_append_file(path).ok();
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
                },

                WalCommand::Shutdown => {
                    log::info!("WAL worker shutting down for stream={}", stream_key);
                    break;
                }

            }

        }

    });
    
    (tx, ready_rx)

}


#[cfg(test)]
#[path = "wal_test.rs"]
mod tests;
