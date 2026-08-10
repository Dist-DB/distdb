use std::collections::HashMap;
use std::borrow::Cow;
use std::fs;
use std::io::BufReader;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::sync::OnceLock;
use std::thread;
use std::time::Instant;

use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;

use common::helpers::format::{make_header, verify_header, FileKind, HEADER_SIZE};
use common::helpers::{read_bytes, stable_id, write_bytes};

use crate::core::identity::UserId;
use crate::engine::database::row_payload::{
    AesGcmRowPayloadCryptoProvider,
    looks_like_encrypted_row_payload, EncryptedRowPayloadTransform,
    RowPayloadDecryptionTransform, RowPayloadEncryptionWriteTransform,
};
use crate::engine::database::transaction::record::{
    ChainedTransactionPayloadResolver, ChainedTransactionPayloadWriter, PayloadTransformError,
    TransactionPayloadContext, TransactionPayloadResolver, TransactionPayloadTransform,
    TransactionPayloadWriteTransform,
};
use crate::engine::database::transaction::{TransactionId, TransactionLog, TransactionRecord};
use crate::TransactionKind;

static NEXT_WAL_CACHE_SCOPE_ID: AtomicUsize = AtomicUsize::new(1);
static DEFAULT_TRANSACTION_PAYLOAD_CONTEXT: OnceLock<TransactionPayloadContext> = OnceLock::new();

fn next_wal_cache_scope_id() -> usize {
    NEXT_WAL_CACHE_SCOPE_ID.fetch_add(1, Ordering::Relaxed)
}

fn default_transaction_payload_context() -> &'static TransactionPayloadContext {
    DEFAULT_TRANSACTION_PAYLOAD_CONTEXT.get_or_init(TransactionPayloadContext::default)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WalCompressionPayloadTransform;

impl TransactionPayloadTransform for WalCompressionPayloadTransform {

    fn transform_payload(
        &self,
        payload: &[u8],
        _context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError> {

        match maybe_decode_compressed_payload_bytes(payload) {

            Ok(Some(decoded)) => Ok(Some(decoded)),

            Ok(None) => Ok(None),

            Err("failed to decompress WAL payload") => {
                Err(PayloadTransformError::InvalidCompressedPayload)
            },

            Err("decompressed WAL payload length mismatch") => {
                Err(PayloadTransformError::IntegrityCheckFailed)
            },

            Err(message) => Err(PayloadTransformError::InternalTransformError(
                message.to_string(),
            )),

        }

    }

}

fn resolve_wal_storage_payload<'a>(
    raw_payload: Option<&'a [u8]>,
    context: &TransactionPayloadContext,
) -> Result<Option<Cow<'a, [u8]>>, PayloadTransformError> {

    let Some(payload) = raw_payload else {
        return Ok(None);
    };

    // Hot path: default-context reads of plain payload bytes can bypass the
    // transform chain entirely.
    if context == default_transaction_payload_context() && !looks_like_zlib_payload(payload) {
        return Ok(Some(Cow::Borrowed(payload)));
    }

    let mut resolver = ChainedTransactionPayloadResolver::new();
    resolver.push_transform(WalCompressionPayloadTransform);
    resolver.push_transform(RowPayloadDecryptionTransform::new(AesGcmRowPayloadCryptoProvider));
    resolver.push_transform(EncryptedRowPayloadTransform::preserve_opaque());

    let resolved = resolver.resolve_payload(Some(payload), context)?;

    Ok(resolved.map(|resolved_payload| {
        if resolved_payload.as_slice() == payload {
            Cow::Borrowed(payload)
        } else {
            Cow::Owned(resolved_payload)
        }
    }))

}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WalCompressionPayloadWriteTransform;

impl TransactionPayloadWriteTransform for WalCompressionPayloadWriteTransform {

    fn transform_payload_for_write(
        &self,
        record: &TransactionRecord,
        payload: &[u8],
        _context: &TransactionPayloadContext,
    ) -> Result<Option<Vec<u8>>, PayloadTransformError> {

        if should_skip_payload_compression(record, payload) {
            return Ok(None);
        }

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());

        encoder
            .write_all(payload)
            .map_err(|_| PayloadTransformError::InternalTransformError(
                "failed to compress WAL payload".to_string(),
            ))?;

        let compressed = encoder
            .finish()
            .map_err(|_| PayloadTransformError::InternalTransformError(
                "failed to finish WAL payload compression".to_string(),
            ))?;

        Ok(Some(compressed))
    
    }

}

fn write_wal_storage_payload<'a>(
    record: &TransactionRecord,
    raw_payload: Option<&'a [u8]>,
    context: &TransactionPayloadContext,
) -> Result<Option<Cow<'a, [u8]>>, PayloadTransformError> {

    let Some(payload) = raw_payload else {
        return Ok(None);
    };

    let mut writer = ChainedTransactionPayloadWriter::new();
    writer.push_transform(RowPayloadEncryptionWriteTransform::new(AesGcmRowPayloadCryptoProvider));
    writer.push_transform(EncryptedRowPayloadTransform::preserve_opaque());
    writer.push_transform(WalCompressionPayloadWriteTransform);

    let transformed = writer.write_payload_with_context(record, Some(payload), context)?;

    Ok(transformed.map(|transformed_payload| {
        if transformed_payload.as_slice() == payload {
            Cow::Borrowed(payload)
        } else {
            Cow::Owned(transformed_payload)
        }
    }))

}

fn into_owned_payload(payload: Option<Cow<'_, [u8]>>) -> Option<Vec<u8>> {

    match payload {
        Some(Cow::Owned(payload)) => Some(payload),
        Some(Cow::Borrowed(_)) | None => None,
    }

}

fn first_record_index_after_id(entries: &[TransactionRecord], from: TransactionId) -> usize {
    entries.partition_point(|entry| entry.id.0 <= from.0)
}

fn record_for_storage_with_payload(
    record: &TransactionRecord,
    payload: Option<Vec<u8>>,
) -> TransactionRecord {

    TransactionRecord::new(
        record.id,
        record.groupid,
        record.refid,
        record.timestamp_epoch_ms,
        record.actor.clone(),
        record.kind,
        payload,
    )

}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalStreamMode {
    Durable,
    Ephemeral,
}

#[expect(clippy::large_enum_variant, reason="WalCommand variants are small enough to be efficient, and we want to avoid heap allocations for the enum itself")]
#[derive(Debug)]
enum WalCommand {
    Append {
        record: TransactionRecord,
        context: TransactionPayloadContext,
        ack: Sender<Result<(), &'static str>>,
    },
    AppendBatch {
        records: Vec<TransactionRecord>,
        context: TransactionPayloadContext,
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
    storage: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<TransactionRecord>>>>>>,
    cache_scope_id: usize,
    write_high_water_by_stream: Mutex<HashMap<String, u64>>,
    stream_modes: Mutex<HashMap<String, WalStreamMode>>,
    data_dir: Option<Arc<PathBuf>>,
}

impl Default for ConcurrentWalManager {

    fn default() -> Self {
        Self {
            workers: Mutex::new(HashMap::new()),
            storage: Arc::new(Mutex::new(HashMap::new())),
            cache_scope_id: next_wal_cache_scope_id(),
            write_high_water_by_stream: Mutex::new(HashMap::new()),
            stream_modes: Mutex::new(HashMap::new()),
            data_dir: None,
        }
    }

}

impl ConcurrentWalManager {

    pub fn cache_scope_id(&self) -> usize {
        self.cache_scope_id
    }

    pub fn data_dir_path(&self) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|path| path.as_ref().clone())
    }

    pub fn with_records<T, F>(&self, wal_id: &str, func: F) -> Option<T>
    where
        F: FnOnce(&[TransactionRecord]) -> T,
    {

        let stream_key = obfuscated_stream_key(wal_id).ok()?;

        self.hydrate_stream_if_needed(wal_id, &stream_key);

        let entries = self.stream_entries_handle(&stream_key)?;
        let entries = entries.lock().ok()?;

        Some(func(entries.as_slice()))

    }

    pub fn scan_durable_records_if_unloaded<F>(
        &self,
        wal_id: &str,
        mut on_record: F,
    ) -> Result<bool, &'static str>
    where
        F: FnMut(TransactionRecord),
    {

        let stream_key = obfuscated_stream_key(wal_id)?;

        if self.stream_mode(wal_id) != WalStreamMode::Durable {
            return Ok(false);
        }

        let stream_loaded = self
            .storage
            .lock()
            .map_err(|_| "failed to lock WAL storage")?
            .contains_key(&stream_key);

        if stream_loaded {
            return Ok(false);
        }

        let Some(data_dir) = &self.data_dir else {
            return Ok(false);
        };

        let wal_path = data_dir.join(FileKind::Data.file_name(&stream_key));
        if !wal_path.exists() {
            return Ok(false);
        }

        let file = fs::File::open(&wal_path).map_err(|_| "failed to open WAL file")?;
        let mut reader = BufReader::new(file);

        let mut header = [0u8; HEADER_SIZE];
        reader
            .read_exact(&mut header)
            .map_err(|_| "failed to read WAL header")?;

        verify_header(FileKind::Data, &header)
            .map_err(|_| "invalid WAL header/version")?;

        let context = default_transaction_payload_context();
        let mut frame_offset = HEADER_SIZE as u64;
        let mut len_buf = [0u8; 8];
        let mut frame = Vec::new();
        let max_frame_size = wal_max_frame_size_bytes();

        loop {
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    log::warn!(
                        "failed to read WAL frame length at byte offset {}: {}",
                        frame_offset,
                        err,
                    );
                    break;
                }
            }

            frame_offset = frame_offset.saturating_add(8);

            let len_u64 = u64::from_le_bytes(len_buf);

            if len_u64 > usize::MAX as u64 {
                log::warn!(
                    "invalid WAL frame length {} at byte offset {}, stopping replay",
                    len_u64,
                    frame_offset,
                );
                break;
            }

            let len = len_u64 as usize;

            if len > max_frame_size {
                log::warn!(
                    "WAL frame length {} exceeds max {} at byte offset {}, stopping replay",
                    len,
                    max_frame_size,
                    frame_offset,
                );
                break;
            }

            frame.resize(len, 0);

            if let Err(err) = reader.read_exact(&mut frame[..]) {
                if err.kind() == ErrorKind::UnexpectedEof {
                    log::warn!(
                        "truncated WAL frame at byte offset {}, stopping replay",
                        frame_offset,
                    );
                } else {
                    log::warn!(
                        "failed to read WAL frame at byte offset {}: {}",
                        frame_offset,
                        err,
                    );
                }
                break;
            }

            match decode_record_from_storage_with_context(&frame, context) {
                Ok(record) => on_record(record),
                Err(err) => {
                    log::error!(
                        "failed to deserialize WAL frame at byte {}: {}",
                        frame_offset,
                        err,
                    );
                    break;
                }
            }

            frame_offset = frame_offset.saturating_add(len_u64);
        }

        Ok(true)

    }

    fn stream_entries_handle(&self, stream_key: &str) -> Option<Arc<Mutex<Vec<TransactionRecord>>>> {

        let store = self.storage.lock().ok()?;
        store.get(stream_key).cloned()

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
            if let Ok(mut store) = self.storage.lock() {
                // Mark stream as hydrated with an empty record set to avoid
                // repeated disk-existence checks on hot read paths.
                store
                    .entry(stream_key.to_string())
                    .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));
            }
            return;
        }

        let existing = load_records_from_path(&wal_path);
        let entries_handle = if let Ok(mut store) = self.storage.lock() {
            store
                .entry(stream_key.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
                .clone()
        } else {
            return;
        };

        if let Ok(mut entries) = entries_handle.lock()
            && entries.is_empty()
        {
            entries.extend(existing);
        }

        if let (Some(entries_handle), Ok(mut high_water)) = (
            self.stream_entries_handle(stream_key),
            self.write_high_water_by_stream.lock(),
        ) {
            let max_ts = entries_handle
                .lock()
                .ok()
                .and_then(|entries| latest_write_timestamp(entries.as_slice()));

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
            let entries = match self.stream_entries_handle(&stream_key) {
                Some(entries) => entries,
                None => return false,
            };

            entries
                .lock()
                .ok()
                .and_then(|entries| latest_write_timestamp(entries.as_slice()))
        };

        if let Ok(mut high_water) = self.write_high_water_by_stream.lock() {
            match max_ts {
                Some(ts) => {
                    high_water.insert(stream_key, ts);
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
            cache_scope_id: next_wal_cache_scope_id(),
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

        let previous = modes.insert(stream_key.clone(), mode);

        if previous != Some(mode)
            && let Ok(mut workers) = self.workers.lock()
            && let Some(sender) = workers.remove(&stream_key) {
                let _ = sender.send(WalCommand::Shutdown);
            }

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

        if let (Some(entries), Ok(mut high_water)) = (
            self.stream_entries_handle(&stream_key),
            self.write_high_water_by_stream.lock(),
        ) {

            let max_ts = entries
                .lock()
                .ok()
                .and_then(|entries| latest_write_timestamp(entries.as_slice()));

            match max_ts {

                Some(ts) => {
                    high_water.insert(stream_key, ts);
                },

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

    pub fn clear_stream_records(&self, wal_id: &str) -> Result<(), &'static str> {

        let stream_key = obfuscated_stream_key(wal_id)?;

        let stream_mode = self
            .stream_modes
            .lock()
            .ok()
            .and_then(|modes| modes.get(&stream_key).copied())
            .unwrap_or(WalStreamMode::Durable);

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
            let mut high_water = self
                .write_high_water_by_stream
                .lock()
                .map_err(|_| "failed to lock WAL write high-water map")?;
            high_water.remove(&stream_key);
        }

        if matches!(stream_mode, WalStreamMode::Durable)
            && let Some(data_dir) = &self.data_dir {
                let wal_path = data_dir.join(FileKind::Data.file_name(&stream_key));
                write_bytes(&wal_path, &make_header(FileKind::Data))
                    .map_err(|_| "failed to clear WAL file")?;
            }

        Ok(())

    }

    fn get_or_spawn_worker(&self, wal_id: &str) -> Result<Sender<WalCommand>, &'static str> {

        let stream_key = obfuscated_stream_key(wal_id)?;
        self.get_or_spawn_worker_for_stream_key(&stream_key)

    }

    fn get_or_spawn_worker_for_stream_key(
        &self,
        stream_key: &str,
    ) -> Result<Sender<WalCommand>, &'static str> {

        let mut workers = self
            .workers
            .lock()
            .map_err(|_| "failed to lock WAL workers")?;

        if let Some(existing) = workers.get(stream_key) {
            return Ok(existing.clone());
        }

        let stream_mode = self
            .stream_modes
            .lock()
            .ok()
            .and_then(|modes| modes.get(stream_key).copied())
            .unwrap_or(WalStreamMode::Durable);

        let wal_path = match stream_mode {
            WalStreamMode::Durable => self
                .data_dir
                .as_ref()
                .map(|dir| dir.join(FileKind::Data.file_name(stream_key))),
            WalStreamMode::Ephemeral => None,
        };

        let (sender, ready_rx) = spawn_worker(
            stream_key.to_string(),
            Arc::clone(&self.storage),
            wal_path,
        );

        ready_rx
            .recv()
            .map_err(|_| "failed to receive WAL worker startup acknowledgement")?;
        
        workers.insert(stream_key.to_string(), sender.clone());
        
        Ok(sender)

    }

    pub fn latest_transaction_id(&self, wal_id: &str) -> Option<TransactionId> {

        let stream_key = obfuscated_stream_key(wal_id).ok()?;

        self.hydrate_stream_if_needed(wal_id, &stream_key);

        let entries = self.stream_entries_handle(&stream_key)?;
        let entries = entries.lock().ok()?;
        entries.last().map(|entry| entry.id)

    }

    pub fn latest_transaction_id_if_loaded(&self, wal_id: &str) -> Option<TransactionId> {

        let stream_key = obfuscated_stream_key(wal_id).ok()?;

        let entries = self.stream_entries_handle(&stream_key)?;
        let entries = entries.lock().ok()?;
        entries.last().map(|entry| entry.id)

    }

    pub fn append_batch(
        &self,
        wal_id: &str,
        records: Vec<TransactionRecord>,
    ) -> Result<(), &'static str> {

        self.append_batch_with_context(wal_id, records, default_transaction_payload_context())

    }

    pub fn append_batch_with_context(
        &self,
        wal_id: &str,
        records: Vec<TransactionRecord>,
        context: &TransactionPayloadContext,
    ) -> Result<(), &'static str> {

        if records.is_empty() {
            return Ok(());
        }

        let batch_max_write_ts = records
            .iter()
            .filter_map(write_timestamp_if_data_write)
            .max();

        let stream_key = obfuscated_stream_key(wal_id)?;
        let sender = self.get_or_spawn_worker_for_stream_key(&stream_key)?;
        let (ack_tx, ack_rx) = mpsc::channel::<Result<(), &'static str>>();

        sender
            .send(WalCommand::AppendBatch {
                records,
                context: context.clone(),
                ack: ack_tx,
            })
            .map_err(|_| "failed to send WAL append-batch command")?;

        ack_rx
            .recv()
            .map_err(|_| "failed to receive WAL append-batch acknowledgement")??;

        if let Some(batch_max_write_ts) = batch_max_write_ts
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

    pub fn since_with_context(
        &self,
        wal_id: &str,
        from: Option<TransactionId>,
        context: &TransactionPayloadContext,
    ) -> Result<Vec<TransactionRecord>, &'static str> {
        
        if context == default_transaction_payload_context() {
            return Ok(self.since(wal_id, from));
        }

        let mut records = self.since(wal_id, from);

        for record in &mut records {
            
            let resolved_payload = resolve_wal_storage_payload(record.payload_raw(), context)
                .map_err(map_payload_transform_error)?;

            if let Some(Cow::Owned(payload)) = resolved_payload {
                record.set_payload(Some(payload), Some(context));
            }

        }

        Ok(records)

    }

}

impl TransactionLog for ConcurrentWalManager {

    fn append(&self, wal_id: &str, record: TransactionRecord) -> Result<(), &'static str> {

        self.append_with_context(wal_id, record, default_transaction_payload_context())

    }

    fn since(&self, wal_id: &str, from: Option<TransactionId>) -> Vec<TransactionRecord> {

        let stream_key = match obfuscated_stream_key(wal_id) {
            Ok(k) => k,
            Err(_) => return Vec::new(),
        };

        self.hydrate_stream_if_needed(wal_id, &stream_key);

        let entries = match self.stream_entries_handle(&stream_key) {
            Some(entries) => entries,
            None => return Vec::new(),
        };

        let entries = match entries.lock() {
            Ok(entries) => entries,
            Err(_) => return Vec::new(),
        };

        match from {
            Some(min_id) => {
                let start_idx = first_record_index_after_id(entries.as_slice(), min_id);
                entries[start_idx..].to_vec()
            },
            None => entries.clone(),
        }

    }

    fn for_each_since<F>(&self, wal_id: &str, from: Option<TransactionId>, mut func: F)
    where
        F: FnMut(&TransactionRecord),
    {

        let stream_key = match obfuscated_stream_key(wal_id) {
            Ok(k) => k,
            Err(_) => return,
        };

        self.hydrate_stream_if_needed(wal_id, &stream_key);

        let entries = match self.stream_entries_handle(&stream_key) {
            Some(entries) => entries,
            None => return,
        };

        let entries = match entries.lock() {
            Ok(entries) => entries,
            Err(_) => return,
        };

        let start_idx = from
            .map(|min_id| first_record_index_after_id(entries.as_slice(), min_id))
            .unwrap_or(0);

        for record in &entries[start_idx..] {
            func(record);
        }

    }

    fn with_all_records<T, F>(&self, wal_id: &str, func: F) -> T
    where
        F: FnOnce(&[TransactionRecord]) -> T,
    {

        let stream_key = match obfuscated_stream_key(wal_id) {
            Ok(k) => k,
            Err(_) => return func(&[]),
        };

        self.hydrate_stream_if_needed(wal_id, &stream_key);

        let Some(entries) = self.stream_entries_handle(&stream_key) else {
            return func(&[]);
        };

        let Ok(entries) = entries.lock() else {
            return func(&[]);
        };

        func(entries.as_slice())

    }

    fn since_kinds(
        &self,
        wal_id: &str,
        from: Option<TransactionId>,
        kinds: &[TransactionKind],
    ) -> Vec<TransactionRecord> {

        if kinds.is_empty() {
            return Vec::new();
        }

        let stream_key = match obfuscated_stream_key(wal_id) {
            Ok(k) => k,
            Err(_) => return Vec::new(),
        };

        self.hydrate_stream_if_needed(wal_id, &stream_key);

        let entries = match self.stream_entries_handle(&stream_key) {
            Some(entries) => entries,
            None => return Vec::new(),
        };

        let entries = match entries.lock() {
            Ok(entries) => entries,
            Err(_) => return Vec::new(),
        };

        entries
            .iter()
            .filter(|entry| {
                
                let kind_matched = match kinds {
                    [kind] => entry.kind == *kind,
                    [first, second] => entry.kind == *first || entry.kind == *second,
                    _ => kinds.contains(&entry.kind),
                };

                from.map(|min_id| entry.id.0 > min_id.0).unwrap_or(true)
                    && kind_matched

            })
            .cloned()
            .collect()

    }

}

impl ConcurrentWalManager {

    pub fn append_with_context(
        &self,
        wal_id: &str,
        record: TransactionRecord,
        context: &TransactionPayloadContext,
    ) -> Result<(), &'static str> {

        let write_ts = write_timestamp_if_data_write(&record);
        let stream_key = obfuscated_stream_key(wal_id)?;

        let sender = self.get_or_spawn_worker_for_stream_key(&stream_key)?;
        let (ack_tx, ack_rx) = mpsc::channel::<Result<(), &'static str>>();

        sender
            .send(WalCommand::Append {
                record,
                context: context.clone(),
                ack: ack_tx,
            })
            .map_err(|_| "failed to send WAL append command")?;

        ack_rx
            .recv()
            .map_err(|_| "failed to receive WAL append acknowledgement")??;

        if let Some(write_ts) = write_ts
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
    
}

fn frame_record(record: &TransactionRecord) -> Result<Vec<u8>, &'static str> {

    frame_record_with_context(record, default_transaction_payload_context())

}

fn frame_record_with_context(
    record: &TransactionRecord,
    context: &TransactionPayloadContext,
) -> Result<Vec<u8>, &'static str> {

    let encoded = encode_record_for_storage_with_context(record, context)?;
    let len = encoded.len() as u64;
    let mut frame = Vec::with_capacity(8 + encoded.len());

    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(&encoded);

    Ok(frame)

}

pub(crate) fn encode_record_for_storage(
    record: &TransactionRecord,
) -> Result<Vec<u8>, &'static str> {
    encode_record_for_storage_with_context(record, default_transaction_payload_context())
}

pub(crate) fn encode_record_for_storage_with_context(
    record: &TransactionRecord,
    context: &TransactionPayloadContext,
) -> Result<Vec<u8>, &'static str> {

    let stored_payload = write_wal_storage_payload(record, record.payload_raw(), context)
        .map_err(map_payload_write_transform_error)?;

    if let Some(payload) = into_owned_payload(stored_payload) {
        let record_for_storage = record_for_storage_with_payload(record, Some(payload));
        return common::helpers::bincode_compat::serialize(&record_for_storage).map_err(|_| "failed to serialize WAL record");
    }

    common::helpers::bincode_compat::serialize(record).map_err(|_| "failed to serialize WAL record")

}

pub(crate) fn decode_record_from_storage(
    encoded: &[u8],
) -> Result<TransactionRecord, &'static str> {

    decode_record_from_storage_with_context(encoded, default_transaction_payload_context())

}

fn decode_record_from_storage_internal(
    encoded: &[u8],
    context: &TransactionPayloadContext,
) -> Result<TransactionRecord, &'static str> {

    let mut record = common::helpers::bincode_compat::deserialize::<TransactionRecord>(encoded)
        .map_err(|_| "failed to deserialize WAL record")?;

    if let Some(payload) = into_owned_payload(
        resolve_wal_storage_payload(record.payload_raw(), context)
            .map_err(map_payload_transform_error)?,
    ) {
        record.set_payload(Some(payload), Some(context));
    }

    Ok(record)

}

pub(crate) fn decode_record_from_storage_with_context(
    encoded: &[u8],
    context: &TransactionPayloadContext,
) -> Result<TransactionRecord, &'static str> {

    decode_record_from_storage_internal(encoded, context)

}

fn should_skip_payload_compression(record: &TransactionRecord, payload: &[u8]) -> bool {

    matches!(
        record.kind,
        TransactionKind::Insert | TransactionKind::Update | TransactionKind::Delete
    ) && looks_like_encrypted_row_payload(payload)

}

fn map_payload_transform_error(error: PayloadTransformError) -> &'static str {

    match error {
        
        PayloadTransformError::InvalidCompressedPayload => "failed to decompress WAL payload",
        
        PayloadTransformError::IntegrityCheckFailed => "decompressed WAL payload length mismatch",
        
        PayloadTransformError::UnsupportedFormat => "unsupported WAL payload format",

        PayloadTransformError::InvalidEncryptedPayload |
        PayloadTransformError::DecryptFailed |
        PayloadTransformError::EncryptionNotConfigured |
        PayloadTransformError::InternalTransformError(_) => "failed to deserialize WAL record"
    
    }

}

fn map_payload_write_transform_error(error: PayloadTransformError) -> &'static str {
    
    match error {

        PayloadTransformError::InternalTransformError(message)
            if message == "failed to compress WAL payload" => "failed to compress WAL payload",

        PayloadTransformError::InternalTransformError(message)
            if message == "failed to finish WAL payload compression" => "failed to finish WAL payload compression",
            
        PayloadTransformError::UnsupportedFormat => "unsupported WAL payload format",

        PayloadTransformError::IntegrityCheckFailed => "decompressed WAL payload length mismatch",

        PayloadTransformError::InvalidCompressedPayload => "failed to compress WAL payload",
        
        PayloadTransformError::InvalidEncryptedPayload |
        PayloadTransformError::DecryptFailed |
        PayloadTransformError::EncryptionNotConfigured |
        PayloadTransformError::InternalTransformError(_) => "failed to serialize WAL record",

    }
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

fn get_or_insert_stream_entries_handle(
    state: &mut HashMap<String, Arc<Mutex<Vec<TransactionRecord>>>>,
    stream_key: &str,
) -> Arc<Mutex<Vec<TransactionRecord>>> {
    state
        .entry(stream_key.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
        .clone()
}

fn obfuscated_stream_key(wal_id: &str) -> Result<String, &'static str> {

    let normalized = wal_id.trim().to_ascii_lowercase();

    if normalized.is_empty() {
        return Err("wal_id must not be empty");
    }

    Ok(stable_id(&[&normalized]))

}

fn decode_record_from_frame(
    bytes: &[u8],
    offset: usize,
    len: usize,
    context: &TransactionPayloadContext,
) -> Result<TransactionRecord, &'static str> {
    
    decode_record_from_storage_with_context(&bytes[offset..offset + len], context)

}

fn decode_records_sequential_from_bytes(bytes: &[u8]) -> Vec<TransactionRecord> {

    decode_records_sequential_from_bytes_with_context(bytes, default_transaction_payload_context())

}

fn decode_records_sequential_from_bytes_with_context(
    bytes: &[u8],
    context: &TransactionPayloadContext,
) -> Vec<TransactionRecord> {

    let mut records = Vec::new();
    let mut pos = HEADER_SIZE;
    let max_frame_size = wal_max_frame_size_bytes();

    while pos + 8 <= bytes.len() {

        let len_u64 = u64::from_le_bytes(
            bytes[pos..pos + 8]
                .try_into()
                .expect("slice is exactly 8 bytes"),
        );

        pos += 8;

        if len_u64 > usize::MAX as u64 {
            log::warn!(
                "invalid WAL frame length {} at byte offset {}, stopping replay",
                len_u64,
                pos,
            );
            break;
        }

        let len = len_u64 as usize;

        if len > max_frame_size {
            log::warn!(
                "WAL frame length {} exceeds max {} at byte offset {}, stopping replay",
                len,
                max_frame_size,
                pos,
            );
            break;
        }

        let Some(end) = pos.checked_add(len) else {
            log::warn!(
                "WAL frame length overflow at byte offset {}, stopping replay",
                pos,
            );
            break;
        };

        if end > bytes.len() {
            log::warn!("truncated WAL frame at byte offset {}, stopping replay", pos);
            break;
        }

        match decode_record_from_frame(bytes, pos, len, context) {

            Ok(record) => records.push(record),

            Err(e) => {
                log::error!("failed to deserialize WAL frame at byte {}: {}", pos, e);
                break;
            }
            
        }

        pos = end;

    }

    records

}

fn decode_records_sequential_from_reader_with_context<R: Read>(
    reader: &mut R,
    context: &TransactionPayloadContext,
) -> Vec<TransactionRecord> {

    let mut records = Vec::new();
    let mut frame_offset = HEADER_SIZE as u64;
    let mut len_buf = [0u8; 8];
    let mut frame = Vec::new();
    let max_frame_size = wal_max_frame_size_bytes();

    loop {

        match reader.read_exact(&mut len_buf) {

            Ok(()) => {},

            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,

            Err(err) => {
                log::warn!(
                    "failed to read WAL frame length at byte offset {}: {}",
                    frame_offset,
                    err,
                );
                break;
            }

        }

        frame_offset = frame_offset.saturating_add(8);

        let len_u64 = u64::from_le_bytes(len_buf);

        if len_u64 > usize::MAX as u64 {
            log::warn!(
                "invalid WAL frame length {} at byte offset {}, stopping replay",
                len_u64,
                frame_offset,
            );
            break;
        }

        let len = len_u64 as usize;

        if len > max_frame_size {
            log::warn!(
                "WAL frame length {} exceeds max {} at byte offset {}, stopping replay",
                len,
                max_frame_size,
                frame_offset,
            );
            break;
        }

        frame.resize(len, 0);

        if let Err(err) = reader.read_exact(&mut frame[..]) {
            if err.kind() == ErrorKind::UnexpectedEof {
                log::warn!(
                    "truncated WAL frame at byte offset {}, stopping replay",
                    frame_offset,
                );
            } else {
                log::warn!(
                    "failed to read WAL frame at byte offset {}: {}",
                    frame_offset,
                    err,
                );
            }
            break;
        }

        match decode_record_from_storage_with_context(&frame, context) {

            Ok(record) => records.push(record),

            Err(err) => {
                log::error!(
                    "failed to deserialize WAL frame at byte {}: {}",
                    frame_offset,
                    err,
                );
                break;
            }

        }

        frame_offset = frame_offset.saturating_add(len_u64);

    }

    records

}

fn wal_max_frame_size_bytes() -> usize {

    std::env::var("DISTDB_WAL_MAX_FRAME_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(64 * 1024 * 1024)

}

fn load_records_from_stream(bytes: Vec<u8>) -> Vec<TransactionRecord> {

    if let Err(e) = verify_header(FileKind::Data, &bytes) {
        log::error!("invalid WAL header from byte stream: {}", e);
        return Vec::new();
    }

    let context = default_transaction_payload_context();

    let decode_started_at = Instant::now();
    let records = decode_records_sequential_from_bytes_with_context(&bytes, context);    
    let decode_elapsed_ms = decode_started_at.elapsed().as_millis();

    if decode_elapsed_ms >= 1_000 {
        log::info!(
            "wal stream load timing records={} bytes={} decode_ms={}",
            records.len(),
            bytes.len(),
            decode_elapsed_ms,
        );
    }

    records

}

fn load_records_from_path(path: &Path) -> Vec<TransactionRecord> {

    let started_at = Instant::now();

    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return Vec::new(),
    };

    let byte_len = file.metadata().ok().map(|meta| meta.len()).unwrap_or(0);

    let mut reader = BufReader::new(file);
    let mut header = [0u8; HEADER_SIZE];

    if let Err(err) = reader.read_exact(&mut header) {
        log::warn!(
            "failed to read WAL header from '{}': {}",
            path.display(),
            err,
        );
        return Vec::new();
    }

    if let Err(err) = verify_header(FileKind::Data, &header) {
        log::error!("invalid WAL header in '{}': {}", path.display(), err);
        return Vec::new();
    }

    let context = default_transaction_payload_context();
    let decode_started_at = Instant::now();
    let records = decode_records_sequential_from_reader_with_context(&mut reader, context);
    let decode_elapsed_ms = decode_started_at.elapsed().as_millis();
    let total_elapsed_ms = started_at.elapsed().as_millis();

    if total_elapsed_ms >= 1_000 {
        log::info!(
            "wal file load timing path={} records={} bytes={} read_ms={} decode_ms={} total_ms={}",
            path.display(),
            records.len(),
            byte_len,
            total_elapsed_ms.saturating_sub(decode_elapsed_ms),
            decode_elapsed_ms,
            total_elapsed_ms,
        );
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
            if wal_sync_on_append() {
                file.sync_data()
                    .map_err(|_| "failed to sync WAL bytes to disk")?;
            }
            return Ok(());
        }

    // Recover once by reopening the append handle in case the previous fd
    // became invalid or encountered transient I/O errors.
    *append_file = Some(open_wal_append_file(path)?);

    if let Some(file) = append_file.as_mut() {

        file.write_all(bytes)
            .map_err(|_| "failed to persist WAL bytes to disk")?;

        if wal_sync_on_append() {
            file.sync_data()
                .map_err(|_| "failed to sync WAL bytes to disk")?;
        }

    }

    Ok(())
    
}

fn rewrite_wal_file(path: &Path, records: &[TransactionRecord]) -> Result<(), &'static str> {
    rewrite_wal_file_with_context(path, records, default_transaction_payload_context())
}

fn rewrite_wal_file_with_context(
    path: &Path,
    records: &[TransactionRecord],
    context: &TransactionPayloadContext,
) -> Result<(), &'static str> {
    
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&make_header(FileKind::Data));
    
    for record in records {
        let frame = frame_record_with_context(record, context)?;
        bytes.extend_from_slice(&frame);
    }
    
    write_bytes(path, &bytes).map_err(|_| "failed to rewrite compacted WAL file")?;

    if wal_sync_on_append() {

        let file = fs::OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|_| "failed to reopen WAL file for sync")?;

        file.sync_all()
            .map_err(|_| "failed to sync rewritten WAL file")?;

    }

    Ok(())

}

fn wal_sync_on_append() -> bool {

    std::env::var("DISTDB_WAL_SYNC_ON_APPEND")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)

}

fn compact_entries_to_latest_schema_and_metadata(
    entries: &mut Vec<TransactionRecord>,
    actor: UserId,
    timestamp_epoch_ms: u64,
) {

    let last_id = entries.last().map(|record| record.id).unwrap_or(TransactionId(0));

    let mut latest_schema = None;
    let mut latest_metadata = None;

    for record in entries.iter().rev() {

        if latest_schema.is_none() && record.kind == TransactionKind::SchemaChange {
            latest_schema = Some(record.clone());
        }

        if latest_metadata.is_none()
            && matches!(
                record.kind,
                TransactionKind::MetadataChange | TransactionKind::SecurityChange
            )
        {
            latest_metadata = Some(record.clone());
        }

        if latest_schema.is_some() && latest_metadata.is_some() {
            break;
        }

    }

    let mut retained_ids = std::collections::HashSet::new();
    if let Some(schema) = latest_schema.as_ref() {
        retained_ids.insert(schema.id.0);
    }

    if let Some(metadata) = latest_metadata.as_ref() {
        retained_ids.insert(metadata.id.0);
    }

    for record in entries.iter_mut() {
        if !retained_ids.contains(&record.id.0) {
            record.kind = TransactionKind::Ignore;
            record.refid = None;
            record.clear_payload();
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

    retained.push(TransactionRecord::without_payload(
        TransactionId(last_id.0 + 1),
        None,
        truncate_refid,
        timestamp_epoch_ms,
        actor,
        TransactionKind::Truncate,
    ));

    *entries = retained;

}

fn exact_record_duplicate_at_or_after(
    entries: &[TransactionRecord],
    record: &TransactionRecord,
    start_idx: usize,
) -> bool {

    let mut idx = start_idx;

    while idx < entries.len() && entries[idx].id.0 == record.id.0 {
        if &entries[idx] == record {
            return true;
        }
        idx += 1;
    }

    false

}

fn spawn_worker(
    stream_key: String,
    storage: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<TransactionRecord>>>>>>,
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

            let existing = load_records_from_path(path);
            let mut count = 0usize;

            let entries = if let Ok(mut state) = storage.lock() {
                get_or_insert_stream_entries_handle(&mut state, &stream_key)
            } else {
                Arc::new(Mutex::new(Vec::new()))
            };

            if let Ok(mut entries) = entries.lock() {
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

                WalCommand::Append { record, context, ack } => {

                    let entries = if let Ok(mut state) = storage.lock() {
                        get_or_insert_stream_entries_handle(&mut state, &stream_key)
                    } else {
                        log::error!(
                            "failed to acquire WAL storage lock for stream={}",
                            stream_key
                        );

                        let _ = ack.send(Err("failed to lock WAL storage"));

                        break;
                    };

                    if let Ok(mut entries) = entries.lock() {
                        let is_ordered = entries
                            .last()
                            .map(|last| record.id.0 > last.id.0)
                            .unwrap_or(true);

                        if is_ordered {

                            if let Some(ref path) = wal_path {

                                match frame_record_with_context(&record, &context) {

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

                            let base_pos = entries
                                .binary_search_by_key(&record.id.0, |existing| existing.id.0)
                                .unwrap_or_else(|idx| idx);

                            if exact_record_duplicate_at_or_after(entries.as_slice(), &record, base_pos) {
                                // Exact duplicate already present; treat as idempotent success.
                                let _ = ack.send(Ok(()));
                                continue;
                            }

                            // Insert older records into sorted position so affinity imports from
                            // peers with divergent local id ranges are still preserved.
                            let mut insert_pos = base_pos;

                            while insert_pos < entries.len()
                                && entries[insert_pos].id.0 <= record.id.0
                            {
                                insert_pos += 1;
                            }

                            if let Some(ref path) = wal_path {

                                let mut staged_entries = entries.clone();
                                staged_entries.insert(insert_pos, record);

                                if let Err(e) = rewrite_wal_file_with_context(path, &staged_entries, &context) {
                                    log::error!(
                                        "failed to rewrite WAL file for out-of-order insert stream={}: {}",
                                        stream_key,
                                        e
                                    );
                                    let _ = ack.send(Err(e));
                                    continue;
                                }

                                *entries = staged_entries;

                            } else {
                                entries.insert(insert_pos, record);
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

                WalCommand::AppendBatch { records, context, ack } => {

                    if records.is_empty() {
                        let _ = ack.send(Ok(()));
                        continue;
                    }

                    let entries = if let Ok(mut state) = storage.lock() {
                        get_or_insert_stream_entries_handle(&mut state, &stream_key)
                    } else {
                        log::error!(
                            "failed to acquire WAL storage lock for batch stream={}",
                            stream_key
                        );

                        let _ = ack.send(Err("failed to lock WAL storage"));

                        break;
                    };

                    if let Ok(mut entries) = entries.lock() {

                        let mut previous_id = entries.last().map(|last| last.id.0);

                        let ordered = records
                            .iter()
                            .all(|record| {
                                let is_after = previous_id
                                    .map(|last_id| record.id.0 > last_id)
                                    .unwrap_or(true);

                                if is_after {
                                    previous_id = Some(record.id.0);
                                }

                                is_after
                            });

                        if ordered {

                            if let Some(ref path) = wal_path {
                                
                                let mut frames = Vec::new();
                                let mut frame_error: Option<&'static str> = None;

                                for record in &records {
                                    match frame_record_with_context(record, &context) {
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
                        let mut merged_out_of_order = false;
                        let mut staged_dirty = false;
                        let reserve_hint = records.len().saturating_add(records.len() / 2);

                        if wal_path.is_none() {
                            entries.reserve(reserve_hint);
                        }

                        let mut staged_entries = wal_path
                            .as_ref()
                            .map(|_| {
                                let mut staged = entries.clone();
                                staged.reserve(reserve_hint);
                                staged
                            });

                        let working_entries: &mut Vec<TransactionRecord> = if let Some(staged) = staged_entries.as_mut() {
                            staged
                        } else {
                            &mut entries
                        };

                        for record in records {

                            let is_ordered = working_entries
                                .last()
                                .map(|last| record.id.0 > last.id.0)
                                .unwrap_or(true);

                            if is_ordered {

                                working_entries.push(record);
                                staged_dirty = true;
                                continue;
                                
                            }

                            let base_pos = working_entries
                                .binary_search_by_key(&record.id.0, |existing| existing.id.0)
                                .unwrap_or_else(|idx| idx);

                            if exact_record_duplicate_at_or_after(working_entries, &record, base_pos) {
                                continue;
                            }

                            let mut insert_pos = base_pos;

                            while insert_pos < working_entries.len()
                                && working_entries[insert_pos].id.0 <= record.id.0 {
                                insert_pos += 1;
                            }

                            working_entries.insert(insert_pos, record);
                            merged_out_of_order = true;
                            staged_dirty = true;

                        }

                        if batch_error.is_none()
                            && let Some(ref path) = wal_path
                            && staged_dirty
                            && let Some(staged) = staged_entries.as_ref()
                            && let Err(e) = rewrite_wal_file_with_context(path, staged, &context) {
                                log::error!(
                                    "failed to rewrite WAL file for out-of-order batch merge stream={}: {}",
                                    stream_key,
                                    e
                                );
                                batch_error = Some(e);
                            }

                        if batch_error.is_none()
                            && staged_dirty
                            && let Some(staged) = staged_entries.take() {
                                *entries = staged;
                            }

                        if batch_error.is_none()
                            && merged_out_of_order
                            && let Some(ref path) = wal_path {
                                append_file = open_wal_append_file(path).ok();
                            }

                        if let Some(err) = batch_error {
                            let _ = ack.send(Err(err));
                            continue;
                        }

                        if merged_out_of_order {
                            log::warn!(
                                "out-of-order transaction batch accepted and merged for stream={}",
                                stream_key
                            );
                        } else {
                            log::debug!(
                                "out-of-order transaction batch accepted without merge for stream={}",
                                stream_key
                            );
                        }

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

                    let entries = if let Ok(mut state) = storage.lock() {
                        get_or_insert_stream_entries_handle(&mut state, &stream_key)
                    } else {
                        log::error!(
                            "failed to acquire WAL storage lock during compact for stream={}",
                            stream_key
                        );
                        let _ = ack.send(Err("failed to lock WAL storage"));
                        break;
                    };

                    if let Ok(mut entries) = entries.lock() {

                        compact_entries_to_latest_schema_and_metadata(
                            &mut entries,
                            actor,
                            timestamp_epoch_ms,
                        );

                        if let Some(ref path) = wal_path
                            && let Err(e) = rewrite_wal_file(path, entries.as_slice()) {
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
