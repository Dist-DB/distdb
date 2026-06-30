use std::io::ErrorKind;
use std::path::Path;

use common::helpers::format::{make_header, verify_header, FileKind, HEADER_SIZE};
use common::helpers::{read_bytes, stable_id};

use crate::engine::wal::{
    decode_record_from_storage, decode_record_from_storage_with_context,
    encode_record_for_storage, encode_record_for_storage_with_context,
};

use crate::engine::database::core::{DatabaseError, DatabaseResult};
use crate::engine::database::catalog::DatabaseCatalog;
use crate::engine::database::transaction::TransactionRecord;
use crate::engine::database::transaction::TransactionPayloadContext;

pub fn stream_key_for_table(table_id: &str) -> DatabaseResult<String> {
    let normalized = common::normalize_identifier!(table_id);
    if normalized.is_empty() {
        return Err(DatabaseError::TableNotFound);
    }
    Ok(stable_id(&[&normalized]))
}

pub fn map_io_error_to_catalog_error(err: std::io::Error) -> DatabaseError {
    if err.kind() == ErrorKind::NotFound {
        DatabaseError::CatalogRead
    } else {
        DatabaseError::CatalogWrite
    }
}

pub fn load_records_from_path(path: &Path) -> DatabaseResult<Vec<TransactionRecord>> {
    let context = TransactionPayloadContext::default();
    load_records_from_path_with_context(path, &context)
}

pub fn payload_context_for_table(
    catalog: &DatabaseCatalog,
    table_id: &str,
) -> DatabaseResult<TransactionPayloadContext> {
    let normalized_table_id = common::normalize_identifier!(table_id);
    if normalized_table_id.is_empty() {
        return Err(DatabaseError::TableNotFound);
    }

    let stream_key = stream_key_for_table(&normalized_table_id)?;
    let mut context = TransactionPayloadContext::new()
        .with_database_id(catalog.database_id.0.clone())
        .with_table_id(normalized_table_id)
        .with_stream_id(stream_key);

    if let Some(key_ref) = catalog.at_rest_encryption_key_ref() {
        context = context.with_at_rest_encryption(
            key_ref.to_string(),
            catalog.at_rest_encryption_key_version(),
        );
    }

    Ok(context)
}

pub fn load_records_from_path_with_context(
    path: &Path,
    context: &TransactionPayloadContext,
) -> DatabaseResult<Vec<TransactionRecord>> {

    if !path.exists() {
        return Ok(Vec::new());
    }

    let bytes = read_bytes(path).map_err(|_| DatabaseError::CatalogRead)?;
    verify_header(FileKind::Data, &bytes).map_err(|_| DatabaseError::CatalogInvalidHeader)?;

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
            return Err(DatabaseError::CatalogDeserialize);
        }

        let record = decode_record_from_storage_with_context(&bytes[pos..pos + len], context)
            .map_err(|_| DatabaseError::CatalogDeserialize)?;
        records.push(record);
        pos += len;
    }

    Ok(records)

}

pub fn frame_records_as_wal_file(records: &[TransactionRecord]) -> Result<Vec<u8>, &'static str> {
    let context = TransactionPayloadContext::default();
    frame_records_as_wal_file_with_context(records, &context)
}

pub fn frame_records_as_wal_file_with_context(
    records: &[TransactionRecord],
    context: &TransactionPayloadContext,
) -> Result<Vec<u8>, &'static str> {

    let mut file = Vec::new();
    file.extend_from_slice(&make_header(FileKind::Data));

    for record in records {
        let encoded = encode_record_for_storage_with_context(record, context)
            .map_err(|_| "serialize record")?;
        file.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
        file.extend_from_slice(&encoded);
    }

    Ok(file)
    
}


#[cfg(test)]
#[path = "io_test.rs"]
mod tests;
