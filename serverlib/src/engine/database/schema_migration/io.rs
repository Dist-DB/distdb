use std::io::ErrorKind;
use std::path::Path;

use common::helpers::format::{make_header, verify_header, FileKind, HEADER_SIZE};
use common::helpers::{read_bytes, stable_id};

use super::super::core::{DatabaseError, DatabaseResult};
use super::super::transaction::TransactionRecord;

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

        let record = bincode::deserialize::<TransactionRecord>(&bytes[pos..pos + len])
            .map_err(|_| DatabaseError::CatalogDeserialize)?;
        records.push(record);
        pos += len;
    }

    Ok(records)

}

pub fn frame_records_as_wal_file(records: &[TransactionRecord]) -> Result<Vec<u8>, &'static str> {

    let mut file = Vec::new();
    file.extend_from_slice(&make_header(FileKind::Data));

    for record in records {
        let encoded = bincode::serialize(record).map_err(|_| "serialize record")?;
        file.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
        file.extend_from_slice(&encoded);
    }

    Ok(file)
    
}


#[cfg(test)]
#[path = "io_test.rs"]
mod tests;
