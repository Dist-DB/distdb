use crate::{
    ConcurrentWalManager, DatabaseCatalog, TableSchema, WalStreamMode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedEphemeralTableHandle {
    table_id: String,
    released: bool,
}

impl ScopedEphemeralTableHandle {
    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    pub fn released(&self) -> bool {
        self.released
    }
}

pub fn create_scoped_ephemeral_table(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    table_id: impl Into<String>,
    schema: TableSchema,
) -> Result<ScopedEphemeralTableHandle, String> {

    let normalized_table_id = common::normalize_identifier!(table_id.into());

    catalog
        .register_table(normalized_table_id.clone(), schema)
        .map_err(|err| format!("scoped table create failed: {err}"))?;

    if let Err(err) = wal.set_stream_mode(&normalized_table_id, WalStreamMode::Ephemeral) {
        let _ = catalog.drop_table(&normalized_table_id);
        return Err(format!("scoped table create failed: {err}"));
    }

    Ok(ScopedEphemeralTableHandle {
        table_id: normalized_table_id,
        released: false,
    })

}

pub fn release_scoped_ephemeral_table(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    handle: &mut ScopedEphemeralTableHandle,
) -> Result<(), String> {

    if handle.released {
        return Ok(());
    }

    match catalog.drop_table(&handle.table_id) {
        Ok(()) | Err(crate::DatabaseError::TableNotFound) => {}
        Err(err) => return Err(format!("scoped table release failed: {err}")),
    }

    wal.delete_stream(&handle.table_id)
        .map_err(|err| format!("scoped table release failed: {err}"))?;

    handle.released = true;

    Ok(())
    
}

#[cfg(test)]
#[path = "scoped_table_test.rs"]
mod tests;
