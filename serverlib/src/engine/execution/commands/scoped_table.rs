use crate::{
    ConcurrentWalManager, DatabaseCatalog, TableSchema, WalStreamMode,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedEphemeralTableHandle {
    table_id: String,
    stream_id: String,
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

#[derive(Debug, Clone)]
pub struct ScopedEphemeralTableScope {
    scope_id: String,
    handles: Vec<ScopedEphemeralTableHandle>,
}

impl ScopedEphemeralTableScope {

    pub fn new(scope_id: impl Into<String>) -> Self {

        Self {
            scope_id: common::normalize_identifier!(scope_id.into()),
            handles: Vec::new(),
        }
        
    }

    pub fn scope_id(&self) -> &str {
        &self.scope_id
    }

    pub fn create_table(
        &mut self,
        catalog: &mut DatabaseCatalog,
        wal: &ConcurrentWalManager,
        logical_table_id: impl Into<String>,
        schema: TableSchema,
    ) -> Result<String, String> {

        let physical_table_id = make_scoped_ephemeral_table_id(
            &self.scope_id,
            logical_table_id,
        );

        let handle = create_scoped_ephemeral_table(
            catalog,
            wal,
            physical_table_id.clone(),
            schema,
        )?;

        self.handles.push(handle);

        Ok(physical_table_id)

    }

    pub fn cleanup(
        &mut self,
        catalog: &mut DatabaseCatalog,
        wal: &ConcurrentWalManager,
    ) -> Result<(), String> {

        let mut first_error = None;

        for handle in self.handles.iter_mut().rev() {
            if let Err(err) = release_scoped_ephemeral_table(catalog, wal, handle)
                && first_error.is_none()
            {
                first_error = Some(err);
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }

        self.handles.clear();
        Ok(())

    }

    pub fn mark_table_released(&mut self, table_id: &str) -> bool {

        let normalized = common::normalize_identifier!(table_id);

        for handle in self.handles.iter_mut().rev() {
            if handle.table_id == normalized {
                handle.released = true;
                return true;
            }
        }

        false

    }

}

pub fn make_scoped_ephemeral_table_id(
    scope_id: &str,
    logical_table_id: impl Into<String>,
) -> String {

    let normalized_scope_id = common::normalize_identifier!(scope_id);
    let normalized_logical_table_id = common::normalize_identifier!(logical_table_id.into());

    format!(
        "__scope_{}_{}",
        normalized_scope_id,
        normalized_logical_table_id,
    )

}

pub fn create_scoped_ephemeral_table(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    table_id: impl Into<String>,
    schema: TableSchema,
) -> Result<ScopedEphemeralTableHandle, String> {

    let normalized_table_id = common::normalize_identifier!(table_id.into());

    if let Some(existing_table) = catalog.table(&normalized_table_id) {
        if !existing_table.is_temporary() {
            return Err(format!(
                "scoped table create failed: table '{}' already exists and is not temporary",
                normalized_table_id,
            ));
        }

        if existing_table.schema != schema {
            return Err(format!(
                "scoped table create failed: temporary table '{}' schema mismatch",
                normalized_table_id,
            ));
        }

        let stream_id = catalog
            .entity_wal_stream_id(&normalized_table_id)
            .unwrap_or_else(|| normalized_table_id.clone());

        if let Err(err) = wal.set_stream_mode(&stream_id, WalStreamMode::Ephemeral) {
            return Err(format!("scoped table create failed: {err}"));
        }

        if stream_id != normalized_table_id {
            let _ = wal.delete_stream(&normalized_table_id);
        }

        wal.clear_stream_records(&stream_id)
            .map_err(|err| format!("scoped table create failed: {err}"))?;

        return Ok(ScopedEphemeralTableHandle {
            table_id: normalized_table_id,
            stream_id,
            released: false,
        });
    }

    catalog
        .create_temporary_table(normalized_table_id.clone(), schema)
        .map_err(|err| format!("scoped table create failed: {err}"))?;

    let stream_id = catalog
        .entity_wal_stream_id(&normalized_table_id)
        .unwrap_or_else(|| normalized_table_id.clone());

    if let Err(err) = wal.set_stream_mode(&stream_id, WalStreamMode::Ephemeral) {
        let _ = catalog.drop_table(&normalized_table_id);
        return Err(format!("scoped table create failed: {err}"));
    }

    if stream_id != normalized_table_id {
        let _ = wal.delete_stream(&normalized_table_id);
    }

    Ok(ScopedEphemeralTableHandle {
        table_id: normalized_table_id,
        stream_id,
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

    if wal.stream_mode(&handle.stream_id) == WalStreamMode::Ephemeral {
        wal.clear_stream_records(&handle.stream_id)
            .map_err(|err| format!("scoped table release failed: {err}"))?;
    }

    match catalog.drop_table(&handle.table_id) {
        Ok(()) | Err(crate::DatabaseError::TableNotFound) => {}
        Err(err) => return Err(format!("scoped table release failed: {err}")),
    }

    wal.delete_stream(&handle.stream_id)
        .map_err(|err| format!("scoped table release failed: {err}"))?;

    if handle.stream_id != handle.table_id {
        let _ = wal.delete_stream(&handle.table_id);
    }

    handle.released = true;

    Ok(())
    
}

#[cfg(test)]
#[path = "scoped_table_test.rs"]
mod tests;
