
use crate::engine::database::core::DatabaseResult;
use crate::engine::database::catalog::DatabaseCatalog;
use crate::engine::database::schema::change_state::SchemaChangePhase;
use crate::engine::database::table::schema::{FieldDef, TableSchema};
use crate::engine::database::transaction::SchemaChangePayload;

/// An in-progress schema change transaction. Obtained from
/// [`DatabaseCatalog::begin_schema_change`]; the table is held in `Lock` state
/// until this value is either committed or aborted.
///
/// Typical usage:
/// ```ignore
/// let mut tx = catalog.begin_schema_change("users")?;
/// tx.add_field(field)?;
/// tx.commit(&mut catalog, |payload| wal.append(wal_id, make_record(payload)))?;
/// ```


#[derive(Debug, Clone)]
pub struct SchemaChangeTx {
    table_id: String,
    next_revision: u64,
    pending_schema: TableSchema,
}

impl SchemaChangeTx {

    pub(crate) fn new(table_id: String, next_revision: u64, pending_schema: TableSchema) -> Self {
        Self {
            table_id,
            next_revision,
            pending_schema,
        }
    }

    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    pub fn next_revision(&self) -> u64 {
        self.next_revision
    }

    /// Inspect the pending (not yet committed) schema.
    pub fn pending_schema(&self) -> &TableSchema {
        &self.pending_schema
    }

    pub fn add_field(&mut self, field: FieldDef) -> DatabaseResult<()> {
        
        self.pending_schema
            .add_field(field)
            .map_err(crate::engine::database::core::DatabaseError::SchemaChange)

    }

    pub fn remove_field(&mut self, name: &str) -> DatabaseResult<()> {

        self.pending_schema
            .remove_field(name)
            .map_err(crate::engine::database::core::DatabaseError::SchemaChange)

    }

    pub fn update_field(&mut self, field: FieldDef) -> DatabaseResult<()> {

        self.pending_schema
            .update_field(field)
            .map_err(crate::engine::database::core::DatabaseError::SchemaChange)

    }

    /// Persist the change via `persist`, then if successful apply the schema
    /// and drive the table `Lock -> Sync -> Ready`.
    ///
    /// If `persist` returns an error the lock is released (`Lock -> Ready`)
    /// and the schema is left unchanged. The persist error is returned.
    pub fn commit<E, F>(self, catalog: &mut DatabaseCatalog, persist: F) -> Result<(), E>
    where
        F: FnOnce(&SchemaChangePayload) -> Result<(), E>,
        E: From<crate::engine::database::core::DatabaseError>,
    {

        catalog.transition_schema_change_phase(&self.table_id, SchemaChangePhase::Rewriting)?;
        catalog.checkpoint_schema_change_progress(&self.table_id, 0, None, None)?;

        let payload = SchemaChangePayload {
            table_id: self.table_id.clone(),
            schema_revision: self.next_revision,
            schema_epoch: catalog.schema_epoch().saturating_add(1),
            entity_id: None,
            schema: self.pending_schema,
        };

        if let Err(e) = persist(&payload) {
            // Best-effort abort: release the lock even if abort itself fails.
            let _ = catalog.release_schema_lock(&self.table_id);
            return Err(e);
        }

        catalog.transition_schema_change_phase(&self.table_id, SchemaChangePhase::Reindexing)?;

        catalog.finalize_schema_change(payload).map_err(E::from)

    }

    /// Release the lock without altering the schema.
    pub fn abort(self, catalog: &mut DatabaseCatalog) -> DatabaseResult<()> {
        catalog.release_schema_lock(&self.table_id)
    }
    
}
