
use super::core::{DatabaseError, DatabaseResult, ObjectStatus};
use super::table_schema::TableSchema;

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseTable {
    pub table_id: String,
    pub status: ObjectStatus,
    pub schema_revision: u64,
    pub schema: TableSchema,
    pub indexes: HashMap<String, super::index::DatabaseIndex>,
}

impl DatabaseTable {
    
    pub fn new(
        table_id: String,
        schema: TableSchema,
        indexes: HashMap<String, super::index::DatabaseIndex>,
    ) -> Self {
        Self {
            table_id,
            status: ObjectStatus::Load,
            schema_revision: 0,
            schema,
            indexes,
        }
    }

    pub fn status(&self) -> ObjectStatus {
        self.status
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn schema_revision(&self) -> u64 {
        self.schema_revision
    }

    /// Acquire the transaction lock: `Ready → Lock`.
    /// Must be called before any write or schema-change transaction.
    pub fn lock(&mut self) -> DatabaseResult<()> {
        self.transition(ObjectStatus::Lock)
    }

    /// Release the lock without applying any change: `Lock → Ready`.
    /// Called when a transaction is aborted before or after WAL append fails.
    pub fn abort(&mut self) -> DatabaseResult<()> {
        self.transition(ObjectStatus::Ready)
    }

    /// Move into the sync-pending state: `Lock → Sync`.
    /// Called after the change has been durably written but before replication
    /// acknowledgement is confirmed.
    pub fn begin_sync(&mut self) -> DatabaseResult<()> {
        self.transition(ObjectStatus::Sync)
    }

    /// Complete sync and make the table writable again: `Sync → Ready`.
    /// Called once the required acknowledgements have been received.
    pub fn complete_sync(&mut self) -> DatabaseResult<()> {
        self.transition(ObjectStatus::Ready)
    }

    pub fn replace_schema(
        &mut self,
        revision: u64,
        schema: TableSchema,
        indexes: HashMap<String, super::index::DatabaseIndex>,
    ) {
        self.schema_revision = revision;
        self.schema = schema;
        self.indexes = indexes;
    }

    fn transition(&mut self, next: ObjectStatus) -> DatabaseResult<()> {
        if !self.status.can_transition_to(next) {
            return Err(DatabaseError::InvalidStatusTransition);
        }
        self.status = next;
        Ok(())
    }

}