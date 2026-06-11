
use super::core::{DatabaseError, DatabaseResult, ObjectStatus};
use super::entity_aspect::DatabaseEntityAspect;
use super::entity_kind::DatabaseEntityKind;
use super::entity_metadata::EntityMetadata;
use super::index::IndexId;
use super::table_schema::TableSchema;

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseTable {
    pub table_id: String,
    pub status: ObjectStatus,
    pub schema_revision: u64,
    pub schema: TableSchema,
    pub indexes: HashMap<String, super::index::DatabaseIndex>,
    pub metadata: EntityMetadata,
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
            metadata: EntityMetadata::default(),
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

impl DatabaseEntityAspect for DatabaseTable {

    fn kind(&self) -> DatabaseEntityKind {
        DatabaseEntityKind::Table
    }

    fn storage_key(&self) -> String {
        common::normalize_identifier!(&self.table_id)
    }

    fn status(&self) -> ObjectStatus {
        self.status()
    }

    fn metadata(&self) -> &EntityMetadata {
        &self.metadata
    }

    fn wal_stream_id(&self, _database_wal_id: &str) -> String {
        self.storage_key()
    }

    fn schema_revision(&self) -> Option<u64> {
        Some(self.schema_revision())
    }

    fn schema(&self) -> Option<&TableSchema> {
        Some(self.schema())
    }

    fn normalize_in_place(&mut self) {

        let normalized_table_id = common::normalize_identifier!(&self.table_id);
        self.table_id = normalized_table_id.clone();

        let mut normalized_indexes = HashMap::with_capacity(self.indexes.len());
        
        for (_, mut index) in std::mem::take(&mut self.indexes) {
            index.table_id = normalized_table_id.clone();
            index.field_name = common::normalize_identifier!(&index.field_name);
            index.index_id = IndexId(format!("{}:{}", index.table_id, index.field_name));
            normalized_indexes.insert(index.index_id.0.clone(), index);
        }
        
        self.indexes = normalized_indexes;

    }

}