use crate::engine::database::{DatabaseError, DatabaseResult, ObjectStatus};
use crate::engine::table_schema::{FieldDef, TableSchema};

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IndexId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseTable {
    pub table_id: String,
    pub status: ObjectStatus,
    pub schema_revision: u64,
    pub schema: TableSchema,
    pub indexes: HashMap<String, DatabaseIndex>,
}

impl DatabaseTable {
    pub fn new(table_id: String, schema: TableSchema, indexes: HashMap<String, DatabaseIndex>) -> Self {
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
        indexes: HashMap<String, DatabaseIndex>,
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseIndex {
    pub index_id: IndexId,
    pub table_id: String,
    pub field_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseRelationship {
    pub left_table_id: String,
    pub right_table_id: String,
    pub relation_name: String,
}

/// A named, stored SQL query.  Views are never writable; their schema is
/// derived once at definition time and stored so `SHOW COLUMNS` and schema
/// inspection work identically to tables without re-executing the query.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseView {
    pub view_id: String,
    /// The SQL SELECT expression that defines this view.
    pub sql: String,
    /// Column schema derived at `CREATE VIEW` time from the referenced tables.
    pub schema: TableSchema,
}

impl DatabaseIndex {

    pub fn from_table_field(table_id: &str, field: &FieldDef) -> Self {
        let table_id = common::normalize_identifier!(table_id);
        let field_name = common::normalize_identifier!(&field.field_name);
        let index_id = IndexId(format!("{}:{}", table_id, field_name));

        Self {
            index_id,
            table_id,
            field_name,
        }
    }

}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::engine::table_schema::FieldType;

    #[test]
    fn index_id_is_normalized_from_table_and_field() {
        let field = FieldDef {
            seqno: 1,
            field_name: "UserId".to_string(),
            field_type: FieldType::UInt(64),
            nullable: false,
            indexed: true,
            default_value: None,
        };

        let index = DatabaseIndex::from_table_field("UserAccounts", &field);

        assert_eq!(index.table_id, "useraccounts");
        assert_eq!(index.field_name, "userid");
        assert_eq!(index.index_id.0, "useraccounts:userid");
    }

}