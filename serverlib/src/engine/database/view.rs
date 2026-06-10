
use super::core::ObjectStatus;
use super::entity::{DatabaseEntityAspect, DatabaseEntityKind};
use super::table_schema::TableSchema;
use super::entity_metadata::EntityMetadata;

/// A named, stored SQL query. Views are never writable; their schema is
/// derived once at definition time and stored so schema inspection does not
/// need to re-execute the view SQL.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseView {
    pub view_id: String,
    pub sql: String,
    pub schema: TableSchema,
    pub metadata: EntityMetadata,
}

impl DatabaseView {
    pub fn new(view_id: String, sql: String, schema: TableSchema) -> Self {
        Self {
            view_id,
            sql,
            schema,
            metadata: EntityMetadata::default(),
        }
    }
}

impl DatabaseEntityAspect for DatabaseView {

    fn kind(&self) -> DatabaseEntityKind {
        DatabaseEntityKind::View
    }

    fn storage_key(&self) -> String {
        common::normalize_identifier!(&self.view_id)
    }

    fn status(&self) -> ObjectStatus {
        ObjectStatus::Ready
    }

    fn metadata(&self) -> &EntityMetadata {
        &self.metadata
    }

    fn wal_stream_id(&self, database_wal_id: &str) -> String {
        database_wal_id.to_string()
    }

    fn schema_revision(&self) -> Option<u64> {
        None
    }

    fn schema(&self) -> Option<&TableSchema> {
        Some(&self.schema)
    }

    fn normalize_in_place(&mut self) {
        self.view_id = common::normalize_identifier!(&self.view_id);
    }

}
