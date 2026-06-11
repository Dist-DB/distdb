
use super::core::ObjectStatus;
use super::entity_aspect::DatabaseEntityAspect;
use super::entity_kind::DatabaseEntityKind;
use super::entity_metadata::EntityMetadata;
use super::table_schema::TableSchema;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseStoredProcedure {
    pub procedure_id: String,
    pub sql: String,
    pub dependencies: Vec<String>,
    pub metadata: EntityMetadata,
}

impl DatabaseStoredProcedure {

    pub fn new(procedure_id: String, sql: String, dependencies: Vec<String>) -> Self {
        Self {
            procedure_id,
            sql,
            dependencies,
            metadata: EntityMetadata::default(),
        }
    }
    
}

impl DatabaseEntityAspect for DatabaseStoredProcedure {

    fn kind(&self) -> DatabaseEntityKind {
        DatabaseEntityKind::StoredProcedure
    }

    fn storage_key(&self) -> String {
        common::normalize_identifier!(&self.procedure_id)
    }

    fn status(&self) -> ObjectStatus {
        ObjectStatus::Ready
    }

    fn metadata(&self) -> &EntityMetadata {
        &self.metadata
    }

    fn wal_stream_id(&self, _database_wal_id: &str) -> String {
        self.storage_key()
    }

    fn schema_revision(&self) -> Option<u64> {
        None
    }

    fn schema(&self) -> Option<&TableSchema> {
        None
    }

    fn normalize_in_place(&mut self) {
        self.procedure_id = common::normalize_identifier!(&self.procedure_id);
        self.dependencies = self
            .dependencies
            .iter()
            .map(|dep| common::normalize_identifier!(dep))
            .collect();
    }
    
}
