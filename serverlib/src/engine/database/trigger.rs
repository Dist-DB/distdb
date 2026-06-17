
use super::core::ObjectStatus;
use super::entity_aspect::DatabaseEntityAspect;
use super::entity_kind::DatabaseEntityKind;
use super::entity_metadata::EntityMetadata;
use super::table_schema::TableSchema;


#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseTrigger {
    #[serde(default)]
    pub entity_id: String,
    pub trigger_id: String,
    pub sql: String,
    pub dependencies: Vec<String>,
    pub metadata: EntityMetadata,
}

impl DatabaseTrigger {

    pub fn new(trigger_id: String, sql: String, dependencies: Vec<String>) -> Self {
        Self {
            entity_id: common::helpers::utils::unique_id(),
            trigger_id,
            sql,
            dependencies,
            metadata: EntityMetadata::default(),
        }
    }

}

impl DatabaseEntityAspect for DatabaseTrigger {

    fn name(&self) -> &str {
        &self.trigger_id
    }

    fn kind(&self) -> DatabaseEntityKind {
        DatabaseEntityKind::Trigger
    }

    fn storage_key(&self) -> String {
        self.entity_id.clone()
    }

    fn set_entity_id(&mut self, entity_id: String) {
        self.entity_id = entity_id;
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
        self.trigger_id = common::normalize_identifier!(&self.trigger_id);
        self.dependencies = self
            .dependencies
            .iter()
            .map(|dep| common::normalize_identifier!(dep))
            .collect();
    }
    
}
