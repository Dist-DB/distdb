
use super::core::ObjectStatus;
use super::entity_aspect::DatabaseEntityAspect;
use super::entity_kind::DatabaseEntityKind;
use super::entity_metadata::EntityMetadata;
use super::table_schema::TableSchema;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseRelationship {
    #[serde(default)]
    pub entity_id: String,
    pub left_table_id: String,
    pub right_table_id: String,
    pub relation_name: String,
    pub metadata: EntityMetadata,
}

impl DatabaseRelationship {
    
    pub fn new(left_table_id: String, right_table_id: String, relation_name: String) -> Self {
        Self {
            entity_id: common::helpers::utils::unique_id(),
            left_table_id,
            right_table_id,
            relation_name,
            metadata: EntityMetadata::default(),
        }
    }

}

impl DatabaseEntityAspect for DatabaseRelationship {

    fn name(&self) -> &str {
        &self.relation_name
    }

    fn kind(&self) -> DatabaseEntityKind {
        DatabaseEntityKind::Relationship
    }

    fn storage_key(&self) -> String {
        self.entity_id.clone()
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
        self.left_table_id = common::normalize_identifier!(&self.left_table_id);
        self.right_table_id = common::normalize_identifier!(&self.right_table_id);
        self.relation_name = common::normalize_identifier!(&self.relation_name);
    }

}
