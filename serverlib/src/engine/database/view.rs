
use crate::engine::database::core::ObjectStatus;
use crate::engine::database::entity::aspect::DatabaseEntityAspect;
use crate::engine::database::entity::kind::DatabaseEntityKind;
use crate::engine::database::table::schema::TableSchema;
use crate::engine::database::entity::metadata::EntityMetadata;

/// A named, stored SQL query. Views are never writable; their schema is
/// derived once at definition time and stored so schema inspection does not
/// need to re-execute the view SQL.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseView {
    #[serde(default)]
    pub entity_id: String,
    pub view_id: String,
    pub sql: String,
    pub schema: TableSchema,
    pub dependencies: Vec<String>,
    pub metadata: EntityMetadata,
}

impl DatabaseView {
    
    pub fn new(view_id: String, sql: String, schema: TableSchema) -> Self {
        Self {
            entity_id: common::helpers::utils::unique_id(),
            view_id,
            sql,
            schema,
            dependencies: Vec::new(),
            metadata: EntityMetadata::default(),
        }
    }

}

impl DatabaseEntityAspect for DatabaseView {

    fn name(&self) -> &str {
        &self.view_id
    }

    fn kind(&self) -> DatabaseEntityKind {
        DatabaseEntityKind::View
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
        Some(&self.schema)
    }

    fn normalize_in_place(&mut self) {
        self.view_id = common::normalize_identifier!(&self.view_id);
        self.dependencies = self
            .dependencies
            .iter()
            .map(|dep| common::normalize_identifier!(dep))
            .collect();
    }

}
