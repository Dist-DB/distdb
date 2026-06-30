
use crate::engine::database::core::ObjectStatus;
use crate::engine::database::entity::aspect::DatabaseEntityAspect;
use crate::engine::database::entity::kind::DatabaseEntityKind;
use crate::engine::database::entity::metadata::EntityMetadata;
use crate::engine::database::table::schema::TableSchema;

use crate::engine::sql::{
    parse_trigger_invocation_binding_from_create_trigger_statement,
    TriggerInvocationBinding,
};


#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseTrigger {
    #[serde(default)]
    pub entity_id: String,
    pub trigger_id: String,
    pub sql: String,
    pub dependencies: Vec<String>,
    pub metadata: EntityMetadata,
    #[serde(skip)]
    pub invocation_binding: Option<TriggerInvocationBinding>,
}

impl DatabaseTrigger {

    pub fn new(trigger_id: String, sql: String, dependencies: Vec<String>) -> Self {
        
        let mut trigger = Self {
            entity_id: common::helpers::utils::unique_id(),
            trigger_id,
            sql,
            dependencies,
            metadata: EntityMetadata::default(),
            invocation_binding: None,
        };

        trigger.refresh_invocation_binding_cache();
        trigger

    }

    pub fn set_sql(&mut self, sql: String) {
        self.sql = sql;
        self.refresh_invocation_binding_cache();
    }

    pub fn refresh_invocation_binding_cache(&mut self) {

        self.invocation_binding =
            parse_trigger_invocation_binding_from_create_trigger_statement(&self.sql)
                .ok()
                .flatten();

    }

    pub fn invocation_binding(&self) -> Option<&TriggerInvocationBinding> {
        self.invocation_binding.as_ref()
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
        
        self.refresh_invocation_binding_cache();

    }
    
}
