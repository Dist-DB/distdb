
use super::core::ObjectStatus;
use super::entity_aspect::DatabaseEntityAspect;
use super::entity_kind::DatabaseEntityKind;
use super::entity_metadata::EntityMetadata;
use super::table_schema::TableSchema;
use crate::engine::sql::{
    parse_if_else_end_plan_from_create_procedure_statement, IfElseEndPlan,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseStoredProcedure {
    #[serde(default)]
    pub entity_id: String,
    pub procedure_id: String,
    pub sql: String,
    pub dependencies: Vec<String>,
    pub metadata: EntityMetadata,
    #[serde(skip)]
    pub if_else_end_plan: Option<IfElseEndPlan>,
}

impl DatabaseStoredProcedure {

    pub fn new(procedure_id: String, sql: String, dependencies: Vec<String>) -> Self {
        let mut procedure = Self {
            entity_id: common::helpers::utils::unique_id(),
            procedure_id,
            sql,
            dependencies,
            metadata: EntityMetadata::default(),
            if_else_end_plan: None,
        };

        procedure.refresh_control_flow_plan_cache();
        procedure
    }

    pub fn set_sql(&mut self, sql: String) {
        self.sql = sql;
        self.refresh_control_flow_plan_cache();
    }

    pub fn refresh_control_flow_plan_cache(&mut self) {
        self.if_else_end_plan =
            parse_if_else_end_plan_from_create_procedure_statement(&self.sql)
                .ok()
                .flatten();
    }

    pub fn if_else_end_plan(&self) -> Option<&IfElseEndPlan> {
        self.if_else_end_plan.as_ref()
    }
    
}

impl DatabaseEntityAspect for DatabaseStoredProcedure {

    fn name(&self) -> &str {
        &self.procedure_id
    }

    fn kind(&self) -> DatabaseEntityKind {
        DatabaseEntityKind::StoredProcedure
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
        self.procedure_id = common::normalize_identifier!(&self.procedure_id);
        self.dependencies = self
            .dependencies
            .iter()
            .map(|dep| common::normalize_identifier!(dep))
            .collect();
        self.refresh_control_flow_plan_cache();
    }
    
}
