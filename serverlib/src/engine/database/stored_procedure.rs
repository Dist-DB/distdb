
use crate::engine::database::core::ObjectStatus;
use crate::engine::database::entity::aspect::DatabaseEntityAspect;
use crate::engine::database::entity::kind::DatabaseEntityKind;
use crate::engine::database::entity::metadata::EntityMetadata;
use crate::engine::database::table::schema::TableSchema;
use crate::engine::ir_compiler::{
    compile_sql_programatic_sql_with_services, DefaultSQLProgramaticCompilerServices,
    SQLProgramaticIr,
};
use crate::engine::sql::IfElseEndPlan;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseStoredProcedure {
    #[serde(default)]
    pub entity_id: String,
    pub procedure_id: String,
    pub sql: String,
    pub dependencies: Vec<String>,
    pub metadata: EntityMetadata,
    #[serde(skip)]
    pub compiled_ir: Option<SQLProgramaticIr>,
}

impl DatabaseStoredProcedure {

    pub fn new(procedure_id: String, sql: String, dependencies: Vec<String>) -> Self {

        let mut procedure = Self {
            entity_id: common::helpers::utils::unique_id(),
            procedure_id,
            sql,
            dependencies,
            metadata: EntityMetadata::default(),
            compiled_ir: None,
        };

        procedure.refresh_compiled_ir_cache();
        procedure

    }

    pub fn set_sql(&mut self, sql: String) {

        self.sql = sql;
        self.refresh_compiled_ir_cache();
        
    }

    pub fn refresh_compiled_ir_cache(&mut self) {

        self.compiled_ir = Some(compile_sql_programatic_sql_with_services(
            &self.sql,
            &DefaultSQLProgramaticCompilerServices,
        ));

    }

    pub fn if_else_end_plan(&self) -> Option<&IfElseEndPlan> {
        self.compiled_ir.as_ref().and_then(SQLProgramaticIr::if_else_end_plan)
    }

    pub fn compiled_ir(&self) -> Option<&SQLProgramaticIr> {
        self.compiled_ir.as_ref()
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

        self.refresh_compiled_ir_cache();

    }
    
}
