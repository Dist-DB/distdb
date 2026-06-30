use crate::engine::database::catalog::DatabaseCatalog;
use crate::engine::database::core::DatabaseResult;
use crate::engine::database::table::schema::FieldType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMigrationProgress {
    pub rows_rewritten: u64,
    pub rows_total: Option<u64>,
    pub resume_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SchemaMutationRuleSet {
    pub renames: Vec<(String, String)>,
    pub removals: Vec<String>,
    pub additions: Vec<(String, Vec<u8>)>,
    pub type_changes: Vec<FieldTypeChangeRule>,
    pub conversion_policy: TypeConversionPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldTypeChangeRule {
    pub field_name: String,
    pub target_type: FieldType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TypeConversionPolicy {
    #[default]
    Safe,
    Force,
}

pub trait SchemaMigrationExecutor {

    fn rewrite_rows(
        &self,
        catalog: &DatabaseCatalog,
        table_id: &str,
    ) -> DatabaseResult<SchemaMigrationProgress>;

    fn rebuild_indexes(&self, catalog: &DatabaseCatalog, table_id: &str) -> DatabaseResult<()>;

    fn flush_temp_image(&self, catalog: &DatabaseCatalog, table_id: &str) -> DatabaseResult<()>;

    fn cutover(&self, catalog: &DatabaseCatalog, table_id: &str) -> DatabaseResult<()>;
    
}
