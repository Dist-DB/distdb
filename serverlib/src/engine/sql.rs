
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDirective {
    Create,
    Retrieve,
    Update,
    Delete,
    AlterSchema,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlRequest {
    pub database_id: String,
    pub sql: String,
    pub directive: SqlDirective,
}