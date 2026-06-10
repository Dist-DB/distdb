
use super::table_schema::TableSchema;

/// A named, stored SQL query. Views are never writable; their schema is
/// derived once at definition time and stored so schema inspection does not
/// need to re-execute the view SQL.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseView {
    pub view_id: String,
    pub sql: String,
    pub schema: TableSchema,
}
