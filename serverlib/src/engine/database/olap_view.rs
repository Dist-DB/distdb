
use crate::engine::database::core::ObjectStatus;
use crate::engine::database::entity::aspect::DatabaseEntityAspect;
use crate::engine::database::entity::kind::DatabaseEntityKind;
use crate::engine::database::entity::metadata::EntityMetadata;
use crate::engine::database::table::schema::TableSchema;

/// A named, persisted OLAP view definition.
///
/// Syntax:  `CREATE OLAPVIEW <name> USING <col1>, <col2>, ... AS <select_sql>`
///
/// The `z_dimension_columns` are pivot axes: columns whose distinct
/// values become coordinates in the hypercube.
/// - `z_dimension_columns[0]` is the primary pivot (z-axis in Gentia terms)
/// - `z_dimension_columns[1..n]` are secondary pivot axes for multi-dimensional analysis
///
/// The definition is catalog-persisted (WAL-backed) so it survives restarts.
/// The hypercube itself is memory-resident only and is rebuilt from committed
/// live rows at bootstrap or after invalidation — it is never stored on disk.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseOlapView {
    #[serde(default)]
    pub entity_id: String,
    /// The name of this OLAP view.
    pub view_id: String,
    /// The source SELECT SQL that defines the data projection.
    pub sql: String,
    /// The columns nominated as pivot axes (z-dimension).
    /// Index 0 is primary, 1+ are secondary dimensions for multi-dimensional slicing.
    pub z_dimension_columns: Vec<String>,
    /// Schema derived from the SELECT at definition time. Used for field
    /// validation and dimension/measure classification at cube build time.
    pub schema: TableSchema,
    /// Source table names this view depends on (normalized, lower-case).
    pub dependencies: Vec<String>,
    pub metadata: EntityMetadata,
}

impl DatabaseOlapView {

    pub fn new(
        view_id: String,
        sql: String,
        z_dimension_columns: Vec<String>,
        schema: TableSchema,
        dependencies: Vec<String>,
    ) -> Self {
        Self {
            entity_id: common::helpers::utils::unique_id(),
            view_id,
            sql,
            z_dimension_columns,
            schema,
            dependencies,
            metadata: EntityMetadata::default(),
        }
    }

}

impl DatabaseEntityAspect for DatabaseOlapView {

    fn name(&self) -> &str {
        &self.view_id
    }

    fn kind(&self) -> DatabaseEntityKind {
        DatabaseEntityKind::OlapView
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

        self.z_dimension_columns = self
            .z_dimension_columns
            .iter()
            .map(|col| col.trim().to_ascii_lowercase())
            .collect();

        self.dependencies = self
            .dependencies
            .iter()
            .map(|dep| common::normalize_identifier!(dep))
            .collect();

    }

}
