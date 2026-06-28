pub mod conversion;
pub mod executor;
pub mod io;
pub mod types;

pub use conversion::{
    apply_schema_rules_to_payload, compare_stored_field_values,
    convert_value_to_field_type, display_stored_field_value, render_stored_field_value,
};
pub use executor::{DiskToMemorySchemaMigrationExecutor, NoopSchemaMigrationExecutor};
pub use io::{frame_records_as_wal_file, load_records_from_path, stream_key_for_table};
pub use types::{FieldTypeChangeRule, SchemaMigrationExecutor, SchemaMigrationProgress, SchemaMutationRuleSet, TypeConversionPolicy};

use super::catalog::DatabaseCatalog;
use super::core::{DatabaseError, DatabaseResult};
use super::schema_change_state::SchemaChangePhase;

pub fn run_schema_migration<E: SchemaMigrationExecutor>(
    catalog: &mut DatabaseCatalog,
    table_id: &str,
    executor: &E,
) -> DatabaseResult<()> {
    let table_id = common::normalize_identifier!(table_id);

    catalog.transition_schema_change_phase(&table_id, SchemaChangePhase::Rewriting)?;
    let progress = executor.rewrite_rows(catalog, &table_id)?;
    catalog.checkpoint_schema_change_progress(
        &table_id,
        progress.rows_rewritten,
        progress.rows_total,
        progress.resume_token,
    )?;

    catalog.transition_schema_change_phase(&table_id, SchemaChangePhase::Reindexing)?;
    executor.rebuild_indexes(catalog, &table_id)?;

    catalog.transition_schema_change_phase(&table_id, SchemaChangePhase::Syncing)?;
    executor.flush_temp_image(catalog, &table_id)?;

    catalog.transition_schema_change_phase(&table_id, SchemaChangePhase::Cutover)?;
    executor.cutover(catalog, &table_id)
}


#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;
