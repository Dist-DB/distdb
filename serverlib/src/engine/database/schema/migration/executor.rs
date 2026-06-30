use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use crate::engine::database::catalog::DatabaseCatalog;
use crate::engine::database::core::{DatabaseError, DatabaseResult};
use crate::engine::database::transaction::{TransactionKind, TransactionRecord};
use super::conversion::apply_schema_rules_to_payload;

use super::io::{
    frame_records_as_wal_file_with_context, load_records_from_path_with_context,
    map_io_error_to_catalog_error, payload_context_for_table, stream_key_for_table,
};

use super::types::{SchemaMigrationExecutor, SchemaMigrationProgress, SchemaMutationRuleSet};

#[derive(Debug, Clone, Copy)]
pub struct NoopSchemaMigrationExecutor;

impl SchemaMigrationExecutor for NoopSchemaMigrationExecutor {

    fn rewrite_rows(
        &self,
        _catalog: &DatabaseCatalog,
        _table_id: &str,
    ) -> DatabaseResult<SchemaMigrationProgress> {

        Ok(SchemaMigrationProgress {
            rows_rewritten: 0,
            rows_total: Some(0),
            resume_token: None,
        })

    }

    fn rebuild_indexes(&self, _catalog: &DatabaseCatalog, _table_id: &str) -> DatabaseResult<()> {
        Ok(())
    }

    fn flush_temp_image(&self, _catalog: &DatabaseCatalog, _table_id: &str) -> DatabaseResult<()> {
        Ok(())
    }

    fn cutover(&self, _catalog: &DatabaseCatalog, _table_id: &str) -> DatabaseResult<()> {
        Ok(())
    }

}

#[derive(Debug, Clone)]
struct StagedMigrationImage {
    stream_key: String,
    final_path: PathBuf,
    temp_path: PathBuf,
    backup_path: PathBuf,
    records: Vec<TransactionRecord>,
}

#[derive(Debug)]
pub struct DiskToMemorySchemaMigrationExecutor {
    data_dir: PathBuf,
    staged: Mutex<HashMap<String, StagedMigrationImage>>,
    rules: Mutex<HashMap<String, SchemaMutationRuleSet>>,
}

impl DiskToMemorySchemaMigrationExecutor {

    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            staged: Mutex::new(HashMap::new()),
            rules: Mutex::new(HashMap::new()),
        }
    }

    pub fn set_rules_for_table(
        &self,
        table_id: &str,
        rules: SchemaMutationRuleSet,
    ) -> DatabaseResult<()> {

        let mut all_rules = self.rules.lock().map_err(|_| DatabaseError::CatalogWrite)?;
        all_rules.insert(common::normalize_identifier!(table_id), rules);
        
        Ok(())

    }

}

impl SchemaMigrationExecutor for DiskToMemorySchemaMigrationExecutor {

    fn rewrite_rows(
        &self,
        _catalog: &DatabaseCatalog,
        table_id: &str,
    ) -> DatabaseResult<SchemaMigrationProgress> {

        let stream_key = stream_key_for_table(table_id)?;
        let final_path = self.data_dir.join(common::helpers::format::FileKind::Data.file_name(&stream_key));
        
        let temp_path = self
            .data_dir
            .join(format!("{}.migrate.tmp", common::helpers::format::FileKind::Data.file_name(&stream_key)));
        
        let backup_path = self
            .data_dir
            .join(format!("{}.migrate.bak", common::helpers::format::FileKind::Data.file_name(&stream_key)));

        let payload_context = payload_context_for_table(_catalog, table_id)?;
        let source_records = load_records_from_path_with_context(&final_path, &payload_context)?;
        let source_total = source_records.len() as u64;

        let rules = self
            .rules
            .lock()
            .map_err(|_| DatabaseError::CatalogWrite)?
            .get(&common::normalize_identifier!(table_id))
            .cloned();

        let schema = _catalog
            .table_schema(table_id)
            .ok_or(DatabaseError::TableNotFound)?;

        let mut rewritten = Vec::new();

        for mut record in source_records
            .into_iter()
            .filter(|record| record.kind != TransactionKind::Delete)
        {
            if matches!(record.kind, TransactionKind::Insert | TransactionKind::Update)
                && let Some(ref rule_set) = rules {
                    let payload = record
                        .payload_logical()
                        .ok_or(DatabaseError::CatalogWrite)?;
                    record.set_payload(Some(apply_schema_rules_to_payload(payload, rule_set, schema)?));
                }
            rewritten.push(record);
        }

        let rewritten_count = rewritten.len() as u64;
        let resume_token = rewritten.last().map(|record| format!("txid:{}", record.id.0));

        let mut staged = self
            .staged
            .lock()
            .map_err(|_| DatabaseError::CatalogWrite)?;

        staged.insert(
            common::normalize_identifier!(table_id),
            StagedMigrationImage {
                stream_key,
                final_path,
                temp_path,
                backup_path,
                records: rewritten,
            },
        );

        Ok(SchemaMigrationProgress {
            rows_rewritten: rewritten_count,
            rows_total: Some(source_total),
            resume_token,
        })

    }

    fn rebuild_indexes(&self, _catalog: &DatabaseCatalog, table_id: &str) -> DatabaseResult<()> {

        let staged = self
            .staged
            .lock()
            .map_err(|_| DatabaseError::CatalogWrite)?;

        let key = common::normalize_identifier!(table_id);
        
        if staged.contains_key(&key) {
            Ok(())
        } else {
            Err(DatabaseError::TableNotLocked)
        }

    }

    fn flush_temp_image(&self, _catalog: &DatabaseCatalog, table_id: &str) -> DatabaseResult<()> {

        let key = common::normalize_identifier!(table_id);
        
        let staged = self
            .staged
            .lock()
            .map_err(|_| DatabaseError::CatalogWrite)?;

        let image = staged.get(&key).ok_or(DatabaseError::TableNotLocked)?;

        let payload_context = payload_context_for_table(_catalog, table_id)?;
        let file_bytes = frame_records_as_wal_file_with_context(&image.records, &payload_context)
            .map_err(|_| DatabaseError::CatalogWrite)?;
        
        common::helpers::write_bytes(&image.temp_path, &file_bytes).map_err(|_| DatabaseError::CatalogWrite)

    }

    fn cutover(&self, _catalog: &DatabaseCatalog, table_id: &str) -> DatabaseResult<()> {

        let key = common::normalize_identifier!(table_id);
        
        let mut staged = self
            .staged
            .lock()
            .map_err(|_| DatabaseError::CatalogWrite)?;

        let image = staged.remove(&key).ok_or(DatabaseError::TableNotLocked)?;

        let _ = fs::remove_file(&image.backup_path);

        if image.final_path.exists() {
            fs::rename(&image.final_path, &image.backup_path)
                .map_err(|_| DatabaseError::CatalogWrite)?;
        }

        if let Err(err) = fs::rename(&image.temp_path, &image.final_path) {
            if image.backup_path.exists() {
                let _ = fs::rename(&image.backup_path, &image.final_path);
            }
            return Err(map_io_error_to_catalog_error(err));
        }

        if image.backup_path.exists() {
            fs::remove_file(&image.backup_path).map_err(map_io_error_to_catalog_error)?;
        }

        Ok(())

    }

}


#[cfg(test)]
#[path = "executor_test.rs"]
mod tests;
