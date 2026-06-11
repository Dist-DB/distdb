use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use common::helpers::format::{make_header, verify_header, FileKind, HEADER_SIZE};
use common::helpers::{read_bytes, stable_id, write_bytes};

use super::catalog::DatabaseCatalog;
use super::core::{DatabaseError, DatabaseResult};
use super::schema_change_state::SchemaChangePhase;
use super::transaction::{TransactionKind, TransactionRecord};

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

impl SchemaMigrationExecutor for DiskToMemorySchemaMigrationExecutor {
    fn rewrite_rows(
        &self,
        _catalog: &DatabaseCatalog,
        table_id: &str,
    ) -> DatabaseResult<SchemaMigrationProgress> {
        let stream_key = stream_key_for_table(table_id)?;
        let final_path = self.data_dir.join(FileKind::Data.file_name(&stream_key));
        let temp_path = self
            .data_dir
            .join(format!("{}.migrate.tmp", FileKind::Data.file_name(&stream_key)));
        let backup_path = self
            .data_dir
            .join(format!("{}.migrate.bak", FileKind::Data.file_name(&stream_key)));

        let source_records = load_records_from_path(&final_path)?;
        let source_total = source_records.len() as u64;

        let rules = self
            .rules
            .lock()
            .map_err(|_| DatabaseError::CatalogWrite)?
            .get(&common::normalize_identifier!(table_id))
            .cloned();

        // Purge deleted records during rewrite by removing explicit delete events
        // from the staged image before flush/cutover.
        let rewritten = source_records
            .into_iter()
            .filter(|record| record.kind != TransactionKind::Delete)
            .map(|mut record| {
                if matches!(record.kind, TransactionKind::Insert | TransactionKind::Update) {
                    if let Some(ref rule_set) = rules {
                        record.payload = apply_schema_rules_to_payload(&record.payload, rule_set);
                    }
                }
                record
            })
            .collect::<Vec<_>>();

        let rewritten_count = rewritten.len() as u64;
        let resume_token = rewritten
            .last()
            .map(|record| format!("txid:{}", record.id.0));

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

        let file_bytes = frame_records_as_wal_file(&image.records).map_err(|_| DatabaseError::CatalogWrite)?;
        write_bytes(&image.temp_path, &file_bytes).map_err(|_| DatabaseError::CatalogWrite)
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

fn stream_key_for_table(table_id: &str) -> DatabaseResult<String> {
    let normalized = common::normalize_identifier!(table_id);
    if normalized.is_empty() {
        return Err(DatabaseError::TableNotFound);
    }
    Ok(stable_id(&[&normalized]))
}

fn map_io_error_to_catalog_error(err: std::io::Error) -> DatabaseError {
    if err.kind() == ErrorKind::NotFound {
        DatabaseError::CatalogRead
    } else {
        DatabaseError::CatalogWrite
    }
}

fn load_records_from_path(path: &Path) -> DatabaseResult<Vec<TransactionRecord>> {

    if !path.exists() {
        return Ok(Vec::new());
    }

    let bytes = read_bytes(path).map_err(|_| DatabaseError::CatalogRead)?;
    verify_header(FileKind::Data, &bytes).map_err(|_| DatabaseError::CatalogInvalidHeader)?;

    let mut records = Vec::new();
    let mut pos = HEADER_SIZE;

    while pos + 8 <= bytes.len() {
        let len = u64::from_le_bytes(
            bytes[pos..pos + 8]
                .try_into()
                .expect("slice is exactly 8 bytes"),
        ) as usize;

        pos += 8;
        if pos + len > bytes.len() {
            return Err(DatabaseError::CatalogDeserialize);
        }

        let record = bincode::deserialize::<TransactionRecord>(&bytes[pos..pos + len])
            .map_err(|_| DatabaseError::CatalogDeserialize)?;
        records.push(record);
        pos += len;
    }

    Ok(records)

}

fn frame_records_as_wal_file(records: &[TransactionRecord]) -> Result<Vec<u8>, &'static str> {
    
    let mut file = Vec::new();
    file.extend_from_slice(&make_header(FileKind::Data));

    for record in records {
        let encoded = bincode::serialize(record).map_err(|_| "serialize record")?;
        file.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
        file.extend_from_slice(&encoded);
    }

    Ok(file)

}

fn apply_schema_rules_to_payload(payload: &[u8], rules: &SchemaMutationRuleSet) -> Vec<u8> {

    let mut row = match bincode::deserialize::<HashMap<String, Vec<u8>>>(payload) {
        Ok(row) => row,
        Err(_) => return payload.to_vec(),
    };

    for (from, to) in &rules.renames {

        let from_key = common::normalize_identifier!(from);
        let to_key = common::normalize_identifier!(to);

        if let Some(value) = row.remove(&from_key) {
            row.entry(to_key).or_insert(value);
        }

    }

    for field in &rules.removals {
        row.remove(&common::normalize_identifier!(field));
    }

    for (field, default_value) in &rules.additions {
        row.entry(common::normalize_identifier!(field))
            .or_insert_with(|| default_value.clone());
    }

    bincode::serialize(&row).unwrap_or_else(|_| payload.to_vec())
    
}

pub fn run_schema_migration<E: SchemaMigrationExecutor>(
    catalog: &mut DatabaseCatalog,
    table_id: &str,
    executor: &E,
) -> DatabaseResult<()> {
    let table_id = common::normalize_identifier!(table_id);

    // Phase 1: rewrite rows from disk to memory-resident migration image.
    catalog.transition_schema_change_phase(&table_id, SchemaChangePhase::Rewriting)?;
    let progress = executor.rewrite_rows(catalog, &table_id)?;
    catalog.checkpoint_schema_change_progress(
        &table_id,
        progress.rows_rewritten,
        progress.rows_total,
        progress.resume_token,
    )?;

    // Phase 2: rebuild indexes over the rewritten image.
    catalog.transition_schema_change_phase(&table_id, SchemaChangePhase::Reindexing)?;
    executor.rebuild_indexes(catalog, &table_id)?;

    // Phase 3: flush rewritten image to temporary disk artifact.
    catalog.transition_schema_change_phase(&table_id, SchemaChangePhase::Syncing)?;
    executor.flush_temp_image(catalog, &table_id)?;

    // Phase 4: cutover atomically to the rewritten table image.
    catalog.transition_schema_change_phase(&table_id, SchemaChangePhase::Cutover)?;
    executor.cutover(catalog, &table_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DatabaseError;
    use crate::core::identity::UserId;
    use crate::engine::database::core::ObjectStatus;
    use crate::engine::database::table_schema::TableSchema;
    use crate::engine::database::transaction::TransactionId;

    #[derive(Default)]
    struct SpyExecutor {
        calls: std::sync::Mutex<Vec<&'static str>>,
    }

    impl SpyExecutor {
        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().expect("mutex should lock").clone()
        }
    }

    impl SchemaMigrationExecutor for SpyExecutor {
        fn rewrite_rows(
            &self,
            _catalog: &DatabaseCatalog,
            _table_id: &str,
        ) -> DatabaseResult<SchemaMigrationProgress> {
            self.calls
                .lock()
                .expect("mutex should lock")
                .push("rewrite");
            Ok(SchemaMigrationProgress {
                rows_rewritten: 10,
                rows_total: Some(20),
                resume_token: Some("pk:users:10".to_string()),
            })
        }

        fn rebuild_indexes(
            &self,
            _catalog: &DatabaseCatalog,
            _table_id: &str,
        ) -> DatabaseResult<()> {
            self.calls
                .lock()
                .expect("mutex should lock")
                .push("reindex");
            Ok(())
        }

        fn flush_temp_image(&self, _catalog: &DatabaseCatalog, _table_id: &str) -> DatabaseResult<()> {
            self.calls
                .lock()
                .expect("mutex should lock")
                .push("flush");
            Ok(())
        }

        fn cutover(&self, _catalog: &DatabaseCatalog, _table_id: &str) -> DatabaseResult<()> {
            self.calls
                .lock()
                .expect("mutex should lock")
                .push("cutover");
            Ok(())
        }
    }

    #[test]
    fn run_schema_migration_updates_progress_and_order() {
        let mut catalog = DatabaseCatalog::create_empty_from_name("MainDb")
            .expect("catalog should be created");
        catalog
            .create_table("users", TableSchema::new(Vec::new()))
            .expect("table should be created");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let _tx = catalog
            .begin_schema_change("users")
            .expect("schema change should begin");

        let executor = SpyExecutor::default();
        run_schema_migration(&mut catalog, "users", &executor)
            .expect("migration should succeed");

        let calls = executor.calls();
        assert_eq!(calls, vec!["rewrite", "reindex", "flush", "cutover"]);

        let active = catalog
            .active_schema_change()
            .expect("active schema change should exist");
        assert_eq!(active.phase, SchemaChangePhase::Cutover);
        assert_eq!(active.rows_rewritten, 10);
        assert_eq!(active.rows_total, Some(20));
        assert_eq!(active.resume_token.as_deref(), Some("pk:users:10"));
    }

    #[test]
    fn run_schema_migration_requires_active_lock() {
        let mut catalog = DatabaseCatalog::create_empty_from_name("MainDb")
            .expect("catalog should be created");
        catalog
            .create_table("users", TableSchema::new(Vec::new()))
            .expect("table should be created");

        let err = run_schema_migration(&mut catalog, "users", &NoopSchemaMigrationExecutor)
            .expect_err("migration should fail when table is not locked");

        assert_eq!(err, DatabaseError::TableNotLocked);
    }

    #[test]
    fn disk_executor_rewrites_flushes_and_cuts_over() {
        let mut catalog = DatabaseCatalog::create_empty_from_name("MainDb")
            .expect("catalog should be created");
        catalog
            .create_table("users", TableSchema::new(Vec::new()))
            .expect("table should be created");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let _tx = catalog
            .begin_schema_change("users")
            .expect("schema change should begin");

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-schema-migration-{}-{}",
            std::process::id(),
            common::epochabs!()
        ));
        std::fs::create_dir_all(&temp_root).expect("temp dir should be created");

        let stream_key = stream_key_for_table("users").expect("stream key should resolve");
        let wal_path = temp_root.join(FileKind::Data.file_name(&stream_key));

        let actor = UserId::from_username("migrator");
        let seed_records = vec![
            TransactionRecord {
                id: TransactionId(1),
                refid: None,
                timestamp_epoch_ms: 1,
                actor: actor.clone(),
                kind: TransactionKind::Insert,
                payload: vec![1],
            },
            TransactionRecord {
                id: TransactionId(2),
                refid: Some(TransactionId(1)),
                timestamp_epoch_ms: 2,
                actor: actor.clone(),
                kind: TransactionKind::Delete,
                payload: vec![2],
            },
            TransactionRecord {
                id: TransactionId(3),
                refid: Some(TransactionId(2)),
                timestamp_epoch_ms: 3,
                actor,
                kind: TransactionKind::Update,
                payload: vec![3],
            },
        ];

        let wal_file = frame_records_as_wal_file(&seed_records).expect("wal file should frame");
        write_bytes(&wal_path, &wal_file).expect("seed wal should write");

        let executor = DiskToMemorySchemaMigrationExecutor::new(temp_root.clone());

        run_schema_migration(&mut catalog, "users", &executor)
            .expect("migration should succeed");

        let rewritten = load_records_from_path(&wal_path).expect("rewritten wal should load");
        assert_eq!(rewritten.len(), 2);
        assert!(rewritten.iter().all(|record| record.kind != TransactionKind::Delete));

        let active = catalog
            .active_schema_change()
            .expect("active schema change should exist");
        assert_eq!(active.phase, SchemaChangePhase::Cutover);

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn disk_executor_applies_schema_mutation_rules_to_row_payloads() {
        let mut catalog = DatabaseCatalog::create_empty_from_name("MainDb")
            .expect("catalog should be created");
        catalog
            .create_table("users", TableSchema::new(Vec::new()))
            .expect("table should be created");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let _tx = catalog
            .begin_schema_change("users")
            .expect("schema change should begin");

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-schema-rules-{}-{}",
            std::process::id(),
            common::epochabs!()
        ));
        std::fs::create_dir_all(&temp_root).expect("temp dir should be created");

        let stream_key = stream_key_for_table("users").expect("stream key should resolve");
        let wal_path = temp_root.join(FileKind::Data.file_name(&stream_key));

        let actor = UserId::from_username("migrator");
        let mut row = HashMap::new();
        row.insert("first_name".to_string(), b"sam".to_vec());
        row.insert("legacy".to_string(), b"drop".to_vec());

        let seed_records = vec![TransactionRecord {
            id: TransactionId(1),
            refid: None,
            timestamp_epoch_ms: 1,
            actor,
            kind: TransactionKind::Insert,
            payload: bincode::serialize(&row).expect("row should encode"),
        }];

        let wal_file = frame_records_as_wal_file(&seed_records).expect("wal file should frame");
        write_bytes(&wal_path, &wal_file).expect("seed wal should write");

        let executor = DiskToMemorySchemaMigrationExecutor::new(temp_root.clone());
        executor
            .set_rules_for_table(
                "users",
                SchemaMutationRuleSet {
                    renames: vec![("first_name".to_string(), "given_name".to_string())],
                    removals: vec!["legacy".to_string()],
                    additions: vec![("status".to_string(), b"active".to_vec())],
                },
            )
            .expect("rules should be set");

        run_schema_migration(&mut catalog, "users", &executor)
            .expect("migration should succeed");

        let rewritten = load_records_from_path(&wal_path).expect("rewritten wal should load");
        assert_eq!(rewritten.len(), 1);

        let out_row: HashMap<String, Vec<u8>> =
            bincode::deserialize(&rewritten[0].payload).expect("payload should decode");

        assert_eq!(out_row.get("given_name"), Some(&b"sam".to_vec()));
        assert_eq!(out_row.get("status"), Some(&b"active".to_vec()));
        assert!(!out_row.contains_key("first_name"));
        assert!(!out_row.contains_key("legacy"));

        let _ = std::fs::remove_dir_all(temp_root);
    }
}
