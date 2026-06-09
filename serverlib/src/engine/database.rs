
use crate::core::identity::NodeId;
use crate::engine::table_schema::{FieldDef, SchemaError, TableSchema};
use crate::engine::transaction::{SchemaChangePayload, TransactionId, TransactionKind, TransactionLog};

use common::helpers::format::FileKind;
use common::helpers::{read_bytes, stable_id, write_bytes};

use std::collections::HashMap;
use std::path::Path;

pub type DatabaseResult<T> = Result<T, DatabaseError>;
pub use crate::engine::database_table::{DatabaseIndex, DatabaseRelationship, DatabaseTable, IndexId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseError {
    InvalidDatabaseName,
    DuplicateTable,
    TableNotFound,
    InvalidStatusTransition,
    NotReadyForWrite,
    SyncPending,
    CatalogRead,
    CatalogInvalidHeader,
    CatalogPayloadMissing,
    CatalogDeserialize,
    CatalogSerialize,
    CatalogWrite,
    SchemaPayloadDeserialize,
    SchemaRevisionOutOfOrder,
    SchemaChange(SchemaError),
    TableNotLocked,
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDatabaseName => write!(f, "database name must not be empty"),
            Self::DuplicateTable => write!(f, "table already registered in database catalog"),
            Self::TableNotFound => write!(f, "table not found in database catalog"),
            Self::InvalidStatusTransition => write!(f, "invalid database/table status transition"),
            Self::NotReadyForWrite => write!(f, "database/table is not ready for write operations"),
            Self::SyncPending => write!(f, "database/table sync has not been acknowledged yet"),
            Self::CatalogRead => write!(f, "failed to read catalog file"),
            Self::CatalogInvalidHeader => write!(f, "invalid catalog file header/version"),
            Self::CatalogPayloadMissing => write!(f, "catalog payload missing"),
            Self::CatalogDeserialize => write!(f, "failed to deserialize catalog file"),
            Self::CatalogSerialize => write!(f, "failed to serialize catalog"),
            Self::CatalogWrite => write!(f, "failed to write catalog file"),
            Self::SchemaPayloadDeserialize => {
                write!(f, "failed to deserialize schema change payload")
            }
            Self::SchemaRevisionOutOfOrder => {
                write!(f, "schema revision must advance monotonically")
            }
            Self::SchemaChange(e) => write!(f, "schema mutation error: {e}"),
            Self::TableNotLocked => write!(f, "table must be locked before a schema change can be prepared or committed"),
        }
    }
}

impl std::error::Error for DatabaseError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ObjectStatus {
    Load,
    Sync,
    Ready,
    Lock,
}

impl ObjectStatus {
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Load, Self::Sync)
                | (Self::Load, Self::Ready)
                | (Self::Load, Self::Lock)
                | (Self::Sync, Self::Ready)
                | (Self::Sync, Self::Lock)
                | (Self::Ready, Self::Sync)
                | (Self::Ready, Self::Lock)
                | (Self::Lock, Self::Sync)
                | (Self::Lock, Self::Ready)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct DatabaseId(pub String);

impl DatabaseId {
    pub fn from_database_name(name: &str) -> DatabaseResult<Self> {
        let normalized = common::normalize_identifier!(name);
        if normalized.is_empty() {
            return Err(DatabaseError::InvalidDatabaseName);
        }
        Ok(Self(stable_id(&[&normalized])))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseReplicaState {
    pub database_id: DatabaseId,
    pub local_node_id: NodeId,
    pub last_applied_tx: Option<TransactionId>,
}

/// An in-progress schema change transaction.  Obtained from
/// [`DatabaseCatalog::begin_schema_change`]; the table is held in `Lock` state
/// until this value is either committed or aborted.
///
/// Typical usage:
/// ```ignore
/// let mut tx = catalog.begin_schema_change("users")?;
/// tx.add_field(field)?;
/// tx.commit(&mut catalog, |payload| wal.append(wal_id, make_record(payload)))?;
/// ```
#[derive(Debug, Clone)]
pub struct SchemaChangeTx {
    table_id: String,
    next_revision: u64,
    pending_schema: TableSchema,
}

impl SchemaChangeTx {
    pub fn table_id(&self) -> &str {
        &self.table_id
    }

    pub fn next_revision(&self) -> u64 {
        self.next_revision
    }

    /// Inspect the pending (not yet committed) schema.
    pub fn pending_schema(&self) -> &TableSchema {
        &self.pending_schema
    }

    pub fn add_field(&mut self, field: FieldDef) -> DatabaseResult<()> {
        self.pending_schema
            .add_field(field)
            .map_err(DatabaseError::SchemaChange)
    }

    pub fn remove_field(&mut self, name: &str) -> DatabaseResult<()> {
        self.pending_schema
            .remove_field(name)
            .map_err(DatabaseError::SchemaChange)
    }

    pub fn update_field(&mut self, field: FieldDef) -> DatabaseResult<()> {
        self.pending_schema
            .update_field(field)
            .map_err(DatabaseError::SchemaChange)
    }

    /// Persist the change via `persist`, then if successful apply the schema
    /// and drive the table `Lock → Sync → Ready`.
    ///
    /// If `persist` returns an error the lock is released (`Lock → Ready`)
    /// and the schema is left unchanged.  The persist error is returned.
    pub fn commit<E, F>(self, catalog: &mut DatabaseCatalog, persist: F) -> Result<(), E>
    where
        F: FnOnce(&SchemaChangePayload) -> Result<(), E>,
        E: From<DatabaseError>,
    {
        let payload = SchemaChangePayload {
            table_id: self.table_id.clone(),
            schema_revision: self.next_revision,
            schema: self.pending_schema,
        };

        if let Err(e) = persist(&payload) {
            // Best-effort abort — release the lock even if abort itself fails.
            let _ = catalog.release_schema_lock(&self.table_id);
            return Err(e);
        }

        catalog
            .finalize_schema_change(payload)
            .map_err(E::from)
    }

    /// Release the lock without altering the schema.
    pub fn abort(self, catalog: &mut DatabaseCatalog) -> DatabaseResult<()> {
        catalog.release_schema_lock(&self.table_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseCatalog {
    pub database_id: DatabaseId,
    status: ObjectStatus,
    tables: HashMap<String, DatabaseTable>,
    relationships: Vec<DatabaseRelationship>,
}

impl DatabaseCatalog {

    pub fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            status: ObjectStatus::Load,
            tables: HashMap::new(),
            relationships: Vec::new(),
        }
    }

    pub fn create_empty_from_name(name: &str) -> DatabaseResult<Self> {
        let database_id = DatabaseId::from_database_name(name)?;
        Ok(Self::new(database_id))
    }

    pub fn create_new_database(
        name: &str,
        directory: impl AsRef<Path>,
    ) -> DatabaseResult<Self> {
        let mut catalog = Self::create_empty_from_name(name)?;
        catalog.transition_status(ObjectStatus::Sync)?;
        catalog.save_in_directory(&directory)?;

        if !catalog.database_sync_acknowledged_stub() {
            return Err(DatabaseError::SyncPending);
        }

        catalog.transition_status(ObjectStatus::Ready)?;
        catalog.save_in_directory(directory)?;
        Ok(catalog)
    }

    pub fn register_table(
        &mut self,
        table_id: impl Into<String>,
        schema: TableSchema,
    ) -> DatabaseResult<()> {
        
        let table_id = common::normalize_identifier!(table_id.into());

        if self.tables.contains_key(&table_id) {
            return Err(DatabaseError::DuplicateTable);
        }

        let indexes = Self::indexes_for_schema(&table_id, &schema);

        self.tables.insert(
            table_id.clone(),
            DatabaseTable::new(table_id.clone(), schema, indexes),
        );

        Ok(())

    }

    pub fn create_table(
        &mut self,
        table_id: impl Into<String>,
        schema: TableSchema,
    ) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(table_id.into());
        self.register_table(table_id.clone(), schema)?;

        let table = self.tables.get_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.begin_sync()?;

        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }

        let table = self.tables.get_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.complete_sync()
    }

    pub fn register_relationship(&mut self, relationship: DatabaseRelationship) {
        self.relationships.push(relationship);
    }

    pub fn table(&self, table_id: &str) -> Option<&DatabaseTable> {
        self.tables.get(&common::normalize_identifier!(table_id))
    }

    pub fn index(&self, index_id: &str) -> Option<&DatabaseIndex> {
        let normalized = common::normalize_identifier!(index_id);
        self.tables
            .values()
            .find_map(|table| table.indexes.get(&normalized))
    }

    pub fn relationships(&self) -> &[DatabaseRelationship] {
        &self.relationships
    }

    pub fn status(&self) -> ObjectStatus {
        self.status
    }

    pub fn transition_status(&mut self, next: ObjectStatus) -> DatabaseResult<()> {
        if !self.status.can_transition_to(next) {
            return Err(DatabaseError::InvalidStatusTransition);
        }
        self.status = next;
        Ok(())
    }

    pub fn table_schema(&self, table_id: &str) -> Option<&TableSchema> {
        self.table(table_id).map(DatabaseTable::schema)
    }

    pub fn table_schema_revision(&self, table_id: &str) -> Option<u64> {
        self.table(table_id).map(DatabaseTable::schema_revision)
    }

    /// Lock `table_id` (`Ready → Lock`) and return a [`SchemaChangeTx`] that
    /// owns the pending schema mutations.  The table stays locked until the
    /// returned transaction is either committed or aborted.
    pub fn begin_schema_change(
        &mut self,
        table_id: &str,
    ) -> DatabaseResult<SchemaChangeTx> {
        let table_id = common::normalize_identifier!(table_id);
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DatabaseError::TableNotFound)?;

        let pending_schema = table.schema().clone();
        let next_revision = table.schema_revision() + 1;

        table.lock()?;

        Ok(SchemaChangeTx {
            table_id,
            next_revision,
            pending_schema,
        })
    }

    /// Internal: apply a payload and drive `Lock → Sync → Ready`.
    /// Called only from `SchemaChangeTx::commit`.
    fn finalize_schema_change(&mut self, payload: SchemaChangePayload) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(&payload.table_id);
        self.apply_schema_change(payload)?;
        let table = self.tables.get_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.begin_sync()?;
        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }
        let table = self.tables.get_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.complete_sync()
    }

    /// Internal: release the lock without changing the schema (`Lock → Ready`).
    /// Called only from `SchemaChangeTx::abort`.
    fn release_schema_lock(&mut self, table_id: &str) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(table_id);
        let table = self.tables.get_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.abort()
    }

    /// Internal: apply a schema payload directly.  Does not enforce or alter
    /// table status.  Used by `finalize_schema_change` and WAL replay.
    pub fn apply_schema_change(&mut self, payload: SchemaChangePayload) -> DatabaseResult<()> {

        let table_id = common::normalize_identifier!(payload.table_id);
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or(DatabaseError::TableNotFound)?;

        if payload.schema_revision <= table.schema_revision() {
            return Err(DatabaseError::SchemaRevisionOutOfOrder);
        }

        let indexes = Self::indexes_for_schema(&table_id, &payload.schema);

        table.replace_schema(payload.schema_revision, payload.schema, indexes);
        Ok(())

    }

    pub fn replay_schema_from_log<L: TransactionLog>(
        &mut self,
        wal_id: &str,
        log: &L,
    ) -> DatabaseResult<usize> {
        let mut applied = 0usize;

        for record in log.since(wal_id, None) {
            if record.kind != TransactionKind::SchemaChange {
                continue;
            }

            let payload = SchemaChangePayload::decode(&record.payload)
                .map_err(|_| DatabaseError::SchemaPayloadDeserialize)?;
            self.apply_schema_change(payload)?;
            applied += 1;
        }

        Ok(applied)
    }

    pub fn ensure_ready_for_write(&self, table_id: &str) -> DatabaseResult<()> {
        
        if self.status != ObjectStatus::Ready {
            return Err(DatabaseError::NotReadyForWrite);
        }

        let table = self
            .table(table_id)
            .ok_or(DatabaseError::TableNotFound)?;

        if table.status() != ObjectStatus::Ready {
            return Err(DatabaseError::NotReadyForWrite);
        }

        Ok(())
    }

    pub fn table_status(&self, table_id: &str) -> Option<ObjectStatus> {
        self.table(table_id).map(DatabaseTable::status)
    }

    pub fn file_name(&self) -> String {
        FileKind::Catalog.file_name(common::normalize_identifier!(self.database_id.0.clone()))
    }

    pub fn from_file_stem(stem: &str) -> Self {
        Self::new(DatabaseId(common::normalize_identifier!(stem)))
    }

    pub fn table_ids(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> DatabaseResult<Self> {

        let bytes = read_bytes(path).map_err(|_| DatabaseError::CatalogRead)?;

        common::helpers::format::verify_header(FileKind::Catalog, &bytes)
            .map_err(|_| DatabaseError::CatalogInvalidHeader)?;

        if bytes.len() <= common::helpers::format::HEADER_SIZE {
            return Err(DatabaseError::CatalogPayloadMissing);
        }

        bincode::deserialize::<Self>(&bytes[common::helpers::format::HEADER_SIZE..])
            .map_err(|_| DatabaseError::CatalogDeserialize)
            
    }

    pub fn save_in_directory(&self, directory: impl AsRef<Path>) -> DatabaseResult<()> {
        let path = directory.as_ref().join(self.file_name());
        self.save_to_path(path)
    }

    fn save_to_path(&self, path: impl AsRef<Path>) -> DatabaseResult<()> {
        let payload = bincode::serialize(self).map_err(|_| DatabaseError::CatalogSerialize)?;
        let mut file = Vec::with_capacity(common::helpers::format::HEADER_SIZE + payload.len());
        file.extend_from_slice(&common::helpers::format::make_header(FileKind::Catalog));
        file.extend_from_slice(&payload);
        write_bytes(path, &file).map_err(|_| DatabaseError::CatalogWrite)
    }

    // Stub for future p2p/quorum integration.
    // With zero configured replicas, sync can promote to Ready immediately.
    fn database_sync_acknowledged_stub(&self) -> bool {
        self.received_database_replica_acks_stub() >= self.required_database_replica_acks_stub()
    }

    // Stub for future p2p/quorum integration.
    // With zero configured replicas, sync can promote to Ready immediately.
    fn table_sync_acknowledged_stub(&self, _table_id: &str) -> bool {
        self.received_table_replica_acks_stub(_table_id) >= self.required_table_replica_acks_stub(_table_id)
    }

    fn required_database_replica_acks_stub(&self) -> usize {
        0
    }

    fn received_database_replica_acks_stub(&self) -> usize {
        0
    }

    fn required_table_replica_acks_stub(&self, _table_id: &str) -> usize {
        0
    }

    fn received_table_replica_acks_stub(&self, _table_id: &str) -> usize {
        0
    }

    fn indexes_for_schema(
        table_id: &str,
        schema: &TableSchema,
    ) -> HashMap<String, DatabaseIndex> {
        let mut indexes = HashMap::new();
        for field in &schema.fields {
            if field.indexed {
                let index = DatabaseIndex::from_table_field(table_id, field);
                indexes.insert(index.index_id.0.clone(), index);
            }
        }
        indexes
    }

}

#[cfg(test)]
mod tests {
    
    use super::*;

    #[test]
    fn database_id_is_obscured_from_normalized_name() {
        let id_a = DatabaseId::from_database_name("Sales").expect("valid database name");
        let id_b = DatabaseId::from_database_name("sales").expect("valid database name");

        assert_eq!(id_a, id_b);
        assert_ne!(id_a.0, "sales");
    }

    #[test]
    fn create_empty_catalog_from_name_sets_obscured_id() {
        let catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        assert!(catalog.table_ids().is_empty());
        assert!(!catalog.database_id.0.is_empty());
        assert_ne!(catalog.database_id.0, "maindb");
    }

    #[test]
    fn empty_database_name_is_rejected() {
        let created = DatabaseCatalog::create_empty_from_name("   ");
        assert!(created.is_err());
    }

    #[test]
    fn duplicate_table_registration_is_rejected() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        let schema = TableSchema { fields: Vec::new() };

        let first = catalog.register_table("users", schema.clone());
        let second = catalog.register_table("users", schema);

        assert!(first.is_ok());
        assert!(second.is_err());
    }

    #[test]
    fn catalog_and_table_start_in_load_state() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        let schema = TableSchema { fields: Vec::new() };

        catalog
            .register_table("users", schema)
            .expect("table register should succeed");

        assert_eq!(catalog.status(), ObjectStatus::Load);
        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Load));
    }

    #[test]
    fn lock_moves_to_sync_then_ready() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .transition_status(ObjectStatus::Lock)
            .expect("load->lock is valid");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("lock->sync is valid");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready is valid");

        assert_eq!(catalog.status(), ObjectStatus::Ready);
    }

    #[test]
    fn lock_to_ready_is_valid_for_abort_path() {
        // Lock → Ready is permitted so that table transactions can be aborted.
        // The catalog's own status follows the same state machine.
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .transition_status(ObjectStatus::Lock)
            .expect("load->lock is valid");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("lock->ready is valid as an abort path");

        assert_eq!(catalog.status(), ObjectStatus::Ready);
    }

    #[test]
    fn create_table_moves_load_sync_ready() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .create_table("users", TableSchema { fields: Vec::new() })
            .expect("create table should succeed");

        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
    }

    #[test]
    fn write_requires_database_and_table_ready() {

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .create_table("users", TableSchema { fields: Vec::new() })
            .expect("create table should succeed");

        let denied = catalog.ensure_ready_for_write("users");
        assert!(matches!(denied, Err(DatabaseError::NotReadyForWrite)));

        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let allowed = catalog.ensure_ready_for_write("users");
        assert!(allowed.is_ok());

    }

    #[test]
    fn schema_can_be_retrieved_from_table() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        let schema = TableSchema::new(Vec::new());

        catalog
            .register_table("users", schema.clone())
            .expect("table register should succeed");

        assert_eq!(catalog.table_schema("users"), Some(&schema));
        assert_eq!(catalog.table_schema_revision("users"), Some(0));
    }

    #[test]
    fn schema_change_payload_updates_existing_table() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        catalog
            .register_table("users", TableSchema::new(Vec::new()))
            .expect("table register should succeed");

        let updated_schema = TableSchema::new(Vec::new());
        let payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 3,
            schema: updated_schema.clone(),
        };

        catalog
            .apply_schema_change(payload)
            .expect("schema change should apply");

        assert_eq!(catalog.table_schema("users"), Some(&updated_schema));
        assert_eq!(catalog.table_schema_revision("users"), Some(3));
    }

    #[test]
    fn schema_change_tx_commit_applies_schema_and_returns_ready() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        catalog
            .create_table("users", TableSchema::new(Vec::new()))
            .expect("table should be created");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let mut tx = catalog
            .begin_schema_change("users")
            .expect("begin should lock the table");

        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Lock));

        tx.add_field(crate::engine::table_schema::FieldDef {
            seqno: 1,
            field_name: "email".to_string(),
            field_type: crate::engine::table_schema::FieldType::Text,
            nullable: false,
            indexed: true,
            default_value: None,
        })
        .expect("add_field should succeed");

        let mut captured_payload: Option<SchemaChangePayload> = None;
        tx.commit::<DatabaseError, _>(&mut catalog, |payload| {
            captured_payload = Some(payload.clone());
            Ok(())
        })
        .expect("commit should succeed");

        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
        assert_eq!(catalog.table_schema_revision("users"), Some(1));
        assert!(catalog.table_schema("users").and_then(|s| s.field("email")).is_some());
        assert_eq!(captured_payload.unwrap().schema_revision, 1);
    }

    #[test]
    fn schema_change_tx_abort_returns_table_to_ready_without_schema_change() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        let initial_schema = TableSchema::new(vec![crate::engine::table_schema::FieldDef {
            seqno: 1,
            field_name: "name".to_string(),
            field_type: crate::engine::table_schema::FieldType::Text,
            nullable: false,
            indexed: false,
            default_value: None,
        }]);
        catalog
            .create_table("users", initial_schema.clone())
            .expect("table should be created");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let mut tx = catalog
            .begin_schema_change("users")
            .expect("begin should lock the table");
        tx.remove_field("name").expect("remove should succeed on pending schema");

        tx.abort(&mut catalog).expect("abort should release lock");

        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
        assert_eq!(catalog.table_schema("users"), Some(&initial_schema));
    }

    #[test]
    fn schema_change_tx_commit_aborts_when_persist_fails() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        let initial_schema = TableSchema::new(Vec::new());
        catalog
            .create_table("users", initial_schema.clone())
            .expect("table should be created");
        catalog
            .transition_status(ObjectStatus::Sync)
            .expect("load->sync");
        catalog
            .transition_status(ObjectStatus::Ready)
            .expect("sync->ready");

        let tx = catalog
            .begin_schema_change("users")
            .expect("begin should lock the table");

        let result = tx.commit::<DatabaseError, _>(&mut catalog, |_payload| {
            Err(DatabaseError::NotReadyForWrite) // stand-in for a WAL failure
        });

        assert!(result.is_err());
        // table should be back to Ready, schema unchanged
        assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
        assert_eq!(catalog.table_schema("users"), Some(&initial_schema));
    }

    #[test]
    fn schema_replay_uses_latest_transaction_payload() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        catalog
            .register_table("users", TableSchema::new(Vec::new()))
            .expect("table register should succeed");

        let wal = crate::engine::wal::ConcurrentWalManager::new();
        let actor = crate::core::identity::UserId::from_username("schema-tester");

        let first_schema = TableSchema::new(vec![crate::engine::table_schema::FieldDef {
            seqno: 1,
            field_name: "name".to_string(),
            field_type: crate::engine::table_schema::FieldType::Text,
            nullable: false,
            indexed: false,
            default_value: None,
        }]);
        let first_payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 1,
            schema: first_schema,
        };
        wal.append(
            "users",
            crate::engine::transaction::TransactionRecord {
                id: crate::engine::transaction::TransactionId(1),
                refid: None,
                timestamp_epoch_ms: 1,
                actor: actor.clone(),
                kind: crate::engine::transaction::TransactionKind::SchemaChange,
                payload: first_payload.encode().expect("schema payload should encode"),
            },
        )
        .expect("first schema append should succeed");

        let second_schema = TableSchema::new(vec![crate::engine::table_schema::FieldDef {
            seqno: 1,
            field_name: "email".to_string(),
            field_type: crate::engine::table_schema::FieldType::Text,
            nullable: false,
            indexed: true,
            default_value: None,
        }]);
        let second_payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 2,
            schema: second_schema.clone(),
        };
        wal.append(
            "users",
            crate::engine::transaction::TransactionRecord {
                id: crate::engine::transaction::TransactionId(2),
                refid: None,
                timestamp_epoch_ms: 2,
                actor,
                kind: crate::engine::transaction::TransactionKind::SchemaChange,
                payload: second_payload.encode().expect("schema payload should encode"),
            },
        )
        .expect("second schema append should succeed");

        let applied = catalog
            .replay_schema_from_log("users", &wal)
            .expect("schema replay should succeed");

        assert_eq!(applied, 2);
        assert_eq!(catalog.table_schema("users"), Some(&second_schema));
        assert_eq!(catalog.table_schema_revision("users"), Some(2));
        assert_eq!(catalog.index("users:email").is_some(), true);
    }

}