
use std::collections::HashMap;
use std::path::Path;

use common::helpers::format::FileKind;
use common::helpers::{read_bytes, write_bytes};

use super::core::{DatabaseError, DatabaseResult, ObjectStatus};
use super::entity::DatabaseEntity;
use super::id::DatabaseId;
use super::index::DatabaseIndex;
use super::relationship::DatabaseRelationship;
use super::schema_change_tx::SchemaChangeTx;
use super::table::DatabaseTable;
use super::table_schema::{FieldIndex, TableSchema};
use super::transaction::{SchemaChangePayload, TransactionKind, TransactionLog};
use super::view::DatabaseView;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseCatalog {
    pub database_id: DatabaseId,
    status: ObjectStatus,
    entities: HashMap<String, DatabaseEntity>,
}

impl DatabaseCatalog {
    
    pub fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            status: ObjectStatus::Load,
            entities: HashMap::new(),
        }
    }

    pub fn create_empty_from_name(name: &str) -> DatabaseResult<Self> {
        let database_id = DatabaseId::from_database_name(name)?;
        Ok(Self::new(database_id))
    }

    pub fn create_new_database(name: &str, directory: impl AsRef<Path>) -> DatabaseResult<Self> {
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

        if self.entities.contains_key(&table_id) {
            return Err(DatabaseError::DuplicateTable);
        }

        let indexes = Self::indexes_for_schema(&table_id, &schema);

        self.entities.insert(
            table_id.clone(),
            DatabaseEntity::Table(DatabaseTable::new(table_id, schema, indexes)),
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

        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.begin_sync()?;

        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }

        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.complete_sync()
    }

    pub fn register_relationship(&mut self, relationship: DatabaseRelationship) {
        let left = common::normalize_identifier!(&relationship.left_table_id);
        let right = common::normalize_identifier!(&relationship.right_table_id);
        let name = common::normalize_identifier!(&relationship.relation_name);
        let entity_id = format!("rel:{left}:{right}:{name}");
        self.entities
            .insert(entity_id, DatabaseEntity::Relationship(relationship));
    }

    pub fn table(&self, table_id: &str) -> Option<&DatabaseTable> {
        let normalized = common::normalize_identifier!(table_id);
        match self.entities.get(&normalized) {
            Some(DatabaseEntity::Table(table)) => Some(table),
            _ => None,
        }
    }

    pub fn index(&self, index_id: &str) -> Option<&DatabaseIndex> {
        let normalized = common::normalize_identifier!(index_id);
        self.entities.values().find_map(|entity| match entity {
            DatabaseEntity::Table(table) => table.indexes.get(&normalized),
            _ => None,
        })
    }

    pub fn relationships(&self) -> Vec<&DatabaseRelationship> {
        self.entities
            .values()
            .filter_map(|entity| match entity {
                DatabaseEntity::Relationship(relationship) => Some(relationship),
                _ => None,
            })
            .collect()
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

    /// Lock `table_id` (`Ready -> Lock`) and return a [`SchemaChangeTx`] that
    /// owns the pending schema mutations. The table stays locked until the
    /// returned transaction is either committed or aborted.
    pub fn begin_schema_change(&mut self, table_id: &str) -> DatabaseResult<SchemaChangeTx> {
        let table_id = common::normalize_identifier!(table_id);
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;

        let pending_schema = table.schema().clone();
        let next_revision = table.schema_revision() + 1;

        table.lock()?;

        Ok(SchemaChangeTx::new(table_id, next_revision, pending_schema))
    }

    /// Internal: apply a payload and drive `Lock -> Sync -> Ready`.
    /// Called only from `SchemaChangeTx::commit`.
    pub(crate) fn finalize_schema_change(
        &mut self,
        payload: SchemaChangePayload,
    ) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(&payload.table_id);
        self.apply_schema_change(payload)?;
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.begin_sync()?;
        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.complete_sync()
    }

    /// Internal: release the lock without changing the schema (`Lock -> Ready`).
    /// Called only from `SchemaChangeTx::abort`.
    pub(crate) fn release_schema_lock(&mut self, table_id: &str) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(table_id);
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;
        table.abort()
    }

    /// Internal: apply a schema payload directly. Does not enforce or alter
    /// table status. Used by `finalize_schema_change` and WAL replay.
    pub fn apply_schema_change(&mut self, payload: SchemaChangePayload) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(payload.table_id);
        let table = self.table_mut(&table_id).ok_or(DatabaseError::TableNotFound)?;

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

        let table = self.table(table_id).ok_or(DatabaseError::TableNotFound)?;

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
        self.entities
            .iter()
            .filter_map(|(entity_id, entity)| match entity {
                DatabaseEntity::Table(_) => Some(entity_id.clone()),
                _ => None,
            })
            .collect()
    }

    /// Register a view definition with a pre-derived schema. The schema is
    /// resolved by the caller at `CREATE VIEW` time against the current table
    /// catalog and stored here so schema inspection never needs to re-execute
    /// the view SQL.
    pub fn register_view(
        &mut self,
        view_id: impl Into<String>,
        sql: impl Into<String>,
        schema: TableSchema,
    ) -> DatabaseResult<()> {
        let view_id = common::normalize_identifier!(view_id.into());

        if self.entities.contains_key(&view_id) {
            return Err(DatabaseError::DuplicateView);
        }

        self.entities.insert(
            view_id.clone(),
            DatabaseEntity::View(DatabaseView {
                view_id,
                sql: sql.into(),
                schema,
            }),
        );

        Ok(())
    }

    pub fn view(&self, view_id: &str) -> Option<&DatabaseView> {
        let normalized = common::normalize_identifier!(view_id);
        match self.entities.get(&normalized) {
            Some(DatabaseEntity::View(view)) => Some(view),
            _ => None,
        }
    }

    pub fn view_ids(&self) -> Vec<String> {
        self.entities
            .iter()
            .filter_map(|(entity_id, entity)| match entity {
                DatabaseEntity::View(_) => Some(entity_id.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn view_schema(&self, view_id: &str) -> Option<&TableSchema> {
        self.view(view_id).map(|v| &v.schema)
    }

    /// Returns `true` for tables, `false` for views. Used at the query
    /// routing layer to reject write operations against view sources before
    /// any execution begins.
    pub fn is_writable(&self, object_id: &str) -> bool {
        let normalized = common::normalize_identifier!(object_id);
        matches!(
            self.entities.get(&normalized),
            Some(DatabaseEntity::Table(_))
        )
    }

    fn table_mut(&mut self, table_id: &str) -> Option<&mut DatabaseTable> {
        let normalized = common::normalize_identifier!(table_id);
        match self.entities.get_mut(&normalized) {
            Some(DatabaseEntity::Table(table)) => Some(table),
            _ => None,
        }
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
    fn table_sync_acknowledged_stub(&self, table_id: &str) -> bool {
        self.received_table_replica_acks_stub(table_id)
            >= self.required_table_replica_acks_stub(table_id)
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

    fn indexes_for_schema(table_id: &str, schema: &TableSchema) -> HashMap<String, DatabaseIndex> {
        let mut indexes = HashMap::new();
        for field in &schema.fields {
            if matches!(field.indexed, FieldIndex::Indexed | FieldIndex::PrimaryKey) {
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
        // Lock -> Ready is permitted so that table transactions can be aborted.
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

        tx.add_field(crate::engine::database::table_schema::FieldDef {
            seqno: 1,
            field_name: "email".to_string(),
            field_type: crate::engine::database::table_schema::FieldType::Text,
            nullable: false,
            indexed: FieldIndex::Indexed,
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
        assert!(catalog
            .table_schema("users")
            .and_then(|s| s.field("email"))
            .is_some());
        assert_eq!(captured_payload.expect("captured payload").schema_revision, 1);
    }

    #[test]
    fn schema_change_tx_abort_returns_table_to_ready_without_schema_change() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
        let initial_schema = TableSchema::new(vec![crate::engine::database::table_schema::FieldDef {
            seqno: 1,
            field_name: "name".to_string(),
            field_type: crate::engine::database::table_schema::FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
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
        tx.remove_field("name")
            .expect("remove should succeed on pending schema");

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
            Err(DatabaseError::NotReadyForWrite)
        });

        assert!(result.is_err());
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

        let first_schema = TableSchema::new(vec![crate::engine::database::table_schema::FieldDef {
            seqno: 1,
            field_name: "name".to_string(),
            field_type: crate::engine::database::table_schema::FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
        }]);
        let first_payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 1,
            schema: first_schema,
        };
        wal.append(
            "users",
            crate::engine::database::transaction::TransactionRecord {
                id: crate::engine::database::transaction::TransactionId(1),
                refid: None,
                timestamp_epoch_ms: 1,
                actor: actor.clone(),
                kind: crate::engine::database::transaction::TransactionKind::SchemaChange,
                payload: first_payload.encode().expect("schema payload should encode"),
            },
        )
        .expect("first schema append should succeed");

        let second_schema = TableSchema::new(vec![crate::engine::database::table_schema::FieldDef {
            seqno: 1,
            field_name: "email".to_string(),
            field_type: crate::engine::database::table_schema::FieldType::Text,
            nullable: false,
            indexed: FieldIndex::Indexed,
            default_value: None,
        }]);
        let second_payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 2,
            schema: second_schema.clone(),
        };
        wal.append(
            "users",
            crate::engine::database::transaction::TransactionRecord {
                id: crate::engine::database::transaction::TransactionId(2),
                refid: None,
                timestamp_epoch_ms: 2,
                actor,
                kind: crate::engine::database::transaction::TransactionKind::SchemaChange,
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
        assert!(catalog.index("users:email").is_some());
    }
}
