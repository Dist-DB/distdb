
use crate::core::identity::NodeId;
use crate::engine::schema::{FieldDef, TableSchema};
use crate::engine::transaction::TransactionId;

use common::helpers::format::FileKind;
use common::helpers::{read_bytes, stable_id, write_bytes};

use std::collections::HashMap;
use std::path::Path;

pub type DatabaseResult<T> = Result<T, DatabaseError>;

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
        }
    }
}

impl std::error::Error for DatabaseError {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IndexId(pub String);

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
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseTable {
    pub table_id: String,
    pub status: ObjectStatus,
    pub schema: TableSchema,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseIndex {
    pub index_id: IndexId,
    pub table_id: String,
    pub field_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseRelationship {
    pub left_table_id: String,
    pub right_table_id: String,
    pub relation_name: String,
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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseCatalog {
    pub database_id: DatabaseId,
    status: ObjectStatus,
    tables: HashMap<String, DatabaseTable>,
    indexes: HashMap<String, DatabaseIndex>,
    relationships: Vec<DatabaseRelationship>,
}

impl DatabaseCatalog {

    pub fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            status: ObjectStatus::Load,
            tables: HashMap::new(),
            indexes: HashMap::new(),
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

        self.tables.insert(
            table_id.clone(),
            DatabaseTable {
                table_id: table_id.clone(),
                status: ObjectStatus::Load,
                schema: schema.clone(),
            },
        );

        for field in &schema.fields {
            if field.indexed {
                let index = DatabaseIndex::from_table_field(&table_id, field);
                self.indexes.insert(index.index_id.0.clone(), index);
            }
        }

        Ok(())

    }

    pub fn create_table(
        &mut self,
        table_id: impl Into<String>,
        schema: TableSchema,
    ) -> DatabaseResult<()> {
        let table_id = common::normalize_identifier!(table_id.into());
        self.register_table(table_id.clone(), schema)?;
        self.transition_table_status(&table_id, ObjectStatus::Sync)?;

        if !self.table_sync_acknowledged_stub(&table_id) {
            return Err(DatabaseError::SyncPending);
        }

        self.transition_table_status(&table_id, ObjectStatus::Ready)?;
        Ok(())
    }

    pub fn register_relationship(&mut self, relationship: DatabaseRelationship) {
        self.relationships.push(relationship);
    }

    pub fn table(&self, table_id: &str) -> Option<&DatabaseTable> {
        self.tables.get(&common::normalize_identifier!(table_id))
    }

    pub fn index(&self, index_id: &str) -> Option<&DatabaseIndex> {
        self.indexes.get(&common::normalize_identifier!(index_id))
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

    pub fn table_status(&self, table_id: &str) -> Option<ObjectStatus> {
        self.table(table_id).map(|table| table.status)
    }

    pub fn ensure_ready_for_write(&self, table_id: &str) -> DatabaseResult<()> {
        if self.status != ObjectStatus::Ready {
            return Err(DatabaseError::NotReadyForWrite);
        }

        let table = self
            .table(table_id)
            .ok_or(DatabaseError::TableNotFound)?;

        if table.status != ObjectStatus::Ready {
            return Err(DatabaseError::NotReadyForWrite);
        }

        Ok(())
    }

    pub fn transition_table_status(
        &mut self,
        table_id: &str,
        next: ObjectStatus,
    ) -> DatabaseResult<()> {
        let table = self
            .tables
            .get_mut(&common::normalize_identifier!(table_id))
            .ok_or(DatabaseError::TableNotFound)?;

        if !table.status.can_transition_to(next) {
            return Err(DatabaseError::InvalidStatusTransition);
        }

        table.status = next;
        Ok(())
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

}

impl DatabaseIndex {

    pub fn from_table_field(table_id: &str, field: &FieldDef) -> Self {

        let table_id = common::normalize_identifier!(table_id);
        let field_name = common::normalize_identifier!(&field.field_name);
        let index_id = IndexId(format!("{}:{}", table_id, field_name));

        Self {
            index_id,
            table_id,
            field_name,
        }
        
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
    fn lock_to_ready_direct_is_rejected() {
        let mut catalog =
            DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

        catalog
            .transition_status(ObjectStatus::Lock)
            .expect("load->lock is valid");
        let invalid = catalog.transition_status(ObjectStatus::Ready);

        assert!(matches!(invalid, Err(DatabaseError::InvalidStatusTransition)));
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

}