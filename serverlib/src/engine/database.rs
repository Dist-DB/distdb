
use crate::core::identity::NodeId;
use crate::engine::schema::{FieldDef, TableSchema};
use crate::engine::transaction::TransactionId;

use common::helpers::format::FileKind;
use common::helpers::{read_bytes, stable_id, write_bytes};

use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IndexId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseTable {
    pub table_id: String,
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
    pub fn from_database_name(name: &str) -> Result<Self, &'static str> {
        let normalized = common::normalize_identifier!(name);
        if normalized.is_empty() {
            return Err("database name must not be empty");
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
    tables: HashMap<String, DatabaseTable>,
    indexes: HashMap<String, DatabaseIndex>,
    relationships: Vec<DatabaseRelationship>,
}

impl DatabaseCatalog {

    pub fn new(database_id: DatabaseId) -> Self {
        Self {
            database_id,
            tables: HashMap::new(),
            indexes: HashMap::new(),
            relationships: Vec::new(),
        }
    }

    pub fn create_empty_from_name(name: &str) -> Result<Self, &'static str> {
        let database_id = DatabaseId::from_database_name(name)?;
        Ok(Self::new(database_id))
    }

    pub fn register_table(&mut self, table_id: impl Into<String>, schema: TableSchema) {
        let table_id = common::normalize_identifier!(table_id.into());

        self.tables.insert(
            table_id.clone(),
            DatabaseTable {
                table_id: table_id.clone(),
                schema: schema.clone(),
            },
        );

        for field in &schema.fields {
            if field.indexed {
                let index = DatabaseIndex::from_table_field(&table_id, field);
                self.indexes.insert(index.index_id.0.clone(), index);
            }
        }
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

    pub fn file_name(&self) -> String {
        FileKind::Catalog.file_name(common::normalize_identifier!(self.database_id.0.clone()))
    }

    pub fn from_file_stem(stem: &str) -> Self {
        Self::new(DatabaseId(common::normalize_identifier!(stem)))
    }

    pub fn table_ids(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, &'static str> {
        let bytes = read_bytes(path).map_err(|_| "failed to read catalog file")?;

        common::helpers::format::verify_header(FileKind::Catalog, &bytes)
            .map_err(|_| "invalid catalog file header/version")?;

        if bytes.len() <= common::helpers::format::HEADER_SIZE {
            return Err("catalog payload missing");
        }

        bincode::deserialize::<Self>(&bytes[common::helpers::format::HEADER_SIZE..])
            .map_err(|_| "failed to deserialize catalog file")
    }

    pub fn save_to_path(&self, path: impl AsRef<Path>) -> Result<(), &'static str> {
        let payload = bincode::serialize(self).map_err(|_| "failed to serialize catalog")?;
        let mut file = Vec::with_capacity(common::helpers::format::HEADER_SIZE + payload.len());
        file.extend_from_slice(&common::helpers::format::make_header(FileKind::Catalog));
        file.extend_from_slice(&payload);
        write_bytes(path, &file).map_err(|_| "failed to write catalog file")
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

}