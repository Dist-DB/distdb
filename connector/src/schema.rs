use common::schema::{FieldIndex, FieldKind, FieldMetadata};

/// The data type a client assigns to a field.
///
/// This is the connector-layer representation: stable, wire-safe, and
/// independent of the server's internal storage types.  The server maps these
/// to its own `FieldType` during ingestion.
/// A field as described by a client application.
///
/// Clients construct `FieldSpec` values to describe the schema they want;
/// the server converts them into its internal representation.  Clients never
/// see or construct the server-side `TableSchema` directly.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldSpec {
    pub name: String,
    pub kind: FieldKind,
    pub nullable: bool,
    pub indexed: FieldIndex,
    pub default_value: Option<Vec<u8>>,
    #[serde(default)]
    pub metadata: Option<FieldMetadata>,
}

impl FieldSpec {

    pub fn new(name: impl Into<String>, kind: FieldKind) -> Self {
        Self {
            name: name.into(),
            kind,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn indexed(mut self) -> Self {
        self.indexed = FieldIndex::Indexed;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.indexed = FieldIndex::PrimaryKey;
        self
    }
    
}

/// A client-side description of a schema change: which table to alter and
/// what fields to add, remove, or update.
///
/// All three lists are applied in order: removals first, then updates, then
/// additions.  The server validates and converts each `FieldSpec` before
/// applying the change.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaChangeRequest {
    pub table_id: String,
    pub add: Vec<FieldSpec>,
    pub remove: Vec<String>,
    pub update: Vec<FieldSpec>,
}

impl SchemaChangeRequest {

    pub fn new(table_id: impl Into<String>) -> Self {
        Self {
            table_id: table_id.into(),
            add: Vec::new(),
            remove: Vec::new(),
            update: Vec::new(),
        }
    }

    pub fn add_field(mut self, spec: FieldSpec) -> Self {
        self.add.push(spec);
        self
    }

    pub fn remove_field(mut self, name: impl Into<String>) -> Self {
        self.remove.push(name.into());
        self
    }

    pub fn update_field(mut self, spec: FieldSpec) -> Self {
        self.update.push(spec);
        self
    }
    
}

#[cfg(test)]
#[path = "schema_test.rs"]
mod tests;
