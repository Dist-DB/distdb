use common::schema::{FieldIndex, FieldKind};

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
}

impl FieldSpec {
    pub fn new(name: impl Into<String>, kind: FieldKind) -> Self {
        Self {
            name: name.into(),
            kind,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
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
mod tests {
    use super::*;

    #[test]
    fn field_spec_builder_defaults_to_non_nullable_non_indexed() {
        let spec = FieldSpec::new("email", FieldKind::Text);
        assert!(!spec.nullable);
        assert_eq!(spec.indexed, FieldIndex::None);
        assert_eq!(spec.name, "email");
    }

    #[test]
    fn field_spec_builder_sets_flags() {
        let spec = FieldSpec::new("email", FieldKind::Text).nullable().indexed();
        assert!(spec.nullable);
        assert_eq!(spec.indexed, FieldIndex::Indexed);
    }

    #[test]
    fn field_spec_builder_sets_primary_key() {
        let spec = FieldSpec::new("id", FieldKind::UInt(64)).primary_key();
        assert_eq!(spec.indexed, FieldIndex::PrimaryKey);
    }

    #[test]
    fn schema_change_request_builder_collects_operations() {
        let req = SchemaChangeRequest::new("users")
            .add_field(FieldSpec::new("email", FieldKind::Text).indexed())
            .remove_field("legacy_col")
            .update_field(FieldSpec::new("name", FieldKind::Text).nullable());

        assert_eq!(req.table_id, "users");
        assert_eq!(req.add.len(), 1);
        assert_eq!(req.remove.len(), 1);
        assert_eq!(req.update.len(), 1);
    }
}
