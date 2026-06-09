
use common::schema::{
    normalize_field_name, validate_field_kind, FieldKind,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
    DuplicateField,
    FieldNotFound,
    SeqnoConflict,
    InvalidFieldType,
    InvalidFieldName,
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateField => write!(f, "a field with that name already exists"),
            Self::FieldNotFound => write!(f, "field not found in schema"),
            Self::SeqnoConflict => write!(f, "a field with that seqno already exists"),
            Self::InvalidFieldType => write!(f, "field type definition is invalid"),
            Self::InvalidFieldName => write!(f, "field name is invalid"),
        }
    }
}

impl std::error::Error for SchemaError {}

pub type SchemaResult<T> = Result<T, SchemaError>;

pub type FieldType = FieldKind;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldDef {
    pub seqno: u32,
    pub field_name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub indexed: bool,
    pub default_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
    pub fields: Vec<FieldDef>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableSchemaRevision {
    pub revision: u64,
    pub schema: TableSchema,
}

impl TableSchema {
    pub fn new(fields: Vec<FieldDef>) -> Self {
        Self { fields }
    }

    pub fn field(&self, name: &str) -> Option<&FieldDef> {
        let normalized = name.trim().to_ascii_lowercase();
        self.fields.iter().find(|f| f.field_name == normalized)
    }

    /// Append a new field to the schema.  The field name is normalized to
    /// lower-case; callers may pass any casing.
    pub fn add_field(&mut self, mut field: FieldDef) -> SchemaResult<()> {
        field.field_name = normalize_field_name(&field.field_name)
            .map_err(|_| SchemaError::InvalidFieldName)?;
        validate_field_kind(&field.field_type)
            .map_err(|_| SchemaError::InvalidFieldType)?;

        if self.fields.iter().any(|f| f.field_name == field.field_name) {
            return Err(SchemaError::DuplicateField);
        }

        if self.fields.iter().any(|f| f.seqno == field.seqno) {
            return Err(SchemaError::SeqnoConflict);
        }

        self.fields.push(field);
        Ok(())
    }

    /// Remove a field by name.  Returns `FieldNotFound` when no such field exists.
    pub fn remove_field(&mut self, name: &str) -> SchemaResult<()> {
        let normalized = name.trim().to_ascii_lowercase();
        let pos = self
            .fields
            .iter()
            .position(|f| f.field_name == normalized)
            .ok_or(SchemaError::FieldNotFound)?;
        self.fields.remove(pos);
        Ok(())
    }

    /// Replace the definition of an existing field (matched by name).
    /// The incoming `field.field_name` is normalized and must match a field
    /// that is already in the schema.
    pub fn update_field(&mut self, mut field: FieldDef) -> SchemaResult<()> {
        field.field_name = normalize_field_name(&field.field_name)
            .map_err(|_| SchemaError::InvalidFieldName)?;
        validate_field_kind(&field.field_type)
            .map_err(|_| SchemaError::InvalidFieldType)?;
        let target = self
            .fields
            .iter_mut()
            .find(|f| f.field_name == field.field_name)
            .ok_or(SchemaError::FieldNotFound)?;
        *target = field;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn text_field(seqno: u32, name: &str) -> FieldDef {
        FieldDef {
            seqno,
            field_name: name.to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: false,
            default_value: None,
        }
    }

    #[test]
    fn add_field_normalizes_name() {
        let mut schema = TableSchema::new(Vec::new());
        schema.add_field(text_field(1, "Email")).unwrap();
        assert!(schema.field("email").is_some());
    }

    #[test]
    fn add_field_rejects_duplicate_name() {
        let mut schema = TableSchema::new(vec![text_field(1, "email")]);
        let err = schema.add_field(text_field(2, "Email")).unwrap_err();
        assert!(matches!(err, SchemaError::DuplicateField));
    }

    #[test]
    fn add_field_rejects_duplicate_seqno() {
        let mut schema = TableSchema::new(vec![text_field(1, "email")]);
        let err = schema.add_field(text_field(1, "name")).unwrap_err();
        assert!(matches!(err, SchemaError::SeqnoConflict));
    }

    #[test]
    fn remove_field_removes_by_normalized_name() {
        let mut schema = TableSchema::new(vec![text_field(1, "email"), text_field(2, "name")]);
        schema.remove_field("Email").unwrap();
        assert!(schema.field("email").is_none());
        assert_eq!(schema.fields.len(), 1);
    }

    #[test]
    fn remove_field_returns_error_when_not_found() {
        let mut schema = TableSchema::new(Vec::new());
        let err = schema.remove_field("missing").unwrap_err();
        assert!(matches!(err, SchemaError::FieldNotFound));
    }

    #[test]
    fn update_field_replaces_existing_definition() {
        let mut schema = TableSchema::new(vec![text_field(1, "email")]);
        let updated = FieldDef {
            seqno: 1,
            field_name: "email".to_string(),
            field_type: FieldType::Text,
            nullable: true,
            indexed: true,
            default_value: None,
        };
        schema.update_field(updated.clone()).unwrap();
        assert_eq!(schema.field("email"), Some(&updated));
    }

    #[test]
    fn update_field_returns_error_when_not_found() {
        let mut schema = TableSchema::new(Vec::new());
        let err = schema.update_field(text_field(1, "ghost")).unwrap_err();
        assert!(matches!(err, SchemaError::FieldNotFound));
    }
    
}