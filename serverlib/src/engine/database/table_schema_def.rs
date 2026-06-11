
use common::schema::{normalize_field_name, validate_field_kind};
use std::collections::HashSet;

use super::field_def::FieldDef;
use super::schema_error::{SchemaError, SchemaResult};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
    pub fields: Vec<FieldDef>,
}

impl TableSchema {

    pub fn new(fields: Vec<FieldDef>) -> Self {
        Self { fields }
    }

    pub fn field(&self, name: &str) -> Option<&FieldDef> {
        let normalized = name.trim().to_ascii_lowercase();
        self.fields.iter().find(|f| f.field_name == normalized)
    }

    /// Validate schema-level invariants required by row and schema-change
    /// paths: each field must have a valid name/type and unique name/seqno.
    pub fn validate(&self) -> SchemaResult<()> {

        let mut seen_names = HashSet::with_capacity(self.fields.len());
        let mut seen_seqnos = HashSet::with_capacity(self.fields.len());

        for field in &self.fields {

            let normalized_name = normalize_field_name(&field.field_name)
                .map_err(|_| SchemaError::InvalidFieldName)?;

            validate_field_kind(&field.field_type).map_err(|_| SchemaError::InvalidFieldType)?;

            if !seen_names.insert(normalized_name) {
                return Err(SchemaError::DuplicateField);
            }

            if !seen_seqnos.insert(field.seqno) {
                return Err(SchemaError::SeqnoConflict);
            }

        }

        Ok(())

    }

    /// Append a new field to the schema. The field name is normalized to
    /// lower-case; callers may pass any casing.
    pub fn add_field(&mut self, mut field: FieldDef) -> SchemaResult<()> {

        field.field_name = normalize_field_name(&field.field_name)
            .map_err(|_| SchemaError::InvalidFieldName)?;

        validate_field_kind(&field.field_type).map_err(|_| SchemaError::InvalidFieldType)?;

        if self.fields.iter().any(|f| f.field_name == field.field_name) {
            return Err(SchemaError::DuplicateField);
        }

        if self.fields.iter().any(|f| f.seqno == field.seqno) {
            return Err(SchemaError::SeqnoConflict);
        }

        self.fields.push(field);

        Ok(())

    }

    /// Remove a field by name. Returns FieldNotFound when no such field exists.
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
    /// The incoming field_name is normalized and must match a field that
    /// is already in the schema.
    pub fn update_field(&mut self, mut field: FieldDef) -> SchemaResult<()> {

        field.field_name = normalize_field_name(&field.field_name)
            .map_err(|_| SchemaError::InvalidFieldName)?;
        
        validate_field_kind(&field.field_type).map_err(|_| SchemaError::InvalidFieldType)?;
        
        let target_idx = self
            .fields
            .iter()
            .position(|f| f.field_name == field.field_name)
            .ok_or(SchemaError::FieldNotFound)?;

        if self
            .fields
            .iter()
            .enumerate()
            .any(|(idx, f)| idx != target_idx && f.seqno == field.seqno)
        {
            return Err(SchemaError::SeqnoConflict);
        }

        self.fields[target_idx] = field;
        
        Ok(())

    }

}

#[cfg(test)]
mod tests {
    
    use super::*;
    use crate::engine::database::field_types::{FieldIndex, FieldType};

    fn text_field(seqno: u32, name: &str) -> FieldDef {
        FieldDef {
            seqno,
            field_name: name.to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
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
            indexed: FieldIndex::Indexed,
            default_value: None,
            metadata: None,
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

    #[test]
    fn update_field_rejects_seqno_conflict_with_other_field() {

        let mut schema = TableSchema::new(vec![text_field(1, "email"), text_field(2, "name")]);

        let err = schema
            .update_field(FieldDef {
                seqno: 2,
                field_name: "email".to_string(),
                field_type: FieldType::Text,
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            })
            .unwrap_err();

        assert!(matches!(err, SchemaError::SeqnoConflict));

    }

    #[test]
    fn validate_rejects_duplicate_seqno_from_raw_schema() {
        let schema = TableSchema::new(vec![text_field(1, "email"), text_field(1, "name")]);
        let err = schema.validate().unwrap_err();
        assert!(matches!(err, SchemaError::SeqnoConflict));
    }

}
