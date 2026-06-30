
use common::schema::{normalize_field_name, validate_field_kind};
use std::collections::HashSet;

use crate::engine::database::field_def::FieldDef;
use crate::engine::database::schema::error::{SchemaError, SchemaResult};

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
#[path = "schema_def_test.rs"]
mod schema_def_test;
