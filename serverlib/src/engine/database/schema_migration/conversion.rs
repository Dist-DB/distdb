use std::collections::HashMap;

use super::super::core::DatabaseError;
use super::super::row_payload::{decode_row_payload, encode_row_payload};
use super::super::table_schema::TableSchema;
use super::super::table_schema::FieldType;
use super::types::{SchemaMutationRuleSet, TypeConversionPolicy};

pub fn apply_schema_rules_to_payload(
    payload: &[u8],
    rules: &SchemaMutationRuleSet,
    schema: &TableSchema,
) -> Result<Vec<u8>, DatabaseError> {

    let mut row: HashMap<String, Vec<u8>> = match decode_row_payload(schema, payload) {
        Ok(row) => row,
        Err(_) => return Ok(payload.to_vec()),
    };

    for rule in &rules.type_changes {
        let key = common::normalize_identifier!(&rule.field_name);

        if let Some(current) = row.get(&key).cloned() {
            let converted = convert_value_to_field_type(&current, &rule.target_type, rules.conversion_policy)
                .map_err(|_| DatabaseError::SchemaChange(super::super::schema_error::SchemaError::InvalidFieldType))?;
            row.insert(key, converted);
        }
    }

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

    encode_row_payload(schema, &row).map_err(|_| DatabaseError::CatalogSerialize)

}

#[expect(clippy::result_unit_err, reason="the error context is sufficiently conveyed by the variant and additional information would not be helpful for handling the error")]
pub fn convert_value_to_field_type(
    value: &[u8],
    target_type: &FieldType,
    policy: TypeConversionPolicy,
) -> Result<Vec<u8>, ()> {

    match target_type {
        
        FieldType::Int(_) => {
            if let Ok(v) = parse_i128_bytes(value) {
                return Ok(v.to_string().into_bytes());
            }

            match policy {
                TypeConversionPolicy::Force => Ok(b"0".to_vec()),
                TypeConversionPolicy::Safe => Err(()),
            }
        },

        FieldType::UInt(_) => {
            if let Ok(v) = parse_u128_bytes(value) {
                return Ok(v.to_string().into_bytes());
            }

            match policy {
                TypeConversionPolicy::Force => Ok(b"0".to_vec()),
                TypeConversionPolicy::Safe => Err(()),
            }
        },

        FieldType::Float(_) => {
            if let Ok(v) = parse_f64_bytes(value) {
                return Ok(v.to_string().into_bytes());
            }

            match policy {
                TypeConversionPolicy::Force => Ok(b"0.0".to_vec()),
                TypeConversionPolicy::Safe => Err(()),
            }
        },

        FieldType::StringFixed(_)
        | FieldType::Text
        | FieldType::Date
        | FieldType::DateTime
        | FieldType::Timestamp
        | FieldType::Enum(_) => match std::str::from_utf8(value) {
            Ok(valid) => Ok(valid.as_bytes().to_vec()),
            Err(_) => match policy {
                TypeConversionPolicy::Force => Ok(String::from_utf8_lossy(value).into_owned().into_bytes()),
                TypeConversionPolicy::Safe => Err(()),
            },
        },

        FieldType::Blob | FieldType::Spatial => Ok(value.to_vec()),

    }

}

#[expect(clippy::result_unit_err, reason="the error context is sufficiently conveyed by the variant and additional information would not be helpful for handling the error")]
pub fn parse_i128_bytes(value: &[u8]) -> Result<i128, ()> {
    let text = std::str::from_utf8(value).map_err(|_| ())?.trim();
    text.parse::<i128>().map_err(|_| ())
}

#[expect(clippy::result_unit_err, reason="the error context is sufficiently conveyed by the variant and additional information would not be helpful for handling the error")]
pub fn parse_u128_bytes(value: &[u8]) -> Result<u128, ()> {
    let text = std::str::from_utf8(value).map_err(|_| ())?.trim();
    text.parse::<u128>().map_err(|_| ())
}

#[expect(clippy::result_unit_err, reason="the error context is sufficiently conveyed by the variant and additional information would not be helpful for handling the error")]   
pub fn parse_f64_bytes(value: &[u8]) -> Result<f64, ()> {
    let text = std::str::from_utf8(value).map_err(|_| ())?.trim();
    text.parse::<f64>().map_err(|_| ())
}


#[cfg(test)]
#[path = "conversion_test.rs"]
mod tests;
