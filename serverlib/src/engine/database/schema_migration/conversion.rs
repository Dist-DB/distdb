use std::collections::HashMap;

use super::super::core::DatabaseError;
use super::super::table_schema::FieldType;
use super::types::{SchemaMutationRuleSet, TypeConversionPolicy};

pub fn apply_schema_rules_to_payload(
    payload: &[u8],
    rules: &SchemaMutationRuleSet,
) -> Result<Vec<u8>, DatabaseError> {

    let mut row = match bincode::deserialize::<HashMap<String, Vec<u8>>>(payload) {
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

    bincode::serialize(&row).map_err(|_| DatabaseError::CatalogSerialize)

}

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

pub fn parse_i128_bytes(value: &[u8]) -> Result<i128, ()> {
    let text = std::str::from_utf8(value).map_err(|_| ())?.trim();
    text.parse::<i128>().map_err(|_| ())
}

pub fn parse_u128_bytes(value: &[u8]) -> Result<u128, ()> {
    let text = std::str::from_utf8(value).map_err(|_| ())?.trim();
    text.parse::<u128>().map_err(|_| ())
}

pub fn parse_f64_bytes(value: &[u8]) -> Result<f64, ()> {
    let text = std::str::from_utf8(value).map_err(|_| ())?.trim();
    text.parse::<f64>().map_err(|_| ())
}

#[cfg(test)]
mod tests {
    
    use super::*;

    #[test]
    fn convert_numeric_string_to_int() {
        let result = convert_value_to_field_type(b"42", &FieldType::Int(64), TypeConversionPolicy::Safe);
        assert_eq!(result, Ok(b"42".to_vec()));
    }

    #[test]
    fn convert_invalid_to_int_safe_mode_fails() {
        let result = convert_value_to_field_type(b"not-a-number", &FieldType::Int(32), TypeConversionPolicy::Safe);
        assert_eq!(result, Err(()));
    }

    #[test]
    fn convert_invalid_to_int_force_mode_coerces() {
        let result = convert_value_to_field_type(b"not-a-number", &FieldType::Int(32), TypeConversionPolicy::Force);
        assert_eq!(result, Ok(b"0".to_vec()));
    }

    #[test]
    fn convert_text_preserves_valid_utf8() {
        let result = convert_value_to_field_type(b"hello", &FieldType::Text, TypeConversionPolicy::Safe);
        assert_eq!(result, Ok(b"hello".to_vec()));
    }

}
