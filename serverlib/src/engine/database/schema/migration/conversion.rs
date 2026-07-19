use std::collections::HashMap;
use common::Uuid;

use crate::engine::database::core::DatabaseError;
use crate::engine::database::row_payload::{decode_row_payload, encode_row_payload};
use crate::engine::database::table::schema::TableSchema;
use crate::engine::database::table::schema::FieldType;
use super::types::{SchemaMutationRuleSet, TypeConversionPolicy};

const STORED_I8_TAG: u8 = 0x01;
const STORED_I16_TAG: u8 = 0x02;
const STORED_I32_TAG: u8 = 0x03;
const STORED_I64_TAG: u8 = 0x04;
const STORED_I128_TAG: u8 = 0x05;
const STORED_U8_TAG: u8 = 0x11;
const STORED_U16_TAG: u8 = 0x12;
const STORED_U32_TAG: u8 = 0x13;
const STORED_U64_TAG: u8 = 0x14;
const STORED_U128_TAG: u8 = 0x15;
const STORED_F32_TAG: u8 = 0x21;
const STORED_F64_TAG: u8 = 0x22;
const STORED_UUID_TAG: u8 = 0x31;

#[derive(Debug, Clone, Copy, PartialEq)]
enum StoredNumericValue {
    Signed(i128),
    Unsigned(u128),
    Float(f64),
}

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
                .map_err(|_| DatabaseError::SchemaChange(crate::engine::database::schema::error::SchemaError::InvalidFieldType))?;
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

            if let Some(v) = decode_numeric_value(value).and_then(numeric_as_i128) {
                return encode_signed_numeric(target_type.clone(), v);
            }

            match policy {
                TypeConversionPolicy::Force => encode_signed_numeric(target_type.clone(), 0),
                TypeConversionPolicy::Safe => Err(()),
            }

        },

        FieldType::UInt(_) => {

            if let Some(v) = decode_numeric_value(value).and_then(numeric_as_u128) {
                return encode_unsigned_numeric(target_type.clone(), v);
            }

            match policy {
                TypeConversionPolicy::Force => encode_unsigned_numeric(target_type.clone(), 0),
                TypeConversionPolicy::Safe => Err(()),
            }

        },

        FieldType::Float(_) => {

            if let Some(v) = decode_numeric_value(value).map(numeric_as_f64) {
                return encode_float_numeric(target_type.clone(), v);
            }

            match policy {
                TypeConversionPolicy::Force => encode_float_numeric(target_type.clone(), 0.0),
                TypeConversionPolicy::Safe => Err(()),
            }

        },

        FieldType::StringFixed(_) |
        FieldType::Text |
        FieldType::Date |
        FieldType::DateTime |
        FieldType::Timestamp |
        FieldType::Enum(_) => match std::str::from_utf8(&render_stored_field_value(value)) {

            Ok(valid) => Ok(valid.as_bytes().to_vec()),

            Err(_) => match policy {
                TypeConversionPolicy::Force => Ok(String::from_utf8_lossy(&render_stored_field_value(value)).into_owned().into_bytes()),
                TypeConversionPolicy::Safe => Err(()),
            },

        },

        FieldType::Blob | FieldType::Spatial => Ok(value.to_vec()),

        FieldType::Uuid => {
            if let Some(uuid_bytes) = decode_db_uuid_binary_for_conversion(value) {
                return Ok(tagged_bytes(STORED_UUID_TAG, &uuid_bytes));
            }

            let rendered = render_stored_field_value(value);
            if let Ok(as_text) = std::str::from_utf8(&rendered) {
                if let Ok(uuid) = Uuid::parse_str(as_text.trim()) {
                    return Ok(tagged_bytes(STORED_UUID_TAG, uuid.as_bytes()));
                }
            }

            match policy {
                TypeConversionPolicy::Force => Ok(tagged_bytes(STORED_UUID_TAG, Uuid::nil().as_bytes())),
                TypeConversionPolicy::Safe => Err(()),
            }
        },

    }

}

pub fn render_stored_field_value(value: &[u8]) -> Vec<u8> {

    if let Some(uuid_bytes) = decode_tagged_db_uuid_binary(value) {
        return Uuid::from_bytes(uuid_bytes).to_string().into_bytes();
    }
    
    match decode_numeric_value(value) {
        
        Some(StoredNumericValue::Signed(v)) => v.to_string().into_bytes(),

        Some(StoredNumericValue::Unsigned(v)) => v.to_string().into_bytes(),

        Some(StoredNumericValue::Float(v)) => v.to_string().into_bytes(),

        None => value.to_vec(),

    }

}

pub fn display_stored_field_value(value: &[u8]) -> String {
    String::from_utf8_lossy(&render_stored_field_value(value)).into_owned()
}

pub fn compare_stored_field_values(left: &[u8], right: &[u8]) -> std::cmp::Ordering {

    if let (Some(left_uuid), Some(right_uuid)) = (
        decode_db_uuid_binary_for_compare(left),
        decode_db_uuid_binary_for_compare(right),
    ) {
        return left_uuid.cmp(&right_uuid);
    }

    let left_numeric = decode_numeric_value(left);
    let right_numeric = decode_numeric_value(right);

    match (left_numeric, right_numeric) {

        (Some(StoredNumericValue::Signed(lhs)), Some(StoredNumericValue::Signed(rhs))) => lhs.cmp(&rhs),
        
        (Some(StoredNumericValue::Unsigned(lhs)), Some(StoredNumericValue::Unsigned(rhs))) => lhs.cmp(&rhs),

        (Some(lhs), Some(rhs)) => compare_mixed_numeric_values(lhs, rhs),
        
        _ => {
            let left_rendered = render_stored_field_value(left);
            let right_rendered = render_stored_field_value(right);
            left_rendered.cmp(&right_rendered)
        }
        
    }

}

fn decode_tagged_db_uuid_binary(value: &[u8]) -> Option<[u8; 16]> {

    match value {
        [STORED_UUID_TAG, bytes @ ..] if bytes.len() == 16 => bytes.try_into().ok(),
        _ => None,
    }

}

fn decode_db_uuid_binary_for_conversion(value: &[u8]) -> Option<[u8; 16]> {

    if let Some(existing) = decode_tagged_db_uuid_binary(value) {
        return Some(existing);
    }

    if value.len() == 16 {
        return value.try_into().ok();
    }

    None

}

fn decode_db_uuid_binary_for_compare(value: &[u8]) -> Option<[u8; 16]> {

    if let Some(existing) = decode_tagged_db_uuid_binary(value) {
        return Some(existing);
    }

    if value.len() == 16 {
        return value.try_into().ok();
    }

    let text = std::str::from_utf8(value).ok()?.trim();
    let parsed = Uuid::parse_str(text).ok()?;
    Some(*parsed.as_bytes())

}

fn compare_mixed_numeric_values(
    left: StoredNumericValue,
    right: StoredNumericValue,
) -> std::cmp::Ordering {

    match (left, right) {

        (StoredNumericValue::Signed(lhs), StoredNumericValue::Unsigned(rhs)) => {
            if lhs < 0 {
                std::cmp::Ordering::Less
            } else {
                (lhs as u128).cmp(&rhs)
            }
        },

        (StoredNumericValue::Unsigned(lhs), StoredNumericValue::Signed(rhs)) => {
            if rhs < 0 {
                std::cmp::Ordering::Greater
            } else {
                lhs.cmp(&(rhs as u128))
            }
        },

        (lhs, rhs) => {
            let lhs = numeric_as_f64(lhs);
            let rhs = numeric_as_f64(rhs);
            lhs.partial_cmp(&rhs).unwrap_or(std::cmp::Ordering::Equal)
        }

    }

}

fn encode_signed_numeric(target_type: FieldType, value: i128) -> Result<Vec<u8>, ()> {

    match target_type {
        FieldType::Int(8) => i8::try_from(value).map(|v| vec![STORED_I8_TAG, v as u8]).map_err(|_| ()),
        FieldType::Int(16) => i16::try_from(value).map(|v| tagged_bytes(STORED_I16_TAG, &v.to_le_bytes())).map_err(|_| ()),
        FieldType::Int(32) => i32::try_from(value).map(|v| tagged_bytes(STORED_I32_TAG, &v.to_le_bytes())).map_err(|_| ()),
        FieldType::Int(64) => i64::try_from(value).map(|v| tagged_bytes(STORED_I64_TAG, &v.to_le_bytes())).map_err(|_| ()),
        FieldType::Int(_) => Ok(tagged_bytes(STORED_I128_TAG, &value.to_le_bytes())),
        _ => Err(()),
    }

}

fn encode_unsigned_numeric(target_type: FieldType, value: u128) -> Result<Vec<u8>, ()> {

    match target_type {
        FieldType::UInt(8) => u8::try_from(value).map(|v| vec![STORED_U8_TAG, v]).map_err(|_| ()),
        FieldType::UInt(16) => u16::try_from(value).map(|v| tagged_bytes(STORED_U16_TAG, &v.to_le_bytes())).map_err(|_| ()),
        FieldType::UInt(32) => u32::try_from(value).map(|v| tagged_bytes(STORED_U32_TAG, &v.to_le_bytes())).map_err(|_| ()),
        FieldType::UInt(64) => u64::try_from(value).map(|v| tagged_bytes(STORED_U64_TAG, &v.to_le_bytes())).map_err(|_| ()),
        FieldType::UInt(_) => Ok(tagged_bytes(STORED_U128_TAG, &value.to_le_bytes())),
        _ => Err(()),
    }

}

fn encode_float_numeric(target_type: FieldType, value: f64) -> Result<Vec<u8>, ()> {
    
    match target_type {
        FieldType::Float(bits) if bits <= 32 => Ok(tagged_bytes(STORED_F32_TAG, &(value as f32).to_le_bytes())),
        FieldType::Float(_) => Ok(tagged_bytes(STORED_F64_TAG, &value.to_le_bytes())),
        _ => Err(()),
    }

}

fn tagged_bytes(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 1);
    out.push(tag);
    out.extend_from_slice(bytes);
    out
}

fn decode_numeric_value(value: &[u8]) -> Option<StoredNumericValue> {

    match value {
        [STORED_I8_TAG, byte] => Some(StoredNumericValue::Signed(i8::from_le_bytes([*byte]) as i128)),
        [STORED_I16_TAG, bytes @ ..] if bytes.len() == 2 => Some(StoredNumericValue::Signed(i16::from_le_bytes(bytes.try_into().ok()?) as i128)),
        [STORED_I32_TAG, bytes @ ..] if bytes.len() == 4 => Some(StoredNumericValue::Signed(i32::from_le_bytes(bytes.try_into().ok()?) as i128)),
        [STORED_I64_TAG, bytes @ ..] if bytes.len() == 8 => Some(StoredNumericValue::Signed(i64::from_le_bytes(bytes.try_into().ok()?) as i128)),
        [STORED_I128_TAG, bytes @ ..] if bytes.len() == 16 => Some(StoredNumericValue::Signed(i128::from_le_bytes(bytes.try_into().ok()?))),
        [STORED_U8_TAG, byte] => Some(StoredNumericValue::Unsigned(*byte as u128)),
        [STORED_U16_TAG, bytes @ ..] if bytes.len() == 2 => Some(StoredNumericValue::Unsigned(u16::from_le_bytes(bytes.try_into().ok()?) as u128)),
        [STORED_U32_TAG, bytes @ ..] if bytes.len() == 4 => Some(StoredNumericValue::Unsigned(u32::from_le_bytes(bytes.try_into().ok()?) as u128)),
        [STORED_U64_TAG, bytes @ ..] if bytes.len() == 8 => Some(StoredNumericValue::Unsigned(u64::from_le_bytes(bytes.try_into().ok()?) as u128)),
        [STORED_U128_TAG, bytes @ ..] if bytes.len() == 16 => Some(StoredNumericValue::Unsigned(u128::from_le_bytes(bytes.try_into().ok()?))),
        [STORED_F32_TAG, bytes @ ..] if bytes.len() == 4 => Some(StoredNumericValue::Float(f32::from_le_bytes(bytes.try_into().ok()?) as f64)),
        [STORED_F64_TAG, bytes @ ..] if bytes.len() == 8 => Some(StoredNumericValue::Float(f64::from_le_bytes(bytes.try_into().ok()?))),
        _ => decode_legacy_numeric_value(value),
    }

}

fn decode_legacy_numeric_value(value: &[u8]) -> Option<StoredNumericValue> {

    if let Ok(v) = parse_i128_bytes(value) {
        return Some(StoredNumericValue::Signed(v));
    }

    if let Ok(v) = parse_u128_bytes(value) {
        return Some(StoredNumericValue::Unsigned(v));
    }

    if let Ok(v) = parse_f64_bytes(value) {
        return Some(StoredNumericValue::Float(v));
    }

    None
}

fn numeric_as_i128(value: StoredNumericValue) -> Option<i128> {

    match value {
        StoredNumericValue::Signed(v) => Some(v),
        StoredNumericValue::Unsigned(v) => i128::try_from(v).ok(),
        StoredNumericValue::Float(v) => {
            if v.fract() == 0.0 && v.is_finite() {
                Some(v as i128)
            } else {
                None
            }
        }
    }

}

fn numeric_as_u128(value: StoredNumericValue) -> Option<u128> {

    match value {
        StoredNumericValue::Signed(v) if v >= 0 => Some(v as u128),
        StoredNumericValue::Signed(_) => None,
        StoredNumericValue::Unsigned(v) => Some(v),
        StoredNumericValue::Float(v) => {
            if v.fract() == 0.0 && v.is_finite() && v >= 0.0 {
                Some(v as u128)
            } else {
                None
            }
        }
    }

}

fn numeric_as_f64(value: StoredNumericValue) -> f64 {

    match value {
        StoredNumericValue::Signed(v) => v as f64,
        StoredNumericValue::Unsigned(v) => v as f64,
        StoredNumericValue::Float(v) => v,
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
