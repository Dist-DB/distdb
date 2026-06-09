#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldKind {
    Int(u8),
    UInt(u8),
    Float(u8),
    StringFixed(usize),
    Text,
    Enum(Vec<String>),
    Spatial,
    Blob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaValidationError {
    EmptyFieldName,
    InvalidBitWidth,
    InvalidFixedStringLen,
    EmptyEnumVariants,
    EmptyEnumVariantValue,
}

impl std::fmt::Display for SchemaValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyFieldName => write!(f, "field name must not be empty"),
            Self::InvalidBitWidth => {
                write!(f, "numeric bit width must be one of 8, 16, 32, 64")
            }
            Self::InvalidFixedStringLen => {
                write!(f, "fixed-length string size must be greater than zero")
            }
            Self::EmptyEnumVariants => write!(f, "enum field must define at least one variant"),
            Self::EmptyEnumVariantValue => {
                write!(f, "enum variants must not contain empty values")
            }
        }
    }
}

impl std::error::Error for SchemaValidationError {}

pub fn normalize_field_name(name: &str) -> Result<String, SchemaValidationError> {
    let normalized = name.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(SchemaValidationError::EmptyFieldName);
    }
    Ok(normalized)
}

pub fn validate_field_kind(kind: &FieldKind) -> Result<(), SchemaValidationError> {
    match kind {
        FieldKind::Int(bits) | FieldKind::UInt(bits) | FieldKind::Float(bits) => {
            if !matches!(*bits, 8 | 16 | 32 | 64) {
                return Err(SchemaValidationError::InvalidBitWidth);
            }
        }
        FieldKind::StringFixed(len) => {
            if *len == 0 {
                return Err(SchemaValidationError::InvalidFixedStringLen);
            }
        }
        FieldKind::Enum(variants) => {
            if variants.is_empty() {
                return Err(SchemaValidationError::EmptyEnumVariants);
            }
            if variants.iter().any(|value| value.trim().is_empty()) {
                return Err(SchemaValidationError::EmptyEnumVariantValue);
            }
        }
        FieldKind::Text | FieldKind::Spatial | FieldKind::Blob => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_field_kind_rejects_invalid_bit_width() {
        let err = validate_field_kind(&FieldKind::Int(7)).unwrap_err();
        assert_eq!(err, SchemaValidationError::InvalidBitWidth);
    }

    #[test]
    fn validate_field_kind_rejects_empty_enum() {
        let err = validate_field_kind(&FieldKind::Enum(Vec::new())).unwrap_err();
        assert_eq!(err, SchemaValidationError::EmptyEnumVariants);
    }

    #[test]
    fn normalize_field_name_rejects_empty() {
        let err = normalize_field_name("   ").unwrap_err();
        assert_eq!(err, SchemaValidationError::EmptyFieldName);
    }
}