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
