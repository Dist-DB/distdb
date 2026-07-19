
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldKind {
    Int(u8),
    UInt(u8),
    Float(u8),
    Date,
    DateTime,
    Timestamp,
    // Database UUID datatype stored as fixed-width 16-byte binary.
    Uuid,
    StringFixed(usize),
    Text,
    Enum(Vec<String>),
    Spatial,
    Blob,
}

impl FieldKind {
    
    pub fn sql_variant_display_name(&self) -> String {
        
        match self {

            Self::Int(bits) => int_sql_display_name(*bits).to_string(),

            Self::UInt(bits) => uint_sql_display_name(*bits).to_string(),

            Self::Float(bits) => format!("FLOAT{bits}"),

            Self::Date => "DATE".to_string(),

            Self::DateTime => "DATETIME".to_string(),

            Self::Timestamp => "TIMESTAMP".to_string(),

            Self::Uuid => "UUID".to_string(),

            Self::StringFixed(len) => format!("VARCHAR({len})"),

            Self::Text => "TEXT".to_string(),

            Self::Enum(variants) => {
                let rendered = variants
                    .iter()
                    .map(|variant| format!("'{}'", variant.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("ENUM({rendered})")
            },

            Self::Spatial => "SPATIAL".to_string(),

            Self::Blob => "BLOB".to_string(),

        }

    }

    pub fn to_sql_string(&self) -> String {
        self.sql_variant_display_name()
    }

}

fn int_sql_display_name(bits: u8) -> &'static str {
    
    match bits {
        64 => "BIGINT",
        16 => "INTEGER",
        8 => "TINYINT",
        32 => "INT",
        _ => "INT",
    }

}

fn uint_sql_display_name(bits: u8) -> &'static str {

    match bits {
        64 => "BIGINT",
        16 => "INTEGER",
        8 => "TINYINT",
        32 => "INT",
        _ => "INT",
    }
    
}

#[cfg(test)]
#[path = "field_kind_test.rs"]
mod tests;
