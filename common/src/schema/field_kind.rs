
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldKind {
    Int(u8),
    UInt(u8),
    Float(u8),
    Date,
    DateTime,
    Timestamp,
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
            Self::StringFixed(len) => format!("VARCHAR({len})"),
            Self::Text => "TEXT".to_string(),
            Self::Enum(variants) => {
                let rendered = variants
                    .iter()
                    .map(|variant| format!("'{}'", variant.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("ENUM({rendered})")
            }
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
mod tests {
    use super::FieldKind;

    #[test]
    fn sql_variant_display_name_formats_scalar_types() {
        assert_eq!(FieldKind::Int(8).sql_variant_display_name(), "TINYINT");
        assert_eq!(FieldKind::Int(16).sql_variant_display_name(), "INTEGER");
        assert_eq!(FieldKind::Int(32).sql_variant_display_name(), "INT");
        assert_eq!(FieldKind::Int(64).sql_variant_display_name(), "BIGINT");
        assert_eq!(FieldKind::UInt(8).sql_variant_display_name(), "TINYINT");
        assert_eq!(FieldKind::UInt(16).sql_variant_display_name(), "INTEGER");
        assert_eq!(FieldKind::UInt(64).sql_variant_display_name(), "BIGINT");
        assert_eq!(FieldKind::Float(32).sql_variant_display_name(), "FLOAT32");
        assert_eq!(FieldKind::Date.sql_variant_display_name(), "DATE");
        assert_eq!(FieldKind::DateTime.sql_variant_display_name(), "DATETIME");
        assert_eq!(FieldKind::Timestamp.sql_variant_display_name(), "TIMESTAMP");
        assert_eq!(FieldKind::StringFixed(255).sql_variant_display_name(), "VARCHAR(255)");
        assert_eq!(FieldKind::Text.sql_variant_display_name(), "TEXT");
        assert_eq!(FieldKind::Spatial.sql_variant_display_name(), "SPATIAL");
        assert_eq!(FieldKind::Blob.sql_variant_display_name(), "BLOB");
    }

    #[test]
    fn sql_variant_display_name_formats_enum_variants() {
        let kind = FieldKind::Enum(vec!["draft".to_string(), "pub'lished".to_string()]);
        assert_eq!(
            kind.sql_variant_display_name(),
            "ENUM('draft', 'pub''lished')"
        );
    }

    #[test]
    fn to_sql_string_matches_variant_display_name() {
        let kind = FieldKind::StringFixed(32);
        assert_eq!(kind.to_sql_string(), kind.sql_variant_display_name());
    }
}
