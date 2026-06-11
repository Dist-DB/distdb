
use super::field_types::{FieldIndex, FieldType};
use common::schema::FieldMetadata;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldDef {
    pub seqno: u32,
    pub field_name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub indexed: FieldIndex,
    pub default_value: Option<Vec<u8>>,
    #[serde(default)]
    pub metadata: Option<FieldMetadata>,
}


impl FieldDef {

    pub fn to_sql_string(&self) -> String {
        
        let mut sql = format!("{} {}", self.field_name, self.sql_type_declaration());

        if let Some(signedness) = self.sql_signedness_modifier() {
            sql.push(' ');
            sql.push_str(signedness);
        }
        
        if !self.nullable {
            sql.push_str(" NOT NULL");
        }

        if let Some(comment) = self.sql_comment() {
            sql.push_str(&format!(" COMMENT '{}'", comment.replace('\'', "''")));
        }

        if let Some(character_set) = self.sql_character_set() {
            sql.push_str(" CHARACTER SET ");
            sql.push_str(character_set);
        }

        if let Some(collation) = self.sql_collation() {
            sql.push_str(" COLLATE ");
            sql.push_str(collation);
        }
        
        if let Some(default) = &self.default_value {
            sql.push_str(&format!(" DEFAULT '{}'", String::from_utf8_lossy(default)));
        }

        if self.is_auto_incrementing() {
            sql.push_str(" AUTO_INCREMENT");
        }
        
        sql

    }

    fn sql_signedness_modifier(&self) -> Option<&'static str> {

        if self
            .sql_type_declaration()
            .split_whitespace()
            .any(|segment| segment.eq_ignore_ascii_case("signed") || segment.eq_ignore_ascii_case("unsigned"))
        {
            return None;
        }

        match self.field_type {
            FieldType::Int(_) => Some("SIGNED"),
            FieldType::UInt(_) => Some("UNSIGNED"),
            _ => None,
        }
        
    }

    fn sql_type_declaration(&self) -> String {
        self.metadata
            .as_ref()
            .and_then(|metadata| metadata.original_sql_type.as_ref())
            .cloned()
            .unwrap_or_else(|| self.field_type.to_sql_string())
    }

    fn sql_comment(&self) -> Option<&str> {
        self.metadata.as_ref()?.comment.as_deref()
    }

    fn sql_collation(&self) -> Option<&str> {
        self.metadata.as_ref()?.collation.as_deref()
    }

    fn sql_character_set(&self) -> Option<&str> {
        self.metadata.as_ref()?.character_set.as_deref()
    }

    fn is_auto_incrementing(&self) -> bool {
        self.metadata
            .as_ref()
            .map(|metadata| metadata.auto_increment)
            .unwrap_or(false)
    }

}

#[cfg(test)]
mod tests {
    
    use super::*;

    fn field_with_type(field_type: FieldType) -> FieldDef {
        FieldDef {
            seqno: 1,
            field_name: "value".to_string(),
            field_type,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        }
    }

    #[test]
    fn to_sql_string_marks_signed_integer_fields() {
        assert_eq!(field_with_type(FieldType::Int(64)).to_sql_string(), "value BIGINT SIGNED NOT NULL");
    }

    #[test]
    fn to_sql_string_marks_unsigned_integer_fields() {
        assert_eq!(field_with_type(FieldType::UInt(8)).to_sql_string(), "value TINYINT UNSIGNED NOT NULL");
    }

    #[test]
    fn to_sql_string_skips_signedness_for_non_integer_fields() {
        assert_eq!(field_with_type(FieldType::Float(32)).to_sql_string(), "value FLOAT32 NOT NULL");
    }

    #[test]
    fn to_sql_string_includes_collation_from_metadata() {
        let mut field = field_with_type(FieldType::StringFixed(32));
        field.metadata = Some(FieldMetadata {
            original_sql_type: Some("LONGTEXT".to_string()),
            collation: Some("utf8mb4_general_ci".to_string()),
            ..FieldMetadata::default()
        });

        assert_eq!(
            field.to_sql_string(),
            "value LONGTEXT NOT NULL COLLATE utf8mb4_general_ci"
        );
    }

    #[test]
    fn to_sql_string_includes_character_set_from_metadata() {
        let mut field = field_with_type(FieldType::StringFixed(32));
        field.metadata = Some(FieldMetadata {
            original_sql_type: Some("TEXT".to_string()),
            character_set: Some("utf8mb4".to_string()),
            ..FieldMetadata::default()
        });

        assert_eq!(
            field.to_sql_string(),
            "value TEXT NOT NULL CHARACTER SET utf8mb4"
        );
    }

    #[test]
    fn to_sql_string_includes_character_set_and_collation_together() {
        let mut field = field_with_type(FieldType::StringFixed(32));
        field.metadata = Some(FieldMetadata {
            original_sql_type: Some("LONGTEXT".to_string()),
            character_set: Some("utf8mb4".to_string()),
            collation: Some("utf8mb4_bin".to_string()),
            ..FieldMetadata::default()
        });

        assert_eq!(
            field.to_sql_string(),
            "value LONGTEXT NOT NULL CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
        );
    }

    #[test]
    fn to_sql_string_includes_auto_increment_from_metadata() {
        let mut field = field_with_type(FieldType::UInt(64));
        field.metadata = Some(FieldMetadata {
            original_sql_type: Some("BIGINT UNSIGNED".to_string()),
            auto_increment: true,
            ..FieldMetadata::default()
        });

        assert_eq!(
            field.to_sql_string(),
            "value BIGINT UNSIGNED NOT NULL AUTO_INCREMENT"
        );
    }

    #[test]
    fn to_sql_string_includes_collation_and_auto_increment_together() {
        let mut field = field_with_type(FieldType::UInt(64));
        field.metadata = Some(FieldMetadata {
            original_sql_type: Some("BIGINT UNSIGNED".to_string()),
            auto_increment: true,
            collation: Some("utf8mb4_bin".to_string()),
            ..FieldMetadata::default()
        });

        assert_eq!(
            field.to_sql_string(),
            "value BIGINT UNSIGNED NOT NULL COLLATE utf8mb4_bin AUTO_INCREMENT"
        );
    }

    #[test]
    fn to_sql_string_uses_original_sql_type_for_temporal_fields() {
        let mut field = field_with_type(FieldType::DateTime);
        field.metadata = Some(FieldMetadata {
            original_sql_type: Some("DATETIME".to_string()),
            ..FieldMetadata::default()
        });

        assert_eq!(field.to_sql_string(), "value DATETIME NOT NULL");
    }
    
}