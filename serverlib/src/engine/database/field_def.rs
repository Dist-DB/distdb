
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
#[path = "field_def_test.rs"]
mod tests;
