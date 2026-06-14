
use super::core::{DatabaseError, DatabaseResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct DatabaseId(pub String);

impl DatabaseId {
    
    pub fn from_database_name(name: &str) -> DatabaseResult<Self> {
        let normalized = common::normalize_identifier!(name);
        if normalized.is_empty() {
            return Err(DatabaseError::InvalidDatabaseName);
        }
        Ok(Self(common::helpers::stable_id(&[&normalized])))
    }

}


#[cfg(test)]
#[path = "id_test.rs"]
mod tests;
