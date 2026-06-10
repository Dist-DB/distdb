
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
mod tests {
    use super::*;

    #[test]
    fn database_id_is_obscured_from_normalized_name() {
        let id_a = DatabaseId::from_database_name("Sales").expect("valid database name");
        let id_b = DatabaseId::from_database_name("sales").expect("valid database name");

        assert_eq!(id_a, id_b);
        assert_ne!(id_a.0, "sales");
    }
}
