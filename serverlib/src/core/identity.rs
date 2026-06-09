use crate::helpers::hash::stable_id;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PasswordKey(pub String);

impl UserId {
    pub fn from_username(username: &str) -> Self {
        let normalized = username.trim().to_ascii_lowercase();
        Self(stable_id(&[&normalized]))
    }
}

impl PasswordKey {
    pub fn from_database_user_password(database_id: &str, username: &str, password: &str) -> Self {
        let normalized = username.trim().to_ascii_lowercase();
        Self(stable_id(&[database_id, &normalized, password]))
    }
}