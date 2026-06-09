use crate::core::identity::{PasswordKey, UserId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserCredential {
    pub user_id: UserId,
    pub password_key: PasswordKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleGrant {
    pub user_id: UserId,
    pub database_id: String,
    pub role_name: String,
}