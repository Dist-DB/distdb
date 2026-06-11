
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PeerSession {
    pub current_database: Option<String>,
    pub auth_token: Option<String>,
    pub shared_authorization: Option<String>,
    pub user_id: Option<String>,
}

impl PeerSession {
    
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.current_database = Some(database.into());
        self
    }

    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    pub fn with_shared_authorization(mut self, token: impl Into<String>) -> Self {
        self.shared_authorization = Some(token.into());
        self
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

}
