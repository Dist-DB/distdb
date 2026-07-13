
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum PeerServiceType {
    /// Interactive client (console, application driver, etc.)
    #[default]
    Client,
    /// Peer data node in the cluster
    DataNode,
}


#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PeerSession {
    pub service_type: PeerServiceType,
    pub current_database: Option<String>,
    pub auth_token: Option<String>,
    pub session_id: Option<String>,
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

    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    pub fn with_service_type(mut self, service_type: PeerServiceType) -> Self {
        self.service_type = service_type;
        self
    }

    pub fn clear_connection_state(&mut self) {
        self.current_database = None;
        self.auth_token = None;
        self.session_id = None;
        self.user_id = None;
    }

}

#[cfg(test)]
#[path = "peer_session_test.rs"]
mod tests;
