
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PeerServiceType {
    /// Interactive client (console, application driver, etc.)
    Client,
    /// Peer data node in the cluster
    DataNode,
}

impl Default for PeerServiceType {
    fn default() -> Self {
        Self::Client
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PeerSession {
    pub service_type: PeerServiceType,
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

    pub fn with_service_type(mut self, service_type: PeerServiceType) -> Self {
        self.service_type = service_type;
        self
    }

    pub fn clear_connection_state(&mut self) {
        self.current_database = None;
        self.auth_token = None;
        self.shared_authorization = None;
        self.user_id = None;
    }

}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn default_service_type_is_client() {
        assert_eq!(PeerSession::new().service_type, PeerServiceType::Client);
    }

    #[test]
    fn with_service_type_sets_data_node() {
        let session = PeerSession::new().with_service_type(PeerServiceType::DataNode);
        assert_eq!(session.service_type, PeerServiceType::DataNode);
    }

    #[test]
    fn service_type_is_copy() {
        let t = PeerServiceType::Client;
        let copy = t;
        assert_eq!(t, copy);
    }

    #[test]
    fn clear_connection_state_resets_connection_fields() {
        let mut session = PeerSession::new()
            .with_service_type(PeerServiceType::DataNode)
            .with_database("main")
            .with_auth_token("token")
            .with_shared_authorization("shared")
            .with_user_id("root");

        session.clear_connection_state();

        assert_eq!(session.service_type, PeerServiceType::DataNode);
        assert_eq!(session.current_database, None);
        assert_eq!(session.auth_token, None);
        assert_eq!(session.shared_authorization, None);
        assert_eq!(session.user_id, None);
    }

}
