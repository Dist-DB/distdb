
use common::helpers::stable_id;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct UserId(pub String);

impl UserId {

    pub fn from_username(username: &str) -> Self {
        let normalized = username.trim().to_ascii_lowercase();
        Self(stable_id(&[&normalized]))
    }

}