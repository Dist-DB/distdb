use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerLibError {
    InvalidConfig(String),
    InvalidState(String),
    Storage(String),
    Network(String),
}

impl Display for ServerLibError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(msg) => write!(f, "invalid config: {msg}"),
            Self::InvalidState(msg) => write!(f, "invalid state: {msg}"),
            Self::Storage(msg) => write!(f, "storage error: {msg}"),
            Self::Network(msg) => write!(f, "network error: {msg}"),
        }
    }
}

impl std::error::Error for ServerLibError {}

pub type Result<T> = std::result::Result<T, ServerLibError>;