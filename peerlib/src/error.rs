use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerError {
    InvalidConfig(String),
    InvalidState(String),
    Network(String),
    Codec(String),
    Storage(String),
}

impl Display for PeerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(msg) => write!(f, "invalid config: {msg}"),
            Self::InvalidState(msg) => write!(f, "invalid state: {msg}"),
            Self::Network(msg) => write!(f, "network error: {msg}"),
            Self::Codec(msg) => write!(f, "codec error: {msg}"),
            Self::Storage(msg) => write!(f, "storage error: {msg}"),
        }
    }
}

impl std::error::Error for PeerError {}

pub type Result<T> = std::result::Result<T, PeerError>;

#[cfg(test)]
#[path = "error_test.rs"]
mod tests;
