use connector::ConnectorError as WireError;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "message", rename_all = "snake_case")]
pub enum ClientError {
    Config(String),
    Transport(String),
    Protocol(String),
    Decode(String),
    Runtime(String),
}

impl fmt::Display for ClientError {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "config error: {msg}"),
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::Decode(msg) => write!(f, "decode error: {msg}"),
            Self::Runtime(msg) => write!(f, "runtime error: {msg}"),
        }
    }

}

impl std::error::Error for ClientError {}

impl From<WireError> for ClientError {

    fn from(value: WireError) -> Self {
        
        match value {
            
            WireError::Transport(msg) => Self::Transport(msg),

            WireError::Rejected(msg) => Self::Protocol(msg),

            WireError::InvalidResponse(msg) => Self::Protocol(msg),

        }

    }

}

#[cfg(test)]
#[path = "error_test.rs"]
mod tests;
