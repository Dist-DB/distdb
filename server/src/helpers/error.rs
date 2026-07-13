use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerAppError {
    InvalidConfig(String),
    Runtime(String),
}

impl Display for ServerAppError {

    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {

        match self {

            Self::InvalidConfig(msg) => write!(f, "invalid server config: {msg}"),

            Self::Runtime(msg) => write!(f, "server runtime error: {msg}"),

        }

    }
    
}

impl std::error::Error for ServerAppError {}
