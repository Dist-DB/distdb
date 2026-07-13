use crate::core::command::ConnectorRequest;
use crate::core::result::{ConnectorResponse, ConnectorResult, ResponseStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectorError {
    Transport(String),
    Rejected(String),
    InvalidResponse(String),
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::Rejected(msg) => write!(f, "command rejected: {msg}"),
            Self::InvalidResponse(msg) => write!(f, "invalid response: {msg}"),
        }
    }
}

impl std::error::Error for ConnectorError {}

pub trait ConnectorTransport {
    fn request(&self, request: &ConnectorRequest) -> Result<ConnectorResponse, ConnectorError>;
}

#[derive(Debug, Clone)]
pub struct ConnectorClient<T: ConnectorTransport> {
    transport: T,
}

impl<T: ConnectorTransport> ConnectorClient<T> {

    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn execute(&self, request: &ConnectorRequest) -> Result<ConnectorResponse, ConnectorError> {

        let response = self.transport.request(request)?;

        if response.request_id != request.request_id {
            return Err(ConnectorError::InvalidResponse(
                "response request_id mismatch".to_string(),
            ));
        }

        if response.status == ResponseStatus::Rejected {
            if let ConnectorResult::Error(message) = &response.result {
                return Err(ConnectorError::Rejected(message.clone()));
            }
            return Err(ConnectorError::InvalidResponse(
                "rejected response missing error message".to_string(),
            ));
        }

        Ok(response)

    }
    
}

#[cfg(test)]
#[path = "client_test.rs"]
mod tests;
