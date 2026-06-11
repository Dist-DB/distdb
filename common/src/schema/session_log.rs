
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SessionLogEventType {
    Connect,
    Disconnect,
    Authenticate,
    DatabaseSwitch,
    QueryExecute,
    QueryError,
    SchemaChange,
    Authorization,
    Other,
}

impl std::fmt::Display for SessionLogEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect           => write!(f, "CONNECT"),
            Self::Disconnect        => write!(f, "DISCONNECT"),
            Self::Authenticate      => write!(f, "AUTHENTICATE"),
            Self::DatabaseSwitch    => write!(f, "DATABASE_SWITCH"),
            Self::QueryExecute      => write!(f, "QUERY_EXECUTE"),
            Self::QueryError        => write!(f, "QUERY_ERROR"),
            Self::SchemaChange      => write!(f, "SCHEMA_CHANGE"),
            Self::Authorization     => write!(f, "AUTHORIZATION"),
            Self::Other             => write!(f, "OTHER"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionLogEntry {
    pub timestamp_ms: u64,
    pub event_type: SessionLogEventType,
    pub details: String,
    pub success: bool,
}

impl SessionLogEntry {
    pub fn new(event_type: SessionLogEventType, details: impl Into<String>, success: bool) -> Self {
        Self {
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            event_type,
            details: details.into(),
            success,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SessionLog {
    pub entries: Vec<SessionLogEntry>,
}

impl SessionLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_entry(&mut self, event_type: SessionLogEventType, details: impl Into<String>, success: bool) {
        self.entries.push(SessionLogEntry::new(event_type, details, success));
    }

    pub fn last_entry(&self) -> Option<&SessionLogEntry> {
        self.entries.last()
    }

    pub fn entries_by_type(&self, event_type: SessionLogEventType) -> Vec<&SessionLogEntry> {
        self.entries.iter().filter(|e| e.event_type == event_type).collect()
    }
}
