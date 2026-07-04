use std::collections::BTreeMap;

pub type TransferHeaders = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransferEnvelope {
    pub channel: String,
    pub message_type: String,
    pub request_id: Option<String>,
    pub payload: Vec<u8>,
    #[serde(default)]
    pub headers: TransferHeaders,
}

impl TransferEnvelope {
    pub fn new(
        channel: impl Into<String>,
        message_type: impl Into<String>,
        request_id: Option<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            channel: channel.into(),
            message_type: message_type.into(),
            request_id,
            payload,
            headers: TransferHeaders::new(),
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

pub trait TransferCodec<M> {
    fn encode(&self, message: &M) -> std::result::Result<TransferEnvelope, String>;
    fn decode(&self, envelope: &TransferEnvelope) -> std::result::Result<M, String>;
}
