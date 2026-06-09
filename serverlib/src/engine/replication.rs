
use crate::engine::database::DatabaseId;
use crate::engine::transaction::TransactionId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    DataChanged,
    SchemaChanged,
    SecurityChanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationEvent {
    pub timestamp_epoch_ms: u64,
    pub service_id: String,
    pub transaction_id: TransactionId,
    pub event_type: EventType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionKey {
    pub database_id: DatabaseId,
    pub table_id: String,
}

impl SubscriptionKey {
    pub fn as_wire_key(&self) -> String {
        format!("{}:{}", self.database_id.0, self.table_id)
    }
}

pub trait ReplicationBus {
    fn publish(&mut self, key: &SubscriptionKey, event: PublicationEvent);
}