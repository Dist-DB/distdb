use crate::core::identity::UserId;
use crate::engine::table_schema::TableSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct TransactionId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TransactionKind {
    Insert,
    Update,
    Delete,
    SchemaChange,
    SecurityChange,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionRecord {
    pub id: TransactionId,
    pub refid: Option<TransactionId>,
    pub timestamp_epoch_ms: u64,
    pub actor: UserId,
    pub kind: TransactionKind,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaChangePayload {
    pub table_id: String,
    pub schema_revision: u64,
    pub schema: TableSchema,
}

impl SchemaChangePayload {

    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        bincode::serialize(self).map_err(|_| "failed to serialize schema change payload")
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        bincode::deserialize(payload).map_err(|_| "failed to deserialize schema change payload")
    }

}

pub trait TransactionLog {

    fn append(&self, wal_id: &str, record: TransactionRecord) -> Result<(), &'static str>;
    // When `from` is provided, return records after that transaction id (exclusive).
    // When `from` is None, return all records for the WAL stream.
    fn since(&self, wal_id: &str, from: Option<TransactionId>) -> Vec<TransactionRecord>;
    
}