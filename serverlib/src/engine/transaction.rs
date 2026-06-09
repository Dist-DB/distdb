use crate::core::identity::UserId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransactionId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionKind {
    Insert,
    Update,
    Delete,
    SchemaChange,
    SecurityChange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionRecord {
    pub table_id: String,
    pub id: TransactionId,
    pub timestamp_epoch_ms: u64,
    pub actor: UserId,
    pub kind: TransactionKind,
    pub payload: Vec<u8>,
}

pub trait TransactionLog {
    fn append(&self, record: TransactionRecord) -> Result<(), &'static str>;
    // When `from` is provided, return records after that transaction id (exclusive).
    // When `from` is None, return all records for the table.
    fn since(&self, table_id: &str, from: Option<TransactionId>) -> Vec<TransactionRecord>;
}