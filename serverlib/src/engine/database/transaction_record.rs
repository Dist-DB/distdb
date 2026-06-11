
use crate::core::identity::UserId;

use super::transaction_id::TransactionId;
use super::transaction_kind::TransactionKind;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionRecord {
    pub id: TransactionId,
    pub refid: Option<TransactionId>,
    pub timestamp_epoch_ms: u64,
    pub actor: UserId,
    pub kind: TransactionKind,
    pub payload: Vec<u8>,
}
