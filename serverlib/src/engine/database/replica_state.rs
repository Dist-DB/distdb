
use crate::core::identity::NodeId;

use super::id::DatabaseId;
use super::transaction::TransactionId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseReplicaState {
    pub database_id: DatabaseId,
    pub local_node_id: NodeId,
    pub last_applied_tx: Option<TransactionId>,
}
