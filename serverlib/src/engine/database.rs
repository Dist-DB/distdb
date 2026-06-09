
use crate::core::identity::NodeId;
use crate::engine::transaction::TransactionId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseId(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseReplicaState {
    pub database_id: DatabaseId,
    pub local_node_id: NodeId,
    pub last_applied_tx: Option<TransactionId>,
}