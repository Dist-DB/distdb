use crate::core::cluster::NodeDescriptor;
use crate::engine::replication::PublicationEvent;
use crate::engine::transaction::TransactionId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceMessage {
    NodeAnnounce(NodeDescriptor),
    Publication {
        subscription_key: String,
        event: PublicationEvent,
    },
    TransactionsSince {
        database_id: String,
        from: Option<TransactionId>,
    },
}