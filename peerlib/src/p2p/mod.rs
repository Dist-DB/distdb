pub mod discovery;
pub mod network;
pub mod protocol;
pub mod pubsub;
pub mod runtime;
pub mod transport;
pub mod types;

pub use discovery::{
	DiscoveryMode, 
	KademliaDiscoveryConfig, 
	KademliaDiscoveryService
};

pub use network::ServerP2pNetwork;
pub use protocol::ServiceMessage;
pub use types::{
	PeerNode, WireAffinityDocument, WireAffinityMember, WireAffinityMemberStatus,
	WireDatabaseSchemaSummary, WireReplicationSecuritySummary, WireTransactionId,
};
pub use runtime::{
	ServerP2pEvent, 
	ServerP2pHandleOutcome, 
	ServerP2pRuntime,
	ServerSwarmEventSource,
};
