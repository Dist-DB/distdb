#![allow(dead_code)]

pub mod error;
pub mod interface;

#[cfg(feature = "connector-stack")]
pub mod connector;

#[cfg(feature = "server-p2p")]
pub mod p2p;

#[cfg(feature = "connector-stack")]
pub use connector::{
    ConnectorDiscoveryMode, ConnectorP2pConfig, ConnectorP2pEvent,
    ConnectorP2pHandleOutcome, ConnectorP2pRuntime, ConnectorP2pTransport,
    ConnectorPeer, ConnectorSwarmEventSource, ConnectorTlsConfig,
};

#[cfg(feature = "server-p2p")]
pub use p2p::protocol::{
    AffinityJoinRequest, AffinityJoinResponse, AffinityReplicationAction,
    DataSnapshotRequest, DataSnapshotResponse,
    SchemaCatalogRequest, SchemaCatalogResponse, ServiceAnnounce, ServiceMessage,
    TableLockState, TlsCaDistribution, TlsCertEnrollRequest, TlsCertEnrollResponse,
    TransactionsSinceRequest, TransactionsSinceResponse,
};

#[cfg(feature = "server-p2p")]
pub use p2p::{
    DiscoveryMode,
    KademliaDiscoveryConfig, KademliaDiscoveryService,
    PeerNode, WireAffinityDocument, WireAffinityMember, WireAffinityMemberStatus,
    WireDatabaseSchemaSummary, WireReplicationSecuritySummary, WireTransactionId,
    ServerP2pEvent, ServerP2pHandleOutcome, ServerP2pNetwork, ServerP2pRuntime,
};

pub use error::{PeerError, Result};
pub use interface::{TransferCodec, TransferEnvelope, TransferHeaders};
