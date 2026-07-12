/*

	This file is part of DistDB.

	DistDB is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	DistDB is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  
	See the GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with DistDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>.
	
	This library provides peer-to-peer networking and coordination for DistDB, 
	including connection management, message routing, and network services. 

	This library is distributed under the GNU Affero General Public License v3.0. 
    See the LICENSE file in the project root for more information.

	Written in 2026 by Sam Colak <sam@samcolak.com>
	For information on the author and contributors, see the DistDB 
	website (www.distdb.com) or the GitHub repository (www.github.com/dist-db).

    Copyright (c) 2026 Sam Colak. All rights reserved.

*/

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
