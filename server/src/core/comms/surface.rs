use std::collections::BTreeSet;

use connector::{ConnectorRequest, ConnectorResponse};
use peerlib::ServiceMessage;

use crate::core::comms::p2p_wire::decode_service_message;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConnectorSurfaceCapability {
    PersistentConnection,
    BidirectionalMessaging,
    ChallengeHandshake,
    SessionStickiness,
    BinaryBincodeFrames,
    RequestResponseCorrelation,
    ControlledTeardown,
    ServiceControlPlane,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorSurfaceRoute {
    RustP2p,
    RustWss,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorSurfaceDescriptor {
    pub provider_name: &'static str,
    pub route: ConnectorSurfaceRoute,
    pub protocol_version: &'static str,
    pub required_capabilities: BTreeSet<ConnectorSurfaceCapability>,
    pub optional_capabilities: BTreeSet<ConnectorSurfaceCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorSurfaceContract {
    pub protocol_version: &'static str,
    pub required_capabilities: BTreeSet<ConnectorSurfaceCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectorSurfaceContractError {
    ProtocolVersionMismatch {
        provider: &'static str,
        expected: &'static str,
        actual: &'static str,
    },
    MissingCapabilities {
        provider: &'static str,
        missing: BTreeSet<ConnectorSurfaceCapability>,
    },
}

impl std::fmt::Display for ConnectorSurfaceContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProtocolVersionMismatch {
                provider,
                expected,
                actual,
            } => write!(
                f,
                "surface contract version mismatch for provider='{}': expected='{}' actual='{}'",
                provider, expected, actual
            ),
            Self::MissingCapabilities { provider, missing } => write!(
                f,
                "surface contract capability mismatch for provider='{}': missing={:?}",
                provider, missing
            ),
        }
    }
}

impl std::error::Error for ConnectorSurfaceContractError {}

pub trait ConnectorSurfaceProvider {
    fn describe_surface(&self) -> ConnectorSurfaceDescriptor;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundChannelMessage {
    Connector(ConnectorRequest),
    Service(ServiceMessage),
}

pub trait InboundChannelSurface {
    fn route(&self) -> ConnectorSurfaceRoute;

    fn protocol_version(&self) -> &'static str {
        "rust-connector-v1"
    }

    fn supports_service_control_plane(&self) -> bool;

    fn supports_affinity_replication_control_plane(&self) -> bool {
        self.supports_service_control_plane()
    }

    fn decode_inbound_payload(
        &self,
        payload: &[u8],
    ) -> Result<InboundChannelMessage, String>;

    fn encode_connector_response_payload(
        &self,
        response: &ConnectorResponse,
    ) -> Result<Vec<u8>, String> {
        bincode::serialize(response).map_err(|err| err.to_string())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RustP2pSurface;

#[derive(Debug, Clone, Copy, Default)]
pub struct RustWssSurface;

#[derive(Debug, Clone, Copy, Default)]
pub struct RustP2pInboundSurface;

#[derive(Debug, Clone, Copy, Default)]
pub struct RustWssInboundSurface;

impl ConnectorSurfaceContract {
    pub fn rust_connector_v1() -> Self {
        Self {
            protocol_version: "rust-connector-v1",
            required_capabilities: core_capabilities(),
        }
    }
}

impl ConnectorSurfaceProvider for RustP2pSurface {
    fn describe_surface(&self) -> ConnectorSurfaceDescriptor {
        let mut optional_capabilities = BTreeSet::new();
        optional_capabilities.insert(ConnectorSurfaceCapability::ServiceControlPlane);

        ConnectorSurfaceDescriptor {
            provider_name: "rust/p2p",
            route: ConnectorSurfaceRoute::RustP2p,
            protocol_version: "rust-connector-v1",
            required_capabilities: core_capabilities(),
            optional_capabilities,
        }
    }
}

impl ConnectorSurfaceProvider for RustWssSurface {
    fn describe_surface(&self) -> ConnectorSurfaceDescriptor {
        ConnectorSurfaceDescriptor {
            provider_name: "rust/wss",
            route: ConnectorSurfaceRoute::RustWss,
            protocol_version: "rust-connector-v1",
            required_capabilities: core_capabilities(),
            optional_capabilities: BTreeSet::new(),
        }
    }
}

impl InboundChannelSurface for RustP2pInboundSurface {
    fn route(&self) -> ConnectorSurfaceRoute {
        ConnectorSurfaceRoute::RustP2p
    }

    fn supports_service_control_plane(&self) -> bool {
        true
    }

    fn decode_inbound_payload(
        &self,
        payload: &[u8],
    ) -> Result<InboundChannelMessage, String> {
        if let Some(message) = decode_service_message(payload) {
            return Ok(InboundChannelMessage::Service(message));
        }

        bincode::deserialize::<ConnectorRequest>(payload)
            .map(InboundChannelMessage::Connector)
            .map_err(|err| err.to_string())
    }
}

impl InboundChannelSurface for RustWssInboundSurface {
    fn route(&self) -> ConnectorSurfaceRoute {
        ConnectorSurfaceRoute::RustWss
    }

    fn supports_service_control_plane(&self) -> bool {
        false
    }

    fn supports_affinity_replication_control_plane(&self) -> bool {
        false
    }

    fn decode_inbound_payload(
        &self,
        payload: &[u8],
    ) -> Result<InboundChannelMessage, String> {
        bincode::deserialize::<ConnectorRequest>(payload)
            .map(InboundChannelMessage::Connector)
            .map_err(|err| err.to_string())
    }
}

pub fn validate_surface_contract<P: ConnectorSurfaceProvider>(
    provider: &P,
    contract: &ConnectorSurfaceContract,
) -> Result<(), ConnectorSurfaceContractError> {
    let descriptor = provider.describe_surface();

    if descriptor.protocol_version != contract.protocol_version {
        return Err(ConnectorSurfaceContractError::ProtocolVersionMismatch {
            provider: descriptor.provider_name,
            expected: contract.protocol_version,
            actual: descriptor.protocol_version,
        });
    }

    let missing = contract
        .required_capabilities
        .difference(&descriptor.required_capabilities)
        .copied()
        .collect::<BTreeSet<_>>();

    if !missing.is_empty() {
        return Err(ConnectorSurfaceContractError::MissingCapabilities {
            provider: descriptor.provider_name,
            missing,
        });
    }

    Ok(())
}

pub fn default_surface_descriptors() -> Vec<ConnectorSurfaceDescriptor> {
    vec![
        RustP2pSurface.describe_surface(),
        RustWssSurface.describe_surface(),
    ]
}

fn core_capabilities() -> BTreeSet<ConnectorSurfaceCapability> {
    [
        ConnectorSurfaceCapability::PersistentConnection,
        ConnectorSurfaceCapability::BidirectionalMessaging,
        ConnectorSurfaceCapability::ChallengeHandshake,
        ConnectorSurfaceCapability::SessionStickiness,
        ConnectorSurfaceCapability::BinaryBincodeFrames,
        ConnectorSurfaceCapability::RequestResponseCorrelation,
        ConnectorSurfaceCapability::ControlledTeardown,
    ]
    .into_iter()
    .collect()
}

#[cfg(test)]
#[path = "surface_test.rs"]
mod tests;
