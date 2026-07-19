use std::collections::BTreeSet;

use connector::{ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, MutationResult, ResponseStatus};
use peerlib::{PeerNode, ServiceMessage};

use super::{
    ConnectorSurfaceCapability,
    ConnectorSurfaceContract,
    ConnectorSurfaceContractError,
    ConnectorSurfaceDescriptor,
    ConnectorSurfaceProvider,
    ConnectorSurfaceRoute,
    InboundChannelMessage,
    InboundChannelSurface,
    RustP2pInboundSurface,
    RustP2pSurface,
    RustWssInboundSurface,
    RustWssSurface,
    default_surface_descriptors,
    validate_surface_contract,
};
use crate::core::comms::p2p_wire::encode_service_message;

#[test]
fn rust_p2p_satisfies_rust_connector_v1_contract() {
    let contract = ConnectorSurfaceContract::rust_connector_v1();
    let provider = RustP2pSurface;

    let result = validate_surface_contract(&provider, &contract);
    assert!(result.is_ok());
}

#[test]
fn rust_wss_satisfies_rust_connector_v1_contract() {
    let contract = ConnectorSurfaceContract::rust_connector_v1();
    let provider = RustWssSurface;

    let result = validate_surface_contract(&provider, &contract);
    assert!(result.is_ok());
}

#[test]
fn default_surface_descriptors_contains_exactly_p2p_and_wss() {
    let descriptors = default_surface_descriptors();
    assert_eq!(descriptors.len(), 2);

    assert!(descriptors
        .iter()
        .any(|descriptor| descriptor.route == ConnectorSurfaceRoute::RustP2p));

    assert!(descriptors
        .iter()
        .any(|descriptor| descriptor.route == ConnectorSurfaceRoute::RustWss));
}

#[test]
fn contract_validation_fails_on_missing_required_capability() {
    #[derive(Debug, Clone)]
    struct IncompleteProvider;

    impl ConnectorSurfaceProvider for IncompleteProvider {
        fn describe_surface(&self) -> ConnectorSurfaceDescriptor {
            let mut required = BTreeSet::new();
            required.insert(ConnectorSurfaceCapability::PersistentConnection);

            ConnectorSurfaceDescriptor {
                provider_name: "broken/provider",
                route: ConnectorSurfaceRoute::RustP2p,
                protocol_version: "rust-connector-v1",
                required_capabilities: required,
                optional_capabilities: BTreeSet::new(),
            }
        }
    }

    let contract = ConnectorSurfaceContract::rust_connector_v1();
    let provider = IncompleteProvider;

    let result = validate_surface_contract(&provider, &contract);
    assert!(matches!(
        result,
        Err(ConnectorSurfaceContractError::MissingCapabilities { .. })
    ));
}

#[test]
fn contract_validation_fails_on_protocol_version_mismatch() {
    #[derive(Debug, Clone)]
    struct VersionMismatchProvider;

    impl ConnectorSurfaceProvider for VersionMismatchProvider {
        fn describe_surface(&self) -> ConnectorSurfaceDescriptor {
            ConnectorSurfaceDescriptor {
                provider_name: "version/mismatch",
                route: ConnectorSurfaceRoute::RustWss,
                protocol_version: "rust-connector-v2",
                required_capabilities: ConnectorSurfaceContract::rust_connector_v1()
                    .required_capabilities,
                optional_capabilities: BTreeSet::new(),
            }
        }
    }

    let contract = ConnectorSurfaceContract::rust_connector_v1();
    let provider = VersionMismatchProvider;

    let result = validate_surface_contract(&provider, &contract);
    assert!(matches!(
        result,
        Err(ConnectorSurfaceContractError::ProtocolVersionMismatch { .. })
    ));
}

#[test]
fn inbound_surface_parity_supports_connector_payload_decode_for_both_channels() {
    let request = ConnectorRequest::new(
        "req-connector-1",
        ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    );
    let payload = bincode::serialize(&request).expect("request payload should serialize");

    let p2p = RustP2pInboundSurface;
    let wss = RustWssInboundSurface;

    let p2p_decoded = p2p
        .decode_inbound_payload(&payload)
        .expect("p2p should decode connector payload");
    let wss_decoded = wss
        .decode_inbound_payload(&payload)
        .expect("wss should decode connector payload");

    assert!(matches!(p2p_decoded, InboundChannelMessage::Connector(_)));
    assert!(matches!(wss_decoded, InboundChannelMessage::Connector(_)));
}

#[test]
fn inbound_surface_parity_supports_connector_response_encode_for_both_channels() {
    let response = ConnectorResponse {
        request_id: "req-resp-1".to_string(),
        status: ResponseStatus::Applied,
        result: ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    };

    let p2p = RustP2pInboundSurface;
    let wss = RustWssInboundSurface;

    let p2p_payload = p2p
        .encode_connector_response_payload(&response)
        .expect("p2p should encode response payload");
    let wss_payload = wss
        .encode_connector_response_payload(&response)
        .expect("wss should encode response payload");

    assert_eq!(p2p_payload, wss_payload);
}

#[test]
fn inbound_surface_p2p_supports_service_control_plane_wss_does_not() {
    let service = ServiceMessage::NodeAnnounce(PeerNode {
        id: "node-a".to_string(),
        addrs: vec!["/ip4/127.0.0.1/tcp/19401".to_string()],
        is_local: false,
    });
    let payload = encode_service_message(&service).expect("service message should encode");

    let p2p = RustP2pInboundSurface;
    let wss = RustWssInboundSurface;

    assert!(p2p.supports_service_control_plane());
    assert!(p2p.supports_affinity_replication_control_plane());
    assert!(!wss.supports_service_control_plane());
    assert!(!wss.supports_affinity_replication_control_plane());

    let p2p_decoded = p2p
        .decode_inbound_payload(&payload)
        .expect("p2p should decode service payload");
    assert!(matches!(p2p_decoded, InboundChannelMessage::Service(_)));

    let wss_decoded = wss.decode_inbound_payload(&payload);
    assert!(wss_decoded.is_err());
}
