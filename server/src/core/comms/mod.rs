pub mod outbound_transport;
pub mod p2p;
pub mod p2p_wire;
pub mod surface;
pub mod tls_support;
pub mod wire_io;
pub mod wss;

pub use outbound_transport::{
    configure_outbound_tls_state,
    send_service_message_to_addr,
    send_service_request_to_addr,
};
pub use p2p::{
    TcpServerTransport,
    initialize_server_p2p_runtime,
    spawn_p2p_heartbeat_task,
    spawn_service_announce_task,
};
pub use p2p_wire::{
    advertised_listen_addr_from_args,
    affinity_document_to_wire,
    bootstrap_nodes_from_server_list,
    decode_service_message,
    encode_service_message,
    multiaddr_to_socket_addr,
    node_descriptor_to_peer_node,
    normalize_bootstrap_addr,
    transaction_id_to_wire,
    wire_affinity_document_to_domain,
    wire_transaction_id_to_transaction_id,
};
pub use surface::{
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
pub use tls_support::{
    BoxedConnectorStream,
    build_tls_acceptor,
    build_tls_client_config,
    negotiate_connector_stream,
    parse_tls_config_from_args,
    parse_tls_mode_from_args,
};
pub use wire_io::{write_response_frame, write_service_message_to_stream};
pub use wss::{
    DISTDB_WSS_PATH,
    DISTDB_WSS_SUBPROTOCOL,
    WssHandlerError,
    WssFrameError,
    decode_connector_request_message,
    encode_connector_response_message,
    handle_wss_inbound_stream,
    handle_wss_inbound_stream_with_surface,
    is_wss_path,
    validate_wss_tls_policy,
};
