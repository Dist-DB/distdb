
mod tls_config;
mod tls_mode;
mod p2p_tls;
mod tls_transport;

pub use p2p_tls::{
	AutoTlsPaths, TlsEnrollmentRequestMaterial, build_tls_enrollment_request,
	ensure_or_generate_p2p_tls, import_p2p_ca_pem_if_missing, install_signed_p2p_tls,
	load_p2p_ca_pem, sign_tls_enrollment_csr,
};
pub use tls_config::{TlsConfig, parse_tls_config_from_args};
pub use tls_mode::parse_tls_mode_from_args;
pub use tls_transport::{
	AsyncReadWrite,
	BoxedConnectorStream,
	build_tls_acceptor,
	build_tls_client_config,
	negotiate_connector_stream,
};

#[cfg(test)]
mod tls_config_test;

#[cfg(test)]
mod tls_mode_test;

#[cfg(test)]
mod tls_transport_test;
