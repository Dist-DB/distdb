
mod tls_config;
mod tls_mode;
mod p2p_tls;
mod tls_transport;

pub use p2p_tls::{
	AutoTlsPaths, TlsEnrollmentRequestMaterial, build_tls_enrollment_request,
	ensure_or_generate_tls_cert, install_signed_p2p_tls, sign_tls_enrollment_csr,
};

pub use tls_config::{TlsConfig, parse_tls_config_from_args};
pub use tls_mode::parse_tls_mode_from_args;
pub use tls_transport::{
	AsyncReadWrite,
	BoxedConnectorStream,
	build_tls_acceptor,
	build_tls_acceptor_from_pem,
	build_tls_client_config,
	build_tls_client_config_from_pem,
	negotiate_connector_stream,
	validate_tls_certificate_subject_alt_names,
	validate_tls_certificate_subject_alt_names_pem,
};

#[cfg(test)]
mod tls_config_test;

#[cfg(test)]
mod tls_mode_test;

#[cfg(test)]
mod tls_transport_test;
