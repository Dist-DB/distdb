
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
	
	This library provides security and TLS utilities for DistDB, 
	including certificate management, TLS configuration, and secure communication.

	This library is distributed under the GNU Affero General Public License v3.0. 
    See the LICENSE file in the project root for more information.

	Written in 2026 by Sam Colak <sam@samcolak.com>
	For information on the author and contributors, see the DistDB 
	website (www.distdb.com) or the GitHub repository (www.github.com/dist-db).

    Copyright (c) 2026 Sam Colak. All rights reserved.

*/

mod tls;
mod caroot;

pub use caroot::platform_ca::{
	PLATFORM_TLS_ROOT_CERT_PEM,
	PLATFORM_TLS_ROOT_FINGERPRINT_SHA256,
	PLATFORM_TLS_ISSUING_CA_CERT_PEM,
	PLATFORM_TLS_ISSUING_CA_FINGERPRINT_SHA256,
	PLATFORM_TLS_ISSUING_CA_KEY_PEM,
	platform_tls_issuing_ca_cert_pem,
	platform_tls_issuing_ca_fingerprint_sha256,
	platform_tls_issuing_ca_key_pem,
	platform_tls_leaf_chain_pem,
	platform_tls_root_cert_pem,
	platform_tls_root_fingerprint_sha256,
};

pub use tls::{TlsConfig, parse_tls_config_from_args, parse_tls_mode_from_args};
pub use tls::{
	AsyncReadWrite,
	AutoTlsPaths,
	BoxedConnectorStream,
	TlsEnrollmentRequestMaterial,
	build_tls_enrollment_request,
	build_tls_acceptor,
	build_tls_acceptor_from_pem,
	build_tls_client_config,
	build_tls_client_config_from_pem,
	ensure_or_generate_tls_cert,
	install_signed_p2p_tls,
	negotiate_connector_stream,
	sign_tls_enrollment_csr,
	validate_tls_certificate_subject_alt_names,
	validate_tls_certificate_subject_alt_names_pem,
};