
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
	
	This application provides TLS certificate management and enrollment services for DistDB,
    allowing nodes to request and receive signed TLS certificates for secure communication.

	This application is distributed under the GNU Affero General Public License v3.0. 
    See the LICENSE file in the project root for more information.

	Written in 2026 by Sam Colak <sam@samcolak.com>
	For information on the author and contributors, see the DistDB 
	website (www.distdb.com) or the GitHub repository (www.github.com/dist-db).

    Copyright (c) 2026 Sam Colak. All rights reserved.

*/

use security::ensure_or_generate_tls_cert;
use tlsserver::service::serve_tls_certificate_requests;

const DEFAULT_TLS_SERVER_NODE_ID: &str = "tlsserver-node-01";
const DEFAULT_TLS_SERVER_PORT: u16 = 5443;

fn parse_ca_root_from_args(args: &[String]) -> bool {

    if args.iter().any(|arg| arg == "ca_root") {
        return true;
    }

    args
        .iter()
        .find_map(|arg| arg.strip_prefix("ca_root="))
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes"))
        .unwrap_or(true)

}

fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::init();

    let args = std::env::args().collect::<Vec<_>>();
    
    let node_id = args
        .iter()
        .find_map(|arg| arg.strip_prefix("node_id=").map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_TLS_SERVER_NODE_ID.to_string());

    let data_dir = args
        .iter()
        .find_map(|arg| arg.strip_prefix("datadir=").map(ToOwned::to_owned))
        .unwrap_or_else(|| "./data".to_string());

    let listen_addr = args
        .iter()
        .find_map(|arg| arg.strip_prefix("listen_addr=").map(ToOwned::to_owned))
        .unwrap_or_else(|| "0.0.0.0".to_string());

    let port = args
        .iter()
        .find_map(|arg| arg.strip_prefix("port=").and_then(|v| v.parse::<u16>().ok()))
        .unwrap_or(DEFAULT_TLS_SERVER_PORT);

    let ca_root_enabled = parse_ca_root_from_args(&args);
    let bind_addr = format!("{}:{}", listen_addr, port);
    let node_data_dir = std::path::PathBuf::from(data_dir).join(&node_id);

    let issued_paths = ensure_or_generate_tls_cert(&node_data_dir, &node_id, &bind_addr, &[])
        .map_err(std::io::Error::other)?;

    log::info!(
        "tlsserver startup material ready before accept loop cert={} key={} ca={} ca_root={}",
        issued_paths.cert_path.display(),
        issued_paths.key_path.display(),
        issued_paths.ca_path.display(),
        ca_root_enabled,
    );

    serve_tls_certificate_requests(&bind_addr, &node_data_dir, ca_root_enabled)
        .map_err(std::io::Error::other)?;

    Ok(())
    
}
