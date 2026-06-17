
/*

	This file is part of DistDB.

	DistDB is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	DistDB is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with DistDB.  If not, see <http://www.gnu.org/licenses/>.

    The client application provides a basis and set of examples for building 
    DistDB client applications, including the interactive console client. 
    It demonstrates how to use the `distdb-client` library to connect to a 
    DistDB server, send commands, and handle responses. It is not 
    intended for production use, but rather as a reference implementation 
    and testing ground for client-side features.

    The client application is distributed under the MIT License.
    See the LICENSE file in the project root for more information.
	
	Written in 2026 by Sam Colak <sam@samcolak.com>
	For information on the author and contributors, see the DistDB 
	website (www.distdb.com) or the GitHub repository (www.github.com/dist-db).

    Copyright (c) 2026 Sam Colak. All rights reserved.

*/

use connector::{
    ConnectorClient, ConnectorCommand, ConnectorP2pConfig, ConnectorP2pEvent,
    ConnectorP2pRuntime, ConnectorP2pTransport, ConnectorRequest,
    ConnectorResponse, ConnectorResult, ConnectorPeer,
    FieldKind, FieldSpec, SchemaChangeRequest, SchemaCommand, SchemaResult,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args = std::env::args().skip(1).collect::<Vec<_>>();

    let tls_mode = match args.iter().find_map(|arg| arg.strip_prefix("tls=")) {
        Some(raw) => common::TlsMode::parse(raw).ok_or_else(|| {
            format!("invalid tls mode '{}'; expected off|optional|required", raw)
        })?,
        None => common::TlsMode::Optional,
    };

    let tls_ca_path = args
        .iter()
        .find_map(|arg| arg.strip_prefix("tls_ca="))
        .map(ToOwned::to_owned);

    // Client bootstraps connector transport over p2p.
    let mut p2p_config = ConnectorP2pConfig::new("/distdb/kad/1.0.0")
        .with_bootstrap_peers(vec!["bootstrap-peer-1".to_string()])
        .with_tls_mode(tls_mode);

    if let Some(ca_path) = tls_ca_path {
        p2p_config = p2p_config.with_tls_ca_path(ca_path);
    }

    let transport = ConnectorP2pTransport::new(p2p_config);
    
    let mut runtime = ConnectorP2pRuntime::new(transport);

    // In production these are swarm-originated events. For now we inject them
    // to demonstrate the end-to-end command path.
    runtime.handle_event(ConnectorP2pEvent::PeerDiscovered(ConnectorPeer {
        peer_id: "server-node-01".to_string(),
        addrs: vec!["/ip4/10.0.0.42/tcp/4001".to_string()],
        is_discovered: true,
    }))?;

    let create_database = ConnectorRequest::new(
        "req-create-db-1",
        ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    );

    let alter_table = ConnectorRequest::new(
        "req-alter-users-1",
        ConnectorCommand::Schema {
            database_id: "main".to_string(),
            command: SchemaCommand::AlterTable {
                change: SchemaChangeRequest::new("users")
                    .add_field(FieldSpec::new("email", FieldKind::Text).indexed())
                    .update_field(FieldSpec::new("display_name", FieldKind::Text).nullable())
                    .remove_field("legacy_username"),
            },
        },
    );

    runtime.handle_event(ConnectorP2pEvent::ResponseReceived(
        ConnectorResponse::applied(
            create_database.request_id.clone(),
            ConnectorResult::Schema(SchemaResult {
                table_id: "__database__".to_string(),
                schema_revision: 1,
            }),
        ),
    ))?;

    runtime.handle_event(ConnectorP2pEvent::ResponseReceived(
        ConnectorResponse::applied(
            alter_table.request_id.clone(),
            ConnectorResult::Schema(SchemaResult {
                table_id: "users".to_string(),
                schema_revision: 2,
            }),
        ),
    ))?;

    let client = ConnectorClient::new(runtime.into_transport());

    let create_db_response = client.execute(&create_database)?;
    println!(
        "create-database request={} status={:?}",
        create_db_response.request_id,
        create_db_response.status
    );

    let alter_table_response = client.execute(&alter_table)?;
    println!(
        "alter-table request={} status={:?}",
        alter_table_response.request_id,
        alter_table_response.status
    );

    Ok(())
    
}
