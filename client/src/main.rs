
use connector::{
    ConnectorClient, ConnectorCommand, ConnectorP2pConfig, ConnectorP2pEvent,
    ConnectorP2pRuntime, ConnectorP2pTransport, ConnectorRequest,
    ConnectorResponse, ConnectorResult, ConnectorPeer, FieldKind, FieldSpec,
    SchemaChangeRequest, SchemaCommand, SchemaResult,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {

    // Client bootstraps connector transport over p2p.
    let transport = ConnectorP2pTransport::new(
        ConnectorP2pConfig::new("/distdb/kad/1.0.0")
            .with_bootstrap_peers(vec!["bootstrap-peer-1".to_string()]),
    );
    
    let mut runtime = ConnectorP2pRuntime::new(transport);

    // In production these are swarm-originated events. For now we inject them
    // to demonstrate the end-to-end command path.
    runtime.handle_event(ConnectorP2pEvent::PeerDiscovered(ConnectorPeer {
        peer_id: "server-node-01".to_string(),
        addrs: vec!["/ip4/10.0.0.42/tcp/4001".to_string()],
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
