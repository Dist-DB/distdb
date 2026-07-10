use super::*;

fn sample_options(servers: Vec<String>) -> ClientOptions {
    ClientOptions {
        servers,
        tls_mode: crate::TlsMode::Off,
        tls_ca_path: None,
        user: Some("root".to_string()),
        password: None,
        database: Some("main".to_string()),
        peer_id: None,
    }
}

fn make_inner(connected: bool) -> ClientInner {
    let options = sample_options(vec!["/ip4/127.0.0.1/tcp/4001".to_string()]);
    let config = ConnectorP2pConfig::new("/distdb/kad/1.0.0")
        .with_bootstrap_peers(options.servers.clone())
        .with_tls_mode(options.tls_mode.as_common());

    ClientInner {
        transport: ConnectorP2pTransport::new(config),
        options,
        request_seq: 0,
        connected,
        current_database: Some("main".to_string()),
    }
}

#[test]
fn new_rejects_empty_normalized_servers() {
    let options = sample_options(vec![" ".to_string()]);
    let err = DistDbClient::new(options).expect_err("new should reject empty servers");
    assert!(matches!(err, ClientError::Config(_)));
}

#[test]
fn ensure_connected_requires_active_connection() {
    let disconnected = make_inner(false);
    let err = ensure_connected(&disconnected).expect_err("should fail when disconnected");
    assert!(matches!(err, ClientError::Transport(_)));

    let connected = make_inner(true);
    ensure_connected(&connected).expect("should pass when connected");
}

#[test]
fn next_request_id_increments_sequence() {
    let mut inner = make_inner(false);
    assert_eq!(next_request_id(&mut inner), "clientlib-req-1");
    assert_eq!(next_request_id(&mut inner), "clientlib-req-2");
}

#[test]
fn decode_query_value_decodes_known_kinds_and_fallbacks() {
    assert_eq!(
        decode_query_value(b"-5", &common::schema::FieldKind::Int(64)),
        QueryValue::Int(-5)
    );

    assert_eq!(
        decode_query_value(b"bad-int", &common::schema::FieldKind::Int(64)),
        QueryValue::Text("bad-int".to_string())
    );

    assert_eq!(
        decode_query_value(b"9", &common::schema::FieldKind::UInt(64)),
        QueryValue::UInt(9)
    );

    assert_eq!(
        decode_query_value(b"bad-uint", &common::schema::FieldKind::UInt(64)),
        QueryValue::Text("bad-uint".to_string())
    );

    assert_eq!(
        decode_query_value(b"1.5", &common::schema::FieldKind::Float(32)),
        QueryValue::Float("1.5".to_string())
    );

    assert_eq!(
        decode_query_value(b"abc", &common::schema::FieldKind::Text),
        QueryValue::Text("abc".to_string())
    );

    assert_eq!(
        decode_query_value(&[1, 2], &common::schema::FieldKind::Blob),
        QueryValue::Bytes(vec![1, 2])
    );

    assert_eq!(
        decode_query_value(&[], &common::schema::FieldKind::Text),
        QueryValue::Null
    );
}

#[test]
fn query_value_to_json_maps_variants() {
    assert_eq!(query_value_to_json(&QueryValue::Null), serde_json::Value::Null);
    assert_eq!(query_value_to_json(&QueryValue::Int(-2)), serde_json::json!(-2));
    assert_eq!(query_value_to_json(&QueryValue::UInt(2)), serde_json::json!(2));
    assert_eq!(
        query_value_to_json(&QueryValue::Float("2.5".to_string())),
        serde_json::json!("2.5")
    );
    assert_eq!(
        query_value_to_json(&QueryValue::Text("abc".to_string())),
        serde_json::json!("abc")
    );
    assert_eq!(
        query_value_to_json(&QueryValue::Bytes(vec![10, 11])),
        serde_json::json!([10, 11])
    );
}

#[test]
fn query_response_from_wire_maps_columns_rows_and_timings() {
    
    let wire = connector::QueryResult {
        columns: vec![
            connector::FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: connector::FieldType::UInt(64),
                nullable: false,
                indexed: connector::FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            },
            connector::FieldDef {
                seqno: 2,
                field_name: "name".to_string(),
                field_type: connector::FieldType::Text,
                nullable: true,
                indexed: connector::FieldIndex::None,
                default_value: None,
                metadata: None,
            },
        ],
        rows: vec![
            vec![b"7".to_vec(), b"sam".to_vec()],
            vec![b"8".to_vec(), Vec::new(), vec![1, 2, 3]],
        ],
        timings: connector::QueryTimings {
            server_parse_ms: 1,
            server_execute_ms: 2,
            server_total_ms: 3,
            network_round_trip_ms: Some(4),
            cache: Some(connector::QueryCacheObservation::Miss {
                lookup_ms: 5,
                reason: Some("cold".to_string()),
            }),
        },
    };

    let response = query_response_from_wire(
        "req-1".to_string(),
        connector::ResponseStatus::Applied,
        wire,
    );

    assert_eq!(response.request_id, "req-1");
    assert_eq!(response.status, "applied");
    assert_eq!(response.row_count, 2);

    assert_eq!(response.columns.len(), 2);
    assert_eq!(response.columns[0].ordinal, 0);
    assert_eq!(response.columns[0].name, "id");
    assert_eq!(response.columns[0].sql_type, "BIGINT");
    assert_eq!(response.columns[0].nullable, false);
    assert_eq!(response.columns[0].indexed, "PrimaryKey");

    assert_eq!(response.rows[0].values[0], QueryValue::UInt(7));
    assert_eq!(response.rows[0].values[1], QueryValue::Text("sam".to_string()));

    assert_eq!(response.rows[1].values[0], QueryValue::UInt(8));
    assert_eq!(response.rows[1].values[1], QueryValue::Null);
    assert_eq!(response.rows[1].values[2], QueryValue::Bytes(vec![1, 2, 3]));

    assert_eq!(response.timings.server_parse_ms, 1);
    assert_eq!(response.timings.server_execute_ms, 2);
    assert_eq!(response.timings.server_total_ms, 3);
    assert_eq!(response.timings.network_round_trip_ms, Some(4));
    assert_eq!(response.timings.cache, Some("Miss { lookup_ms: 5, reason: Some(\"cold\") }".to_string()));

}
