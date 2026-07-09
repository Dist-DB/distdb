use super::*;

#[test]
fn parse_server_list_from_args_dedups_and_normalizes() {
    let args = vec![
        "server".to_string(),
        "servers=127.0.0.1:9400,node.local:9400,127.0.0.1:9400".to_string(),
    ];

    let parsed = parse_server_list_from_args(&args);

    assert_eq!(
        parsed,
        vec![
            "/ip4/127.0.0.1/tcp/9400".to_string(),
            "/dns/node.local/tcp/9400".to_string(),
        ]
    );
}

#[test]
fn parse_affinity_startup_config_parses_key_colon_password() {
    let args = vec![
        "server".to_string(),
        "affinity=team-a:secret".to_string(),
    ];

    let cfg = parse_affinity_startup_config(&args).expect("config should parse");
    assert_eq!(cfg.affinity_id, "team-a");
    assert!(!cfg.affinity_key.is_empty());

    let missing_password = vec!["server".to_string(), "affinity=team-a".to_string()];
    assert!(parse_affinity_startup_config(&missing_password).is_none());

    let empty_spec = vec!["server".to_string(), "affinity=:".to_string()];
    assert!(parse_affinity_startup_config(&empty_spec).is_none());
}

#[test]
fn build_affinity_document_snapshot_includes_local_and_discovered_nodes() {
    let cfg = AffinityStartupConfig {
        affinity_id: "team-a".to_string(),
        affinity_key: "k1".to_string(),
    };
    let local_node = PeerNode {
        id: "sam01".to_string(),
        addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
        is_local: true,
    };
    let discovered = vec![PeerNode {
        id: "sam02".to_string(),
        addrs: vec!["/ip4/127.0.0.1/tcp/4002".to_string()],
        is_local: false,
    }];

    let doc = build_affinity_document_snapshot(&cfg, &local_node, discovered);
    assert_eq!(doc.affinity_id, "team-a");
    assert_eq!(doc.members.len(), 2);
    assert!(doc
        .members
        .iter()
        .any(|member| member.node_id.0 == "sam01" && member.status == AffinityMemberStatus::Online));
    assert!(doc.members.iter().any(|member| member.node_id.0 == "sam02"));
}
