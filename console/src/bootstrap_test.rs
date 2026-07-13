use super::*;

#[test]
fn normalize_bootstrap_addr_accepts_multiaddr_passthrough() {
    let addr = "/ip4/127.0.0.1/tcp/9400";
    assert_eq!(normalize_bootstrap_addr(addr), Some(addr.to_string()));
}

#[test]
fn normalize_bootstrap_addr_parses_host_port() {
    assert_eq!(
        normalize_bootstrap_addr("127.0.0.1:9400"),
        Some("/ip4/127.0.0.1/tcp/9400".to_string())
    );
    assert_eq!(
        normalize_bootstrap_addr("node.local:9400"),
        Some("/dns/node.local/tcp/9400".to_string())
    );
}

#[test]
fn normalize_bootstrap_addr_parses_bare_port() {
    assert_eq!(
        normalize_bootstrap_addr("4001"),
        Some("/ip4/127.0.0.1/tcp/4001".to_string())
    );
    assert_eq!(
        normalize_bootstrap_addr(":4002"),
        Some("/ip4/127.0.0.1/tcp/4002".to_string())
    );
}

#[test]
fn bootstrap_peers_from_cli_args_dedups_and_preserves_order() {
    let args = vec![
        "127.0.0.1:9400".to_string(),
        "servers=node.local:9400,127.0.0.1:9400".to_string(),
    ];

    let peers = bootstrap_peers_from_cli_args(&args);

    assert_eq!(
        peers,
        vec![
            "/ip4/127.0.0.1/tcp/9400".to_string(),
            "/dns/node.local/tcp/9400".to_string(),
        ]
    );
}
