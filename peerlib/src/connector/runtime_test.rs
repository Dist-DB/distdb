
    use super::*;
    use connector::{
        ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult,
        MutationResult, ResponseStatus,
    };
    use connector::ConnectorTransport;
    use crate::connector::ConnectorP2pConfig;

    #[derive(Debug)]
    struct StubSwarmSource {
        events: Vec<ConnectorP2pEvent>,
    }

    impl ConnectorSwarmEventSource for StubSwarmSource {
        fn next_event(&mut self, _idle_wait: Duration) -> Option<ConnectorP2pEvent> {
            if self.events.is_empty() {
                None
            } else {
                Some(self.events.remove(0))
            }
        }
    }

    #[test]
    fn runtime_processes_peer_and_response_events() {

        let transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0")
                .with_bootstrap_peers(vec!["bootstrap".to_string()]),
        );
        
        let mut runtime = ConnectorP2pRuntime::new(transport);

        let (tx, rx) = std::sync::mpsc::channel();

        tx.send(ConnectorP2pEvent::PeerDiscovered(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec!["/ip4/10.0.0.1/tcp/4001".to_string()],
            is_discovered: true,
        }))
        .expect("event send should succeed");

        tx.send(ConnectorP2pEvent::ResponseReceived(ConnectorResponse {
            request_id: "req-7".to_string(),
            status: ResponseStatus::Applied,
            result: ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
        }))
        .expect("event send should succeed");

        tx.send(ConnectorP2pEvent::Shutdown)
            .expect("event send should succeed");

        runtime.run_loop(&rx).expect("runtime loop should succeed");

        let request = ConnectorRequest::new(
            "req-7",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let response = runtime
            .transport()
            .request(&request)
            .expect("queued response should be available");

        assert_eq!(response.request_id, "req-7");
        assert_eq!(runtime.transport().discovered_peers().len(), 1);
    }

    #[test]
    fn runtime_can_run_from_swarm_source() {
        let transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0")
                .with_bootstrap_peers(vec!["bootstrap".to_string()]),
        );
        let mut runtime = ConnectorP2pRuntime::new(transport);

        let mut source = StubSwarmSource {
            events: vec![
                ConnectorP2pEvent::PeerDiscovered(ConnectorPeer {
                    peer_id: "peer-2".to_string(),
                    addrs: vec!["/ip4/10.0.0.2/tcp/4001".to_string()],
                    is_discovered: true,
                }),
                ConnectorP2pEvent::Shutdown,
            ],
        };

        runtime
            .run_swarm_loop(&mut source)
            .expect("swarm loop should succeed");
        assert_eq!(runtime.transport().discovered_peers().len(), 1);
    }

    #[test]
    fn runtime_returns_error_for_error_event() {
        let transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0")
                .with_bootstrap_peers(vec!["bootstrap".to_string()]),
        );
        let mut runtime = ConnectorP2pRuntime::new(transport);

        let result = runtime.handle_event(ConnectorP2pEvent::ErrorReceived {
            request_id: Some("req-err".to_string()),
            message: "response decode failed".to_string(),
        });

        assert!(matches!(result, Err(ConnectorError::Transport(_))));
    }