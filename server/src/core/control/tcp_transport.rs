use crate::core::control::outbound_transport::send_service_message_to_addr;
use crate::core::control::p2p_wire::{bootstrap_nodes_from_server_list, multiaddr_to_socket_addr};
use serverlib::core::cluster::NodeDescriptor;
use serverlib::core::identity::NodeId;
use serverlib::p2p::transport::Transport;
use serverlib::p2p::protocol::{ServiceAnnounce, ServiceMessage};
use serverlib::{
    KademliaDiscoveryConfig, KademliaDiscoveryService, ServerP2pEvent, ServerP2pNetwork,
    ServerP2pRuntime,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};

#[derive(Debug, Default)]
pub struct TcpServerTransport {
    peer_addrs: Vec<String>,
}

impl TcpServerTransport {
    pub fn new(peer_addrs: Vec<String>) -> Self {
        Self { peer_addrs }
    }
}

impl Transport for TcpServerTransport {

    fn send(&mut self, peer_id: &str, message: ServiceMessage) -> serverlib::helpers::error::Result<()> {
        let addr = multiaddr_to_socket_addr(peer_id)
            .ok_or_else(|| serverlib::helpers::error::ServerLibError::Network(format!("invalid peer address '{peer_id}'")))?;
        send_service_message_to_addr(&addr, &message)
    }

    fn broadcast(&mut self, message: ServiceMessage) -> serverlib::helpers::error::Result<()> {
        
        if self.peer_addrs.is_empty() {
            return Ok(());
        }

        let mut success_count = 0usize;
        for peer in &self.peer_addrs {
            let Some(addr) = multiaddr_to_socket_addr(peer) else {
                log::warn!("server p2p transport cannot parse peer addr='{}'", peer);
                continue;
            };

            match send_service_message_to_addr(&addr, &message) {
                Ok(()) => {
                    success_count += 1;
                    log::debug!("server p2p transport delivered message to {}", addr);
                }
                Err(err) => {
                    log::debug!("server p2p transport delivery failed to {}: {}", addr, err);
                }
            }
        }

        if success_count == 0 {
            log::warn!(
                "server p2p transport broadcast could not reach any configured peer (message={:?})",
                message
            );
            return Ok(());
        }

        Ok(())
    
    }

}

pub fn initialize_server_p2p_runtime(
    node_id: &str,
    swarm_id: &str,
    advertise_addr: &str,
    port: u16,
    server_list: &[String],
) -> Result<ServerP2pRuntime<TcpServerTransport>, Box<dyn std::error::Error>> {

    let bootstrap_nodes = bootstrap_nodes_from_server_list(server_list);
    let discovery = KademliaDiscoveryService::new(
        NodeId(node_id.to_string()),
        KademliaDiscoveryConfig::new(format!("/distdb/kad/{swarm_id}"))
            .with_bootstrap_nodes(bootstrap_nodes),
    );

    let network = ServerP2pNetwork::new(discovery, TcpServerTransport::new(server_list.to_vec()));
    let mut runtime = ServerP2pRuntime::new(network);

    for peer in runtime.network().discover_peers() {
        runtime.handle_event(ServerP2pEvent::PeerDiscovered(peer))?;
    }

    let local_node = NodeDescriptor {
        id: NodeId(node_id.to_string()),
        addrs: vec![format!("/ip4/{advertise_addr}/tcp/{port}")],
        is_local: true,
    };
    runtime.network_mut().broadcast_announce(local_node)?;

    Ok(runtime)

}

pub fn spawn_p2p_heartbeat_task(
    runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    local_node: NodeDescriptor,
) -> JoinHandle<()> {

    tokio::spawn(async move {

        let mut ticker = interval(Duration::from_secs(30));

        loop {
            ticker.tick().await;

            let mut runtime = runtime.lock().await;
            if let Err(err) = runtime
                .network_mut()
                .broadcast_announce(local_node.clone())
            {
                log::warn!("server p2p heartbeat announce failed: {}", err);
                continue;
            }

            let peer_count = runtime.network().discover_peers().len();
            log::debug!("server p2p heartbeat ok discovered_peers={}", peer_count);
        }

    })

}

pub fn spawn_service_announce_task(
    runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    local_node: NodeDescriptor,
    services: Vec<String>,
) -> JoinHandle<()> {

    tokio::spawn(async move {

        let mut ticker = interval(Duration::from_secs(30));

        loop {
            ticker.tick().await;

            let timestamp_epoch_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_millis() as u64)
                .unwrap_or(0);

            let peers = {
                let runtime = runtime.lock().await;
                runtime.network().discover_peers()
            };

            for peer in peers {
                if peer.is_local {
                    continue;
                }

                for addr in peer.addrs {
                    let mut runtime = runtime.lock().await;
                    if let Err(err) = runtime.network_mut().send_message(
                        &addr,
                        ServiceMessage::ServiceAnnounce(ServiceAnnounce {
                            node_id: local_node.id.0.clone(),
                            addrs: local_node.addrs.clone(),
                            services: services.clone(),
                            timestamp_epoch_ms,
                        }),
                    ) {
                        log::debug!(
                            "server service announce send failed to {}: {}",
                            addr,
                            err
                        );
                    }
                }
            }
        }

    })
    
}
