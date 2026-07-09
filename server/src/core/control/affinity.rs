use std::sync::Arc;

use common::helpers::stable_id;
use common::helpers::utils::md5_hash;
use peerlib::{
    AffinityJoinRequest, AffinityJoinResponse, AffinityReplicationAction, PeerNode, ServiceMessage,
};
use serverlib::core::identity::NodeId;
use serverlib::{
    AffinityDocument, AffinityMember, AffinityMemberStatus, AffinityProcessor, AffinityStorage,
    AffinitySyncPhase, DatabaseSchemaSummary,
};
use tokio::sync::Mutex;

use crate::core::app::ServerApp;
use crate::core::control::outbound_transport::send_service_request_to_addr;
use crate::core::control::p2p_wire::{
    multiaddr_to_socket_addr, normalize_bootstrap_addr, wire_affinity_document_to_domain,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffinityStartupConfig {
    pub affinity_id: String,
    pub affinity_key: String,
}

pub fn parse_server_list_from_args(args: &[String]) -> Vec<String> {

    let server_entries = args
        .iter()
        .find_map(|arg| arg.strip_prefix("servers=").map(ToOwned::to_owned))
        .map(|list| {
            list.split(',')
                .map(|addr| addr.trim().to_string())
                .filter_map(|addr| normalize_bootstrap_addr(&addr))
                .collect::<Vec<String>>()
        })
        .unwrap_or_default();

    let mut seen = std::collections::HashSet::new();
    let mut server_list = Vec::new();

    for addr in server_entries {
        if seen.insert(addr.clone()) {
            server_list.push(addr);
        }
    }

    server_list

}

pub fn parse_affinity_startup_config(args: &[String]) -> Option<AffinityStartupConfig> {

    let affinity_spec = args
        .iter()
        .find_map(|arg| arg.strip_prefix("affinity=").map(str::trim))?;

    if affinity_spec.is_empty() {
        return None;
    }

    let (affinity_id, affinity_password) = match affinity_spec.split_once(':') {
        Some((id, pwd)) => {
            let id_str = id.trim();
            let pwd_str = pwd.trim();
            if id_str.is_empty() || pwd_str.is_empty() {
                return None;
            }
            (id_str.to_string(), pwd_str.to_string())
        }
        None => return None,
    };

    let affinity_key = stable_id(&[affinity_id.as_str(), affinity_password.as_str()]);

    Some(AffinityStartupConfig {
        affinity_id,
        affinity_key,
    })

}

pub fn build_affinity_document_snapshot(
    config: &AffinityStartupConfig,
    local_node: &PeerNode,
    discovered_peers: Vec<PeerNode>,
) -> AffinityDocument {

    let mut members = discovered_peers
        .into_iter()
        .map(|peer| AffinityMember {
            node_id: NodeId(peer.id),
            addrs: peer.addrs,
            status: AffinityMemberStatus::Unknown,
            last_seen_epoch_ms: now_millis(),
        })
        .collect::<Vec<_>>();

    members.push(AffinityMember {
        node_id: NodeId(local_node.id.clone()),
        addrs: local_node.addrs.clone(),
        status: AffinityMemberStatus::Online,
        last_seen_epoch_ms: now_millis(),
    });

    let mut dedup = std::collections::HashMap::new();
    for member in members {
        dedup.insert(member.node_id.0.clone(), member);
    }

    AffinityDocument {
        affinity_id: config.affinity_id.clone(),
        affinity_revision: 1,
        members: dedup.into_values().collect(),
        databases: Vec::<DatabaseSchemaSummary>::new(),
        replication_security: serverlib::ReplicationSecuritySummary {
            policy_revision: 1,
            key_id: Some(config.affinity_key.clone()),
            updated_epoch_ms: now_millis(),
        },
    }

}

pub fn build_database_schema_summaries_from_app(app: &ServerApp) -> Vec<DatabaseSchemaSummary> {

    let mut summaries = app
        .catalogs()
        .values()
        .map(|catalog| {
            let database_id = catalog.database_id.0.clone();
            let mut table_ids = catalog.table_ids();
            table_ids.sort();

            let schema_identifier = catalog.schema_epoch().max(1);
            let schema_fingerprint = md5_hash(
                format!("{}:{}:{}", database_id, schema_identifier, table_ids.join(",")).as_str(),
            );

            DatabaseSchemaSummary {
                database_id: database_id.clone(),
                database_name: catalog.database_name().to_string(),
                schema_identifier,
                schema_hash: Some(schema_fingerprint),
            }
        })
        .collect::<Vec<_>>();

    summaries.sort_by(|a, b| a.database_id.cmp(&b.database_id));
    summaries

}

pub fn send_affinity_join_requests(
    config: &AffinityStartupConfig,
    local_node: &PeerNode,
    discovered_peers: &[PeerNode],
) -> Vec<AffinityJoinResponse> {

    let mut responses = Vec::new();

    for peer in discovered_peers {

        let request_id = format!(
            "{}_{}",
            local_node.id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        );

        let join_req = AffinityJoinRequest {
            request_id,
            affinity_id: config.affinity_id.clone(),
            requester_node_id: local_node.id.clone(),
            requester_addrs: local_node.addrs.clone(),
            affinity_key: config.affinity_key.clone(),
        };

        let message = ServiceMessage::AffinityJoinRequest(join_req);
        let mut delivered = false;

        for peer_addr in &peer.addrs {

            let Some(socket_addr) = multiaddr_to_socket_addr(peer_addr) else {
                continue;
            };

            log::debug!(
                "sending affinity replication action={} to={} affinity_id={}",
                AffinityReplicationAction::JoinRequest.as_str(),
                socket_addr,
                config.affinity_id
            );

            match send_service_request_to_addr(&socket_addr, &message) {
                Ok(Some(ServiceMessage::AffinityJoinResponse(resp))) => {
                    responses.push(resp);
                    delivered = true;
                    log::debug!(
                        "sent affinity join request and received response peer_id={} addr={}",
                        peer.id,
                        socket_addr
                    );
                    break;
                }
                Ok(Some(other)) => {
                    log::warn!(
                        "unexpected message while awaiting join response from peer_id={} addr={}: {:?}",
                        peer.id,
                        socket_addr,
                        other
                    );
                }
                Ok(None) => {
                    log::debug!(
                        "no join response received from peer_id={} addr={}",
                        peer.id,
                        socket_addr
                    );
                }
                Err(err) => {
                    log::warn!(
                        "failed to send affinity join request to peer_id={} addr={}: {}",
                        peer.id,
                        socket_addr,
                        err
                    );
                }
            }

        }

        if !delivered {
            log::warn!(
                "failed to deliver affinity join request to any address for peer_id={}",
                peer.id
            );
        }

    }

    responses

}

pub fn merge_affinity_documents_from_responses(
    base_document: &mut AffinityDocument,
    responses: Vec<AffinityJoinResponse>,
) {

    for response in responses {

        if !response.ok {
            log::warn!("affinity join response failed: {:?}", response.error);
            continue;
        }

        if let Some(remote_doc) = response.document {

            let remote_doc = wire_affinity_document_to_domain(&remote_doc);
            let member_count = remote_doc.members.len();
            let database_count = remote_doc.databases.len();

            for member in remote_doc.members {
                base_document.upsert_member(member);
            }

            for database in remote_doc.databases {
                base_document.upsert_database_schema(database);
            }

            log::debug!(
                "merged affinity document from peer: members={} databases={}",
                member_count,
                database_count
            );

        }

    }

}

pub async fn execute_affinity_join_sequence(
    affinity_processor: Arc<Mutex<Option<AffinityProcessor>>>,
    affinity_storage: Arc<AffinityStorage>,
    config: &AffinityStartupConfig,
    local_node: &PeerNode,
    discovered_peers: &[PeerNode],
) {
    
    if discovered_peers.is_empty() {
        log::debug!("no discovered peers for affinity join");
        return;
    }

    log::info!(
        "starting affinity join sequence with {} peers",
        discovered_peers.len()
    );

    let responses = send_affinity_join_requests(config, local_node, discovered_peers);

    if !responses.is_empty() {
        
        log::info!(
            "affinity join received {} responses from peers",
            responses.len()
        );

        let mut processor = affinity_processor.lock().await;

        if let Some(proc) = processor.as_mut()
            && let Some(base_doc) = proc.document() {
                let mut updated_doc = base_doc.clone();
                merge_affinity_documents_from_responses(&mut updated_doc, responses);
                proc.apply_affinity_document(updated_doc.clone());

                proc.mark_sync_step_completed(0);

                if let Err(err) = affinity_storage.save(&updated_doc) {
                    log::error!("failed to save affinity document after join: {}", err);
                } else {
                    log::info!(
                        "saved affinity document after join affinity_id={} revision={}",
                        updated_doc.affinity_id,
                        updated_doc.affinity_revision
                    );
                }

                if let Some(checkpoint) = proc.checkpoint()
                    && let Err(err) = affinity_storage.save_checkpoint(checkpoint)
                {
                    log::error!("failed to save checkpoint after join: {}", err);
                }
            }

    }

    log::debug!("affinity join sequence completed");

}

pub fn initialize_affinity_with_persistence(
    config: Option<&AffinityStartupConfig>,
    local_node: &PeerNode,
    discovered_peers: Vec<PeerNode>,
    data_dir: &std::path::Path,
) -> (Option<AffinityProcessor>, AffinityStorage) {

    let storage = AffinityStorage::new(data_dir);

    let Some(config) = config else {
        return (None, storage);
    };

    let document = match storage.load(&config.affinity_id) {
        
        Ok(Some(doc)) => {
            log::info!(
                "loaded persisted affinity document affinity_id={} revision={}",
                doc.affinity_id,
                doc.affinity_revision
            );
            doc
        },

        Ok(None) => {
            log::debug!("no persisted affinity document found, building from peers");
            build_affinity_document_snapshot(config, local_node, discovered_peers)
        },

        Err(err) => {
            log::warn!(
                "failed to load persisted affinity document: {}, building from peers",
                err
            );
            build_affinity_document_snapshot(config, local_node, discovered_peers)
        }

    };

    let mut processor = AffinityProcessor::new(NodeId(local_node.id.clone()));
    processor.begin_join();
    processor.apply_affinity_document(document);

    match storage.load_checkpoint(&config.affinity_id) {

        Ok(Some(checkpoint)) => {
            processor.restore_checkpoint(checkpoint);
            log::info!(
                "restored checkpoint for resumable replication affinity_id={}",
                config.affinity_id
            );
        },

        Ok(None) => {
            log::debug!("no checkpoint found, starting fresh");
            processor.initialize_checkpoint(AffinitySyncPhase::ControlPlane);
        },

        Err(err) => {
            log::warn!("failed to load checkpoint: {}, starting fresh", err);
            processor.initialize_checkpoint(AffinitySyncPhase::ControlPlane);
        }

    }

    match processor.build_sync_plan() {

        Ok(plan) => {
            log::info!(
                "affinity processor initialized with persistence affinity_id={} planned_steps={}",
                config.affinity_id,
                plan.len()
            );
        },

        Err(err) => {
            log::warn!("affinity processor initialization failed: {}", err);
            processor.set_degraded(err.to_string());
        }

    }

    (Some(processor), storage)

}

fn now_millis() -> u64 {
    
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)

}

#[cfg(test)]
#[path = "affinity_test.rs"]
mod tests;
