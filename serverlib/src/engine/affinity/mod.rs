
pub mod storage;

mod checkpoint;
mod document;
mod processor;
mod state;
mod sync;

pub use checkpoint::CheckpointMetadata;
pub use document::{
    AffinityDocument, AffinityMember, AffinityMemberStatus, DatabaseSchemaSummary,
    ReplicationSecuritySummary,
};

pub use processor::AffinityProcessor;
pub use state::{AffinityProcessorError, AffinityProcessorState};
pub use sync::{AffinitySyncPhase, AffinitySyncStep};


#[cfg(test)]
mod tests {

    use crate::core::identity::NodeId;

    use super::*;

    fn sample_document() -> AffinityDocument {

        AffinityDocument {
            
            affinity_id: "finance-eu-01".to_string(),
            affinity_revision: 7,
            
            members: vec![AffinityMember {
                node_id: NodeId("sam01".to_string()),
                addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
                status: AffinityMemberStatus::Online,
                last_seen_epoch_ms: 10,
            }],

            databases: vec![
                DatabaseSchemaSummary {
                    database_id: "orders".to_string(),
                    database_name: "orders".to_string(),
                    schema_identifier: 200,
                    schema_hash: Some("abc".to_string()),
                },
                DatabaseSchemaSummary {
                    database_id: "billing".to_string(),
                    database_name: "billing".to_string(),
                    schema_identifier: 100,
                    schema_hash: Some("def".to_string()),
                },
            ],

            replication_security: ReplicationSecuritySummary {
                policy_revision: 1,
                key_id: Some("k-2026-06".to_string()),
                updated_epoch_ms: 11,
            },

        }

    }

    #[test]
    fn processor_builds_sync_plan_sorted_by_schema_identifier() {
        let mut processor = AffinityProcessor::new(NodeId("sam03".to_string()));
        processor.begin_join();
        processor.apply_affinity_document(sample_document());

        let plan = processor.build_sync_plan().expect("plan should build");
        assert_eq!(plan[0].phase, AffinitySyncPhase::ControlPlane);
        assert_eq!(plan[1].database_id.as_deref(), Some("orders"));
        assert_eq!(plan[4].database_id.as_deref(), Some("billing"));
    }

    #[test]
    fn schema_change_requires_at_least_one_partner() {
        let processor = AffinityProcessor::new(NodeId("sam01".to_string()));
        let result = processor.validate_schema_change_partner_count(0);
        assert!(matches!(
            result,
            Err(AffinityProcessorError::SchemaValidationPartnerRequired)
        ));
        assert!(processor.validate_schema_change_partner_count(1).is_ok());
    }

}