use super::*;
use crate::core::identity::NodeId;
use crate::engine::affinity::{AffinitySyncStep,
    DatabaseSchemaSummary,
    ReplicationSecuritySummary,
    AffinityDocument,
    AffinityMember,
    AffinityMemberStatus};

fn create_test_processor() -> AffinityProcessor {

    let mut processor = AffinityProcessor::new(NodeId("test-node".to_string()));

    let doc = AffinityDocument {

        affinity_id: "test-affinity".to_string(),
        affinity_revision: 1,

        members: vec![AffinityMember {
            node_id: NodeId("peer1".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4002".to_string()],
            status: AffinityMemberStatus::Online,
            last_seen_epoch_ms: 1234567890,
        }],

        databases: vec![DatabaseSchemaSummary {
            database_id: "db1".to_string(),
            database_name: "db1".to_string(),
            schema_identifier: 1,
            schema_hash: Some("hash1".to_string()),
        }],

        replication_security: ReplicationSecuritySummary {
            policy_revision: 1,
            key_id: Some("key1".to_string()),
            updated_epoch_ms: 1234567890,
        },

    };

    processor.apply_affinity_document(doc);
    processor.initialize_checkpoint(AffinitySyncPhase::ControlPlane);
    processor

}

#[test]
fn executor_tracks_phase_progression() {
    let mut executor = ReplicationPhaseExecutor::new();
    let mut processor = create_test_processor();

    let sync_plan = vec![
        AffinitySyncStep {
            phase: AffinitySyncPhase::ControlPlane,
            database_id: None,
            schema_identifier: None,
        },
        AffinitySyncStep {
            phase: AffinitySyncPhase::SchemaCatalog,
            database_id: Some("db1".to_string()),
            schema_identifier: Some(1),
        },
    ];

    assert_eq!(executor.current_sync_index, 0);

    let result = executor.execute_next_phase(&mut processor, &sync_plan);
    assert!(result.is_ok());
    assert!(!result.unwrap());
    assert_eq!(executor.current_sync_index, 1);
}

#[test]
fn executor_marks_steps_complete() {
    let mut executor = ReplicationPhaseExecutor::new();
    let mut processor = create_test_processor();

    let sync_plan = vec![AffinitySyncStep {
        phase: AffinitySyncPhase::ControlPlane,
        database_id: None,
        schema_identifier: None,
    }];

    executor.execute_next_phase(&mut processor, &sync_plan).expect("execute");

    let checkpoint = processor.checkpoint().expect("checkpoint");
    assert!(checkpoint.is_step_completed(0));
}

#[test]
fn executor_detects_completion() {
    let mut executor = ReplicationPhaseExecutor::new();
    let mut processor = create_test_processor();

    let sync_plan = vec![AffinitySyncStep {
        phase: AffinitySyncPhase::ControlPlane,
        database_id: None,
        schema_identifier: None,
    }];

    let result = executor.execute_next_phase(&mut processor, &sync_plan);
    assert!(result.is_ok());
    assert!(result.unwrap()); // Should be true - all phases complete
}

#[test]
fn executor_rejects_schema_phase_without_database() {
    let executor = ReplicationPhaseExecutor::new();
    let processor = create_test_processor();

    let step = AffinitySyncStep {
        phase: AffinitySyncPhase::SchemaCatalog,
        database_id: None,
        schema_identifier: Some(1),
    };

    let result = executor.execute_schema_catalog_phase(&processor, &step);
    assert!(result.is_err());
}

#[test]
fn executor_rejects_schema_phase_without_document_database_entry() {
    let executor = ReplicationPhaseExecutor::new();
    let processor = create_test_processor();

    let step = AffinitySyncStep {
        phase: AffinitySyncPhase::SchemaCatalog,
        database_id: Some("missing-db".to_string()),
        schema_identifier: Some(1),
    };

    let result = executor.execute_schema_catalog_phase(&processor, &step);
    assert!(result.is_err());
}

#[test]
fn executor_rejects_schema_phase_when_plan_schema_mismatches_document() {
    let executor = ReplicationPhaseExecutor::new();
    let processor = create_test_processor();

    let step = AffinitySyncStep {
        phase: AffinitySyncPhase::SchemaCatalog,
        database_id: Some("db1".to_string()),
        schema_identifier: Some(99),
    };

    let result = executor.execute_schema_catalog_phase(&processor, &step);
    assert!(result.is_err());
}
