use super::*;
use crate::core::identity::NodeId;
use crate::engine::affinity::{
    AffinityMember, AffinityMemberStatus, AffinityProcessor, DatabaseSchemaSummary, ReplicationSecuritySummary,
};

fn create_test_document() -> AffinityDocument {
    AffinityDocument {
        affinity_id: "test-affinity".to_string(),
        affinity_revision: 1,
        members: vec![AffinityMember {
            node_id: NodeId("node1".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
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
    }
}

#[test]
fn storage_path_generation() {
    let storage = AffinityStorage::new("/tmp");
    let path = storage.affinity_document_path("test-id");
    assert!(path.to_string_lossy().contains(".affinity_test-id"));
}

#[test]
fn document_serialization_roundtrip() {
    let doc = create_test_document();
    let serialized = bincode::serialize(&doc).expect("serialize");
    let deserialized: AffinityDocument = bincode::deserialize(&serialized).expect("deserialize");

    assert_eq!(deserialized.affinity_id, doc.affinity_id);
    assert_eq!(deserialized.affinity_revision, doc.affinity_revision);
    assert_eq!(deserialized.members.len(), doc.members.len());
}

#[test]
fn checkpoint_serialization() {
    use crate::engine::affinity::AffinitySyncPhase;

    let mut checkpoint = CheckpointMetadata::new(
        "test-affinity".to_string(),
        AffinitySyncPhase::DataSnapshot,
    );
    checkpoint.mark_step_completed(0);
    checkpoint.mark_step_completed(1);

    let serialized = bincode::serialize(&checkpoint).expect("serialize");
    let deserialized: CheckpointMetadata = bincode::deserialize(&serialized).expect("deserialize");

    assert_eq!(deserialized.affinity_id, checkpoint.affinity_id);
    assert_eq!(deserialized.current_phase, checkpoint.current_phase);
    assert_eq!(deserialized.completed_step_indices, vec![0, 1]);
}

#[test]
fn checkpoint_step_tracking() {
    use crate::engine::affinity::AffinitySyncPhase;

    let mut checkpoint = CheckpointMetadata::new(
        "test-affinity".to_string(),
        AffinitySyncPhase::ControlPlane,
    );

    assert!(!checkpoint.is_step_completed(0));
    assert_eq!(checkpoint.next_incomplete_step(5), Some(0));

    checkpoint.mark_step_completed(0);
    checkpoint.mark_step_completed(2);

    assert!(checkpoint.is_step_completed(0));
    assert!(!checkpoint.is_step_completed(1));
    assert!(checkpoint.is_step_completed(2));
    assert_eq!(checkpoint.next_incomplete_step(5), Some(1));
    assert_eq!(checkpoint.progress_percentage(5), 40);
}

#[test]
fn checkpoint_save_and_load_roundtrip() {

    use crate::engine::affinity::AffinitySyncPhase;
    use std::fs;

    // Create a temporary directory for the test
    let temp_dir = std::env::temp_dir()
        .join(format!("checkpoint_test_{}", std::process::id()));

    let _ = fs::remove_dir_all(&temp_dir);

    fs::create_dir_all(&temp_dir).expect("create temp dir");

    let storage = AffinityStorage::new(&temp_dir);

    // Create and save a checkpoint
    let mut checkpoint = CheckpointMetadata::new(
        "test-affinity".to_string(),
        AffinitySyncPhase::DataSnapshot,
    );
    checkpoint.mark_step_completed(0);
    checkpoint.mark_step_completed(1);
    checkpoint.mark_step_completed(3);

    storage
        .save_checkpoint(&checkpoint)
        .expect("save checkpoint");

    // Load the checkpoint and verify it matches
    let loaded = storage
        .load_checkpoint("test-affinity")
        .expect("load checkpoint")
        .expect("checkpoint should exist");

    assert_eq!(loaded.affinity_id, checkpoint.affinity_id);
    assert_eq!(loaded.current_phase, checkpoint.current_phase);
    assert_eq!(loaded.completed_step_indices, vec![0, 1, 3]);
    assert_eq!(loaded.progress_percentage(10), 30);

    // Clean up
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn processor_checkpoint_integration() {
    use crate::engine::affinity::AffinitySyncPhase;
    use crate::core::identity::NodeId;

    let mut processor = AffinityProcessor::new(NodeId("test-node".to_string()));

    // Create and apply a document
    let doc = create_test_document();
    processor.apply_affinity_document(doc);

    // Initialize checkpoint
    processor.initialize_checkpoint(AffinitySyncPhase::ControlPlane);
    assert!(processor.checkpoint().is_some());
    assert_eq!(processor.next_incomplete_sync_step(5), Some(0));

    // Mark steps as completed
    processor.mark_sync_step_completed(0);
    processor.mark_sync_step_completed(2);

    // Verify progress tracking
    let checkpoint = processor.checkpoint().expect("checkpoint should exist");
    assert!(checkpoint.is_step_completed(0));
    assert!(!checkpoint.is_step_completed(1));
    assert!(checkpoint.is_step_completed(2));
    assert_eq!(processor.next_incomplete_sync_step(5), Some(1));

    // Simulate restoration
    let saved_checkpoint = checkpoint.clone();
    let mut processor2 = AffinityProcessor::new(NodeId("test-node2".to_string()));
    processor2.restore_checkpoint(saved_checkpoint);

    assert_eq!(
        processor2.checkpoint().expect("checkpoint").completed_step_indices,
        vec![0, 2]
    );
}
