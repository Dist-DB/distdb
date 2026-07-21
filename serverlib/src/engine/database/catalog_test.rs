
use super::*;
use crate::engine::database::entity::metadata::EntityMetadata;
use crate::engine::security::{AccountAclEntry, AccountPrivilege, UserCredential};
use crate::core::identity::UserId;
use crate::{FieldDef, FieldType};

use std::path::PathBuf;

#[test]
fn create_empty_catalog_from_name_sets_obscured_id() {
    let catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    assert!(catalog.table_ids().is_empty());
    assert!(!catalog.database_id.0.is_empty());
    assert_ne!(catalog.database_id.0, "maindb");
    assert_eq!(catalog.database_name(), "maindb");
}

#[test]
fn create_empty_catalog_seeds_root_with_full_privileges_and_grant_options() {
    let catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let root_acl = catalog
        .effective_account_acl_entry("root")
        .expect("root ACL should exist by default");

    assert_eq!(root_acl.user_id.0, "root");
    assert!(root_acl.acl.contains("SELECT"));
    assert!(root_acl.acl.contains("CREATE USER"));
    assert!(root_acl.grant_acl.contains("SELECT"));
    assert!(root_acl.grant_acl.contains("CREATE USER"));
    assert_eq!(root_acl.acl.len(), root_acl.grant_acl.len());
}

#[test]
fn effective_account_acl_entry_returns_latest_acl_state_for_user() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let mut first = AccountAclEntry::new(UserId("Alice".to_string()), "MainDb");
    first.append_privilege(AccountPrivilege::Select);
    catalog.upsert_account_acl_entry(first);

    let mut second = AccountAclEntry::new(UserId("alice".to_string()), "MainDb");
    second.append_privilege(AccountPrivilege::Update);
    catalog.upsert_account_acl_entry(second);

    let effective = catalog
        .effective_account_acl_entry("ALICE")
        .expect("latest ACL entry should be available");

    assert!(effective.acl.contains("UPDATE"));
    assert!(!effective.acl.contains("SELECT"));
}

#[test]
fn effective_user_credential_returns_latest_credential_state_for_user() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let first = UserCredential::from_database_user_password(
        UserId("Alice".to_string()),
        "MainDb",
        "first-secret",
        "node-1",
        Some(1),
    );

    let second = UserCredential::from_database_user_password(
        UserId("alice".to_string()),
        "MainDb",
        "second-secret",
        "node-1",
        Some(1),
    );

    catalog.upsert_user_credential(first);
    catalog.upsert_user_credential(second);

    let effective = catalog
        .effective_user_credential("ALICE")
        .expect("latest user credential should be available");

    assert!(effective.verify_password("second-secret", "node-1"));
    assert!(!effective.verify_password("first-secret", "node-1"));
}

#[test]
fn empty_database_name_is_rejected() {
    let created = DatabaseCatalog::create_empty_from_name("   ");
    assert!(created.is_err());
}

#[test]
fn at_rest_encryption_key_reference_can_be_set_once() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    assert!(!catalog.at_rest_encryption_enabled());
    assert_eq!(catalog.at_rest_encryption_key_ref(), None);
    assert_eq!(catalog.at_rest_encryption_key_version(), 0);

    catalog
        .configure_at_rest_encryption_key_ref("enc:node-main:db-main")
        .expect("first key reference set should succeed");

    assert!(catalog.at_rest_encryption_enabled());
    assert_eq!(
        catalog.at_rest_encryption_key_ref(),
        Some("enc:node-main:db-main")
    );
    assert_eq!(catalog.at_rest_encryption_key_version(), 1);
}

#[test]
fn at_rest_encryption_key_reference_is_immutable_after_set() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .configure_at_rest_encryption_key_ref("enc:node-main:db-main")
        .expect("first key reference set should succeed");

    let second = catalog.configure_at_rest_encryption_key_ref("enc:node-main:db-alt");
    assert!(matches!(
        second,
        Err(DatabaseError::ImmutableEncryptionConfiguration)
    ));
}

#[test]
fn recursive_cte_execution_settings_default_values_are_available() {
    let catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let settings = catalog.recursive_cte_execution_settings();

    assert_eq!(settings.max_iterations, 128);
    assert_eq!(settings.max_rows, 50_000);
    assert_eq!(settings.timeout_ms, 0);
    assert!(settings.detect_repeating_union_all_frontier);
}

#[test]
fn recursive_cte_execution_settings_are_sanitized_on_configure() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog.configure_recursive_cte_execution_settings(
        RecursiveCteExecutionSettings {
            max_iterations: 0,
            max_rows: 0,
            timeout_ms: 25,
            detect_repeating_union_all_frontier: false,
        },
    );

    let settings = catalog.recursive_cte_execution_settings();

    assert_eq!(settings.max_iterations, 1);
    assert_eq!(settings.max_rows, 1);
    assert_eq!(settings.timeout_ms, 25);
    assert!(!settings.detect_repeating_union_all_frontier);
}

#[test]
fn recursive_cte_execution_settings_persist_in_catalog_file() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog.configure_recursive_cte_execution_settings(
        RecursiveCteExecutionSettings {
            max_iterations: 9,
            max_rows: 321,
            timeout_ms: 777,
            detect_repeating_union_all_frontier: false,
        },
    );

    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "distdb-catalog-recursive-cte-settings-test-{}",
        common::helpers::utils::unique_id()
    ));

    std::fs::create_dir_all(&dir).expect("temp dir should be created");

    catalog
        .save_in_directory(&dir)
        .expect("catalog save should succeed");

    let loaded = DatabaseCatalog::load_from_path(catalog_path_for_test(&catalog, &dir))
        .expect("catalog load should succeed");

    let settings = loaded.recursive_cte_execution_settings();
    assert_eq!(settings.max_iterations, 9);
    assert_eq!(settings.max_rows, 321);
    assert_eq!(settings.timeout_ms, 777);
    assert!(!settings.detect_repeating_union_all_frontier);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn duplicate_table_registration_is_rejected() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    let schema = TableSchema { fields: Vec::new() };

    let first = catalog.register_table("users", schema.clone());
    let second = catalog.register_table("users", schema);

    assert!(first.is_ok());
    assert!(second.is_err());
}

#[test]
fn cross_type_entity_id_collision_is_rejected() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_view("users", "select 1", TableSchema::new(Vec::new()))
        .expect("view register should succeed");

    let result = catalog.register_table("users", TableSchema::new(Vec::new()));

    assert!(matches!(result, Err(DatabaseError::DuplicateEntity)));
}

#[test]
fn catalog_and_table_start_in_load_state() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    let schema = TableSchema { fields: Vec::new() };

    catalog
        .register_table("users", schema)
        .expect("table register should succeed");

    assert_eq!(catalog.status(), ObjectStatus::Load);
    assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Load));
}

#[test]
fn lock_moves_to_sync_then_ready() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .transition_status(ObjectStatus::Lock)
        .expect("load->lock is valid");
    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("lock->sync is valid");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready is valid");

    assert_eq!(catalog.status(), ObjectStatus::Ready);
}

#[test]
fn lock_to_ready_is_valid_for_abort_path() {
    // Lock -> Ready is permitted so that table transactions can be aborted.
    // The catalog's own status follows the same state machine.
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .transition_status(ObjectStatus::Lock)
        .expect("load->lock is valid");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("lock->ready is valid as an abort path");

    assert_eq!(catalog.status(), ObjectStatus::Ready);
}

#[test]
fn create_table_moves_load_sync_ready() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .create_table("users", TableSchema { fields: Vec::new() })
        .expect("create table should succeed");

    assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
}

#[test]
fn drop_table_removes_registered_table() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_table("users", TableSchema::new(Vec::new()))
        .expect("table register should succeed");

    catalog
        .drop_table("users")
        .expect("drop table should succeed");
    assert!(catalog.table("users").is_none());
}

#[test]
fn write_requires_database_and_table_ready() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .create_table("users", TableSchema { fields: Vec::new() })
        .expect("create table should succeed");

    let denied = catalog.ensure_ready_for_write("users");
    assert!(matches!(denied, Err(DatabaseError::NotReadyForWrite)));

    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let allowed = catalog.ensure_ready_for_write("users");
    assert!(allowed.is_ok());
}

#[test]
fn schema_can_be_retrieved_from_table() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    let schema = TableSchema::new(Vec::new());

    catalog
        .register_table("users", schema.clone())
        .expect("table register should succeed");

    assert_eq!(catalog.table_schema("users"), Some(schema.clone()));
    assert_eq!(catalog.table_schema_revision("users"), Some(0));
}

#[test]
fn schema_change_payload_updates_existing_table() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    catalog
        .register_table("users", TableSchema::new(Vec::new()))
        .expect("table register should succeed");

    let updated_schema = TableSchema::new(Vec::new());
    let payload = SchemaChangePayload {
        table_id: "users".to_string(),
        schema_revision: 3,
        schema_epoch: 1,
        entity_id: None,
        schema: updated_schema.clone(),
    };

    catalog
        .apply_schema_change(payload)
        .expect("schema change should apply");

    assert_eq!(catalog.table_schema("users"), Some(updated_schema.clone()));
    assert_eq!(catalog.table_schema_revision("users"), Some(3));
}

#[test]
fn schema_change_tx_commit_applies_schema_and_returns_ready() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("table should be created");
    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let mut tx = catalog
        .begin_schema_change("users")
        .expect("begin should lock the table");

    assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Lock));

    tx.add_field(crate::engine::database::table::schema::FieldDef {
        seqno: 1,
        field_name: "email".to_string(),
        field_type: crate::engine::database::table::schema::FieldType::Text,
        nullable: false,
        indexed: FieldIndex::Indexed,
        default_value: None,
        metadata: None,
    })
    .expect("add_field should succeed");

    let mut captured_payload: Option<SchemaChangePayload> = None;
    tx.commit::<DatabaseError, _>(&mut catalog, |payload| {
        captured_payload = Some(payload.clone());
        Ok(())
    })
    .expect("commit should succeed");

    assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
    assert_eq!(catalog.table_schema_revision("users"), Some(1));
    assert!(catalog
        .table_schema("users")
        .and_then(|s| s.field("email").cloned())
        .is_some());
    assert_eq!(
        captured_payload.expect("captured payload").schema_revision,
        1
    );
}

#[test]
fn schema_change_tx_abort_returns_table_to_ready_without_schema_change() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let initial_schema = TableSchema::new(vec![crate::engine::database::table::schema::FieldDef {
        seqno: 1,
        field_name: "name".to_string(),
        field_type: crate::engine::database::table::schema::FieldType::Text,
        nullable: false,
        indexed: FieldIndex::None,
        default_value: None,
        metadata: None,
    }]);
    catalog
        .create_table("users", initial_schema.clone())
        .expect("table should be created");
    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let mut tx = catalog
        .begin_schema_change("users")
        .expect("begin should lock the table");
    tx.remove_field("name")
        .expect("remove should succeed on pending schema");

    tx.abort(&mut catalog).expect("abort should release lock");

    assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
    assert_eq!(catalog.table_schema("users"), Some(initial_schema.clone()));
}

#[test]
fn begin_schema_change_rejects_when_another_is_in_progress() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let schema = TableSchema::new(Vec::new());
    catalog
        .create_table("users", schema.clone())
        .expect("users table should be created");
    catalog
        .create_table("accounts", schema)
        .expect("accounts table should be created");

    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let tx = catalog
        .begin_schema_change("users")
        .expect("first schema change should begin");

    let second_attempt = catalog.begin_schema_change("accounts");
    assert!(matches!(
        second_attempt,
        Err(DatabaseError::SchemaChangeInProgress)
    ));

    tx.abort(&mut catalog)
        .expect("abort should release schema lock");

    let retry = catalog.begin_schema_change("accounts");
    assert!(retry.is_ok());
}

#[test]
fn begin_schema_change_records_locked_state() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("users table should be created");
    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let _tx = catalog
        .begin_schema_change("users")
        .expect("begin should lock users");

    let active = catalog
        .active_schema_change()
        .expect("active schema change should be present");
    assert_eq!(active.table_id, "users");
    assert_eq!(active.target_revision, 1);
    assert_eq!(active.phase, super::SchemaChangePhase::Locked);
}

#[test]
fn schema_change_tx_commit_aborts_when_persist_fails() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let initial_schema = TableSchema::new(Vec::new());

    catalog
        .create_table("users", initial_schema.clone())
        .expect("table should be created");

    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");

    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let tx = catalog
        .begin_schema_change("users")
        .expect("begin should lock the table");

    let result = tx.commit::<DatabaseError, _>(&mut catalog, |_payload| {
        Err(DatabaseError::NotReadyForWrite)
    });

    assert!(result.is_err());
    assert_eq!(catalog.table_status("users"), Some(ObjectStatus::Ready));
    assert_eq!(catalog.table_schema("users"), Some(initial_schema.clone()));
}

#[test]
fn schema_change_commit_releases_global_schema_change_guard() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let schema = TableSchema::new(Vec::new());

    catalog
        .create_table("users", schema.clone())
        .expect("users table should be created");

    catalog
        .create_table("accounts", schema)
        .expect("accounts table should be created");

    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");

    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let tx = catalog
        .begin_schema_change("users")
        .expect("begin should lock users");

    tx.commit::<DatabaseError, _>(&mut catalog, |_payload| Ok(()))
        .expect("commit should succeed");

    let next = catalog.begin_schema_change("accounts");

    assert!(next.is_ok());
}

#[test]
fn transition_schema_change_phase_updates_active_state() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("users table should be created");

    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");

    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let _tx = catalog
        .begin_schema_change("users")
        .expect("begin should lock users");

    catalog
        .transition_schema_change_phase("users", super::SchemaChangePhase::Rewriting)
        .expect("phase transition should succeed");

    let phase = catalog
        .active_schema_change()
        .map(|state| state.phase)
        .expect("active schema change should exist");

    assert_eq!(phase, super::SchemaChangePhase::Rewriting);
}

#[test]
fn transition_schema_change_phase_rejects_invalid_order() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("users table should be created");
    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let _tx = catalog
        .begin_schema_change("users")
        .expect("begin should lock users");

    let result = catalog.transition_schema_change_phase("users", super::SchemaChangePhase::Syncing);
    assert!(matches!(
        result,
        Err(DatabaseError::InvalidStatusTransition)
    ));
}

#[test]
fn checkpoint_schema_change_progress_updates_active_state() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("users table should be created");

    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");

    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let _tx = catalog
        .begin_schema_change("users")
        .expect("begin should lock users");

    catalog
        .transition_schema_change_phase("users", super::SchemaChangePhase::Rewriting)
        .expect("phase transition should succeed");

    catalog
        .checkpoint_schema_change_progress("users", 77, Some(1000), Some("pk:users:77".to_string()))
        .expect("progress checkpoint should succeed");

    let active = catalog
        .active_schema_change()
        .expect("active schema change should exist");

    assert_eq!(active.rows_rewritten, 77);
    assert_eq!(active.rows_total, Some(1000));
    assert_eq!(active.resume_token.as_deref(), Some("pk:users:77"));
}

#[test]
fn active_schema_change_state_is_persisted_in_catalog_file() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("users table should be created");

    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");

    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let _tx = catalog
        .begin_schema_change("users")
        .expect("begin should lock users");

    catalog
        .transition_schema_change_phase("users", super::SchemaChangePhase::Rewriting)
        .expect("phase transition should succeed");

    catalog
        .checkpoint_schema_change_progress("users", 12, Some(20), Some("pk:users:12".to_string()))
        .expect("progress checkpoint should succeed");

    let mut dir = std::env::temp_dir();

    dir.push(format!(
        "distdb-catalog-test-{}",
        common::helpers::utils::unique_id()
    ));

    std::fs::create_dir_all(&dir).expect("temp dir should be created");

    catalog
        .save_in_directory(&dir)
        .expect("catalog save should succeed");

    let loaded = DatabaseCatalog::load_from_path(catalog_path_for_test(&catalog, &dir))
        .expect("catalog load should succeed");

    let active = loaded
        .active_schema_change()
        .expect("active schema change should persist");

    assert_eq!(active.table_id, "users");
    assert!(!active.job_id.is_empty());
    assert_eq!(active.phase, super::SchemaChangePhase::Rewriting);
    assert_eq!(active.rows_rewritten, 12);
    assert_eq!(active.rows_total, Some(20));
    assert_eq!(active.resume_token.as_deref(), Some("pk:users:12"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn temporary_tables_are_not_persisted_in_catalog_file() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .create_temporary_table("tmp_users", TableSchema::new(Vec::new()))
        .expect("temporary table should be created");

    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "distdb-catalog-temp-table-test-{}",
        common::helpers::utils::unique_id()
    ));

    std::fs::create_dir_all(&dir).expect("temp dir should be created");

    catalog
        .save_in_directory(&dir)
        .expect("catalog save should succeed");

    let loaded = DatabaseCatalog::load_from_path(catalog_path_for_test(&catalog, &dir))
        .expect("catalog load should succeed");

    assert!(loaded.table("tmp_users").is_none());

    let _ = std::fs::remove_dir_all(&dir);
}

fn catalog_path_for_test(catalog: &DatabaseCatalog, directory: &Path) -> PathBuf {
    directory.join(catalog.file_name())
}

#[test]
fn schema_replay_uses_latest_transaction_payload() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_table("users", TableSchema::new(Vec::new()))
        .expect("table register should succeed");

    let wal = crate::engine::wal::ConcurrentWalManager::new();
    let actor = crate::core::identity::UserId::from_username("schema-tester");

    let first_schema = TableSchema::new(vec![crate::engine::database::table::schema::FieldDef {
        seqno: 1,
        field_name: "name".to_string(),
        field_type: crate::engine::database::table::schema::FieldType::Text,
        nullable: false,
        indexed: FieldIndex::None,
        default_value: None,
        metadata: None,
    }]);

    let first_payload = SchemaChangePayload {
        table_id: "users".to_string(),
        schema_revision: 1,
        schema_epoch: 1,
        entity_id: None,
        schema: first_schema,
    };

    wal.append(
        "users",
        crate::TransactionRecord::with_payload(
            crate::TransactionId(1),
            None,
            None,
            1,
            actor.clone(),
            crate::TransactionKind::SchemaChange,
            first_payload
                .encode()
                .expect("schema payload should encode"),
        ),
    )
    .expect("first schema append should succeed");

    let second_schema = TableSchema::new(vec![crate::FieldDef {
        seqno: 1,
        field_name: "email".to_string(),
        field_type: crate::FieldType::Text,
        nullable: false,
        indexed: FieldIndex::Indexed,
        default_value: None,
        metadata: None,
    }]);

    let second_payload = SchemaChangePayload {
        table_id: "users".to_string(),
        schema_revision: 2,
        schema_epoch: 2,
        entity_id: None,
        schema: second_schema.clone(),
    };

    wal.append(
        "users",
        crate::TransactionRecord::with_payload(
            crate::TransactionId(2),
            None,
            None,
            2,
            actor,
            crate::TransactionKind::SchemaChange,
            second_payload
                .encode()
                .expect("schema payload should encode"),
        ),
    )
    .expect("second schema append should succeed");

    let applied = catalog
        .replay_schema_from_log("users", &wal)
        .expect("schema replay should succeed");

    assert_eq!(applied, 2);
    assert_eq!(catalog.table_schema("users"), Some(second_schema.clone()));
    assert_eq!(catalog.table_schema_revision("users"), Some(2));

    let email_index_id = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string()],
    )
    .index_id
    .0;
    assert!(catalog.index(&email_index_id).is_some());
}

#[test]
fn metadata_and_sql_definition_replay_builds_view_state() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_view(
            "users_view",
            "select id from users",
            TableSchema::new(Vec::new()),
        )
        .expect("view register should succeed");

    let wal = crate::engine::wal::ConcurrentWalManager::new();
    let actor = crate::core::identity::UserId::from_username("view-tester");

    let metadata_payload = EntityMetadataPayload {
        entity_id: "users_view".to_string(),
        metadata: EntityMetadata::default()
            .with_creator("alice")
            .with_created_at(100),
    };

    wal.append(
        "main_db",
        crate::TransactionRecord::with_payload(
            crate::TransactionId(1),
            None,
            None,
            100,
            actor.clone(),
            crate::TransactionKind::MetadataChange,
            metadata_payload
                .encode()
                .expect("metadata payload should encode"),
        ),
    )
    .expect("metadata append should succeed");

    let sql_payload = SqlDefinitionPayload {
        object_id: "users_view".to_string(),
        object_kind: SqlObjectKind::View,
        action: SqlDefinitionAction::Upsert,
        schema_epoch: 1,
        sql: "select id, email from users".to_string(),
        dependencies: vec!["Users".to_string(), "Accounts".to_string()],
    };

    wal.append(
        "main_db",
        crate::TransactionRecord::with_payload(
            crate::TransactionId(2),
            None,
            Some(crate::TransactionId(1)),
            101,
            actor,
            crate::TransactionKind::SqlDefinitionChange,
            sql_payload.encode().expect("sql payload should encode"),
        ),
    )
    .expect("sql append should succeed");

    let applied = catalog
        .replay_entity_construction_from_log("main_db", &wal)
        .expect("replay should succeed");
    assert_eq!(applied, 2);

    let view = catalog.view("users_view").expect("view should exist");
    assert_eq!(view.metadata.created_by.as_deref(), Some("alice"));
    assert_eq!(view.metadata.created_at_epoch_ms, Some(100));
    assert_eq!(view.sql, "select id, email from users");
    assert_eq!(view.dependencies, vec!["users", "accounts"]);
}

#[test]
fn table_lifecycle_replay_honors_create_then_drop() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let wal = crate::engine::wal::ConcurrentWalManager::new();
    let actor = crate::core::identity::UserId::from_username("table-lifecycle");

    let create_payload = TableLifecyclePayload {
        table_id: "users".to_string(),
        action: TableLifecycleAction::Create,
        schema_epoch: 1,
        entity_id: None,
        schema: Some(TableSchema::new(Vec::new())),
    };

    wal.append(
        "main_db",
        crate::TransactionRecord::with_payload(
            crate::TransactionId(1),
            None,
            None,
            1,
            actor.clone(),
            crate::TransactionKind::TableLifecycle,
            create_payload
                .encode()
                .expect("table create payload should encode"),
        ),
    )
    .expect("create lifecycle append should succeed");

    let drop_payload = TableLifecyclePayload {
        table_id: "users".to_string(),
        action: TableLifecycleAction::Drop,
        schema_epoch: 2,
        entity_id: None,
        schema: None,
    };

    wal.append(
        "main_db",
        crate::TransactionRecord::with_payload(
            crate::TransactionId(2),
            None,
            Some(crate::TransactionId(1)),
            2,
            actor,
            crate::TransactionKind::TableLifecycle,
            drop_payload
                .encode()
                .expect("table drop payload should encode"),
        ),
    )
    .expect("drop lifecycle append should succeed");

    let applied = catalog
        .replay_entity_construction_from_log("main_db", &wal)
        .expect("replay should succeed");

    assert_eq!(applied, 2);
    assert!(catalog.table("users").is_none());
}

#[test]
fn index_lifecycle_replay_recreates_user_defined_index() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let wal = crate::engine::wal::ConcurrentWalManager::new();
    let actor = crate::core::identity::UserId::from_username("index-lifecycle");

    let create_table_payload = TableLifecyclePayload {
        table_id: "users".to_string(),
        action: TableLifecycleAction::Create,
        schema_epoch: 1,
        entity_id: None,
        schema: Some(TableSchema::new(vec![
            FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::UInt(64),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            },
            FieldDef {
                seqno: 2,
                field_name: "email".to_string(),
                field_type: FieldType::Text,
                nullable: false,
                indexed: FieldIndex::None,
                default_value: None,
                metadata: None,
            },
        ])),
    };

    wal.append(
        "main_db",
        crate::TransactionRecord::with_payload(
            crate::TransactionId(1),
            None,
            None,
            1,
            actor.clone(),
            crate::TransactionKind::TableLifecycle,
            create_table_payload
                .encode()
                .expect("table payload should encode"),
        ),
    )
    .expect("table lifecycle append should succeed");

    let index = DatabaseIndex::from_table_fields_with_origin(
        "users",
        crate::engine::database::index::DatabaseIndexKind::Indexed,
        crate::engine::database::index::DatabaseIndexOrigin::UserDefined,
        None,
        vec!["email".to_string()],
    );

    let index_payload = crate::engine::database::index_lifecycle_payload::IndexLifecyclePayload {
        table_id: "users".to_string(),
        index_id: "idx_users_email".to_string(),
        action: crate::engine::database::index_lifecycle_payload::IndexLifecycleAction::Create,
        schema_epoch: 2,
        index: Some(DatabaseIndex {
            index_id: crate::engine::database::index_id::IndexId("idx_users_email".to_string()),
            ..index
        }),
    };

    let encoded_index_payload = index_payload
        .encode()
        .expect("index payload should encode");

    let decoded_index_payload =
        crate::engine::database::index_lifecycle_payload::IndexLifecyclePayload::decode(
            &encoded_index_payload,
        )
        .expect("index payload should decode");

    assert!(decoded_index_payload.index.is_some());

    wal.append(
        "main_db",
        crate::TransactionRecord::with_payload(
            crate::TransactionId(2),
            None,
            Some(crate::TransactionId(1)),
            2,
            actor,
            crate::TransactionKind::IndexLifecycle,
            encoded_index_payload,
        ),
    )
    .expect("index lifecycle append should succeed");

    let applied = catalog
        .replay_entity_construction_from_log("main_db", &wal)
        .expect("replay should succeed");

    assert_eq!(applied, 2);
    assert!(catalog.index_in_table("users", "idx_users_email").is_some());
}

#[test]
fn apply_schema_change_preserves_user_defined_indexes() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let schema = TableSchema::new(vec![
        FieldDef {
            seqno: 1,
            field_name: "id".to_string(),
            field_type: FieldType::UInt(64),
            nullable: false,
            indexed: FieldIndex::PrimaryKey,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 2,
            field_name: "email".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ]);

    catalog
        .create_table("users", schema.clone())
        .expect("table should be created");

    catalog
        .create_index("users", Some("idx_users_email"), vec!["email".to_string()])
        .expect("index should be created");

    let before = catalog
        .index_in_table("users", "idx_users_email")
        .expect("user-defined index should exist");

    let payload = SchemaChangePayload {
        table_id: "users".to_string(),
        schema_revision: 1,
        schema_epoch: catalog.schema_epoch().saturating_add(1),
        entity_id: None,
        schema,
    };

    catalog
        .apply_schema_change(payload)
        .expect("schema change should apply");

    let after = catalog
        .index_in_table("users", "idx_users_email")
        .expect("user-defined index should survive schema change");

    assert_eq!(after.field_names, before.field_names);
}

#[test]
fn trigger_and_procedure_registration_and_updates_work() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_trigger(
            "audit_insert",
            "create trigger audit_insert before insert on users for each row set @x = 1",
            vec!["Users".to_string()],
        )
        .expect("trigger register should succeed");

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin select 1; end",
            vec!["Accounts".to_string()],
        )
        .expect("procedure register should succeed");

    catalog
        .set_sql_definition(
            "audit_insert",
            SqlObjectKind::Trigger,
            "create trigger audit_insert before insert on users for each row set @x = 2",
            vec!["users".to_string(), "logs".to_string()],
        )
        .expect("trigger sql update should succeed");

    catalog
        .set_sql_definition(
            "refresh_accounts",
            SqlObjectKind::StoredProcedure,
            "create procedure refresh_accounts() begin select 2; end",
            vec!["accounts".to_string(), "users".to_string()],
        )
        .expect("procedure sql update should succeed");

    catalog
        .set_entity_metadata(
            "audit_insert",
            EntityMetadata::default().with_creator("ops"),
        )
        .expect("metadata update should succeed");

    let trigger = catalog
        .trigger("audit_insert")
        .expect("trigger should exist");
    assert_eq!(trigger.dependencies, vec!["users", "logs"]);
    assert_eq!(trigger.metadata.created_by.as_deref(), Some("ops"));

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist");
    assert_eq!(procedure.dependencies, vec!["accounts", "users"]);

    assert_eq!(catalog.trigger_ids(), vec!["audit_insert".to_string()]);
    assert_eq!(
        catalog.stored_procedure_ids(),
        vec!["refresh_accounts".to_string()]
    );
}

#[test]
fn stored_procedure_caches_if_else_end_plan_on_register_and_update() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin if active = 1 then select 'on'; else select 'off'; end if; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist");
    assert!(procedure.if_else_end_plan().is_some());

    catalog
        .set_sql_definition(
            "refresh_accounts",
            SqlObjectKind::StoredProcedure,
            "create procedure refresh_accounts() begin select 1; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure sql update should succeed");

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist after update");
    assert!(procedure.if_else_end_plan().is_none());
}

#[test]
fn drop_helpers_remove_sql_backed_entities() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_view(
            "users_view",
            "select * from users",
            TableSchema::new(Vec::new()),
        )
        .expect("view register should succeed");

    catalog
        .register_trigger(
            "audit_insert",
            "create trigger audit_insert before insert on users for each row set @x = 1",
            vec!["users".to_string()],
        )
        .expect("trigger register should succeed");

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin select 1; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    catalog
        .drop_view("users_view")
        .expect("view drop should succeed");

    catalog
        .drop_trigger("audit_insert")
        .expect("trigger drop should succeed");

    catalog
        .drop_stored_procedure("refresh_accounts")
        .expect("procedure drop should succeed");

    assert!(catalog.view("users_view").is_none());
    assert!(catalog.trigger("audit_insert").is_none());
    assert!(catalog.stored_procedure("refresh_accounts").is_none());
}

#[test]
fn drop_object_removes_entity_from_catalog() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_table("users", TableSchema::new(Vec::new()))
        .expect("table register should succeed");
    catalog
        .register_view(
            "users_view",
            "select * from users",
            TableSchema::new(Vec::new()),
        )
        .expect("view register should succeed");
    catalog
        .register_trigger(
            "audit_insert",
            "create trigger audit_insert before insert on users for each row set @x = 1",
            vec!["users".to_string()],
        )
        .expect("trigger register should succeed");
    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin select 1; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    catalog
        .drop_object(DatabaseObjectType::Table, "users")
        .expect("table drop should succeed");
    catalog
        .drop_object(DatabaseObjectType::View, "users_view")
        .expect("view drop should succeed");
    catalog
        .drop_object(DatabaseObjectType::Trigger, "audit_insert")
        .expect("trigger drop should succeed");
    catalog
        .drop_object(DatabaseObjectType::StoredProcedure, "refresh_accounts")
        .expect("procedure drop should succeed");

    assert!(catalog.table("users").is_none());
    assert!(catalog.view("users_view").is_none());
    assert!(catalog.trigger("audit_insert").is_none());
    assert!(catalog.stored_procedure("refresh_accounts").is_none());
}

#[test]
fn schema_epoch_advances_for_object_lifecycle_mutations() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    assert_eq!(catalog.schema_epoch(), 0);

    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("table create should succeed");
    assert_eq!(catalog.schema_epoch(), 1);

    catalog
        .register_view(
            "users_view",
            "select * from users",
            TableSchema::new(Vec::new()),
        )
        .expect("view register should succeed");
    assert_eq!(catalog.schema_epoch(), 2);

    catalog
        .drop_object(DatabaseObjectType::View, "users_view")
        .expect("view drop should succeed");
    assert_eq!(catalog.schema_epoch(), 3);
}

#[test]
fn schema_epoch_advances_for_schema_change_and_sql_update() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_table("users", TableSchema::new(Vec::new()))
        .expect("table register should succeed");
    let baseline_epoch = catalog.schema_epoch();

    catalog
        .apply_schema_change(SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 1,
            schema_epoch: baseline_epoch + 1,
            entity_id: None,
            schema: TableSchema::new(Vec::new()),
        })
        .expect("schema change should succeed");

    assert_eq!(catalog.schema_epoch(), baseline_epoch + 1);

    catalog
        .register_trigger(
            "audit_insert",
            "create trigger audit_insert before insert on users for each row set @x = 1",
            vec!["users".to_string()],
        )
        .expect("trigger register should succeed");

    let trigger_epoch = catalog.schema_epoch();
    catalog
        .set_sql_definition(
            "audit_insert",
            SqlObjectKind::Trigger,
            "create trigger audit_insert before insert on users for each row set @x = 2",
            vec!["users".to_string()],
        )
        .expect("trigger sql update should succeed");

    assert_eq!(catalog.schema_epoch(), trigger_epoch + 1);
}

#[test]
fn entity_aspects_expose_status_and_wal_stream() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_table("users", TableSchema::new(Vec::new()))
        .expect("table register should succeed");

    catalog
        .register_view(
            "users_view",
            "select * from users",
            TableSchema::new(Vec::new()),
        )
        .expect("view register should succeed");

    catalog
        .register_relationship(DatabaseRelationship::new(
            "users".to_string(),
            "accounts".to_string(),
            "owns".to_string(),
        ))
        .expect("relationship register should succeed");

    let users_entity = catalog.entity("users").expect("users entity should exist");
    let users_view_entity = catalog.entity("users_view").expect("users_view entity should exist");
    let relationship_entity = catalog
        .entity("rel:users:accounts:owns")
        .expect("relationship entity should exist");
    assert!(!users_entity.storage_key().is_empty());
    assert!(!users_view_entity.storage_key().is_empty());
    assert!(!relationship_entity.storage_key().is_empty());

    assert_eq!(catalog.entity_status("users"), Some(ObjectStatus::Load));
    assert_eq!(
        catalog.entity_wal_stream_id("users"),
        Some(format!(
            "{}:{}",
            catalog.database_id.0,
            users_entity.storage_key()
        ))
    );
    assert_eq!(catalog.entity_schema_revision("users"), Some(0));

    assert_eq!(
        catalog.entity_wal_stream_id("users_view"),
        Some(users_view_entity.storage_key())
    );
    assert_eq!(catalog.entity_schema_revision("users_view"), None);

    assert_eq!(
        catalog.entity_wal_stream_id("rel:users:accounts:owns"),
        Some(relationship_entity.storage_key())
    );
}

#[test]
fn normalize_loaded_entities_rekeys_and_rebuilds_indexes() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let schema = TableSchema::new(vec![crate::FieldDef {
        seqno: 1,
        field_name: "UserId".to_string(),
        field_type: crate::FieldType::UInt(64),
        nullable: false,
        indexed: FieldIndex::Indexed,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("Users", schema)
        .expect("table register should succeed");

    let legacy_key = catalog
        .entity("users")
        .expect("expected normalized table entry")
        .storage_key();
    let mut entity = catalog
        .entity(&legacy_key)
        .expect("expected normalized table entry");

    match &mut entity {
        DatabaseEntity::Table(table) => table.entity_id.clear(),
        _ => unreachable!("expected table entity"),
    }

    let mut legacy_entities = HashMap::new();
    legacy_entities.insert("Users".to_string(), entity);

    catalog
        .normalize_loaded_entities(legacy_entities)
        .expect("normalization should succeed");

    assert!(!catalog.entity_handles.contains_key("users"));
    assert!(!catalog.entity_handles.contains_key("Users"));
    assert_eq!(catalog.table("users").expect("table should exist").table_id, "users");
    let user_id_index_id = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["userid".to_string()],
    )
    .index_id
    .0;
    assert!(catalog.index(&user_id_index_id).is_some());
}

#[test]
fn object_accessor_routes_all_supported_types() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let schema = TableSchema::new(vec![crate::FieldDef {
        seqno: 1,
        field_name: "email".to_string(),
        field_type: crate::FieldType::Text,
        nullable: false,
        indexed: FieldIndex::Indexed,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema.clone())
        .expect("table register should succeed");
    catalog
        .register_view("users_view", "select * from users", schema)
        .expect("view register should succeed");
    catalog
        .register_relationship(DatabaseRelationship::new(
            "users".to_string(),
            "accounts".to_string(),
            "owns".to_string(),
        ))
        .expect("relationship register should succeed");
    catalog
        .register_trigger(
            "audit_insert",
            "create trigger audit_insert before insert on users for each row set @x = 1",
            vec!["users".to_string()],
        )
        .expect("trigger register should succeed");
    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin select 1; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    assert!(matches!(
        catalog.object(DatabaseObjectType::Table, "users"),
        Some(DatabaseObjectRef::Table(_))
    ));

    assert!(matches!(
        catalog.object(DatabaseObjectType::View, "users_view"),
        Some(DatabaseObjectRef::View(_))
    ));

    assert!(matches!(
        catalog.object(DatabaseObjectType::Relationship, "rel:users:accounts:owns"),
        Some(DatabaseObjectRef::Relationship(_))
    ));

    assert!(matches!(
        catalog.object(DatabaseObjectType::Trigger, "audit_insert"),
        Some(DatabaseObjectRef::Trigger(_))
    ));

    assert!(matches!(
        catalog.object(DatabaseObjectType::StoredProcedure, "refresh_accounts"),
        Some(DatabaseObjectRef::StoredProcedure(_))
    ));

    let email_index_id = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string()],
    )
    .index_id
    .0;

    assert!(matches!(
        catalog.object(DatabaseObjectType::Index, &email_index_id),
        Some(DatabaseObjectRef::Index(_))
    ));
}

#[test]
fn object_by_index_returns_untyped_object_by_id() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let schema = TableSchema::new(vec![crate::FieldDef {
        seqno: 1,
        field_name: "email".to_string(),
        field_type: crate::FieldType::Text,
        nullable: false,
        indexed: FieldIndex::Indexed,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema.clone())
        .expect("table register should succeed");

    catalog
        .register_view("users_view", "select * from users", schema)
        .expect("view register should succeed");

    catalog
        .register_relationship(DatabaseRelationship::new(
            "users".to_string(),
            "accounts".to_string(),
            "owns".to_string(),
        ))
        .expect("relationship register should succeed");

    assert!(matches!(
        catalog.object_by_id("users"),
        Some(DatabaseObjectRef::Table(_))
    ));
    assert!(matches!(
        catalog.object_by_id("users_view"),
        Some(DatabaseObjectRef::View(_))
    ));
    assert!(matches!(
        catalog.object_by_id("rel:users:accounts:owns"),
        Some(DatabaseObjectRef::Relationship(_))
    ));

    catalog
        .register_trigger(
            "audit_insert",
            "create trigger audit_insert before insert on users for each row set @x = 1",
            vec!["users".to_string()],
        )
        .expect("trigger register should succeed");

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin select 1; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    assert!(matches!(
        catalog.object_by_id("audit_insert"),
        Some(DatabaseObjectRef::Trigger(_))
    ));

    assert!(matches!(
        catalog.object_by_id("refresh_accounts"),
        Some(DatabaseObjectRef::StoredProcedure(_))
    ));

    let email_index_id = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string()],
    )
    .index_id
    .0;

    assert!(matches!(
        catalog.object_by_id(&email_index_id),
        Some(DatabaseObjectRef::Index(_))
    ));

    assert!(catalog.object_by_id("missing_object").is_none());
}

#[test]
fn index_in_table_scopes_lookup_to_target_table() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    let schema = TableSchema::new(vec![crate::FieldDef {
        seqno: 1,
        field_name: "email".to_string(),
        field_type: crate::FieldType::Text,
        nullable: false,
        indexed: FieldIndex::Indexed,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table register should succeed");
    catalog
        .register_table("admins", schema)
        .expect("admins table register should succeed");

    let users_email_index_id = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string()],
    )
    .index_id
    .0;

    assert!(catalog.index_in_table("users", &users_email_index_id).is_some());
    assert!(catalog.index_in_table("admins", &users_email_index_id).is_none());
}
