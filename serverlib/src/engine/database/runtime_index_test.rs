use std::collections::HashMap;

use ahash::AHashSet;

use super::*;
use crate::{
    DatabaseIndex, DatabaseIndexKind, DatabaseIndexOrigin, DatabaseTable, IndexId, TableSchema,
};

#[test]
fn runtime_index_state_tracks_membership_and_rebuilds() {

    let mut state = RuntimeIndexState::new();
    let first_key = vec![b"alpha".to_vec()];
    let second_key = vec![b"beta".to_vec()];
    let rebuilt_key = vec![b"gamma".to_vec()];

    assert_eq!(state.cardinality(), 0);
    assert!(!state.contains(&first_key));

    state.insert(first_key.clone());
    state.insert(second_key.clone());

    assert!(state.contains(&first_key));
    assert!(state.contains(&second_key));
    assert_eq!(state.cardinality(), 2);

    state.remove(&first_key);

    assert!(!state.contains(&first_key));
    assert_eq!(state.cardinality(), 1);

    let mut rebuilt = AHashSet::new();
    rebuilt.insert(rebuilt_key.clone());
    state.rebuild(rebuilt);

    assert!(state.contains(&rebuilt_key));
    assert_eq!(state.cardinality(), 1);

}

#[test]
fn index_value_tuple_uses_field_names_and_empty_fallbacks() {

    let multi_field_index = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string(), "tenant_id".to_string()],
    );

    let row_map = HashMap::from([
        ("email".to_string(), b"alice@example.com".to_vec()),
    ]);

    assert_eq!(
        index_value_tuple(&multi_field_index, &row_map),
        vec![b"alice@example.com".to_vec(), Vec::new()],
    );

    let fallback_index = DatabaseIndex {
        index_id: IndexId("ind:users:email".to_string()),
        table_id: "users".to_string(),
        kind: DatabaseIndexKind::Indexed,
        origin: DatabaseIndexOrigin::Derived,
        temp_id: None,
        field_names: Vec::new(),
        field_name: "email".to_string(),
    };

    assert_eq!(
        index_value_tuple(&fallback_index, &row_map),
        vec![b"alice@example.com".to_vec()],
    );

}

#[test]
fn parsed_allowlist_entries_are_trimmed_normalized_and_deduplicated() {

    let entries = parse_runtime_index_allowlist_entries(" User_Id , email, , USER_id , tenant_id ");

    assert_eq!(entries.len(), 3);
    assert!(entries.contains("user_id"));
    assert!(entries.contains("email"));
    assert!(entries.contains("tenant_id"));

}

#[test]
fn derived_indexes_for_table_and_primary_key_index_prefer_expected_entries() {

    let derived_index = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string()],
    );

    let temporary_index = DatabaseIndex::temporary(
        "users",
        DatabaseIndexKind::Indexed,
        "tmp-1",
        vec!["session_token".to_string()],
    );
    
    let primary_key_like_index = DatabaseIndex {
        index_id: IndexId("pri:users:id".to_string()),
        table_id: "users".to_string(),
        kind: DatabaseIndexKind::Indexed,
        origin: DatabaseIndexOrigin::Derived,
        temp_id: None,
        field_names: vec!["id".to_string()],
        field_name: "id".to_string(),
    };

    let mut indexes = HashMap::new();
    indexes.insert(derived_index.index_id.0.clone(), derived_index.clone());
    indexes.insert(temporary_index.index_id.0.clone(), temporary_index.clone());
    indexes.insert(
        primary_key_like_index.index_id.0.clone(),
        primary_key_like_index.clone(),
    );

    let table = DatabaseTable::new("users".to_string(), TableSchema::new(Vec::new()), indexes);

    let derived_indexes = derived_indexes_for_table(&table).collect::<Vec<_>>();
    assert_eq!(derived_indexes.len(), 2);
    assert!(derived_indexes.iter().any(|index| index.index_id == derived_index.index_id));
    assert!(derived_indexes.iter().any(|index| index.index_id == primary_key_like_index.index_id));
    assert!(!derived_indexes.iter().any(|index| index.index_id == temporary_index.index_id));

    let primary_key_index = primary_key_index(&table).expect("primary key fallback index");
    assert_eq!(primary_key_index.index_id, primary_key_like_index.index_id);

}

#[test]
fn runtime_index_store_can_remove_scoped_index_and_table_indexes() {
    let mut store = RuntimeIndexStore {
        indexes: AHashMap::new(),
        materialize_non_primary: true,
        non_primary_field_allowlist: AHashSet::new(),
        non_primary_index_allowlist: AHashSet::new(),
        incremental_persist_last_saved_ms: AHashMap::new(),
    };

    let users_email = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string()],
    );

    let users_tenant = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["tenant_id".to_string()],
    );

    let orders_ref = DatabaseIndex::from_table_fields(
        "orders",
        DatabaseIndexKind::Indexed,
        vec!["order_ref".to_string()],
    );

    store.register_index_for_table("users_stream", users_email.clone());
    store.register_index_for_table("users_stream", users_tenant.clone());
    store.register_index_for_table("orders_stream", orders_ref.clone());

    assert!(store
        .index_for_table("users_stream", &users_email.index_id.0)
        .is_some());
    assert!(store
        .index_for_table("users_stream", &users_tenant.index_id.0)
        .is_some());
    assert!(store
        .index_for_table("orders_stream", &orders_ref.index_id.0)
        .is_some());

    store.remove_index_for_table("users_stream", &users_email.index_id.0);

    assert!(store
        .index_for_table("users_stream", &users_email.index_id.0)
        .is_none());
    assert!(store
        .index_for_table("users_stream", &users_tenant.index_id.0)
        .is_some());

    store.remove_table_indexes("users_stream");

    assert!(store
        .index_for_table("users_stream", &users_tenant.index_id.0)
        .is_none());
    assert!(store
        .index_for_table("orders_stream", &orders_ref.index_id.0)
        .is_some());
}

#[test]
fn runtime_index_policy_keeps_unique_indexes_and_skips_non_unique_by_default() {
    let store = RuntimeIndexStore {
        indexes: AHashMap::new(),
        materialize_non_primary: false,
        non_primary_field_allowlist: AHashSet::new(),
        non_primary_index_allowlist: AHashSet::new(),
        incremental_persist_last_saved_ms: AHashMap::new(),
    };

    let primary = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::PrimaryKey,
        vec!["id".to_string()],
    );
    let unique = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Unique,
        vec!["email".to_string()],
    );
    let indexed = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["created_at".to_string()],
    );

    assert!(store.should_track_index(&primary));
    assert!(store.should_track_index(&unique));
    assert!(!store.should_track_index(&indexed));
}
