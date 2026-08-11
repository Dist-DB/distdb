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

    store.register_index_for_table("users_stream", &users_email);
    store.register_index_for_table("users_stream", &users_tenant);
    store.register_index_for_table("orders_stream", &orders_ref);

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

#[test]
fn runtime_index_store_batch_record_and_remove_restores_cardinality() {
    let mut store = RuntimeIndexStore {
        indexes: AHashMap::new(),
        materialize_non_primary: true,
        non_primary_field_allowlist: AHashSet::new(),
        non_primary_index_allowlist: AHashSet::new(),
        incremental_persist_last_saved_ms: AHashMap::new(),
    };

    let by_email = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string()],
    );

    let table_scope_id = "users_stream";
    store.register_index_for_table(table_scope_id, &by_email);

    let row_a = HashMap::from([("email".to_string(), b"a@example.com".to_vec())]);
    let row_b = HashMap::from([("email".to_string(), b"b@example.com".to_vec())]);

    let row_maps = vec![row_a, row_b];
    let indexes = vec![&by_email];

    store.record_table_rows_batch(table_scope_id, &indexes, &row_maps);

    assert_eq!(
        store.cardinality_for_table(table_scope_id, &by_email.index_id.0),
        Some(2),
    );

    store.remove_table_rows_batch(table_scope_id, &indexes, &row_maps);

    assert_eq!(
        store.cardinality_for_table(table_scope_id, &by_email.index_id.0),
        Some(0),
    );
}

#[test]
fn runtime_index_store_keeps_row_refs_for_unique_indexes_only() {
    let mut store = RuntimeIndexStore {
        indexes: AHashMap::new(),
        materialize_non_primary: true,
        non_primary_field_allowlist: AHashSet::new(),
        non_primary_index_allowlist: AHashSet::new(),
        incremental_persist_last_saved_ms: AHashMap::new(),
    };

    let primary = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::PrimaryKey,
        vec!["id".to_string()],
    );

    let non_unique = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["city".to_string()],
    );

    let table_scope_id = "users_stream";
    store.register_index_for_table(table_scope_id, &primary);
    store.register_index_for_table(table_scope_id, &non_unique);

    let row = HashMap::from([
        ("id".to_string(), b"42".to_vec()),
        ("city".to_string(), b"berlin".to_vec()),
    ]);

    store.record_row_for_table(table_scope_id, &primary, &row, Some(101));
    store.record_row_for_table(table_scope_id, &non_unique, &row, Some(202));

    let primary_key = vec![b"42".to_vec()];
    let city_key = vec![b"berlin".to_vec()];

    let primary_state = store
        .index_for_table(table_scope_id, &primary.index_id.0)
        .expect("primary state");
    assert_eq!(primary_state.row_ref(&primary_key), Some(101));

    let non_unique_state = store
        .index_for_table(table_scope_id, &non_unique.index_id.0)
        .expect("non-unique state");
    assert_eq!(non_unique_state.row_ref(&city_key), None);
}

#[test]
fn runtime_index_state_range_scan_returns_non_unique_row_refs_with_bounds() {
    let mut state = RuntimeIndexState::new();
    state.index = Some(DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["city".to_string()],
    ));

    state.insert_with_row_ref(vec![b"athens".to_vec()], Some(13));
    state.insert_with_row_ref(vec![b"athens".to_vec()], Some(7));
    state.insert_with_row_ref(vec![b"berlin".to_vec()], Some(21));
    state.insert_with_row_ref(vec![b"cancun".to_vec()], Some(8));

    let lower = RuntimeIndexRangeBound {
        key: vec![b"athens".to_vec()],
        inclusive: true,
    };

    let upper = RuntimeIndexRangeBound {
        key: vec![b"cancun".to_vec()],
        inclusive: false,
    };

    assert_eq!(
        state.row_refs_for_key_range(Some(&lower), Some(&upper), None),
        vec![7, 13, 21],
    );

    state.remove_with_row_ref(&[b"berlin".to_vec()], Some(21));

    assert_eq!(
        state.row_refs_for_key_range(Some(&lower), Some(&upper), None),
        vec![7, 13],
    );
}

#[test]
fn runtime_index_state_range_scan_returns_unique_row_refs_with_limit() {
    let mut state = RuntimeIndexState::new();
    state.index = Some(DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Unique,
        vec!["email".to_string()],
    ));

    state.insert_with_row_ref(vec![b"a@x.io".to_vec()], Some(301));
    state.insert_with_row_ref(vec![b"b@x.io".to_vec()], Some(302));
    state.insert_with_row_ref(vec![b"c@x.io".to_vec()], Some(303));

    let lower = RuntimeIndexRangeBound {
        key: vec![b"a@x.io".to_vec()],
        inclusive: true,
    };

    let upper = RuntimeIndexRangeBound {
        key: vec![b"c@x.io".to_vec()],
        inclusive: true,
    };

    assert_eq!(
        state.row_refs_for_key_range(Some(&lower), Some(&upper), Some(2)),
        vec![301, 302],
    );
}

#[test]
fn runtime_index_state_btree_probe_paged_returns_candidate_rows_in_key_order_window() {
    let mut state = RuntimeIndexState::new();
    state.index = Some(DatabaseIndex::from_table_fields(
        "places",
        DatabaseIndexKind::Indexed,
        vec!["display_name".to_string()],
    ));

    state.insert_with_row_ref(vec![b"cologne".to_vec()], Some(10));
    state.insert_with_row_ref(vec![b"neuss".to_vec()], Some(20));
    state.insert_with_row_ref(vec![b"neuss".to_vec()], Some(21));
    state.insert_with_row_ref(vec![b"sulz".to_vec()], Some(30));

    let probes = vec![vec![b"neu".to_vec()]];

    assert_eq!(
        state.row_refs_for_probe_keys_paged(&probes, 2, 1, None),
        vec![20, 21, 30],
    );

    assert_eq!(
        state.row_refs_for_probe_keys_paged(&probes, 2, 2, Some(2)),
        vec![20, 21],
    );
}

#[test]
fn runtime_index_state_btree_probe_paged_can_accumulate_multiple_pages() {
    let mut state = RuntimeIndexState::new();
    state.index = Some(DatabaseIndex::from_table_fields(
        "places",
        DatabaseIndexKind::Indexed,
        vec!["display_name".to_string()],
    ));

    state.insert_with_row_ref(vec![b"frankfurt".to_vec()], Some(1));
    state.insert_with_row_ref(vec![b"frechen".to_vec()], Some(2));
    state.insert_with_row_ref(vec![b"freiburg".to_vec()], Some(3));
    state.insert_with_row_ref(vec![b"freising".to_vec()], Some(4));
    state.insert_with_row_ref(vec![b"fremont".to_vec()], Some(5));

    let probes = vec![vec![b"frank".to_vec()]];

    assert_eq!(
        state.row_refs_for_probe_keys_paged(&probes, 2, 1, None),
        vec![2, 5],
    );

    assert_eq!(
        state.row_refs_for_probe_keys_paged(&probes, 2, 3, None),
        vec![1, 2, 3, 4, 5],
    );
}

#[test]
fn clone_for_selected_non_unique_indexes_skips_states_without_postings() {
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = TableSchema::new(vec![
        crate::FieldDef {
            field_name: "id".to_string(),
            seqno: 1,
            field_type: crate::FieldType::UInt(64),
            indexed: crate::FieldIndex::PrimaryKey,
            nullable: false,
            default_value: None,
            metadata: None,
        },
        crate::FieldDef {
            field_name: "display_name".to_string(),
            seqno: 2,
            field_type: crate::FieldType::Text,
            indexed: crate::FieldIndex::Indexed,
            nullable: false,
            default_value: None,
            metadata: None,
        },
    ]);

    catalog
        .register_table("places", schema)
        .expect("places table should register");

    let table = catalog.table("places").expect("places table should exist");
    let display_name_index = table
        .indexes
        .values()
        .find(|index| index.field_names == vec!["display_name".to_string()])
        .cloned()
        .expect("display_name index should exist");

    let table_stream_id = catalog
        .entity_wal_stream_id("places")
        .unwrap_or_else(|| "places".to_string());

    let mut store = RuntimeIndexStore::new();
    let state = store.index_mut_for_table(&table_stream_id, &display_name_index.index_id.0);
    state.index = Some(display_name_index.clone());
    state.insert(vec![b"neuss".to_vec()]);

    assert!(!state.has_row_ref_postings());

    let catalogs = HashMap::from([(catalog.database_id.0.clone(), catalog)]);
    let table_ids = HashSet::from(["places".to_string()]);
    let selected_fields = HashMap::from([(
        "places".to_string(),
        HashSet::from(["display_name".to_string()]),
    )]);

    let scoped = store.clone_for_tables_unique_and_selected_single_field_indexes(
        &catalogs,
        &table_ids,
        &selected_fields,
    );

    assert!(
        scoped
            .index_for_table(&table_stream_id, &display_name_index.index_id.0)
            .is_none(),
        "selected non-unique index without postings should not be cloned",
    );
}
