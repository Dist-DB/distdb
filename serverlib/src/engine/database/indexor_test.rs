use super::{
    DatabaseIndexor, IndexorDefinitionError, IndexorInsertError,
    IndexorRangeBound, IndexorSnapshot, IndexorSnapshotError, IndexorStorageSnapshot,
    IndexorEntrySnapshot,
};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::{DatabaseIndex, DatabaseIndexKind};

#[derive(Default)]
struct BaselineHashIndex {
    postings: HashMap<Vec<u8>, HashSet<u64>>,
}

impl BaselineHashIndex {
    fn insert(&mut self, key: &[u8], row_id: u64) {
        self.postings.entry(key.to_vec()).or_default().insert(row_id);
    }

    fn has_hit(&self, key: &[u8]) -> bool {
        self.postings.contains_key(key)
    }

    fn query_eq(&self, key: &[u8]) -> Vec<u64> {
        let mut rows = self
            .postings
            .get(key)
            .map(|ids| ids.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();

        rows.sort_unstable();
        rows
    }

    fn query_range(
        &self,
        lower: Option<&IndexorRangeBound>,
        upper: Option<&IndexorRangeBound>,
    ) -> Vec<u64> {
        let mut row_set = HashSet::new();

        for (key, ids) in &self.postings {
            if !within_bounds(key, lower, upper) {
                continue;
            }

            row_set.extend(ids.iter().copied());
        }

        let mut rows = row_set.into_iter().collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    }
}

fn within_bounds(
    key: &[u8],
    lower: Option<&IndexorRangeBound>,
    upper: Option<&IndexorRangeBound>,
) -> bool {
    if let Some(lower) = lower {
        let cmp = key.cmp(lower.key.as_slice());
        if (lower.inclusive && cmp.is_lt()) || (!lower.inclusive && !cmp.is_gt()) {
            return false;
        }
    }

    if let Some(upper) = upper {
        let cmp = key.cmp(upper.key.as_slice());
        if (upper.inclusive && cmp.is_gt()) || (!upper.inclusive && !cmp.is_lt()) {
            return false;
        }
    }

    true
}

fn key_for(n: usize) -> Vec<u8> {
    format!("{:08}", n).into_bytes()
}

#[test]
fn indexor_keeps_each_index_storage_isolated() {
    let mut indexor = DatabaseIndexor::new();

    indexor
        .insert("places_lat", b"50.9300", 11)
        .expect("insert should succeed");
    indexor
        .insert("places_lon", b"6.9500", 11)
        .expect("insert should succeed");
    indexor
        .insert("places_lat", b"50.9400", 12)
        .expect("insert should succeed");

    assert_eq!(indexor.query_eq("places_lat", b"50.9300"), vec![11]);
    assert_eq!(indexor.query_eq("places_lon", b"50.9300"), Vec::<u64>::new());
    assert_eq!(indexor.query_eq("places_lon", b"6.9500"), vec![11]);
    assert_eq!(indexor.index_count(), 2);
}

#[test]
fn indexor_hit_and_miss_detection_is_constant_path() {
    let mut indexor = DatabaseIndexor::new();

    indexor
        .insert("users_email", b"sam@example.com", 1001)
        .expect("insert should succeed");

    assert!(indexor.has_hit("users_email", b"sam@example.com"));
    assert!(!indexor.has_hit("users_email", b"alex@example.com"));
    assert!(!indexor.has_hit("users_name", b"sam@example.com"));
}

#[test]
fn indexor_range_query_honors_inclusive_and_exclusive_bounds() {
    let mut indexor = DatabaseIndexor::new();

    indexor
        .insert("locations_lat", b"50.0000", 1)
        .expect("insert should succeed");
    indexor
        .insert("locations_lat", b"50.5000", 2)
        .expect("insert should succeed");
    indexor
        .insert("locations_lat", b"51.0000", 3)
        .expect("insert should succeed");

    let inclusive = indexor.query_range(
        "locations_lat",
        Some(&IndexorRangeBound {
            key: b"50.0000".to_vec(),
            inclusive: true,
        }),
        Some(&IndexorRangeBound {
            key: b"51.0000".to_vec(),
            inclusive: true,
        }),
    );

    let exclusive = indexor.query_range(
        "locations_lat",
        Some(&IndexorRangeBound {
            key: b"50.0000".to_vec(),
            inclusive: false,
        }),
        Some(&IndexorRangeBound {
            key: b"51.0000".to_vec(),
            inclusive: false,
        }),
    );

    assert_eq!(inclusive, vec![1, 2, 3]);
    assert_eq!(exclusive, vec![2]);
}

#[test]
fn indexor_supports_high_volume_eq_and_range_queries() {
    let mut indexor = DatabaseIndexor::new();

    for i in 0..120_000usize {
        let key = key_for(i);
        indexor
            .insert("events_ts", &key, i as u64)
            .expect("insert should succeed");
    }

    let eq_hit = indexor.query_eq("events_ts", &key_for(73_421));
    assert_eq!(eq_hit, vec![73_421]);

    let eq_miss = indexor.query_eq("events_ts", &key_for(130_000));
    assert!(eq_miss.is_empty());

    let range_rows = indexor.query_range(
        "events_ts",
        Some(&IndexorRangeBound {
            key: key_for(40_000),
            inclusive: true,
        }),
        Some(&IndexorRangeBound {
            key: key_for(40_999),
            inclusive: true,
        }),
    );

    assert_eq!(range_rows.len(), 1_000);
    assert_eq!(range_rows.first().copied(), Some(40_000));
    assert_eq!(range_rows.last().copied(), Some(40_999));
}

#[test]
fn indexor_load_profile_vs_plain_hashmap() {
    const TOTAL_ROWS: usize = 160_000;

    let mut indexor = DatabaseIndexor::new();
    let mut baseline = BaselineHashIndex::default();

    let start = Instant::now();
    for i in 0..TOTAL_ROWS {
        let key = key_for(i);
        indexor
            .insert("events_ts", &key, i as u64)
            .expect("insert should succeed");
    }
    let indexor_insert_ms = start.elapsed().as_millis();

    let start = Instant::now();
    for i in 0..TOTAL_ROWS {
        let key = key_for(i);
        baseline.insert(&key, i as u64);
    }
    let baseline_insert_ms = start.elapsed().as_millis();

    assert_eq!(
        indexor.query_eq("events_ts", &key_for(55_555)),
        baseline.query_eq(&key_for(55_555))
    );
    assert_eq!(
        indexor.query_eq("events_ts", &key_for(200_000)),
        baseline.query_eq(&key_for(200_000))
    );
    assert_eq!(
        indexor.has_hit("events_ts", &key_for(12_345)),
        baseline.has_hit(&key_for(12_345))
    );
    assert_eq!(
        indexor.has_hit("events_ts", &key_for(260_000)),
        baseline.has_hit(&key_for(260_000))
    );

    let lower = IndexorRangeBound {
        key: key_for(80_000),
        inclusive: true,
    };
    let upper = IndexorRangeBound {
        key: key_for(80_999),
        inclusive: true,
    };

    let start = Instant::now();
    let indexor_range_rows = indexor.query_range("events_ts", Some(&lower), Some(&upper));
    let indexor_range_ms = start.elapsed().as_millis();

    let start = Instant::now();
    let baseline_range_rows = baseline.query_range(Some(&lower), Some(&upper));
    let baseline_range_ms = start.elapsed().as_millis();

    assert_eq!(indexor_range_rows, baseline_range_rows);
    assert_eq!(indexor_range_rows.len(), 1_000);

    eprintln!(
        "indexor_vs_hashmap insert_ms(indexor={}, baseline={}) range_ms(indexor={}, baseline={})",
        indexor_insert_ms,
        baseline_insert_ms,
        indexor_range_ms,
        baseline_range_ms,
    );
}

#[test]
fn indexor_unique_designation_rejects_second_row_for_same_key() {
    let mut indexor = DatabaseIndexor::new();

    indexor
        .ensure_index_with_kind("users_email", DatabaseIndexKind::Unique)
        .expect("index definition should succeed");

    indexor
        .insert("users_email", b"sam@example.com", 11)
        .expect("first insert should succeed");

    let duplicate = indexor.insert("users_email", b"sam@example.com", 12);

    assert!(matches!(
        duplicate,
        Err(IndexorInsertError::UniqueViolation {
            existing_row_id: 11,
            attempted_row_id: 12,
            ..
        })
    ));
}

#[test]
fn indexor_unique_designation_allows_idempotent_row_reinsert() {
    let mut indexor = DatabaseIndexor::new();

    indexor
        .ensure_index_with_kind("users_email", DatabaseIndexKind::Unique)
        .expect("index definition should succeed");

    indexor
        .insert("users_email", b"sam@example.com", 11)
        .expect("first insert should succeed");
    indexor
        .insert("users_email", b"sam@example.com", 11)
        .expect("same row should be idempotent for unique");

    assert_eq!(indexor.query_eq("users_email", b"sam@example.com"), vec![11]);
}

#[test]
fn indexor_indexed_designation_allows_multiple_rows_per_key() {
    let mut indexor = DatabaseIndexor::new();

    indexor
        .ensure_index_with_kind("users_city", DatabaseIndexKind::Indexed)
        .expect("index definition should succeed");

    indexor
        .insert("users_city", b"london", 1)
        .expect("insert should succeed");
    indexor
        .insert("users_city", b"london", 2)
        .expect("insert should succeed");

    assert_eq!(indexor.query_eq("users_city", b"london"), vec![1, 2]);
}

#[test]
fn indexor_rejects_conflicting_designation_redefinition() {
    let mut indexor = DatabaseIndexor::new();

    indexor
        .ensure_index_with_kind("users_email", DatabaseIndexKind::Unique)
        .expect("initial designation should succeed");

    let redefine =
        indexor.ensure_index_with_kind("users_email", DatabaseIndexKind::Indexed);

    assert!(matches!(
        redefine,
        Err(IndexorDefinitionError::ConflictingDesignation {
            current: DatabaseIndexKind::Unique,
            requested: DatabaseIndexKind::Indexed,
            ..
        })
    ));
}

#[test]
fn indexor_snapshot_roundtrip_recovers_designations_and_data() {
    let mut indexor = DatabaseIndexor::new();

    indexor
        .ensure_index_with_kind("users_email", DatabaseIndexKind::Unique)
        .expect("unique designation should succeed");
    indexor
        .ensure_index_with_kind("users_city", DatabaseIndexKind::Indexed)
        .expect("indexed designation should succeed");

    indexor
        .insert("users_email", b"sam@example.com", 10)
        .expect("insert should succeed");
    indexor
        .insert("users_city", b"london", 10)
        .expect("insert should succeed");
    indexor
        .insert("users_city", b"london", 11)
        .expect("insert should succeed");

    let payload = indexor
        .snapshot_bytes()
        .expect("snapshot should serialize");

    let recovered = DatabaseIndexor::from_snapshot_bytes(&payload)
        .expect("snapshot should recover");

    assert_eq!(recovered.index_count(), 2);
    assert_eq!(
        recovered.index_kind("users_email"),
        Some(DatabaseIndexKind::Unique)
    );
    assert_eq!(
        recovered.index_kind("users_city"),
        Some(DatabaseIndexKind::Indexed)
    );
    assert_eq!(
        recovered.query_eq("users_email", b"sam@example.com"),
        vec![10]
    );
    assert_eq!(recovered.query_eq("users_city", b"london"), vec![10, 11]);

    let city_range = recovered.query_range(
        "users_city",
        Some(&IndexorRangeBound {
            key: b"london".to_vec(),
            inclusive: true,
        }),
        Some(&IndexorRangeBound {
            key: b"london".to_vec(),
            inclusive: true,
        }),
    );
    assert_eq!(city_range, vec![10, 11]);
}

#[test]
fn indexor_recovery_rejects_invalid_unique_snapshot_payload() {
    let snapshot = IndexorSnapshot {
        format_version: 1,
        storages: vec![IndexorStorageSnapshot {
            index_name: "users_email".to_string(),
            kind: DatabaseIndexKind::Unique,
            entries: vec![IndexorEntrySnapshot {
                key: b"sam@example.com".to_vec(),
                row_ids: vec![1, 2],
            }],
        }],
    };

    let payload = common::helpers::bincode_compat::serialize(&snapshot).expect("snapshot should encode");
    let recovered = DatabaseIndexor::from_snapshot_bytes(&payload);

    assert!(matches!(
        recovered,
        Err(IndexorSnapshotError::UniqueIndexHasMultipleRows { .. })
    ));
}

#[test]
fn indexor_uses_database_index_kind_for_designation() {
    let mut indexor = DatabaseIndexor::new();

    let pk_index = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::PrimaryKey,
        vec!["id".to_string()],
    );
    let secondary_index = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["city".to_string()],
    );

    indexor
        .ensure_index_for(&pk_index)
        .expect("pk index should register");
    indexor
        .ensure_index_for(&secondary_index)
        .expect("secondary index should register");

    assert_eq!(
        indexor.index_kind(&pk_index.index_id.0),
        Some(DatabaseIndexKind::PrimaryKey)
    );
    assert_eq!(
        indexor.index_kind(&secondary_index.index_id.0),
        Some(DatabaseIndexKind::Indexed)
    );
}

#[test]
fn indexor_can_insert_rows_directly_from_database_index_definition() {
    let mut indexor = DatabaseIndexor::new();

    let composite_index = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string(), "tenant_id".to_string()],
    );

    let row_a = HashMap::from([
        ("email".to_string(), b"sam@example.com".to_vec()),
        ("tenant_id".to_string(), b"acme".to_vec()),
    ]);

    let row_b = HashMap::from([
        ("email".to_string(), b"sam@example.com".to_vec()),
        ("tenant_id".to_string(), b"acme".to_vec()),
    ]);

    indexor
        .insert_indexed_row(&composite_index, &row_a, 10)
        .expect("first insert should succeed");
    indexor
        .insert_indexed_row(&composite_index, &row_b, 11)
        .expect("second insert should succeed");

    let lookup_key = common::helpers::bincode_compat::serialize(vec![
        b"sam@example.com".to_vec(),
        b"acme".to_vec(),
    ])
    .expect("lookup key should encode");

    assert_eq!(
        indexor.query_eq(&composite_index.index_id.0, &lookup_key),
        vec![10, 11]
    );
}
