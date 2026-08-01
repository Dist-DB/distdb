# Indexor Vs HashMap

This document explains when to create and use `DatabaseIndexor` instead of plain `HashMap`-based postings.

The goal is to keep indexing decisions explicit and predictable across query/runtime code paths.

## Short Answer

Use `DatabaseIndexor` when you need index-like behavior:

- exact lookup plus ordered range queries,
- index-kind semantics (`Indexed` vs `Unique`),
- per-index namespace isolation,
- snapshot/restore of index state.

Use plain `HashMap` when you only need simple exact-key lookup and do not need ordered range scans or index-kind semantics.

## Why DistDB Has Both

DistDB has two different needs:

- general in-memory maps for lightweight local aggregation or temporary lookups,
- index-like structures that model database index behavior.

`DatabaseIndexor` is for the second case.

## Decision Matrix

| Requirement | Prefer `DatabaseIndexor` | Prefer `HashMap` |
| --- | --- | --- |
| Equality lookup (`=`) only | Optional | Yes |
| Ordered range lookup (`>`, `<`, `BETWEEN`) | Yes | No |
| Unique-key enforcement behavior | Yes | No |
| Keep multiple logical indexes isolated in one container | Yes | No |
| Snapshot/restore indexed state | Yes | No |
| Minimal implementation overhead for tiny local maps | No | Yes |

## What `DatabaseIndexor` Adds

`DatabaseIndexor` (in `serverlib/src/engine/database/indexor.rs`) provides:

1. Per-index storage keyed by index name.
2. Equality path via key-to-bucket lookup.
3. Ordered key directory for range scans.
4. Index kind awareness:
   - `DatabaseIndexKind::Indexed`: multiple row IDs per key.
   - `DatabaseIndexKind::Unique`: uniqueness violation on conflicting row ID insert.
5. Snapshot encoding/decoding (`IndexorSnapshot`) with structural validation.

## What Plain `HashMap` Usually Means

A baseline map pattern is typically:

- `HashMap<Vec<u8>, HashSet<u64>>` for postings,
- optional helper methods for equality lookup.

This is acceptable for small, one-off local structures where:

- ordering is irrelevant,
- uniqueness constraints are handled elsewhere,
- persistence/snapshot parity is unnecessary.

## Performance Orientation (Guidance, Not A Guarantee)

In general:

- equality lookups can be efficient in both models,
- range queries are where `DatabaseIndexor` usually has structural advantage because it maintains ordered key navigation,
- map-only approaches often end up scanning all keys for range behavior.

Always validate with representative workload tests before finalizing a hot-path decision.

## Creation And Usage Pattern

```rust
use serverlib::{DatabaseIndexKind, DatabaseIndexor, IndexorRangeBound};

let mut indexor = DatabaseIndexor::new();

indexor
    .ensure_index_with_kind("users_email", DatabaseIndexKind::Unique)
    .expect("index definition should succeed");

indexor
    .insert("users_email", b"sam@example.com", 101)
    .expect("insert should succeed");

let rows = indexor.query_eq("users_email", b"sam@example.com");
assert_eq!(rows, vec![101]);

let range_rows = indexor.query_range(
    "users_email",
    Some(&IndexorRangeBound {
        key: b"a".to_vec(),
        inclusive: true,
    }),
    Some(&IndexorRangeBound {
        key: b"z".to_vec(),
        inclusive: true,
    }),
);
```

## Migration Checklist (HashMap -> Indexor)

1. Identify logical index names.
2. Decide index kind (`Indexed` vs `Unique`) per index.
3. Normalize key encoding once (usually existing index tuple encoding).
4. Replace ad hoc range scans with `query_range`.
5. Preserve behavior with tests:
   - equality hit/miss,
   - inclusive/exclusive bounds,
   - uniqueness violation behavior,
   - snapshot roundtrip if persistence is required.

## Anti-Patterns

Avoid:

- using `HashMap` + full-key scan for range-heavy paths,
- reimplementing uniqueness checks outside an index-aware structure when index semantics are required,
- mixing unrelated logical indexes into one un-namespaced postings map.

## Scope Note

This page is about choosing data structures inside DistDB runtime/execution code.
It is not a SQL feature document and does not change SQL compatibility contracts.
