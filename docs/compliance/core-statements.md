# Core Statement Coverage

## Coverage Matrix (SELECT/INSERT/UPDATE/DELETE)

| Statement | Status | Coverage | Key Limits |
| --- | --- | --- | --- |
| `SELECT` | Partial | Projection-only and relation-backed reads, joins, set queries (`UNION`/`UNION ALL`/`EXCEPT`/`INTERSECT`), `WITH`/CTE materialization, first-pass `DISTINCT`/`GROUP BY`/`HAVING`, `ORDER BY`/`LIMIT`/`OFFSET`, predicate families including `LIKE`/`REGEXP`/subquery variants, `CASE`, and `EXPLAIN` | Window-function semantics, advanced set-query branches/modifiers, full MySQL clause parity, and full optimizer behavior are not implemented |
| `INSERT` | Partial | `INSERT ... VALUES` and `INSERT ... SELECT` are implemented and wired through parser + execution | Advanced MySQL modifiers and full expression/edge-case parity are not complete |
| `UPDATE` | Partial | Single-table and join-driven target selection, assignment expressions with inbuilt functions, PK duplicate protection in update path | Full MySQL modifier/hint surface and advanced optimizer semantics are not complete |
| `DELETE` | Partial | Single-table and join-driven target selection are implemented | Full MySQL modifier/hint surface and advanced optimizer semantics are not complete |

## SELECT Details
- Supported in current model:
  - projection-only `SELECT` (no `FROM`)
  - single-relation and join-backed reads
  - first-pass set-query execution (`UNION`, `UNION ALL`, `EXCEPT`, `INTERSECT`) with query-level `ORDER BY`/`LIMIT`/`OFFSET`
  - first-pass `WITH`/CTE execution via scoped materialization
  - first-pass `DISTINCT`, `GROUP BY`, `HAVING`, and output ordering (including hidden sort-key projections)
  - predicate coverage including comparison, `LIKE`, `REGEXP`, `IN`, `IS NULL`, scalar/`IN`/`EXISTS`/`ANY`/`ALL` subquery variants
  - searched/simple `CASE` projection paths
- Not complete:
  - full SQL window-function execution semantics (`WINDOW` currently tracked as metadata)
  - `QUALIFY`
  - `FOR UPDATE`/`FOR SHARE`
  - dialect-specific clause families not in the current model (`TOP`, `PREWHERE`, `LIMIT BY`, `FETCH`, `CLUSTER/DISTRIBUTE/SORT BY`)

## INSERT/UPDATE/DELETE Details
- `INSERT`:
  - `INSERT ... VALUES`
  - `INSERT ... SELECT`
- `UPDATE`:
  - single-table and join-driven mutation target selection
  - assignment expression evaluation with supported inbuilt functions
- `DELETE`:
  - single-table and join-driven mutation target selection
- Shared DML limitations:
  - MySQL-complete modifier/hint surface is not fully implemented
  - execution is still primarily rule/path based (no broad cost-based optimizer)

## Adjacent Schema Lifecycle (Commonly Used With DML)
- `ALTER TABLE` change-plan path
- `DROP TABLE`, `DROP VIEW`, `DROP DATABASE`
