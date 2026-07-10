# Core Statement Coverage

## Coverage Matrix (SELECT/INSERT/UPDATE/DELETE)

| Statement | Status | Coverage | Key Limits |
| --- | --- | --- | --- |
| `SELECT` | Partial | Projection-only and relation-backed reads, joins, set queries (`UNION`/`UNION ALL`/`EXCEPT`/`INTERSECT`), `WITH`/CTE materialization, first-pass `DISTINCT`/`GROUP BY`/`HAVING`, `ORDER BY`/`LIMIT`/`OFFSET`, predicate families including `LIKE`/`REGEXP`/subquery variants, `CASE`, `QUALIFY`, `FOR UPDATE`/`FOR SHARE` parse acceptance, expanded window-function execution slices (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM`, `AVG`, `MIN`, `MAX`), and `EXPLAIN` | Window-function coverage is still partial (limited function set and frame units), locking semantics are still no-op, advanced set-query branches/modifiers, full MySQL clause parity, and full optimizer behavior are not implemented |
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
  - first window-function execution slices:
    - `ROW_NUMBER() OVER (...)` with direct `PARTITION BY`/`ORDER BY`, named-window reuse, and named-window chaining
    - ranking windows: `RANK() OVER (...)` and `DENSE_RANK() OVER (...)`
    - aggregate windows over a direct single-column argument: `SUM`, `AVG`, `MIN`, `MAX`
    - `ROWS` frame evaluation for supported window-function paths (including explicit bounds)
- Not complete:
  - full SQL window-function execution semantics (implemented set is currently `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM`, `AVG`, `MIN`, and `MAX`)
  - non-`ROWS` frame units (`RANGE`, `GROUPS`) are not implemented in the current window executor
  - broader window aggregate/function parity (e.g., `LAG`, `LEAD`) is not implemented
  - `QUALIFY` semantics remain limited to the current row-filter model and do not yet have window-aware evaluation
  - `FOR UPDATE`/`FOR SHARE` are currently accepted by the parser but remain execution no-ops
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

## Statement Batch Execution

- Connector query payloads that parse into multiple SQL statements are executed sequentially.
- Execution stops at the first rejected statement; earlier applied statements remain applied.
- The response returned is the first rejection response, or the final applied response when all statements succeed.
- Transaction semantics for grouped rollback are still governed by explicit session transaction handling (`BEGIN` / `COMMIT` / `ROLLBACK`) rather than implicit all-or-nothing multi-statement batches.

## Adjacent Schema Lifecycle (Commonly Used With DML)
- `ALTER TABLE` change-plan path
  - supported change operations include `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and `MODIFY COLUMN`
  - connector schema update-field requests are routed through `ALTER TABLE ... MODIFY COLUMN` SQL execution
- `CREATE INDEX` and `DROP INDEX`
  - index lifecycle mutations are dispatched through the SQL query execution surface
  - structured index lifecycle WAL payloads are used for durable replay
- `SHOW INDEX` / `SHOW INDEXES` / `SHOW KEYS`
  - table-scoped index introspection is available and returns index metadata (`table_name`, `index_name`, `index_kind`, `index_origin`, `fields`)
- `DROP TABLE`, `DROP VIEW`, `DROP DATABASE`
