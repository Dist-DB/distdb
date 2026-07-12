# Core Statement Coverage

## Coverage Matrix (SELECT/INSERT/UPDATE/DELETE)

| Statement | Status | Coverage | Key Limits |
| --- | --- | --- | --- |
| `SELECT` | Partial | Projection-only and relation-backed reads, joins, set queries (`UNION`/`UNION ALL`/`EXCEPT`/`INTERSECT`), `WITH`/CTE materialization including first-pass `WITH RECURSIVE` (seed + recursive term), first-pass `DISTINCT`/`GROUP BY`/`HAVING`, `ORDER BY`/`LIMIT`/`OFFSET`, first-pass query-level `FETCH { FIRST | NEXT } ... { ROW(S) | PERCENT } { ONLY | WITH TIES }` (simple and set queries; `WITH TIES` requires `ORDER BY`), first-pass `TOP` (numeric, `PERCENT`, and `WITH TIES` with `ORDER BY`), first-pass `PREWHERE` (merged into filter evaluation), first-pass query-level `LIMIT BY` (direct-column keys for simple and set queries), and first-pass `CLUSTER BY`/`DISTRIBUTE BY`/`SORT BY` compatibility ordering (direct-column refs), predicate families including `LIKE`/`REGEXP`/subquery variants, `CASE`, `QUALIFY`, expanded window-function execution slices (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`), `EXPLAIN`, and first-pass `FOR UPDATE`/`FOR SHARE` lock enforcement for table-backed relation graphs (direct tables, joins, and CTE/view-expanded table dependencies) via scoped table locks during execution | Window-function coverage is still partial (limited function set and frame units), recursive CTE support is intentionally bounded (frontier iteration with configurable guardrails exposed through `SET` variables for max iterations, max rows, timeout, and repeat-frontier detection), lock semantics remain bounded to table-backed relation targets and still exclude projection-only no-FROM forms, advanced set-query branches/modifiers, full MySQL clause parity, and full optimizer behavior are not implemented |
| `INSERT` | Partial | `INSERT ... VALUES`, first-pass `INSERT ... DEFAULT VALUES` (with and without an explicit target-column list), first-pass MySQL `INSERT ... SET` compatibility syntax, and `INSERT ... SELECT` are implemented and wired through parser + execution; `INSERT ... RETURNING` (target-table columns/wildcard) is supported; `INSERT IGNORE` is supported for duplicate unique-key conflict skip semantics; `VALUES(DEFAULT)` keyword literals are accepted in first-pass value parsing (mapped through existing default/nullability write rules); placeholder tokens in INSERT value expressions are accepted as raw literal payloads in the current query model; first-pass `REPLACE INTO` is supported on unique-key conflicts (primary-key and non-primary unique-key indexes, including persisted rows and same-statement staged duplicates); first-pass `ON DUPLICATE KEY UPDATE` is supported for literal/function assignment updates, direct existing/incoming column references (`column` and `VALUES(column)`), broader qualified assignment targets (resolved to the target field), bounded arithmetic forms including parenthesized multi-step compositions with numeric/function operands (for example `col = (col + VALUES(col)) * abs(2)`), and top-level unary assignment expressions (for example `col = -VALUES(col)`) on duplicate unique-key conflicts against existing rows | Advanced MySQL modifiers and full expression/edge-case parity are not complete |
| `UPDATE` | Partial | Single-table and join-driven target selection, `UPDATE ... FROM` relation-source support, assignment expressions with inbuilt functions plus direct existing-row column references (`SET col = other_col`) and bounded arithmetic forms including parenthesized multi-step compositions with numeric/function/unary operands (`SET col = -col + abs(2)`), top-level unary assignment expressions (`SET col = -col`), first-pass `UPDATE ORDER BY ... LIMIT` for target-table direct column ordering plus controlled `LOWER`/`UPPER`/`LCASE`/`UCASE`/`ABS`/`LENGTH`/`CHAR_LENGTH`/`LEN`/`REVERSE`/`TRIM`/`LTRIM`/`RTRIM`/`CEIL`/`CEILING`/`FLOOR`/`ROUND` expression ordering (`ROUND(column, scale)` with integer literal scale included) with numeric literal limits, PK duplicate protection in update path, and `UPDATE ... RETURNING` (target-table columns/wildcard) | Full MySQL modifier/hint surface, broader expression parity beyond bounded arithmetic forms, and advanced optimizer semantics are not complete |
| `DELETE` | Partial | Single-table and join-driven target selection are implemented, including `DELETE ... USING` relation-source support, `DELETE ... RETURNING` (target-table columns/wildcard), and first-pass `DELETE ORDER BY ... LIMIT` for target-table direct column ordering plus controlled `LOWER`/`UPPER`/`LCASE`/`UCASE`/`ABS`/`LENGTH`/`CHAR_LENGTH`/`LEN`/`REVERSE`/`TRIM`/`LTRIM`/`RTRIM`/`CEIL`/`CEILING`/`FLOOR`/`ROUND` expression ordering (`ROUND(column, scale)` with integer literal scale included) with numeric literal limits | Full MySQL modifier/hint surface, broader `ORDER BY` expression parity, and advanced optimizer semantics are not complete |

## SELECT Details
- Supported in current model:
  - projection-only `SELECT` (no `FROM`)
  - single-relation and join-backed reads
  - first-pass set-query execution (`UNION`, `UNION ALL`, `EXCEPT`, `INTERSECT`) with query-level `ORDER BY`/`LIMIT`/`OFFSET`
  - first-pass query-level `FETCH { FIRST | NEXT } ... { ROW(S) | PERCENT } { ONLY | WITH TIES }` for simple and set queries (`WITH TIES` requires `ORDER BY`)
  - first-pass `TOP` compatibility for numeric, `PERCENT`, and `WITH TIES` (when `ORDER BY` is present)
  - first-pass `PREWHERE` (combined with `WHERE` as filter conjunction)
  - first-pass query-level `LIMIT BY` for per-key row caps (direct-column keys)
  - first-pass `CLUSTER BY`/`DISTRIBUTE BY`/`SORT BY` compatibility ordering via direct-column refs
  - first-pass `WITH`/CTE execution via scoped materialization
  - first-pass recursive CTE execution (`WITH RECURSIVE`) for seed + recursive UNION/UNION ALL forms with frontier-style iterative scoped rematerialization
  - first-pass `DISTINCT`, `GROUP BY`, `HAVING`, and output ordering (including hidden sort-key projections)
  - predicate coverage including comparison, `LIKE`, `REGEXP`, `IN`, `IS NULL`, scalar/`IN`/`EXISTS`/`ANY`/`ALL` subquery variants
  - searched/simple `CASE` projection paths
  - first window-function execution slices:
    - `ROW_NUMBER() OVER (...)` with direct `PARTITION BY`/`ORDER BY`, named-window reuse/chaining, and first-pass named-window `PARTITION BY`/`ORDER BY`/frame overrides
    - ranking windows: `RANK() OVER (...)` and `DENSE_RANK() OVER (...)`
    - distribution windows: `PERCENT_RANK() OVER (...)`, `CUME_DIST() OVER (...)`, and `NTILE(n) OVER (...)`
    - offset windows: `LAG(expr[, offset[, default]])` and `LEAD(expr[, offset[, default]])`
    - aggregate windows over a direct single-column argument: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`
    - value windows over a direct single-column argument: `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE(expr, n)`
    - frame evaluation for supported window-function paths:
      - `ROWS` (including explicit bounds)
      - first-pass `RANGE` for single numeric `ORDER BY` expressions
      - first-pass `GROUPS` based on window peer groups
- Not complete:
  - full SQL window-function execution semantics (implemented set is currently `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST`, `NTILE`, `LAG`, `LEAD`, `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE`)
  - recursive CTE execution remains bounded by configurable runtime guardrails (`SET cte.max_iterations`, `SET cte.max_rows`, `SET cte.timeout_ms`, `SET cte.union_all_repeat_detection`) and does not yet implement full MySQL 8 recursive diagnostics/cycle controls
  - recursive CTE guardrails are introspectable through `SHOW VARIABLES` / `SHOW VARIABLE cte.<name>` for current database catalog settings
  - frame-unit parity is still partial (`RANGE` currently requires exactly one numeric `ORDER BY` expression; broader SQL edge semantics are not complete)
  - `QUALIFY` now evaluates as a post-window filter stage, but parity is still partial for advanced subquery/edge semantics
  - `FOR UPDATE`/`FOR SHARE` currently enforce first-pass scoped locking across table-backed relation graphs (including joins and CTE/view-expanded table dependencies); projection-only no-FROM forms remain out of scope
  - clause parity remains first-pass for `TOP`/`PREWHERE`/`LIMIT BY`/`CLUSTER`/`DISTRIBUTE`/`SORT` and does not yet attempt full engine-specific semantics

- Current SELECT dialect-family guardrails:
  - `TOP`: unsigned numeric literals and `PERCENT` are supported; `WITH TIES` is supported only with `ORDER BY`
  - `FETCH`: unsigned numeric, single-row, and `PERCENT` forms are supported; `WITH TIES` is supported only with `ORDER BY`
  - `PREWHERE`: currently merged into standard filter evaluation with `WHERE`
  - `LIMIT BY`: direct column references only; requires resolved row-cap from `LIMIT`; current runtime applies per-key caps first, then global row windowing (`OFFSET` when present in parser-supported `LIMIT ... OFFSET ... BY ...` form); `FETCH ... BY ...` remains unsupported by current parser path
  - `CLUSTER BY`/`DISTRIBUTE BY`/`SORT BY`: direct column references only; cannot be combined with `ORDER BY` in current execution model

## INSERT/UPDATE/DELETE Details
- `INSERT`:
  - `INSERT ... VALUES`
  - first-pass `INSERT ... DEFAULT VALUES` (with or without explicit target-column list)
  - first-pass MySQL `INSERT ... SET` (single-row assignment form)
  - `INSERT ... SELECT`
  - first-pass `REPLACE INTO` replacement behavior for unique-key conflicts
  - `INSERT ... RETURNING` for target table columns and wildcard projection
  - `INSERT IGNORE` for duplicate unique-key conflict skip behavior
  - first-pass `INSERT ... ON DUPLICATE KEY UPDATE` support for assignment updates over duplicate unique-key conflicts
- `UPDATE`:
  - single-table and join-driven mutation target selection
  - `UPDATE ... FROM` relation-source support
  - assignment expression evaluation with supported inbuilt functions, direct existing-row column references, bounded arithmetic forms (including parenthesized multi-step compositions) over numeric literals/columns/function/unary operands, and top-level unary assignment expressions
  - first-pass `UPDATE ... ORDER BY ... LIMIT` support for direct target-table column ordering and controlled `LOWER`/`UPPER`/`LCASE`/`UCASE`/`ABS`/`LENGTH`/`CHAR_LENGTH`/`LEN`/`REVERSE`/`TRIM`/`LTRIM`/`RTRIM`/`CEIL`/`CEILING`/`FLOOR`/`ROUND` expression ordering (including `ROUND(column, scale)` with integer literal scale) with numeric literal limits
  - `UPDATE ... RETURNING` for target table columns and wildcard projection
- `DELETE`:
  - single-table and join-driven mutation target selection
  - `DELETE ... USING` relation-source support
  - `DELETE ... RETURNING` for target table columns and wildcard projection
  - first-pass `DELETE ... ORDER BY ... LIMIT` support for direct target-table column ordering and controlled `LOWER`/`UPPER`/`LCASE`/`UCASE`/`ABS`/`LENGTH`/`CHAR_LENGTH`/`LEN`/`REVERSE`/`TRIM`/`LTRIM`/`RTRIM`/`CEIL`/`CEILING`/`FLOOR`/`ROUND` expression ordering (including `ROUND(column, scale)` with integer literal scale) with numeric literal limits

- Compatibility modifier handling:
  - parser compatibility normalization now accepts common MySQL compatibility modifiers and hint forms as no-op tokens (for example `SELECT SQL_NO_CACHE`, `SELECT SQL_SMALL_RESULT`, `SELECT SQL_BIG_RESULT`, `SELECT SQL_BUFFER_RESULT`, `SELECT SQL_CALC_FOUND_ROWS`, `SELECT HIGH_PRIORITY`, `SELECT STRAIGHT_JOIN`, including `SELECT DISTINCT ...` / `SELECT ALL ...` combinations, optimizer hint comments like `/*+ ... */`, table index/key hints like `USE INDEX|KEY(...)`/`IGNORE INDEX|KEY(...)`/`FORCE INDEX|KEY(...)`, `UPDATE LOW_PRIORITY`, `UPDATE IGNORE`, `DELETE LOW_PRIORITY`, `DELETE QUICK`, `DELETE IGNORE`, `INSERT LOW_PRIORITY`, `INSERT DELAYED`, `INSERT HIGH_PRIORITY`, and `INSERT IGNORE` with priority combinations) in current execution model
- Shared DML limitations:
  - unsupported mutation-clause surfaces are now explicitly rejected instead of being silently ignored
  - MySQL-complete modifier/hint surface is not fully implemented
  - execution is still primarily rule/path based (no broad cost-based optimizer)

- Shared mutation function evaluation model:
  - UPDATE assignment function expressions, INSERT `ON DUPLICATE KEY UPDATE` function expressions, and mutation ORDER BY function expressions now resolve at runtime through the shared inbuilt/UDF SQL expression evaluator path (lookup-aware), instead of parser-time literal folding
  - current parser/planner still keep a bounded set of accepted function expression forms for mutation ORDER BY

- Current INSERT `ON DUPLICATE KEY UPDATE` guardrails:
  - assignments currently support base literal/inbuilt-function expressions, MySQL-style `VALUES(column)` incoming-row references, direct existing-row column references, and bounded arithmetic forms (including parenthesized multi-step compositions) over numeric literals/columns/function/unary operands
  - assignment to primary-key fields is explicitly rejected in current implementation
  - duplicate-key updates now handle both existing persisted unique-key conflicts and intra-statement staged insert collisions
  - broader MySQL expression parity for assignment values remains partial beyond the currently supported value forms

- Current plain INSERT duplicate guardrails:
  - plain `INSERT` now rejects both primary-key and non-primary unique-key duplicate collisions
  - plain `INSERT IGNORE` now skips both primary-key and non-primary unique-key duplicate collisions

- Current `INSERT ... DEFAULT VALUES` guardrails:
  - explicit target-column-list syntax is normalized into per-column `DEFAULT` values in the parser compatibility layer
  - execution applies default/nullability rules per schema field
  - non-null fields without defaults are rejected as missing required columns

- Current `INSERT ... SET` guardrails:
  - compatibility normalization rewrites a single-row assignment list into `INSERT ... (columns) VALUES (...)`
  - qualified assignment targets in the SET list are normalized to target-field leaf names
  - current first-pass normalization preserves trailing `ON DUPLICATE KEY UPDATE` / `RETURNING` clauses

- Current `REPLACE INTO` guardrails:
  - duplicate replacement currently keys off unique-key collisions in existing rows and staged rows in the same statement
  - guardrails are still first-pass around composite/edge SQL semantics

- Current INSERT placeholder guardrails:
  - placeholder tokens in INSERT values are currently treated as raw literal payload bytes by the parser/executor path
  - bound-parameter substitution semantics are not implemented in the current DataQuery SQL execution surface

- Shared relation-source limitations (SELECT/UPDATE/DELETE):
  - table partition selection (`... PARTITION (...)`) is explicitly rejected in current relation binding
  - table-version qualifiers and table-function argument factors are explicitly rejected in current relation binding

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
  - unsupported index variants (for example `CREATE UNIQUE INDEX`, `USING`/`WHERE`/`COMMENT`/`ALGORITHM`/`LOCK` forms) are explicitly rejected in the current MySQL80 compatibility path
- `SHOW INDEX` / `SHOW INDEXES` / `SHOW KEYS`
  - table-scoped index introspection is available and returns index metadata (`table_name`, `index_name`, `index_kind`, `index_origin`, `fields`)
- `DROP TABLE`, `DROP VIEW`, `DROP DATABASE`
