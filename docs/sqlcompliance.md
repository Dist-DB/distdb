
# MySQL Compliance (to Version 8.x)

This document summarizes current support status by feature family.

## CRUD

### In Place
- `CREATE` data operations:
	- `INSERT ... VALUES`
	- `INSERT ... SELECT`
	- `CREATE TABLE`
	- `CREATE DATABASE`
- `READ` data operations:
	- `SELECT` with projection-only (`SELECT <expr>` without `FROM`)
	- `SELECT` with single relation
	- `SELECT` with joins (inner/left/right/full/cross parsing and execution paths)
	- `WHERE` predicates including comparison, `LIKE`, `REGEXP`, `IN`, `IS NULL`, subquery variants (`IN`, scalar, `EXISTS`, `ANY`, `ALL`)
	- `CASE` projection support (searched and simple forms)
	- `EXPLAIN` for `SELECT` and join plans
	- `SELECT` from views through scoped ephemeral materialization
- `UPDATE` data operations:
	- single-table and join-driven target selection
	- assignment expressions with inbuilt function evaluation
	- primary-key duplicate protection logic in update path
- `DELETE` data operations:
	- single-table and join-driven target selection
- Schema lifecycle adjacent to CRUD:
	- `ALTER TABLE` change-plan path
	- `DROP TABLE`, `DROP VIEW`, `DROP DATABASE`

### Not Done / Incomplete
- MySQL-complete DML surface is not fully covered (for example: many advanced `INSERT/UPDATE/DELETE` MySQL modifiers are not explicitly implemented).
- View execution currently uses runtime scoped materialization for query evaluation; deeper optimizer behavior is still minimal.
- No broad SQL optimizer/cost-based planning yet; execution remains rule/path based.

## Stored Procedures

### In Place
- SQL classification and routing:
	- `CREATE PROCEDURE` -> `CreateStoredProcedure`
	- `DROP PROCEDURE` -> `DropStoredProcedure`
- Catalog registration and persistence for stored procedures.
- IF/ELSE control-flow planning support from procedure SQL:
	- parse IF/ELSE/ELSEIF blocks into `IfElseEndPlan`
	- cache plan on stored-procedure entity
- Invocation primitives in execution layer:
	- direct invocation helper
	- cursor-backed invocation helper

### Not Done / Incomplete
- No integrated `CALL` statement execution in the server query mapping path yet (invocation helpers exist but are not fully wired into end-to-end SQL command handling).
- Procedure body execution is currently limited to the implemented control-flow/action model, not full MySQL stored-procedure language coverage.

## Functions

### In Place
- Inbuilt SQL function registry and evaluator for supported functions.
- Inbuilt function usage in:
	- projection-only select
	- relation/join projections
	- CASE branches
	- mutation assignment expressions
	- subquery projections where applicable
- Function evaluation supports runtime column lookups where needed.

### Not Done / Incomplete
- User-defined function DDL is not supported:
	- `CREATE FUNCTION` is explicitly treated as unsupported.
	- `DROP FUNCTION` is explicitly treated as unsupported.
- Coverage is limited to the implemented inbuilt function set, not full MySQL function parity.

## Triggers

### In Place
- SQL classification and routing:
	- `CREATE TRIGGER` -> `CreateTrigger`
	- `DROP TRIGGER` -> `DropTrigger`
- Trigger catalog entity support and SQL storage.
- Trigger invocation binding parsing (`BEFORE/AFTER`, `INSERT/UPDATE/DELETE`, target table).
- Execution-layer trigger invocation helpers, including automatic trigger selection by event and timing.

### Not Done / Incomplete
- Automatic trigger execution is not yet wired into all mutation pathways in the server command execution flow.
- Trigger body semantics are not full MySQL trigger-program support; current behavior is centered on stored SQL/invocation scaffolding.

## Events

### In Place
- No SQL event scheduler feature in place at this time.

### Not Done / Incomplete
- `CREATE EVENT`, `ALTER EVENT`, `DROP EVENT` SQL operations are not implemented.
- No event scheduler/runtime for time-based execution.
- No event metadata lifecycle in catalog comparable to tables/views/triggers/procedures.

## Inbuilt Operations

### In Place
- Inbuilt operation/function parsing and evaluation pipeline is available and integrated into core execution paths.
- Inbuilt usage is supported in:
	- `SELECT` projection-only mode (no `FROM`)
	- relation and join projections
	- `CASE` projection branches (`THEN`/`ELSE`)
	- `WHERE` expressions where parser/evaluator routes through supported function handling
	- mutation assignments (`UPDATE`/`INSERT` expression evaluation paths)
	- subquery projections used by `IN`/scalar/`EXISTS` style predicates where applicable
- Runtime function argument binding supports column-aware lookup (qualified and unqualified forms when available).
- Inbuilt runtime context includes database/user/session metadata and last-insert-id related context fields.

### Not Done / Incomplete
- Only the implemented inbuilt function set is supported; this is not full MySQL built-in parity.
- Some advanced expression combinations are still limited by current parser/execution constraints.
- No user-defined SQL function execution model yet (separate from inbuilt functions).

## Join Support

### In Place
- Join planning and execution support exists for:
	- `INNER JOIN`
	- `LEFT JOIN`
	- `RIGHT JOIN`
	- `FULL JOIN`
	- `CROSS JOIN`
- `JOIN ... ON` supports richer predicate forms (not only simple equality), with condition parsing routed through shared condition logic.
- Join-aware mutation target selection is supported for `UPDATE` and `DELETE` pathways.
- Join projection supports:
	- direct columns
	- wildcard expansion with relation qualification
	- inbuilt function projections
	- `CASE` projections
- Explain/introspection support includes joined select plan explain output.

### Not Done / Incomplete
- Cost-based join reordering/optimization is not present; join execution is currently deterministic/path-driven.
- Advanced MySQL join semantics and optimizer hints are not comprehensively implemented.
- Join performance tuning features are still basic compared with mature SQL engines.

## Notes
- This is an implementation-status overview, not a SQL standard conformance certificate.
- As new parser mappings and server execution wiring are added, this file should be updated alongside tests.

