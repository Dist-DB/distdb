# SELECT Architecture Notes

This document captures implementation assumptions and architectural choices for SELECT support in DistDB.

## Purpose

- Document why the current SELECT pipeline is shaped as it is.
- Record intentionally constrained behavior vs full MySQL 8 semantics.
- Provide a stable reference for future extension work.

## Scope

This note covers:

- `SELECT` planning and execution flow.
- `ORDER BY`, `DISTINCT`, `GROUP BY`/`HAVING`, `WITH` (CTE), and `UNION` behavior.
- Hidden internal sort-key handling for non-projected `ORDER BY` fields.

This note does not replace the support matrix in `docs/sqlcompliance.md`.

## Compatibility Baseline

- MySQL 8.0.x is the compatibility target.
- Current support is first-pass for several advanced SELECT features.
- Unsupported syntax should fail explicitly rather than silently normalize.

## Core Planning Model

SELECT plans are represented by `SelectReadPlan` and companion types in serverlib.

Design assumptions:

- A query is parsed into one execution shape:
  - projection-only (no FROM),
  - relation scan,
  - join plan,
  - or UNION query handling.
- Pushdown conditions are derived when feasible, but correctness takes priority over maximal pushdown.
- Planning remains rule-driven (no cost-based planner currently).

## Execution Pipeline

SELECT execution is split into three primary command paths:

- projection-only execution,
- single-relation execution,
- joined execution.

Common post-processing order is:

1. DISTINCT (visibility-aware keys),
2. ORDER BY,
3. LIMIT/OFFSET windowing,
4. hidden-column strip for client output.

## Hidden Internal Sort Keys

### Why this exists

MySQL allows ordering by fields not present in the final visible projection. To keep deterministic sorting while preserving output shape, the engine injects internal sort keys.

### Current approach

- Missing ORDER BY fields are added to the internal projection for execution.
- Those internal columns are marked using metadata system visibility.
- Sorting can use those columns.
- Final result serialization strips hidden columns.

### Metadata choice

Field metadata includes `SystemFieldVisibility` with default `Visible` and optional `Hidden` usage for system/internal fields.

## GROUP BY / HAVING (First-Pass)

Current assumptions:

- Focus on direct-column grouping shapes.
- HAVING support is integrated in first-pass form and constrained compared with full MySQL expression semantics.
- Behavior is documented as intentionally narrower than full MySQL 8.

## ORDER BY (Current Assumptions)

- ORDER BY is implemented via post-processing over produced rows.
- Non-projected ORDER BY fields are supported through hidden sort keys.
- Projection-only SELECT supports constrained ORDER BY using output aliases or ordinal positions.
- UNION ORDER BY supports direct output columns and ordinal positions.
- Advanced expression-based ORDER BY remains a future extension area.

## CTE Model (Common Table Expressions)

- CTEs are handled via scoped ephemeral materialization.
- Recursive CTEs are not yet supported.
- Materialized/not-materialized CTE modifiers are not yet supported.

## Set-Operation Model

- Supports UNION and UNION ALL, including mixed UNION quantifier chains.
- Supports EXCEPT (distinct semantics).
- Supports INTERSECT (distinct semantics).
- Supports query-level ORDER BY, LIMIT, OFFSET for set-query results.
- Reconciles branch type metadata using first-pass compatibility rules.
- Applies simplified collation-aware behavior for set-query dedupe/sort, with conflict rejection for incompatible collations.
- Mixed operator families (UNION/EXCEPT/INTERSECT) are evaluated according to parser-produced set-expression tree order.

## Collation Assumptions

Current behavior is intentionally simplified:

- Case-insensitive collations affect string comparison behavior in set-query dedupe/sort.
- Conflicting branch collations are rejected.
- Full MySQL collation precedence and locale-specific behavior are not fully modeled.

## Known Gaps

The following remain out of scope today:

- Full window-function runtime semantics.
- QUALIFY.
- Locking clauses (`FOR UPDATE`, `FOR SHARE`).
- Full expression ordering parity for all MySQL shapes.
- Full MySQL type/coercion/collation precedence edge cases.

## Design Priorities

1. Correctness and explicit failure over permissive ambiguity.
2. Deterministic behavior in first-pass feature support.
3. Backward-compatible evolution where practical.
4. Clear tracing from parser constraints to runtime behavior.

## Change Guidance

When extending SELECT support:

- Update parser guardrails and execution behavior in the same change set.
- Add targeted tests for both positive and negative cases.
- Update `docs/sqlcompliance.md` and this document when assumptions change.
