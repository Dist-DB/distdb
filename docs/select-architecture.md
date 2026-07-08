# SELECT Architecture

This page records the main design decisions behind `SELECT` support in DistDB.

## What This Page Covers

- how `SELECT` is planned and executed,
- why some behaviors are intentionally narrower than full MySQL 8,
- where hidden internal mechanics are used to preserve compatible results.

This page complements the feature-status view in [sql-compliance.md](sql-compliance.md). It explains design rationale rather than acting as a support matrix.

## Compatibility Baseline

- MySQL 8.0.x is the compatibility target.
- advanced `SELECT` areas are still first-pass in several places.
- unsupported syntax should fail explicitly rather than being normalized silently.

## Planning Model

`SELECT` plans are represented by `SelectReadPlan` and related types in `serverlib`.

The planner currently chooses one primary execution shape:

- projection-only, with no `FROM`,
- single-relation scan,
- joined relation execution,
- set-query handling such as `UNION`, `EXCEPT`, or `INTERSECT`.

Planning is rule-driven rather than cost-based.

## Why The Planner Is Rule-Driven

The current priority is correctness, explainability, and testability over aggressive optimization. A rule-driven planner keeps behavior explicit while the supported SQL surface is still expanding.

Pushdown is used when it is safe, but the system prefers a correct broader execution path over unsafe maximal pushdown.

## Execution Pipeline

The main execution paths are:

- projection-only execution,
- single-relation execution,
- joined execution.

Common post-processing order is:

1. `DISTINCT`
2. `ORDER BY`
3. `LIMIT` and `OFFSET`
4. hidden-column stripping before client output

## Hidden Sort Keys

### Why they exist

MySQL allows ordering by fields that are not visible in the final projection. DistDB preserves that behavior by carrying hidden sort keys internally during execution.

### How they work

- missing `ORDER BY` fields are injected into the internal projection,
- those fields are marked hidden in metadata,
- sorting uses the hidden values,
- final output removes hidden columns before returning rows to the client.

This is why internal projection shape can be wider than the client-visible result set.

## Current Area Decisions

### `GROUP BY` and `HAVING`

- current support focuses on direct-column grouping shapes,
- `HAVING` exists in a constrained first-pass form,
- full MySQL expression parity is not yet the goal in this area.

### `ORDER BY`

- implemented as post-processing over produced rows,
- supports non-projected order keys through hidden columns,
- projection-only queries support constrained alias and ordinal ordering,
- set-query ordering supports output columns and ordinals,
- full expression-based ordering parity is still a future extension area.

### CTEs

- CTEs use scoped ephemeral materialization,
- recursive CTEs are not supported yet,
- materialized/not-materialized modifiers are not supported yet.

### Set queries

- supports `UNION` and `UNION ALL`,
- supports `EXCEPT` with distinct semantics,
- supports `INTERSECT` with distinct semantics,
- supports query-level `ORDER BY`, `LIMIT`, and `OFFSET` on set results,
- reconciles branch metadata through first-pass compatibility rules,
- rejects incompatible collation combinations rather than guessing.

## Collation And Type Assumptions

Current collation handling is intentionally simplified:

- case-insensitive collations affect dedupe and ordering behavior,
- conflicting collations are rejected,
- full MySQL collation precedence and locale-specific details are not fully modeled.

This is a deliberate tradeoff to keep first-pass semantics explicit.

## Known Gaps

- full window-function runtime semantics,
- `QUALIFY`,
- locking clauses such as `FOR UPDATE` and `FOR SHARE`,
- full expression-ordering parity,
- full MySQL type and collation precedence edge cases.

## Design Priorities

1. correctness before permissive ambiguity,
2. deterministic first-pass behavior,
3. explicit failure when support is incomplete,
4. clear traceability from parser constraints to runtime behavior.

## Change Guidance

When extending `SELECT` support:

- update parser constraints and execution behavior together,
- add positive and negative tests,
- update [sql-compliance.md](sql-compliance.md) and this page when assumptions change.
