# Architecture Boundaries

This page explains the ownership boundary between `serverlib` and `server`.

## Why This Boundary Exists

DistDB has to balance two competing needs:

- reusable, deterministic database behavior,
- runtime orchestration across sessions, WAL, security, networking, and replication.

If those concerns drift into the same layer, two problems appear quickly:

- SQL behavior gets duplicated in runtime entry points,
- reusable logic becomes harder to test and evolve safely.

The boundary therefore exists to keep functional behavior in one place and orchestration in another.

## Ownership Model

### `serverlib` owns functional behavior

`serverlib` is the source of truth for reusable database behavior, including:

- SQL dialect semantics and compatibility behavior,
- statement parsing and plan extraction,
- execution primitives that can be reused by multiple surfaces,
- stored routine control-flow semantics such as `IF`, `CASE`, `WHILE`, `REPEAT`, and cursor flow,
- local-scope resolution rules such as local-first binding behavior.

Logic in this layer should be deterministic and testable with isolated inputs.

### `server` owns orchestration

`server` owns runtime composition and cross-domain coordination, including:

- request lifecycle handling,
- query dispatch entry points,
- session and transaction coordination,
- interaction between catalog, WAL, security, networking, and runtime stores,
- integration wiring between platform domains.

`server` should consume functional APIs from `serverlib` rather than re-implementing dialect or execution semantics locally.

## Placement Rule

Use this rule when deciding where a change belongs:

- if the logic is reusable and does not require server orchestration state, put it in `serverlib`,
- if the logic coordinates runtime domains or request/process flow, put it in `server`.

## Rationale Behind The Rule

This split improves a few things directly:

- tests can target functional behavior without booting the server,
- orchestration code stays thinner and easier to review,
- parser/planner/execution semantics have one canonical implementation path,
- architectural regressions are easier to detect in CI.

## Guardrails

The boundary is enforced by:

- `scripts/check_architecture_boundaries.sh`
- `.github/workflows/architecture-boundaries.yml`

Current checks verify that:

- loop-control functional implementations stay `serverlib`-owned,
- server query dispatch routes through `serverlib` APIs,
- direct `sqlparser` usage does not appear in `server/src`,
- key parser and planner entry points in `server` continue to call `serverlib`.

## What To Update When The Boundary Changes

If a change intentionally moves responsibility across the boundary, update:

- this document,
- the architecture boundary check if needed,
- the relevant tests that assert the expected ownership path.
