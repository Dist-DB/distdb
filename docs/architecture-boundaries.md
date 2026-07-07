# Architecture Boundaries

This document defines ownership boundaries between `serverlib` and `server`.

## Boundary Contract

### `serverlib` owns functional behavior

`serverlib` is the source of truth for reusable functional sets that define how the platform behaves, including:

- SQL dialect semantics and compatibility behavior
- statement parsing and plan extraction logic
- execution primitives used by multiple surfaces
- stored routine control-flow semantics (`IF`, `CASE`, `WHILE`, `REPEAT`, cursor flow)
- local-scope resolution rules (local-first semantics, no-bleed guarantees)

Functional logic in this layer should be deterministic and testable with isolated inputs.

### `server` owns orchestration and cross-domain coordination

`server` owns runtime composition and process coordination, including:

- request lifecycle handling and dispatch entry points
- transaction/session coordination
- orchestration across catalog/WAL/security/network/runtime stores
- integration wiring between domains

`server` should call functional primitives in `serverlib` instead of re-implementing SQL dialect behavior.

## Placement Rule

Use this rule for new changes:

- If logic is functional and reusable without server orchestration state, place it in `serverlib`.
- If logic coordinates runtime domains and process flow, place it in `server`.

## CI Guardrail

Architecture boundaries are enforced by `scripts/check_architecture_boundaries.sh` and the CI workflow in `.github/workflows/architecture-boundaries.yml`.

Current guardrails verify:

- routine loop control-flow functional implementations (`WHILE` / `REPEAT`) remain serverlib-owned
- server query dispatch routes loop execution through serverlib APIs
- direct `sqlparser` usage does not appear in `server/src`
- key parser/planner entry points in server orchestration continue to call serverlib APIs
