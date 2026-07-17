# DistDB Release State

This page tracks current release posture, what is in scope for alpha, and what operators and integrators should expect.

## Current Recommendation

DistDB is ready for a **Developer Alpha** release, with explicitly scoped expectations.

The platform is not yet positioned as broad MySQL parity. It is positioned as a documented, partially complete SQL/runtime system with working durability/replay and validated core execution paths.

## What Alpha Means Here

Alpha scope is:

- core SQL execution paths are wired for the documented statement set,
- durability/replay behavior is validated for schema and index lifecycle changes,
- unsupported or partial SQL shapes are expected to fail explicitly,
- behavior promises are limited to the documented compliance pages.

## Current Strengths

- Operation dispatch and execution wiring covers the main documented DDL/DML/runtime paths.
- Multi-statement query payloads execute sequentially with first-error stop behavior.
- `CREATE INDEX` / `DROP INDEX` lifecycle is persisted through structured WAL payloads and replayed on bootstrap.
- `SHOW INDEX` / `SHOW INDEXES` / `SHOW KEYS` table-scoped introspection is available.
- Table/index invalidation paths now reconcile associated runtime index state.
- Latest local validation baseline is green in this repository state:
	- `serverlib`: 517 passed
	- `server`: 176 passed

## Explicit Alpha Limits

The following are still intentionally partial and should remain explicit in any alpha announcement:

- broad MySQL syntax/behavior parity,
- full optimizer/cost-based planning parity,
- full routine language parity,
- full trigger path uniformity across all mutation pathways,
- complete window-function/frame coverage.

Refer to:

- `docs/sql-compliance.md`
- `docs/compliance/core-statements.md`
- `docs/compliance/stored-procedures.md`
- `docs/compliance/triggers.md`

## Operator Expectations

For alpha users:

- use the documented statement surface only,
- treat unsupported syntax rejection as expected behavior,
- validate behavior against compliance docs before filing compatibility bugs,
- assume interface and behavior may evolve between alpha milestones.

## Suggested Alpha Messaging

Recommended wording:

"DistDB Developer Alpha provides a documented subset of SQL/runtime behavior with validated WAL-backed durability/replay for the currently supported surface. It is not a full MySQL compatibility release."

## Exit Criteria Toward Beta

Use these gates to move from alpha to beta readiness:

- close or significantly narrow currently documented `Partial` coverage in core statement and routine/trigger surfaces,
- complete broader end-to-end resilience testing around restart/replay/replication under failure conditions,
- publish and freeze the consistency/isolation contract for supported behavior (`consistency-isolation.md`),
- publish and continuously validate the node/network failure matrix (`node-failure-matrix.md`),
- maintain a current four-domain beta confidence scorecard (`beta-confidence-scorecard.md`) with objective `Red/Yellow/Green` domain status,
- require all scorecard domains to reach `Green` before declaring beta,
- stabilize and freeze a compatibility contract for supported SQL shapes,
- publish upgrade/migration expectations for persisted catalog/WAL formats,
- maintain green CI and release-candidate soak runs over representative workloads.
