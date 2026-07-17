
# DistDB Documentation

DistDB is a distributed SQL database project built around a Rust execution core, a server runtime, and a peer-to-peer replication model. This documentation is organized to explain three things clearly:

- what each part of the platform does,
- why the design is shaped that way,
- what is implemented today versus what remains intentionally incomplete.

## Release Status

DistDB is currently in **Developer Alpha**.

This means:

- the platform is usable for development and controlled evaluation,
- core behavior is documented and validated for the supported surface,
- compatibility and behavior are still evolving,
- partial/unsupported areas are expected and documented.

For current alpha scope and expectations, see [release.md](release.md).

## Quick-start

To run the platform view the [running.md](running.md) file for a quick start guide.

## Start Here

If you are new to the codebase, read these pages in order:

1. [using.md](using.md): how to run the platform and what the main components do.
2. [readme.md](readme.md): this page, for the platform map and design summary.
3. [security.md](security.md): transport security, CA bootstrapping, and trust decisions.
4. [replication.md](replication.md): affinity-based replication model and synchronization order.
5. [sql-compliance.md](sql-compliance.md): feature coverage and limits relative to MySQL 8.x.
6. [olap.md](olap.md): OLAP views, hypercubes, and multi-dimensional analysis (optional, if using analytics).

For design constraints and implementation ownership:

- [architecture-boundaries.md](architecture-boundaries.md)
- [select-architecture.md](select-architecture.md)
- [at-rest-encryption.md](at-rest-encryption.md)
- [consistency-isolation.md](consistency-isolation.md)
- [node-failure-matrix.md](node-failure-matrix.md)
- [beta-confidence-scorecard.md](beta-confidence-scorecard.md)
- [partition-split-brain-matrix.md](partition-split-brain-matrix.md)
- [non-functional-benchmarking.md](non-functional-benchmarking.md)
- [security-adversarial-matrix.md](security-adversarial-matrix.md)
- [security-findings-log.md](security-findings-log.md)

## Platform Overview

DistDB is split into a few major surfaces:

- `common`: shared helpers, formats, and low-level utilities.
- `serverlib`: reusable database behavior, SQL planning, execution primitives, runtime data structures, and replication/security building blocks.
- `server`: orchestration layer for query handling, sessions, transactions, WAL coordination, security, and peer interaction.
- `connector`: client/server transport types and protocol-facing integration.
- `clientlib`: reusable client-side library for transport/session/query operations used by client-facing binaries.
- `peerlib`: centralized peer-facing library for shared peer coordination/runtime behavior used across node-facing surfaces.
- `console`: operator-facing CLI for exercising the platform.
- `client`: example client surface.

## Current Platform Shape

### SQL and execution

- MySQL 8.0.x is the compatibility target for supported syntax.
- Core DDL and DML are implemented for the currently supported statement set.
- `SELECT`, joins, ordering, limits, and several routine/trigger surfaces are available.
- Unsupported syntax is expected to fail explicitly rather than being silently rewritten.

### OLAP and Analytics

- OLAP views enable multi-dimensional analysis via memory-resident hypercubes.
- `CREATE OLAPVIEW` names and defines coordinate-addressed cells over committed table data.
- `SHOW SLICES FROM <olapview>` returns first-pass slice output with dimension coordinates, row counts, and numeric per-slice aggregates.
- Hypercubes are derived (not persisted) and rebuilt from live rows on startup or after invalidation.
- See [olap.md](olap.md) for design philosophy, examples, and current limitations.

### Transactions and durability

- Explicit transaction entry points exist through `BEGIN`, `COMMIT`, and `ROLLBACK` handling.
- Staged DML is tracked per session.
- WAL-backed durability and replay are central to recovery behavior.
- Grouped WAL commit markers are used to keep transaction visibility commit-gated.
- security mutations (ACL and credentials) are WAL-persisted as full snapshots and replayed with latest-record-wins precedence per user.
- index lifecycle mutations (`CREATE INDEX` / `DROP INDEX`) are WAL-persisted and replayed during bootstrap.

### Authorization and ACL

- SQL request authorization is enforced for non-root sessions using required-privilege metadata.
- Object-level privilege checks apply across all referenced objects for multi-object statements.
- SQL `GRANT` and `REVOKE` support ACL mutation under current root-only policy.
- `CREATE USER '<userid>' IDENTIFIED BY '<password>'` persists encrypted credentials and ACL state.

### Replication and runtime infrastructure

- Peer discovery is built on a Kademlia-based P2P foundation.
- Catalog and runtime index state are persisted and reconstructed on startup.
- Replication is affinity-scoped rather than swarm-global.
- TLS-secured transport is available for server, peer, and connector paths.

## Design Rationale

The platform makes a few deliberate architectural choices.

### WAL-first state management

DistDB treats WAL as a core source of truth for durable changes, recovery, and replication catch-up. This keeps restart and sync flows aligned around the same durable event stream instead of separate persistence models.

### Separation between behavior and orchestration

Reusable SQL behavior belongs in `serverlib`, while cross-domain runtime coordination belongs in `server`. The goal is to keep dialect semantics and execution logic deterministic and testable without depending on server lifecycle state.

### Explicit compatibility boundaries

The project targets MySQL 8.x syntax where practical, but it does not claim full conformance. Where support is partial, the preferred behavior is a clear rejection path and corresponding documentation rather than ambiguous best-effort behavior.

### Affinity-scoped replication

Swarm membership is not the same as replication trust. DistDB uses the idea of an affinity to define who shares metadata, schema, and data. That keeps replication policy explicit and separate from raw network discovery.

## Key Decisions

- SQL compatibility is version-targeted, not "accept anything vaguely MySQL-like".
- Runtime behavior is favored over silent normalization when a feature is incomplete.
- Security and replication are treated as first-class platform areas, not add-ons around query execution.
- Documentation is intended to describe both current behavior and the rationale behind constraints.

## Documentation Map

### Platform operation

- [using.md](using.md): running the server, console, and local multi-node setups.
- [release.md](release.md): current release posture, alpha scope, and expectations.
- [security.md](security.md): TLS modes, CA flow, and runtime security tradeoffs.
- [replication.md](replication.md): affinity model, sync sequence, and failure handling.

### Architecture and decisions

- [architecture-boundaries.md](architecture-boundaries.md): ownership rules between `serverlib` and `server`.
- [select-architecture.md](select-architecture.md): SELECT planning and execution decisions.
- [at-rest-encryption.md](at-rest-encryption.md): current at-rest encryption direction and constraints.
- [consistency-isolation.md](consistency-isolation.md): current alpha consistency and isolation contract.
- [node-failure-matrix.md](node-failure-matrix.md): node/network failure expectations and evidence status.
- [beta-confidence-scorecard.md](beta-confidence-scorecard.md): four-domain beta confidence gates and status tracking.
- [partition-split-brain-matrix.md](partition-split-brain-matrix.md): executable partition/split-brain scenario gate matrix.
- [non-functional-benchmarking.md](non-functional-benchmarking.md): baseline performance/recovery benchmark profiles and evidence format.
- [security-adversarial-matrix.md](security-adversarial-matrix.md): adversarial security scenario matrix and baseline evidence status.
- [security-findings-log.md](security-findings-log.md): security severity rubric, triage disposition model, and tracked findings evidence.

### Feature coverage

- [sql-compliance.md](sql-compliance.md): top-level SQL support index.
- [compliance/readme.md](compliance/readme.md): per-area compliance documents.

## Reading Guidance

- If you want to run the system, start with [using.md](using.md).
- If you want to understand security or deployment posture, read [security.md](security.md) and [replication.md](replication.md).
- If you need guarantee boundaries, read [consistency-isolation.md](consistency-isolation.md) and [node-failure-matrix.md](node-failure-matrix.md).
- If you need partition confidence details, read [partition-split-brain-matrix.md](partition-split-brain-matrix.md).
- If you need release confidence posture, read [beta-confidence-scorecard.md](beta-confidence-scorecard.md) alongside [release.md](release.md).
- If you need non-functional evidence, read [non-functional-benchmarking.md](non-functional-benchmarking.md).
- If you need security confidence evidence, read [security-adversarial-matrix.md](security-adversarial-matrix.md).
- If you need security finding triage/disposition detail, read [security-findings-log.md](security-findings-log.md).
- If you are changing execution behavior, read [architecture-boundaries.md](architecture-boundaries.md), [select-architecture.md](select-architecture.md), and the relevant compliance page before editing code.

## Liability Disclaimer

DistDB is provided on an "as is" and "as available" basis, without warranties of any kind, express or implied, including but not limited to fitness for a particular purpose, merchantability, non-infringement, security, availability, or correctness.

By using this software, you accept responsibility for validating behavior, securing deployments, and operating within your own risk tolerance and regulatory obligations.

The project contributors and maintainers are not liable for direct, indirect, incidental, special, consequential, or exemplary damages, including but not limited to data loss, service interruption, financial loss, security incidents, or business impact arising from use or inability to use the software.


