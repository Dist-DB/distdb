
# DistDB Documentation

DistDB is a distributed SQL database project built around a Rust execution core, a server runtime, and a peer-to-peer replication model. This documentation is organized to explain three things clearly:

- what each part of the platform does,
- why the design is shaped that way,
- what is implemented today versus what remains intentionally incomplete.

## Start Here

If you are new to the codebase, read these pages in order:

1. [using.md](using.md): how to run the platform and what the main components do.
2. [readme.md](readme.md): this page, for the platform map and design summary.
3. [security.md](security.md): transport security, CA bootstrapping, and trust decisions.
4. [replication.md](replication.md): affinity-based replication model and synchronization order.
5. [sql-compliance.md](sql-compliance.md): feature coverage and limits relative to MySQL 8.x.

For design constraints and implementation ownership:

- [architecture-boundaries.md](architecture-boundaries.md)
- [select-architecture.md](select-architecture.md)
- [at-rest-encryption.md](at-rest-encryption.md)

## Platform Overview

DistDB is split into a few major surfaces:

- `common`: shared helpers, formats, and low-level utilities.
- `serverlib`: reusable database behavior, SQL planning, execution primitives, runtime data structures, and replication/security building blocks.
- `server`: orchestration layer for query handling, sessions, transactions, WAL coordination, security, and peer interaction.
- `connector`: client/server transport types and protocol-facing integration.
- `console`: operator-facing CLI for exercising the platform.
- `client`: example client surface.

## Current Platform Shape

### SQL and execution

- MySQL 8.0.x is the compatibility target for supported syntax.
- Core DDL and DML are implemented for the currently supported statement set.
- `SELECT`, joins, ordering, limits, and several routine/trigger surfaces are available.
- Unsupported syntax is expected to fail explicitly rather than being silently rewritten.

### Transactions and durability

- Explicit transaction entry points exist through `BEGIN`, `COMMIT`, and `ROLLBACK` handling.
- Staged DML is tracked per session.
- WAL-backed durability and replay are central to recovery behavior.
- Grouped WAL commit markers are used to keep transaction visibility commit-gated.

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
- [security.md](security.md): TLS modes, CA flow, and runtime security tradeoffs.
- [replication.md](replication.md): affinity model, sync sequence, and failure handling.

### Architecture and decisions

- [architecture-boundaries.md](architecture-boundaries.md): ownership rules between `serverlib` and `server`.
- [select-architecture.md](select-architecture.md): SELECT planning and execution decisions.
- [at-rest-encryption.md](at-rest-encryption.md): current at-rest encryption direction and constraints.

### Feature coverage

- [sql-compliance.md](sql-compliance.md): top-level SQL support index.
- [compliance/readme.md](compliance/readme.md): per-area compliance documents.

## Reading Guidance

- If you want to run the system, start with [using.md](using.md).
- If you want to understand security or deployment posture, read [security.md](security.md) and [replication.md](replication.md).
- If you are changing execution behavior, read [architecture-boundaries.md](architecture-boundaries.md), [select-architecture.md](select-architecture.md), and the relevant compliance page before editing code.


