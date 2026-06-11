# Server Library

serverlib is the domain and infrastructure core for distdb. It provides the database catalog model, schema and migration lifecycle, WAL behavior, replication contracts, and p2p interfaces that higher layers compose into a full runtime.

## Architectural Intent

The crate is designed around explicit boundaries:

1. core
- Runtime-facing abstractions and identity surfaces.
- Cross-module types that should remain stable and small.

2. engine
- Catalog, schema, transaction, WAL, SQL intent, and migration orchestration.
- The authoritative state machine for object status and schema evolution.

3. p2p
- Discovery and transport contracts for cluster communication.
- Event propagation surfaces for replication and sync behavior.

4. helpers
- Shared utility helpers and lightweight support functions.

## Design Principles

1. Readability through split responsibilities
- Keep modules focused on one concern.
- Prefer explicit state transitions over hidden behavior.
- Move orchestration into dedicated units instead of long mixed functions.

2. Coverage as a first-class quality gate
- Add targeted unit tests with each behavioral change.
- Validate lifecycle transitions and failure paths, not only happy paths.
- Preserve deterministic outcomes for schema, WAL, and catalog operations.

3. Safety before convenience
- Reject invalid transitions early.
- Keep lock ownership and schema-change phase tracking explicit.
- Make recovery state durable where long-running operations are involved.

4. Deterministic distributed behavior
- Normalize identifiers and derive stable keys consistently.
- Make replay and synchronization rules monotonic and idempotent where possible.

## Enterprise Requirements and ACID Philosophy

distdb targets enterprise reliability characteristics. serverlib contributes the following ACID-oriented design direction:

1. Atomicity
- Schema migration paths are modeled as explicit phases with guarded cutover.
- Cutover operations use temporary artifacts and rollback-aware file replacement.

2. Consistency
- Catalog and schema transitions are validated before acceptance.
- Field-index invariants and schema revision ordering are enforced by engine paths.

3. Isolation
- Schema change locking and single active migration guard prevent overlapping destructive operations.
- Table/object state transitions gate writes when objects are not ready.

4. Durability
- WAL streams provide append-first mutation history.
- Catalog and schema-change progress are serialized for restart recovery.

## WAL and p2p in the Design Philosophy

WAL and p2p are complementary, not competing, layers:

1. WAL
- Table/entity scoped transaction streams.
- In-memory mode for high-speed temporary processing.
- Disk-backed mode for durable replay and restart recovery.

2. p2p
- Distribution layer for cluster state propagation.
- Sync and acknowledgment surfaces that can be extended to quorum workflows.

3. Combined model
- WAL captures ordered local intent.
- p2p propagates and coordinates remote convergence.

## Schema Evolution and Rebuild Strategy

Current schema migration architecture supports a phased rebuild model:

1. Lock and stage schema change metadata.
2. Rewrite records (disk source to memory-resident staging).
3. Rebuild indexes on staged data.
4. Flush temporary artifact.
5. Atomic cutover.

Progress metadata (rows processed and resume token) is checkpointed to support resumable migration behavior.

## Workspace Role

- server composes runtime behavior from serverlib domain and engine components.
- connector uses serverlib contracts for remote interaction.
- common remains the shared utility and schema support layer across crates.