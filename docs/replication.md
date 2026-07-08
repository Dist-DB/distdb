
# Replication

This page describes the current replication principle in DistDB. The core idea is that replication happens inside an explicitly trusted affinity, not across every node visible on the swarm.

## What This Page Covers

- the replication boundary,
- the meaning of affinity membership,
- the synchronization order and why it matters,
- the decision rules around schema state and recovery.

## Core Principle

DistDB separates discovery from replication trust.

- a swarm helps nodes find each other,
- an affinity decides which nodes are allowed to exchange replicated state.

That distinction is intentional. Network visibility alone should not imply access to schema, metadata, or data.

## Scope

- multiple affinities may coexist on one swarm,
- replication occurs only within a single affinity,
- a node participates in at most one affinity at a time,
- an affinity is the boundary for metadata, schema, data, and replication security state.

Node-level transport security is covered separately in [security.md](security.md). Replication security policy is part of affinity state and therefore belongs inside replication scope.

## Why Affinity Exists

The affinity model solves a practical problem: peer discovery and data trust are different concerns.

Without this separation, any discovered node would risk being treated as a replication peer. DistDB instead requires explicit affinity credentials and replicated affinity state before a node can join the data plane.

## Terms

- `affinity_id`: logical identifier for the affinity group.
- `affinity_password`: shared secret used to derive membership validation material.
- `affinity_key`: derived value used to verify affinity membership.
- `affinity_document`: canonical replicated affinity state.
- `schema_identifier`: monotonically increasing schema-state identifier, currently modeled as an epoch-style value.

The `affinity_document` contains at least:

- current member nodes and addresses,
- databases in affinity scope,
- each database's current `schema_identifier`,
- replication security metadata.

Illustrative shape:

```json
{
  "affinity_id": "finance-eu-01",
  "affinity_revision": 42,
  "members": [
    {
      "node_id": "sam01",
      "addrs": ["/ip4/10.0.0.11/tcp/4001"],
      "status": "online",
      "last_seen_epoch_ms": 1760419200000
    },
    {
      "node_id": "sam02",
      "addrs": ["/ip4/10.0.0.12/tcp/4001"],
      "status": "online",
      "last_seen_epoch_ms": 1760419201000
    }
  ],
  "databases": [
    {
      "database_id": "orders",
      "schema_identifier": 1760419100000,
      "schema_hash": "4f4c2a..."
    },
    {
      "database_id": "billing",
      "schema_identifier": 1760419150000,
      "schema_hash": "a1b29e..."
    }
  ],
  "replication_security": {
    "policy_revision": 9,
    "key_id": "k-2026-06",
    "updated_epoch_ms": 1760419180000
  }
}
```

## Membership And Identity

- membership requires valid affinity credentials,
- invalid nodes must not receive affinity metadata or replicated data,
- successful joins must converge membership state across current affinity members,
- existing members may resynchronize without recreating membership.

Membership truth is maintained in the `affinity_document` and replicated to the rest of the affinity.

## Security Decisions Inside Replication

- replication authentication and authorization are protocol-level concerns,
- replication security metadata is replicated inside the affinity,
- security state must be synchronized before data-plane updates,
- being on the same swarm is not enough to read affinity state.

Current principal model target:

- node bootstrap may expose an initial `root` access path,
- database-defined users, roles, and grants remain the long-term authority for normal data access,
- security catalog changes must be versioned and replicated transactionally with catalog state.

## Startup And Join Modes

At startup, a node falls into one of three broad modes.

### Existing affinity state available

1. load local affinity state,
2. contact peers listed in the `affinity_document`,
3. run control-plane sync first,
4. run data-plane sync after control state is current,
5. retry and rotate peers on failure.

### Join requested with no local affinity state

1. attempt join using affinity credentials,
2. fetch the current `affinity_document`,
3. publish membership update into affinity state,
4. compare schema identifiers per database,
5. start synchronization from the highest known schema state.

### Explicit bootstrap-init requested

1. create local affinity state,
2. publish an initial `affinity_document`,
3. remain available for later joins.

Important rule: if join was requested and peers are temporarily unavailable, the node must not silently create a new affinity.

## Required Replication Capabilities

A node participating in affinity replication is expected to support:

- join and affinity-document retrieval,
- database list retrieval,
- catalog and object enumeration,
- snapshot and WAL stream fetches,
- replication-security metadata fetch and apply.

## Synchronization Order

Replication must run in this order:

1. control-plane sync,
2. schema and catalog sync,
3. data snapshot sync,
4. WAL catch-up sync.

### Why the order matters

This order prevents the runtime from applying data against stale catalog or security assumptions.

- control-plane first establishes trust and scope,
- schema and catalog next define object meaning,
- snapshot then provides a consistent baseline,
- WAL catch-up advances from that baseline to current state.

## Schema Conflict Rule

- higher `schema_identifier` is authoritative,
- lower schema state must not overwrite higher schema state,
- ties require deterministic resolution,
- join negotiation must provide per-database schema identifiers before replication begins.

Recommended deterministic tie-breaker order:

1. lexical `node_id`
2. stable hash of schema payload

### Why schema is treated strictly

Replication correctness depends on shared interpretation of row and catalog data. Applying row changes under the wrong schema is more dangerous than temporarily rejecting or deferring the update.

Schema and security-catalog changes are therefore both treated as schema-level events.

## Ordering, Idempotency, And Recovery

- replication apply operations must be idempotent,
- WAL events must carry stable identity and source ordering information,
- replication checkpoints must be persisted,
- partial snapshot application without a matching WAL boundary must be rejected.

These decisions are meant to make restart and resume behavior deterministic rather than opportunistic.

## Failure Handling

- peer failures should trigger bounded retry and rotation,
- repeatedly failing peers may be marked temporarily unavailable,
- recovery should resume from the last durable checkpoint,
- authentication failures should hard-fail the replication session.

## Minimal Compliance Criteria

An implementation matches this replication principle if it:

- validates affinity credentials before membership is accepted,
- synchronizes replication security state before data-plane state,
- applies schema changes monotonically,
- treats security catalog changes as schema events,
- requires same-affinity partner validation for schema changes in affinity mode,
- runs sync in control -> schema -> snapshot -> WAL order,
- resumes from durable checkpoints after failure.


