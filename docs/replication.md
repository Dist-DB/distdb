
# Replication (v2 Principle)

This document defines the replication principle between datanodes in a single affinity group.

## 1. Scope

- Multiple affinities may coexist on the same swarm.
- Swarm participation does not imply replication trust or data replication.
- Replication occurs only between nodes in the same affinity.
- A datanode may participate in exactly one affinity at a time.
- An affinity is the replication boundary for metadata, schema, and data.
- Node/network access security is managed outside this protocol.
- Replication security is part of affinity state and is synchronized through replication.

## 2. Terms

- `affinity_id`: Logical identifier for the affinity group.
- `affinity_password`: Shared secret used to derive replication membership key material.
- `affinity_key`: Derived value from `affinity_id` + `affinity_password`, used to validate affinity membership.
- `affinity_document`: Canonical replicated affinity state document. It contains at least:
	- current affinity member nodes and their addresses,
	- replicated database identifiers in affinity scope,
	- each database's current `schema_identifier`,
	- replication security metadata.
- `schema_identifier`: Epoch timestamp associated with database schema state. Higher is newer.

Example shape (illustrative):

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

## 3. Membership and Identity

- Membership is by owning valid affinity credentials (`affinity_id` + `affinity_password`).
- A node requesting join must prove possession of valid affinity credentials.
- A node with invalid credentials MUST NOT receive affinity metadata or data.
- A node already in the affinity MAY request resynchronization without re-creating membership state.
- On successful join, all currently connected nodes in the same affinity MUST converge on updated membership.
- Membership state is maintained in the `affinity_document` and replicated to all affinity members.

## 4. Security Model

- Replication authentication and authorization are protocol-level concerns and are independent of network-layer controls.
- Replication security state (for example, trust material, policy, rotation metadata) is replicated within the affinity.
- When loading or updating from another node, replication security state MUST be synchronized before applying data-plane updates.
- A node outside the affinity MUST NOT receive affinity document content even if it is on the same swarm.

Security principal model (current target behavior):

- The datanode user is the default bootstrap database principal (`root`) for initial node access.
- Bootstrap `root` authentication may use passthrough credentials from the datanode context.
- Additional database users/roles are stored in the database catalog and replicated via the affinity processor.

Required guardrails:

- Affinity join MUST NOT by itself grant database superuser privileges beyond bootstrap policy.
- Catalog-defined database users/roles/grants are authoritative for normal data access decisions.
- Replicated security catalog changes MUST be versioned and applied transactionally with catalog state.
- Implementations SHOULD support disabling passthrough bootstrap root after first secure initialization.

## 5. Startup and Join Behavior

At startup, each node follows one of these modes:

1. Existing affinity config available:
- Load local affinity state.
- Attempt peer contact using node addresses from `affinity_document` (ordered by local policy).
- On success, run control-plane sync then data-plane sync.
- On failure, rotate to next peer with retry/backoff.

2. No local affinity state, join requested:
- Attempt join using provided affinity credentials.
- On success, fetch `affinity_document` including databases and their `schema_identifier` values.
- Emit membership update into `affinity_document` and propagate to affinity members.
- For each database, compare known `schema_identifier` values and treat the highest as authoritative for replication start.
- Continue with full sync sequence (Section 7).

3. No local affinity state, bootstrap-init requested explicitly:
- Create new affinity state locally.
- Publish `affinity_document` with this node as initial member.
- Node remains available for subsequent joins.

Nodes MUST NOT auto-create a new affinity if join was requested and peers are temporarily unavailable.

## 6. Replication API Capabilities

A node in an affinity MUST support these logical operations for other authenticated affinity members:

- Join affinity and retrieve current `affinity_document`.
- Fetch list of databases in affinity scope.
- Enumerate database objects (catalog entities and replication checkpoints/indexes).
- Fetch object-level replication stream (snapshot and/or WAL segments).
- Fetch and apply control-plane replication security metadata.

## 7. Synchronization Sequence

Replication from source node to target node MUST run in this order:

1. Control-plane sync
- affinity metadata (`affinity_document`)
- membership and database schema summary (from `affinity_document`)
- replication security state

2. Schema/catalog sync
- database list
- catalog entities
- schema metadata
- security catalog entities (users, roles, grants, policy metadata)

3. Data snapshot sync
- object/table baseline at a known checkpoint boundary

4. WAL catch-up sync
- apply changes after snapshot boundary until target is current

## 8. Schema Conflict Rule

- Each database schema has a `schema_identifier` (epoch timestamp).
- Higher `schema_identifier` is authoritative.
- A node MUST NOT downgrade to a lower `schema_identifier`.
- If two schemas share the same `schema_identifier`, implementation MUST apply a deterministic tie-breaker.
- During join negotiation, the node MUST receive database -> `schema_identifier` information from `affinity_document` and start replication from the highest known schema state per database.

Schema changes while running in affinity mode:
- A node MUST validate any schema change with at least one reachable partner in the same affinity before committing.
- If no same-affinity partner is reachable, schema change MUST be rejected or deferred.
- Validation request/response MUST include database identifier and proposed `schema_identifier`.
- Any database catalog security model change (for example users, roles, grants, or auth policy metadata) MUST be treated as a schema change.
- Security model changes MUST increment or otherwise advance effective `schema_identifier` and MUST be replicated through the schema/catalog replication path.

Recommended tie-breaker order:
1. lexical `node_id`
2. stable hash of schema payload

## 9. Idempotency and Ordering

- Replication apply operations MUST be idempotent.
- WAL/events MUST include stable operation identity and source ordering information.
- A node MUST persist replication checkpoints to resume after restart.
- Partial snapshot apply without matching WAL boundary metadata MUST be rejected.

## 10. Failure Handling

- Peer contact failure MUST trigger retry with bounded backoff.
- After repeated failures, node SHOULD mark peer temporarily unavailable and rotate.
- Recovery MUST resume from last durable checkpoint, not restart full sync by default.
- Authentication failure MUST hard-fail the replication session.

## 11. Minimal Compliance Criteria

An implementation is compliant with this principle if it:

- enforces affinity credential validation for replication membership,
- synchronizes replication security state before data-plane state,
- applies schema updates by monotonic `schema_identifier`,
- treats database catalog security changes as schema changes and replicates them through schema/catalog sync,
- requires at least one same-affinity partner validation for schema changes in affinity mode,
- executes sync in control -> schema -> snapshot -> WAL order,
- and supports checkpoint-based restart after failures.


