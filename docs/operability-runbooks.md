# Operability Runbooks (Beta-Ready Scope)

This page provides the minimum operator runbook set required by the beta confidence scorecard.

## 1) Node Crash and Restart

Objective: restore service from durable state and confirm readiness.

Steps:

1. Identify failed node and capture last known node log segment.
2. Restart node with the same node identifier and data directory.
3. Verify readiness using console authentication handshake.
4. Execute table-level sanity reads for known critical datasets.
5. Confirm replay-sensitive state (schema and security metadata) appears consistent.
6. Archive restart log and command transcript into artifacts.

Evidence references:

- [node-failure-matrix.md](node-failure-matrix.md)
- `server/src/core/app/mod_test.rs::bootstrap_replays_latest_schema_from_wal`

## 2) Replication Lag and Convergence Investigation

Objective: detect and bound lag, then confirm convergence after remediation.

Steps:

1. Check peer availability and affinity membership for affected nodes.
2. Review replication executor phase progression and checkpoint behavior.
3. Validate schema sync status and stale WAL rejection handling.
4. Trigger targeted reconvergence validation if lag persists.
5. Confirm recovered node returns to ready and catches up expected state.
6. Record lag window and recovery timing in artifacts.

Evidence references:

- `bash scripts/e2e/partition_reconvergence.sh`
- `serverlib/src/engine/replication_executor_test.rs::executor_tracks_phase_progression`

## 3) Degraded Peer Handling

Objective: preserve bounded availability while isolating unhealthy peers.

Steps:

1. Identify degraded peer and remove it from active operational traffic paths as needed.
2. Confirm remaining peers continue serving documented safe paths.
3. Validate no unauthorized fallback path is used outside affinity boundaries.
4. Reintroduce peer only after readiness and sync checks pass.
5. Re-run targeted reconvergence checks after reintroduction.

Evidence references:

- [consistency-isolation.md](consistency-isolation.md)
- [partition-split-brain-matrix.md](partition-split-brain-matrix.md)

## 4) Schema Migration Rollback Strategy

Objective: recover to a known-good schema/runtime state after migration failure.

Steps:

1. Halt further schema mutation activity on the affected node(s).
2. Capture current WAL and schema metadata evidence snapshot.
3. Apply rollback SQL or restore from validated pre-migration checkpoint path.
4. Restart affected node(s) if required and verify readiness.
5. Run schema and data sanity checks on critical objects.
6. Record root cause, rollback method, and post-rollback verification evidence.

Evidence references:

- [non-functional-benchmarking.md](non-functional-benchmarking.md)
- [release.md](release.md)

## Drill Cadence

Minimum cadence for beta-ready maintenance:

- weekly rolling restart/upgrade-safety drill,
- after any WAL/catalog format-affecting change,
- before release posture changes.
