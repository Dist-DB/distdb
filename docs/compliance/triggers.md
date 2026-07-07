# Triggers Coverage

## Coverage Matrix (Triggers)

| Area | Status | Current Coverage | Notes |
| --- | --- | --- | --- |
| Parse/classify CREATE/DROP TRIGGER | Supported | `CREATE TRIGGER` and `DROP TRIGGER` classify to dedicated trigger operations | Includes trigger object-name routing in SQL request handling |
| Trigger catalog lifecycle | Supported | Trigger entities can be registered, stored, and dropped from catalog | SQL definition storage is included |
| Trigger event/timing binding parse | Supported | Parses `BEFORE/AFTER` and `INSERT/UPDATE/DELETE` with target table bindings | Scope is current implemented binding model |
| Trigger invocation helpers | Supported | Execution-layer helpers exist for direct invocation and automatic selection by event/timing | Primarily execution primitives |
| Automatic trigger execution in all mutation paths | Partial | Automatic trigger selection/execution exists but is not wired through every mutation pathway | End-to-end behavior is path-dependent today |
| Full MySQL trigger body language semantics | Not Supported | No full trigger-program interpreter semantics | Current behavior centers on stored SQL/invocation scaffolding |

## Implemented
- SQL classification and routing:
  - `CREATE TRIGGER` -> `CreateTrigger`
  - `DROP TRIGGER` -> `DropTrigger`
- Trigger catalog entity support and SQL storage.
- Trigger invocation binding parsing (`BEFORE/AFTER`, `INSERT/UPDATE/DELETE`, target table).
- Execution-layer trigger invocation helpers, including automatic trigger selection by event and timing.

## Gaps
- Automatic trigger execution is not yet wired into all mutation pathways in the server command execution flow.
- Trigger body semantics are not full MySQL trigger-program support; current behavior is centered on stored SQL/invocation scaffolding.
