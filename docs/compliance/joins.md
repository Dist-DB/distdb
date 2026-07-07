# Join Coverage

## Coverage Matrix (Joins)

| Area | Status | Current Coverage | Notes |
| --- | --- | --- | --- |
| Join type support | Supported | `INNER`, `LEFT`, `RIGHT`, `FULL`, and `CROSS` join planning/execution paths are available | Coverage follows implemented runtime join model |
| `JOIN ... ON` predicate support | Supported | ON-clause parsing uses shared condition logic with richer predicate forms beyond equality-only | Shared predicate constraints still apply |
| Join-aware UPDATE/DELETE target selection | Supported | Join-based target resolution exists in mutation paths | Behavior tied to current mutation planner/runtime model |
| Join projection features | Supported | Direct columns, wildcard expansion with relation qualification, inbuilt function projections, and CASE projections | Expression support limited to implemented expression model |
| Explain/introspection for join plans | Supported | Explain output includes joined select planning details | Observability is available for current join plan types |
| Cost-based join optimization | Not Supported | No broad cost-based join reordering/optimization | Join execution is deterministic/path-driven |
| Full MySQL join semantics + hints | Partial | Core join forms are present; advanced semantics/hints are incomplete | Performance tuning features remain basic |

## Implemented
- Join planning and execution support exists for:
  - `INNER JOIN`
  - `LEFT JOIN`
  - `RIGHT JOIN`
  - `FULL JOIN`
  - `CROSS JOIN`
- `JOIN ... ON` supports richer predicate forms (not only simple equality), with condition parsing routed through shared condition logic.
- Join-aware mutation target selection is supported for `UPDATE` and `DELETE` pathways.
- Join projection supports:
  - direct columns
  - wildcard expansion with relation qualification
  - inbuilt function projections
  - `CASE` projections
- Explain/introspection support includes joined select plan explain output.

## Gaps
- Cost-based join reordering/optimization is not present; join execution is currently deterministic/path-driven.
- Advanced MySQL join semantics and optimizer hints are not comprehensively implemented.
- Join performance tuning features are still basic compared with mature SQL engines.
