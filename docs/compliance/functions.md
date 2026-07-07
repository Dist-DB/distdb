# Functions Coverage

## Coverage Matrix (Functions)

| Area | Status | Current Coverage | Notes |
| --- | --- | --- | --- |
| Inbuilt function registry/evaluator | Supported | Inbuilt SQL function registry and evaluator are implemented | Coverage is limited to implemented inbuilt set |
| Function usage in SELECT projections | Supported | Works in projection-only and relation/join projection paths | Includes CASE branch usage through expression evaluation |
| Function usage in mutation expressions | Supported | Assignment expressions in mutation paths can evaluate supported inbuilt functions | Applies to current implemented expression paths |
| Function usage in subquery projections | Partial | Supported where parser/execution routes through currently implemented expression handling | Not full MySQL expression/function parity |
| Runtime argument binding | Supported | Column-aware runtime lookup supported for qualified/unqualified forms when available | Behavior follows current expression resolver model |
| User-defined function DDL (`CREATE FUNCTION`, `DROP FUNCTION`) | Not Supported | Explicitly treated as unsupported | No UDF lifecycle/execution model in place |

## Implemented
- Inbuilt SQL function registry and evaluator for supported functions.
- Inbuilt function usage in:
  - projection-only select
  - relation/join projections
  - CASE branches
  - mutation assignment expressions
  - subquery projections where applicable
- Function evaluation supports runtime column lookups where needed.

## Gaps
- User-defined function DDL is not supported:
  - `CREATE FUNCTION` is explicitly treated as unsupported.
  - `DROP FUNCTION` is explicitly treated as unsupported.
- Coverage is limited to the implemented inbuilt function set, not full MySQL function parity.
