# Functions Coverage

## Coverage Matrix (Functions)

| Area | Status | Current Coverage | Notes |
| --- | --- | --- | --- |
| Inbuilt function registry/evaluator | Supported | Inbuilt SQL function registry and evaluator are implemented | Coverage is limited to implemented inbuilt set |
| Function usage in SELECT projections | Supported | Works in projection-only and relation/join projection paths | Includes CASE branch usage through expression evaluation |
| Function usage in mutation expressions | Supported | Assignment expressions in mutation paths can evaluate supported inbuilt functions | Applies to current implemented expression paths |
| Function usage in subquery projections | Supported | Subquery projection paths resolve local SQL functions before inbuilt fallback | Broader expression parity is still incomplete outside currently supported statement shapes |
| Runtime argument binding | Supported | Column-aware runtime lookup supported for qualified/unqualified forms when available | Behavior follows current expression resolver model |
| User-defined function compilation and validation | Supported | `CREATE FUNCTION` uses compiler-driven validation before catalog registration | Current model still reuses the shared routine catalog backing used by stored procedures |
| User-defined function name precedence | Supported | Local catalog functions resolve before inbuilt function fallback | Prevents silent fallback to inbuilt functions when a local function name exists |
| User-defined function SQL lifecycle (`CREATE FUNCTION`, `DROP FUNCTION`, execution) | Supported | Public parser/classifier, DDL wiring, and query-time execution now support end-to-end UDF flow | Current scalar execution model is centered on `RETURN <expr>` and validated single-result routine bodies |

## Implemented
- Inbuilt SQL function registry and evaluator for supported functions.
- Inbuilt function usage in:
  - projection-only select
  - relation/join projections
  - CASE branches
  - mutation assignment expressions
  - subquery projections where applicable
- Function evaluation supports runtime column lookups where needed.
- `CREATE FUNCTION` and `DROP FUNCTION` are recognized by request parsing and routed through the shared routine catalog path.
- Local SQL functions execute at query time with local-first resolution before inbuilt fallback.
- Function arguments can bind literal and row-derived values into local UDF execution.
- SQL-programmatic function artifacts are validated before public function creation is applied.

## Gaps
- The current public UDF model still reuses the shared routine catalog/storage backing used for stored procedures rather than a distinct function catalog object type.
- Mutation-literal planning paths are still centered on eager inbuilt-function evaluation rather than full runtime-local UDF execution parity in every statement shape.
- Coverage is limited to the implemented inbuilt function set, not full MySQL function parity.
