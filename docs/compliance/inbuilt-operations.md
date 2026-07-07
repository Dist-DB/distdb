# Inbuilt Operations Coverage

## Implemented
- Inbuilt operation/function parsing and evaluation pipeline is available and integrated into core execution paths.
- Inbuilt usage is supported in:
  - `SELECT` projection-only mode (no `FROM`)
  - relation and join projections
  - `CASE` projection branches (`THEN`/`ELSE`)
  - `WHERE` expressions where parser/evaluator routes through supported function handling
  - mutation assignments (`UPDATE`/`INSERT` expression evaluation paths)
  - subquery projections used by `IN`/scalar/`EXISTS` style predicates where applicable
- Runtime function argument binding supports column-aware lookup (qualified and unqualified forms when available).
- Inbuilt runtime context includes database/user/session metadata and last-insert-id related context fields.

## Gaps
- Only the implemented inbuilt function set is supported; this is not full MySQL built-in parity.
- Some advanced expression combinations are still limited by current parser/execution constraints.
- No user-defined SQL function execution model yet (separate from inbuilt functions).
