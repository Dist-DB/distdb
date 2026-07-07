# Stored Procedures Coverage

## Implemented
- SQL classification and routing:
  - `CREATE PROCEDURE` -> `CreateStoredProcedure`
  - `DROP PROCEDURE` -> `DropStoredProcedure`
- `CALL <procedure>(...)` is integrated in server query mapping and executes through routine invocation helpers.
- Catalog registration and persistence for stored procedures.
- CALL argument binding:
  - parameter names are parsed from `CREATE PROCEDURE` signature
  - call arguments are bound positionally and injected into the procedure-local value scope
- IF/ELSE control-flow planning support from procedure SQL:
  - parse IF/ELSE/ELSEIF blocks into `IfElseEndPlan`
  - cache plan on stored-procedure entity
- Invocation primitives in execution layer:
  - direct invocation helper
  - cursor-backed invocation helper
- Procedure-local temporary table scope support with scoped physical table IDs and teardown cleanup after invocation.
- End-to-end test coverage includes query-mapping unit tests and e2e smoke coverage (`scripts/e2e/stored_procedure_smoke.sh`).

## Coverage Matrix (Stored Procedures)

| Area | Status | Current Coverage | Notes |
| --- | --- | --- | --- |
| Parse/classify CREATE/DROP/CALL | Supported | `CREATE PROCEDURE`, `DROP PROCEDURE`, `CALL` classify and route to dedicated operations | Includes object-name extraction and request metadata mapping |
| CREATE PROCEDURE lifecycle | Supported | Register in catalog, persist SQL definition and metadata, snapshot persistence | Normalized identifiers and dependency storage are included |
| DROP PROCEDURE lifecycle | Supported | Drop from catalog via object drop flow | `DROP PROCEDURE IF EXISTS` classification is covered |
| CALL execution path | Supported | Server query mapping resolves procedure, binds args, invokes routine, returns action result | Cleanup is attempted after invocation and errors are surfaced |
| CALL argument arity validation | Supported | Mismatch between procedure params and call args is rejected | Error path returns explicit argument mismatch message |
| CALL argument expression shapes | Partial | Supports literals (boolean/number/string), signed numeric unary, identifier and compound identifier forms | Subquery args, placeholders, NULL, and many general expressions are rejected |
| Procedure body control flow | Partial | IF/ELSEIF/ELSE/END IF planning and execution is supported | Execution model is intentionally narrow compared with full MySQL routine language |
| Non-IF top-level procedure body execution | Limited | Invocation currently executes planned IF/ELSE actions; bodies that do not map to that model may produce no branch action result | Not full statement-block interpreter coverage |
| Procedure-local temporary tables | Supported | Temporary table creation in CALL actions is scoped, aliased, and cleaned up | Name rewrite maps logical names to scoped physical IDs during action execution |
| Cursor-driven invocation helper | Supported | Cursor-frame helper executes procedure per frame/row | Primarily execution-layer primitive, not full SQL cursor syntax coverage |
| Delimiter-driven multi-statement routine creation (console/e2e) | Partial | E2E smoke includes delimiter switch, procedure creation, CALLs, and expected result checks | Validates practical creation/call flow in scripted usage |
| Full MySQL stored routine language | Not Supported | No complete implementation of MySQL procedural constructs (loops, handlers, declarations, full variable semantics, etc.) | Current support is focused on implemented control-flow/action model |

## Gaps
- Procedure body execution is currently limited to the implemented control-flow/action model, not full MySQL stored-procedure language coverage.
- CALL argument support is intentionally constrained to currently implemented expression/constant forms.
- Full MySQL routine semantics (for example declarations, handlers, loops, and rich OUT/INOUT behavior) are not implemented yet.
