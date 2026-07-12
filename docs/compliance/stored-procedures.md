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
  - parameter modes (`IN`, `OUT`, `INOUT`) are parsed from routine declarations
  - `OUT` / `INOUT` arguments require identifier targets and are tracked for output propagation
  - when CALL emits no explicit result set/mutation result, OUT/INOUT bindings are surfaced as a single-row query result
- IF/ELSE control-flow planning support from procedure SQL:
  - parse IF/ELSE/ELSEIF blocks into `IfElseEndPlan`
  - cache plan on stored-procedure entity
- CASE control-flow planning support from procedure SQL:
  - parse searched CASE (`CASE WHEN ... THEN ...`) into executable branch plans
  - parse simple CASE (`CASE <expr> WHEN ... THEN ...`) into executable branch plans
  - execute CASE branches through the same condition-evaluation path used by IF/ELSE planning
- LOOP control-flow execution support in CALL action interpreter:
  - parse and execute `LOOP ... END LOOP` blocks
  - support `LEAVE` and `ITERATE` directives inside loop bodies
  - support semicolon-delimited statements inside loop/while/repeat body slices
- Handler control-flow execution support in CALL action interpreter:
  - parse `DECLARE CONTINUE HANDLER FOR SQLEXCEPTION <statement>`
  - parse `DECLARE EXIT HANDLER FOR SQLEXCEPTION <statement>`
  - parse `DECLARE CONTINUE/EXIT HANDLER FOR SQLWARNING <statement>`
  - parse `DECLARE CONTINUE/EXIT HANDLER FOR NOT FOUND <statement>`
  - execute handler statement when action execution returns a SQL exception error
  - execute `SQLWARNING` handler statement for currently classified non-fatal warning/error flow in the CALL action interpreter
  - execute `NOT FOUND` handler statement when cursor fetch reaches end-of-result-set
  - `CONTINUE` resumes at next statement; `EXIT` leaves current action scope
- Cursor statement support in CALL action interpreter:
  - parse `DECLARE <name> CURSOR FOR <SELECT ...>` declarations
  - parse and execute `OPEN <name>`, `FETCH <name> INTO <vars...>`, and `CLOSE <name>`
  - `FETCH ... INTO` uses positional variable assignment and emits `NOT FOUND` when exhausted
- Invocation primitives in execution layer:
  - direct invocation helper
  - cursor-backed invocation helper
- Condition value resolution for stored-procedure control flow:
  - local-first lookup for procedure-local argument/variable bindings
  - fallback to row/global structures when local bindings do not resolve the field
- Scope isolation safeguards:
  - procedure-local argument/variable bindings are scoped per invocation
  - cursor-local bindings are restored to baseline after each cursor execution path (normal, return, and error), preventing cross-run bleed
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
| CALL argument expression shapes | Partial | Supports literals (boolean/number/string/NULL), signed numeric unary, identifier and compound identifier forms | Subquery args, placeholders, and many general expressions are rejected |
| CALL OUT/INOUT argument modes | Supported | Routine parameter declarations preserve `IN`/`OUT`/`INOUT`; bind step enforces identifier targets for OUT/INOUT and tracks output mappings | Current model still follows implemented CALL action/result flow, not full MySQL procedure language parity |
| CALL OUT/INOUT output propagation | Supported | OUT/INOUT values are returned when CALL would otherwise produce the default no-op mutation response | Existing explicit procedure-emitted query/mutation outputs remain authoritative |
| Procedure body control flow | Partial | IF/ELSEIF/ELSE/END IF, CASE/WHEN/THEN/ELSE/END CASE, LOOP/WHILE/REPEAT, and basic cursor statements (DECLARE/OPEN/FETCH/CLOSE) are supported in the CALL action interpreter | Execution model is intentionally narrow compared with full MySQL routine language |
| Non-IF top-level procedure body execution | Limited | Invocation currently executes planned IF/ELSE actions; bodies that do not map to that model may produce no branch action result | Not full statement-block interpreter coverage |
| CASE resolution semantics | Supported | CASE execution reuses the existing condition processor semantics used by query control flow | Supports searched and simple CASE branches in the implemented routine model |
| Stored-procedure variable resolution order | Supported | Local argument/variable bindings resolve first, then row/global provider values | Matches procedure-scoped isolation intent for control-flow evaluation |
| Stored-procedure binding bleed prevention | Supported | Cursor execution restores local bindings after normal completion, return, and error paths | Regression tests cover no-bleed behavior across repeated executions |
| Procedure-local temporary tables | Supported | Temporary table creation in CALL actions is scoped, aliased, and cleaned up | Name rewrite maps logical names to scoped physical IDs during action execution |
| Cursor-driven invocation helper | Supported | Cursor-frame helper executes procedure per frame/row | Execution-layer primitive remains available |
| SQL cursor statements inside routine actions | Partial | `DECLARE CURSOR`, `OPEN`, `FETCH ... INTO`, `CLOSE`, plus `NOT FOUND` handler trigger on cursor exhaustion | Cursor declaration/options and diagnostics are still a subset of MySQL 8 semantics |
| Delimiter-driven multi-statement routine creation (console/e2e) | Partial | E2E smoke includes delimiter switch, procedure creation, CALLs, and expected result checks | Validates practical creation/call flow in scripted usage |
| Full MySQL stored routine language | Not Supported | No complete implementation of MySQL procedural constructs (labels, cursor SQL syntax surface, broad condition-handler variants, full variable semantics, etc.) | Current support is focused on implemented control-flow/action model |

## Gaps
- Procedure body execution is currently limited to the implemented control-flow/action model (IF/ELSE and CASE branches), not full MySQL stored-procedure language coverage.
- CALL argument support is intentionally constrained to currently implemented expression/constant forms.
- Full MySQL routine semantics (for example labeled loop/block semantics and broader handler conditions/actions) are not implemented yet.
