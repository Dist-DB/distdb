# SQL Compliance Docs

This container holds implementation-status coverage docs grouped by feature area.

## Scope
- These documents describe current implementation coverage in distdb.
- They are not a full MySQL 8 conformance certification.
- Status labels used throughout:
  - `Supported`
  - `Partial`
  - `Limited`
  - `Not Supported`
  - Section headings use `Implemented` for current capabilities and `Gaps` for incomplete areas.

## Documents
- [Core Statements](core-statements.md): Coverage for `SELECT`, `INSERT`, `UPDATE`, `DELETE`.
- [Stored Procedures](stored-procedures.md): `CREATE PROCEDURE`, `DROP PROCEDURE`, `CALL`, and routine execution model.
- [Functions](functions.md): Inbuilt function support and UDF gaps.
- [Triggers](triggers.md): Trigger lifecycle and execution coverage.
- [Joins](joins.md): Join types, join predicates, and join-related planning/runtime scope.
- [Inbuilt Operations](inbuilt-operations.md): Inbuilt operation pipeline and current limits.
- [Events](events.md): Event scheduler and DDL support status.

## Notes
- Coverage reflects implemented parser mappings, server execution wiring, and in-tree tests.
- Update these docs alongside parser/execution changes.
