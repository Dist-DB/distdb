
# SQL Compliance

This page is the top-level index for SQL compatibility coverage in DistDB.

## What This Page Covers

- the compatibility target,
- how to interpret the support docs,
- where to find per-feature coverage.

## Compatibility Target

DistDB uses MySQL 8.x as its compatibility baseline for the supported statement set.

That does not mean full MySQL conformance. It means:

- supported syntax is expected to parse and execute according to the documented behavior,
- unsupported syntax should fail explicitly,
- partial areas should be documented rather than implied.

## Why The Coverage Is Split By Area

SQL support in DistDB is not one feature. It spans parser acceptance, planner behavior, execution wiring, and runtime limits. Breaking coverage into focused pages makes it easier to answer practical questions such as:

- does this statement parse,
- does it execute,
- is behavior partial or constrained,
- where are the remaining gaps.

## How To Read The Coverage Docs

The compliance pages describe current implementation status, not a future target state.

Common status labels:

- `Supported`
- `Partial`
- `Limited`
- `Not Supported`

Within individual pages, `Implemented` describes the current working surface and `Gaps` identifies missing or intentionally constrained behavior.

## Coverage Index

- [compliance/readme.md](compliance/readme.md): overview of the compliance container
- [compliance/core-statements.md](compliance/core-statements.md): `SELECT`, `INSERT`, `UPDATE`, `DELETE`
- [compliance/stored-procedures.md](compliance/stored-procedures.md): procedure lifecycle and execution behavior
- [compliance/functions.md](compliance/functions.md): inbuilt function coverage and user-defined function limitations
- [compliance/triggers.md](compliance/triggers.md): trigger lifecycle and execution model
- [compliance/joins.md](compliance/joins.md): join types and join-planning/runtime scope
- [compliance/inbuilt-operations.md](compliance/inbuilt-operations.md): built-in operation pipeline and limits
- [compliance/events.md](compliance/events.md): event-related support status

## Change Guidance

When SQL behavior changes:

- update the relevant per-area compliance page,
- keep parser support and runtime support statements aligned,
- prefer documenting explicit limits over broad compatibility claims.

