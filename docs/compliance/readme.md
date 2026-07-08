# SQL Compliance Documents

This directory contains the detailed SQL feature coverage pages for DistDB.

## Purpose

These documents answer a practical implementation question: what is supported today, how complete is it, and where are the known limits.

They are meant to stay aligned with real parser mappings, execution wiring, and in-tree tests.

## How To Read Them

The pages describe current behavior, not aspirational compatibility.

Status labels used throughout:

- `Supported`
- `Partial`
- `Limited`
- `Not Supported`

Within each page:

- `Implemented` lists the current working surface,
- `Gaps` lists missing or intentionally constrained behavior.

## Coverage Pages

- [core-statements.md](core-statements.md): `SELECT`, `INSERT`, `UPDATE`, `DELETE`, including current `QUALIFY` and locking-clause parser acceptance plus partial window-function execution slices
- [stored-procedures.md](stored-procedures.md): procedure lifecycle, routine execution, and control-flow support
- [functions.md](functions.md): built-in function support and current user-defined function support/limits
- [triggers.md](triggers.md): trigger lifecycle and execution behavior
- [joins.md](joins.md): join types, predicates, and runtime scope
- [inbuilt-operations.md](inbuilt-operations.md): built-in operation coverage and limits
- [events.md](events.md): event-related support status

## Documentation Rule

When parser behavior, planner behavior, or runtime execution changes, update the relevant coverage page in the same change set. That keeps the docs useful as an engineering reference instead of a lagging overview.
