# Using DistDB

This page is the operator-oriented entry point for running DistDB locally, understanding the main binaries, and exercising the current feature set.

## What This Page Covers

- the main workspace components,
- how to start the server and console,
- how to run simple multi-node experiments,
- the current transaction and routine behavior that matters during manual testing.

## Components

### `common`

Shared helpers, formats, and low-level utilities used across the workspace.

### `serverlib`

The reusable database core. This crate contains most functional behavior such as SQL planning, execution primitives, storage structures, and supporting runtime logic.

### `server`

The main runtime process. It owns orchestration such as query dispatch, session/transaction flow, WAL coordination, security integration, and peer-facing behavior.

### `console`

The most practical interactive client for local development and manual validation.

### `client`

An example client surface. It is useful as a reference, but the console is currently the better first stop for feature exploration.

## Quick Start

### Run a local server

```bash
cd ./server
./debug.sh
```

This starts the server with debug-oriented output. For quieter or more production-like behavior, use `cargo run --release` or a custom runtime argument set.

### Run the console

```bash
cd ./console
./debug.sh
```

## Common Startup Options

### Bootstrap peers

You can provide bootstrap peers at startup for discovery:

```bash
cd ./server
cargo run datadir=./data servers=127.0.0.1:9400,10.0.0.5:9400
```

Accepted peer formats:

- `host:port`
- multiaddr values such as `/ip4/127.0.0.1/tcp/9400`

### Runtime index preload mode

The default mode preloads runtime accessor caches during bootstrap so the node is query-ready immediately after startup.

- Default: `DISTDB_RUNTIME_INDEX_PRELOAD_ACCESSORS_ON_BOOTSTRAP=true`
- Tradeoff: slower startup, lower first-query latency on larger datasets

To favor faster process start and accept first-query warmup:

```bash
DISTDB_RUNTIME_INDEX_PRELOAD_ACCESSORS_ON_BOOTSTRAP=false cargo run
```

## Connecting With The Console

You can provide both a direct target and bootstrap peer candidates when launching console:

```bash
cd ./console
cargo run 127.0.0.1:9400 servers=127.0.0.1:4001
```

The direct address tells console where to connect first. Bootstrap peers help discover additional nodes in the swarm.

## First Manual Session

If you are starting from a clean environment, a simple first session looks like this:

```sql
connect root@server-node-01;
password password;
create database main;
show databases;
use main;
disconnect;
```

If the database already exists:

```sql
connect root@server-node-01;
password password;
show databases;
use main;
show tables;
disconnect;
```

The console also exposes `help` for additional commands and operator guidance.

## Multi-Node Local Testing

### Node 1

```bash
RUST_LOG="info,connector::p2p=debug,serverlib::p2p=debug,console=debug" RUST_BACKTRACE=1 cargo run datadir=./data listen_addr=0.0.0.0 port=4001 node_id=sam01
```

### Node 2

```bash
RUST_LOG="info,connector::p2p=debug,serverlib::p2p=debug,console=debug" RUST_BACKTRACE=1 cargo run datadir=./data listen_addr=0.0.0.0 port=4002 node_id=sam02 servers=127.0.0.1:4001
```

Node 2 points to Node 1 through the `servers` argument so it can discover the cluster.

### Connect the console to the cluster

```bash
RUST_LOG="info,serverlib::p2p=debug,console=debug" RUST_BACKTRACE=1 cargo run servers=127.0.0.1:4001
```

## Stored Routine Notes

### Why delimiters matter

The console splits statements on `;`. Multi-statement routine definitions therefore need a temporary delimiter so the full routine body is submitted as one statement.

### Example

```sql
delimiter //
create procedure p_sync(p_active bigint) as begin if p_active = 1 then select abs(1); else select abs(0); end if; end//
delimiter ;
call p_sync(1);
```

### Current routine behavior

- `IF / ELSEIF / ELSE / END IF` is supported.
- searched and simple `CASE` control-flow forms are supported.
- local routine bindings are checked before row/global structures during condition resolution.
- invocation-scoped temporary resources are cleaned up after each call.

### Routine debug introspection

DistDB now supports a lightweight debug introspection command for database entities:

```sql
debug <databaseentitytype> <entityname>;
```

Supported entity types:

- `table`
- `view`
- `trigger`
- `procedure` / `stored_procedure`
- `function` / `stored_function`

The result is returned as `attribute` / `value` rows. For routines, debug output includes cached artifact/resource context such as dependency list, variable resources, and outbound resource entries.

## Current Transaction Contract

The current explicit transaction behavior is closest to a staged DML model with snapshot-aware reads.

### What happens today

- `INSERT`, `UPDATE`, and `DELETE` are staged per session while a transaction is active.
- Staged writes are not visible to other sessions before `COMMIT`.
- `COMMIT` publishes staged writes as one grouped durable change.
- `ROLLBACK` discards staged writes.

### Reads inside a transaction

- `SELECT` executes against the transaction snapshot plus that session's staged writes.
- non-DML statements such as schema changes are still rejected in explicit transaction scope.

## Why The Contract Looks Like This

DistDB prioritizes commit-gated visibility and WAL-backed recovery semantics first. That has allowed the project to harden grouped commit behavior and conflict detection before broadening schema-in-transaction support.

## Next Isolation Milestones

- stronger repeatable-read style guarantees across the full transaction lifetime,
- continued write-write conflict enforcement at commit,
- broader predicate and range conflict handling,
- eventual closing of the remaining phantom/serializable gaps.