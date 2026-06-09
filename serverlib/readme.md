# Server Library

The server component platform enabling the distributed database - The core service components

## High-Level Structure

The `serverlib` crate is split into four functional planes:

1. `core`
- Node bootstrap and runtime boundaries.
- Cluster/node identity and topology state.

2. `engine`
- Database schema, SQL directive intent, transaction log primitives.
- Replication event metadata and subscription key surfaces.
- Security domain primitives for user credentials and role grants.

3. `p2p`
- Discovery, transport, pub/sub topic and wire-message contracts.
- Integration surface for Kademlia/discovery and pub/sub pump behavior.

4. `helpers`
- Shared error/result model.
- Utility helpers for epoch time and deterministic key construction.

## Current Scaffold

`server` composes runtime behavior from `serverlib` traits and domain types.
`connector` binds remote client flow to `serverlib::p2p` message contracts.
`common` remains the cross-crate utility layer used by all crates.

## WAL Concurrency Model

The transaction log is table-scoped and backed by a WAL manager that spawns a dedicated worker per table identifier on demand.
This supports multiple table WAL streams concurrently, while keeping append operations lightweight for the caller.