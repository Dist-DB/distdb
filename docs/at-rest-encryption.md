# At-Rest Encryption

This page describes the current design direction for optional encryption at rest in DistDB.

## What This Page Covers

- what encrypted-at-rest mode means in DistDB,
- the current keying and nonce assumptions,
- why the design avoids self-describing payloads,
- what remains scaffolding versus fully wired behavior.

## Current Status

The at-rest encryption path is now partially active, not only scaffolded.

- encryption metadata is configured per database at `CREATE DATABASE` time,
- row-mutation WAL payloads (`INSERT` / `UPDATE`) now pass through an active encryption transform when an at-rest key reference is present,
- read/decode paths can resolve encrypted row payload envelopes when the same database/table encryption context is supplied,
- the current implementation uses OpenSSL-backed AES-256-GCM for row payload encryption.

This is still not full platform-wide encryption parity. The implemented surface is database-scoped row-payload encryption on the WAL/data-write path, not a complete key-management or per-table policy system.

## Current Scope

- encryption at rest is optional and configured per database,
- there is no separate per-table encryption toggle today,
- replication transport is still protected separately by TLS,
- key material is node-local and not shared automatically across affinity members.

## Current Direction

A database is treated as encrypted-at-rest when creation-time metadata includes an encryption key reference.

Current SQL creation forms:

- `create database <name> --aes`
- `create database <name> --aes=<key_ref>`

Current design assumptions:

- encryption configuration is immutable after creation,
- encryption state is inferred from metadata rather than obvious payload markers,
- row-payload encrypt/decrypt hooks are wired through the active WAL write/read path,
- non-row payloads (for example schema/metadata/control records) are not treated as encrypted row payloads.

## Why The Design Looks Like This

The goal is architectural confidentiality, not simply wrapping bytes in an obvious envelope.

DistDB intentionally avoids making stored payloads self-describing in a way that reveals too much from raw byte inspection alone. Instead, encryption context is split across catalog metadata, object identity, and row-level positioning inputs.

This design means payload interpretation depends on platform context, not just a standalone byte blob.

## Nonce And Keying Decisions

### Core rules

- nonces must be unique, but do not need to be secret,
- chain-derived nonces are unsafe because replay, forks, and no-op records can break uniqueness guarantees,
- deterministic derivation is acceptable only when uniqueness is preserved under the active key scope.

### Preferred derivation inputs

- key reference,
- key version,
- database identifier,
- table identifier,
- stable row index context,
- mutation sequence component when repeated writes could otherwise collide.

### Current implementation notes

- AES key material is currently derived from the configured key reference plus key version,
- payload encryption uses AES-256-GCM,
- nonce generation is currently random 12-byte generation per encrypted row payload,
- authenticated additional data binds the encryption context to database/table/stream identity.

### Derivation contract

- nonce size is fixed to 12 bytes for AES-GCM compatibility,
- identifiers are normalized before hashing,
- numeric values are encoded in fixed-width form,
- the stable digest is truncated to 12 bytes,
- writes must be rejected if nonce uniqueness cannot be guaranteed.

## Architectural Decision: Split Context

Key-resolution context is intentionally distributed:

- the catalog contributes key-reference and key-version state,
- the database and table identifiers contribute scope,
- row-index context contributes per-record uniqueness.

This reduces the value of inspecting raw bytes without also having the surrounding catalog and schema state.

## Replication Boundary

- replication transport remains protected by TLS,
- application-layer replication payloads may use base64 where transport requires it,
- on-disk WAL and table records remain binary payloads,
- encrypted-at-rest mode applies when data is persisted locally,
- receiving nodes are expected to encrypt at their own local write boundaries using their local key material.

## Active Behavior

Encrypted row payloads are persisted as an envelope containing:

- key version,
- nonce,
- auth tag,
- ciphertext.

Current active behavior:

- `CREATE DATABASE <name> --aes` and `CREATE DATABASE <name> --aes=<key_ref>` enable database-scoped at-rest metadata,
- row mutation payloads written through the WAL/data path are encrypted when encryption context is present,
- default non-encrypted databases continue to write plaintext row payloads,
- encrypted payloads are not recompressed by the WAL compression stage,
- context-aware decode paths can recover logical plaintext when the same encryption context is available,
- mismatched encryption context causes payload decode failure.

Current non-goals / limits:

- no distinct table-level encryption policy,
- no external KMS integration in the current implementation,
- no claim that every persistence-adjacent path is encrypted equally; the implemented guarantee is centered on row payload WAL/write flow.

## Testing Expectations

The encryption contract should be backed by:

- deterministic fixture tests for identical-input equality,
- collision-avoidance tests for repeated updates of the same logical row,
- restart and replica consistency tests for canonicalization and derivation rules.

## Remaining Work

- audit and extend encryption coverage across any remaining persistence boundaries that do not yet carry row-payload encryption context,
- harden logging paths so encrypted payload material is not exposed accidentally,
- define a stronger key-management story beyond the current node-local key-reference model,
- decide whether deterministic nonce derivation should replace or complement random nonce generation for future operational requirements,
- finalize any key-rotation policy only after operational requirements are stable.