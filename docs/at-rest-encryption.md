# At-Rest Encryption

This page describes the current design direction for optional encryption at rest in DistDB.

## What This Page Covers

- what encrypted-at-rest mode means in DistDB,
- the current keying and nonce assumptions,
- why the design avoids self-describing payloads,
- what remains scaffolding versus fully wired behavior.

## Current Scope

- encryption at rest is optional and configured per database,
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
- decrypt hooks are not yet fully wired through all persistence paths.

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

## Current Scaffolded Behavior

Row payload support already includes an encrypted-envelope scaffold containing:

- key version,
- nonce,
- auth tag,
- ciphertext.

Plain decode paths intentionally reject encrypted envelopes until full decrypt wiring is in place.

## Testing Expectations

The encryption contract should be backed by:

- deterministic fixture tests for identical-input equality,
- collision-avoidance tests for repeated updates of the same logical row,
- restart and replica consistency tests for canonicalization and derivation rules.

## Remaining Work

- complete encrypt/decrypt hook wiring at persistence boundaries,
- harden logging paths so encrypted payload material is not exposed accidentally,
- finalize any key-rotation policy only after operational requirements are stable.