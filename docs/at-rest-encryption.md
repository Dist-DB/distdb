# At-Rest Encryption (Optional Enterprise Mode)

This document captures the current security direction for optional record encryption at rest.

## Scope

- Encryption at rest is optional and configured per database at creation time.
- Inter-node replication remains plaintext at the application layer and is secured in transit by TLS.
- AES keys are node-local and are not shared between affinity members.

## Current Direction

- Security state is inferred from metadata key presence (`enc_key_ref` style key reference).
- If a database has an encryption key reference at creation time, it is treated as encrypted-at-rest.
- Encryption configuration is immutable after creation.
- SQL creation option is supported:
  - `create database <name> --aes` (auto-generates a local key reference)
  - `create database <name> --aes=<key_ref>` (uses explicit key reference)

## Nonce and Keying Notes

- Nonces must be unique; they do not need to be secret.
- Avoid deriving nonce from previous record hashes or WAL history.
- No-op records and replay/fork behavior make chain-derived nonces unsafe.
- Preferred model:
  - derive encryption context from split metadata sources (catalog-level key reference plus database/table identifiers)
  - derive deterministic per-row nonces using stable row position/index context under that key scope
  - bind database/table/key-version context in AEAD additional authenticated data (AAD)

## Architectural Principle

- The primary design goal is architectural confidentiality by default, not payload self-description.
- Stored payload bytes should not expose explicit format markers that reveal encryption/compression strategy.
- Key-resolution context is intentionally distributed:
  - catalog contributes key reference/version state
  - database and table identifiers contribute scope partitioning
  - row-index context contributes per-record nonce uniqueness
- This split model reduces the utility of raw byte inspection without corresponding catalog + schema context.

## Nonce Derivation Contract

- Nonce size is fixed to 12 bytes for AES-GCM compatibility.
- Nonce derivation must be deterministic for the same logical row event under the same key scope.
- Nonce derivation inputs:
  - key reference (`enc_key_ref`)
  - key version
  - database identifier
  - table identifier
  - stable row index context (for example primary-key tuple digest or canonical row ordinal source)
  - mutation sequence component to prevent reuse for repeated writes of the same row context
- Derivation process:
  - normalize all string identifiers to canonical UTF-8 lower-case forms before hashing
  - encode numeric components in fixed-width little-endian format
  - hash the concatenated canonical input using a stable hash function
  - truncate the digest to 12 bytes for nonce output
- Safety constraints:
  - nonce reuse under the same AES key is forbidden
  - if deterministic inputs could collide, include an additional monotonic sequence component
  - reject write if nonce uniqueness cannot be guaranteed under active key scope
- Cross-node consistency:
  - all affinity members must use identical canonicalization and byte encoding rules
  - contract changes require a key-version bump or explicit compatibility migration path
- Testing requirements:
  - deterministic fixture tests for identical-input equality
  - collision-avoidance tests for repeated updates on the same row index context
  - compatibility tests across process restarts and replica nodes

## Replication Boundary

- Replication transport is protected by TLS.
- Replication frame strings may be base64 for transport compatibility.
- On-disk WAL/table record bytes are never base64 encoded; they are persisted as binary payloads.
- Encrypted-at-rest mode applies when data is persisted locally.
- Receiver nodes should encrypt at local write boundaries using local key material.

## Payload Envelope Scaffold

- Row payload support now includes an encrypted envelope scaffold with:
  - key version
  - nonce
  - auth tag
  - ciphertext
- Plain decode paths intentionally reject encrypted envelopes until decrypt hooks are wired.

## Next Steps

- Add create-database directive support for encryption key reference injection.
- Wire encrypt/decrypt hooks in persistence boundaries.
- Add strict checks to prevent plaintext logging of encrypted payload material.
- Add key rotation policy only when operational requirements are finalized.