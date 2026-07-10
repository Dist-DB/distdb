# Security

This page describes the current security model in DistDB, the runtime options that affect it, and the main design decisions behind the implementation.

## What Security Covers Today

DistDB currently focuses on transport security and trust bootstrapping rather than full end-to-end policy enforcement.

The implemented areas are:

- TLS for server, peer, and connector paths,
- CA bootstrapping and certificate enrollment,
- CA-root role assignment for issuer nodes,
- connector-side CA discovery and certificate validation.

## Security Layers

Security is enforced across three layers:

1. server listener and outbound peer transport in `server`,
2. certificate lifecycle and CA support in `serverlib`,
3. connector and console transport behavior in `connector` and `console`.

## Why The Model Is Shaped This Way

DistDB operates in a distributed environment where nodes may join, discover peers, and exchange replication traffic. That makes two concerns more important than a single-process database:

- secure transport between nodes and clients,
- safe trust bootstrap when certificates are not pre-provisioned.

The current design therefore prioritizes getting nodes onto a trusted transport path without requiring a fully manual certificate lifecycle from the first run.

## Runtime Options

### Server options

#### TLS mode

- `tls=off|optional|required`
- default: `optional`

Behavior:

- `off`: plaintext only
- `optional`: accepts both TLS and plaintext inbound; prefers TLS outbound when possible
- `required`: TLS must succeed for protected paths

#### TLS material

- `tls_cert=/path/to/cert.pem`
- `tls_key=/path/to/key.pem`
- `tls_ca=/path/to/ca.pem`

If these are omitted while TLS is enabled, the runtime can auto-provision or enroll material depending on the path being exercised.

#### CA issuer role

- `ca_root`
- `ca_root=1|true|on|yes`

When enabled, the node is allowed to act as a CA issuer for peer enrollment.

#### Certificate SANs

- `tls_san=host-or-ip`

Multiple SANs are supported through comma-separated values or repeated args.

#### Service announcements

- `service=name`

Default service set includes:

- `sql.query`
- `p2p.discovery`
- `affinity.replication`
- `tls.ca.distribution`

If `ca_root` is enabled, `tls.enrollment.issuer` is also advertised.

### Client and console options

- `tls=off|optional|required`
- `tls_ca=/path/to/ca.pem`

Default mode is `optional`.

If `tls_ca` is not supplied, connector logic attempts CA discovery before TLS verification and uses the discovered CA in memory.

## Enforcement Model

### Inbound server connections

Inbound negotiation follows `TlsMode`:

- `off`: keep plaintext
- `optional`: probe for TLS and upgrade when appropriate
- `required`: handshake must succeed

### Outbound peer connections

Outbound peer transport follows the same mode:

- `required`: fail if TLS configuration or handshake is unavailable
- `optional`: try TLS first and fall back to plaintext when allowed
- `off`: use plaintext only

### CA-root gating

Certificate enrollment signing is intentionally restricted.

- non-issuer nodes reject signing requests,
- CA-root nodes are the intended issuers for enrollment flows.

### Certificate lifecycle safeguards

Auto-provisioning in `serverlib` includes a few guardrails:

- existing CA material is reused,
- lock-file coordination avoids concurrent CA creation races,
- waiting logic avoids duplicate initialization while another process is generating material.

The result is effectively one CA per shared `p2p-tls` storage root.

## Trust Bootstrap Paths

Two trust bootstrap paths exist today:

1. service-level CA distribution for peer propagation,
2. a lightweight CA bootstrap wire path for connector auto-discovery.

Only public CA material is transferred. Private keys are never distributed through these paths.

## Enrollment Flow

When a non-CA-root node needs certificate material, the intended flow is:

1. generate private key and CSR locally,
2. send enrollment request to peers,
3. receive signed certificate and CA certificate from an issuer,
4. install the material locally and proceed with TLS.

Depending on the runtime path, failed enrollment may fall back to local generation.

## Client Verification

Connector-side TLS verification builds a rustls root store from either:

- an explicit `tls_ca` file, or
- an auto-discovered CA certificate.

Server identity is then validated from the dial target using rustls server-name handling.

## Key Decisions

- TLS is treated as a platform concern for both client and node traffic.
- Trust bootstrap is automated enough for local/distributed development, but still keeps private key ownership local.
- `tls=optional` exists for staged rollout and mixed environments, not as the preferred long-term production stance.
- Service announcements help discovery, but they are not yet an authorization policy system.

## Operational Guidance

1. Use `ca_root=1` only on designated issuer nodes.
2. Prefer `tls=required` once all participants are prepared for TLS-only operation.
3. Keep `tls=optional` for compatibility transitions, not steady-state hardened deployments.
4. Provide explicit `tls_san` values for all expected IP and DNS dial targets.
5. Protect CA keys and shared TLS storage with strict filesystem permissions.

## Current Limits

- `tls=optional` still permits plaintext fallback when negotiation fails.
- CA scope is storage-root based, so separate storage roots can form separate trust domains.
- Service announcements are descriptive, not yet policy-authoritative.

## SQL Authorization, Credentials, And WAL Durability

DistDB now enforces SQL authorization using per-request privilege metadata and catalog ACL state.

### Request authorization model

- each parsed SQL request carries a required privilege,
- non-root sessions are checked before execution,
- object-level checks use referenced SQL objects, including multi-object statements such as joins,
- access requires privilege on every referenced object when object scope is involved.

### ACL mutation path

- `GRANT` and `REVOKE` are executed through SQL request handling,
- current runtime policy restricts ACL mutation statements to `root`,
- ACL statements cannot be combined with non-ACL statements in one request payload.

### User creation and credential model

- `CREATE USER` is supported with explicit password syntax:
	- `CREATE USER '<userid>' IDENTIFIED BY '<password>'`
- user creation persists both:
	- an ACL entry for the user,
	- an encrypted user credential snapshot for the user.
- duplicate user creation is rejected unless `IF NOT EXISTS` is supplied.

### WAL persistence and precedence

- security changes append `SecurityChange` records to the database WAL stream immediately,
- security WAL payloads are type-framed so ACL and credential payloads are decoded unambiguously,
- ACL WAL payloads store a complete ACL snapshot for the target user, not a delta patch,
- credential WAL payloads store a complete credential snapshot for the target user,
- replay resolves both ACL and credential state with latest-record-wins semantics per user,
- precedence is determined by transaction id, so older security snapshots are retained historically but do not override newer state.

### Credential nonce design

- password nonce derivation is stable for a database/server context and seed timestamp,
- username is intentionally not part of nonce derivation,
- this avoids password verification breakage if a username identifier changes while credential material is preserved.

This keeps security recovery deterministic after restart and aligns authorization and credential state with WAL-backed durability.
