# Security

This page describes the current security model in DistDB, the runtime options that affect it, and the design rationale for separating certificate issuance into `tlsserver`.

## Security Split and Rationale

DistDB separates security responsibilities into two explicit planes:

1. data-plane/runtime security in `server`, `peerlib`, `connector`, and `console`,
2. trust-plane certificate issuance in standalone `tlsserver`.

The split is intentional:

- runtime nodes terminate and validate TLS but do not need CA-signing authority by default,
- certificate issuance lifecycle policy stays independent from replication/query runtime behavior,
- compromise blast radius is reduced because traffic processing and signer authority are decoupled.

## What Security Covers Today

DistDB currently focuses on transport security and certificate lifecycle enforcement rather than full end-to-end policy enforcement.

The implemented areas are:

- TLS for server, peer, and connector paths,
- standalone `tlsserver` certificate issuance,
- startup-time certificate validation against declared SANs,
- connector-side certificate validation and trust pinning.

## Security Layers

Security is enforced across four layers:

1. server listener and outbound peer transport in `server`,
2. certificate lifecycle and local material generation in `security` and `serverlib`,
3. standalone issuance interface in `tlsserver`,
4. connector and console transport behavior in `connector` and `console`.

## Why the Model Is Shaped This Way

DistDB operates in a distributed environment where nodes join, discover peers, and exchange replication traffic. That makes two concerns especially important:

- secure transport between nodes and clients,
- consistent certificate issuance and validation when certificates are not pre-provisioned.

The current design therefore prioritizes getting nodes onto a trusted transport path without relying on legacy P2P certificate distribution or plaintext CA bootstrap.

## Runtime Options

### Server options

#### TLS mode

TLS mode is fixed to `required` for server, peer, and connector runtime paths.

- `tls=` override arguments are rejected at startup,
- plaintext fallback is not supported.

#### TLS material

- `tls_cert=/path/to/cert.pem`
- `tls_key=/path/to/key.pem`
- `tls_ca=/path/to/ca.pem`
- `tls_server=host:port`

If `tls_cert` and `tls_key` are supplied, the server uses that local keypair directly.

If local certificate material is not supplied, `tls_server` must point to a TLS issuer endpoint that signs a CSR generated locally by the node. All configured `tls_san` values are included in that CSR so multi-SAN certificates can be issued from the remote signer.

At startup, at least one explicit `tls_san` value is required for any server TLS identity configured through either `tls_cert`/`tls_key` or `tls_server`. The server does not enter run mode unless the resolved certificate contains the declared SANs. For WSS-enabled services, those SANs must cover the WSS endpoint as well.

#### Certificate SANs

- `tls_san=host-or-ip`

Multiple SANs are supported through comma-separated values or repeated args.

#### Service announcements

- `service=name`

Default service set includes:

- `sql.query`
- `p2p.discovery`
- `affinity.replication`

Certificate issuance is not advertised or propagated via P2P service messages.

### `tlsserver` options

- `listen_addr=host`
- `port=number`
- `datadir=/path/to/data`
- `node_id=id`
- `ca_root=1|true|on|yes`

When enabled on `tlsserver`, `ca_root` allows the process to sign certificate requests.

`tlsserver` pre-generates its own local CA and serving certificate material before entering its accept loop. This allows application processes to request certificates before starting connector or WSS run mode.

### Client and console options

- `tls_ca=/path/to/ca.pem`

TLS mode is fixed to `required`.

If `tls_ca` is supplied, connector verification uses that CA directly.

If `tls_ca` is not supplied, connector verification uses strict built-in trust pinning rather than runtime network CA bootstrap.

## Enforcement Model

### Inbound server connections

Inbound server paths require TLS handshakes to succeed.

### Outbound peer connections

Outbound peer transport is TLS-only and fails when TLS configuration or handshake is unavailable.

### CA-root gating

Certificate enrollment signing is intentionally restricted:

- non-issuer nodes reject signing requests,
- `tlsserver` instances with `ca_root` enabled are the intended issuers for enrollment flows,
- runtime server nodes are certificate consumers by default, not signing authorities.

### Certificate lifecycle safeguards

Auto-provisioning in `serverlib` includes guardrails:

- existing CA material is reused,
- lock-file coordination avoids concurrent CA creation races,
- waiting logic avoids duplicate initialization while another process is generating material.

The result is effectively one CA per shared `p2p-tls` storage root.

## Enrollment Flow

When a node needs certificate material from a remote issuer, the flow is:

1. generate private key and CSR locally,
2. send enrollment request to the standalone `tls_server` interface,
3. receive signed certificate and CA certificate from an issuer,
4. install the material locally and proceed with TLS.

The private key remains local throughout the process. All configured `tls_san` values remain in the CSR, so the same flow can issue certificates for connector, peer, and WSS-facing endpoints.

For WSS-enabled services, certificate acquisition happens during startup before the service enters its listener loop, and the certificate presented at runtime must contain the declared WSS-facing SAN set.

## Client Verification

Connector-side TLS verification builds a rustls root store from either:

- an explicit `tls_ca` file, or
- built-in trusted fingerprint policy.

Server identity is then validated from the dial target using rustls server-name handling.

## Key Decisions

- TLS is a platform concern for both client and node traffic.
- Certificate issuance is intentionally split into `tlsserver`, while private key ownership stays local.
- TLS mode is immutable (`required`) to prevent downgrade and misconfiguration drift.
- Service announcements help discovery but are not part of certificate distribution.

## Why `tlsserver` Is a Separate Process

`tlsserver` is separated from database runtime for three reasons:

1. blast-radius reduction: signer compromise and query/runtime compromise are decoupled,
2. operability: certificate issuance can be rolled, monitored, and access-controlled independently,
3. policy clarity: trust governance (who can sign which SAN set) is explicitly a trust-plane concern.

## Operational Guidance

1. Use `ca_root=1` only on designated issuer nodes.
2. Do not pass `tls=` runtime arguments; TLS is always required.
3. Provide explicit `tls_ca` material for clients/connectors that need custom trust roots.
4. Provide explicit `tls_san` values for all expected IP and DNS dial targets.
5. Protect CA keys and shared TLS storage with strict filesystem permissions.

## Current Limits

- CA scope is storage-root based, so separate storage roots can form separate trust domains.
- Service announcements are descriptive, not policy-authoritative.

## SQL Authorization, Credentials, and WAL Durability

Authorization is enforced by request metadata and catalog ACL state:

- each parsed SQL request carries a required privilege,
- non-root sessions are checked before execution,
- object-level checks use referenced SQL objects, including multi-object statements such as joins,
- access requires privilege on every referenced object when object scope is involved.

### ACL mutation path

Administrative ACL and credential mutation remains intentionally narrow.

### User creation and credential model

- user creation persists:
  - an ACL entry for the user,
  - an encrypted user credential snapshot for the user.
- duplicate user creation is rejected unless `IF NOT EXISTS` is supplied.

### Root bootstrap credential note

Current runtime behavior still exposes a bootstrap `root` access path for first connection flows.

- initialization should require an explicit root password-set event rather than relying on a shared default root password,
- this event should be valid only during bootstrap/initialization scope,
- provisioning and orchestration systems should persist only a hash, verifier, or equivalent derived representation rather than raw plaintext root password material,
- the instance should not be considered ready for managed/cloud access until the initial root credential is set.

Until this is implemented in runtime behavior, treat any default bootstrap root path as a temporary compatibility mechanism rather than the intended long-term security posture.

- security WAL payloads are type-framed so ACL and credential payloads are decoded unambiguously,
- ACL WAL payloads store a complete ACL snapshot for the target user, not a delta patch,
- credential WAL payloads store a complete credential snapshot for the target user,
- replay resolves both ACL and credential state with latest-record-wins semantics per user,
- precedence is determined by transaction id, so older security snapshots are retained historically but do not override newer state.

This keeps security recovery deterministic after restart and aligns authorization and credential state with WAL-backed durability.
