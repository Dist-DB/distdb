# Security Model And Options

This document describes the currently implemented security controls in DistDB,
the command-line options that affect them, and how enforcement works at runtime.

## Overview

DistDB secures network traffic with TLS and supports cluster CA bootstrapping,
certificate enrollment, and CA-root role assignment.

Security is enforced across three layers:

1. Server listener and outbound peer transport (`server` crate)
2. Cluster certificate authority and certificate lifecycle (`serverlib` crate)
3. Client/console connector transport (`connector` and `console` crates)

## Server Security Options

The server accepts the following security-related runtime args.

### TLS mode

- `tls=off|optional|required`
- Default: `optional`

Behavior:

- `off`: plaintext only
- `optional`: accepts TLS and plaintext inbound; prefers TLS outbound where possible
- `required`: TLS must succeed for protected paths

### TLS material paths

- `tls_cert=/path/to/cert.pem`
- `tls_key=/path/to/key.pem`
- `tls_ca=/path/to/ca.pem`

If omitted and TLS is enabled, DistDB can auto-provision or enroll material.

### CA root role

- `ca_root`
- `ca_root=1|true|on|yes`

When enabled, this node is treated as a CA issuer node.

### Subject Alt Names for generated/enrolled certs

- `tls_san=host-or-ip`
- Multiple values supported by comma separation and repeated args.

### Service announcements

- `service=name`
- Multiple values supported by comma separation and repeated args.

Defaults include:

- `sql.query`
- `p2p.discovery`
- `affinity.replication`
- `tls.ca.distribution`

If `ca_root` is enabled, `tls.enrollment.issuer` is also advertised.

## Client And Console Security Options

Client and console support:

- `tls=off|optional|required`
- `tls_ca=/path/to/ca.pem`

Default mode is `optional`.

If `tls_ca` is not provided, connector logic attempts CA auto-discovery from peers
before TLS handshake, then uses the discovered CA in-memory for certificate verification.

## Enforcement Details

## 1) TLS enforcement on server inbound connections

Server connector negotiation is governed by `TlsMode`:

- `off`: connection remains plaintext
- `required`: TLS handshake must succeed
- `optional`: server probes for TLS ClientHello and upgrades if present

## 2) TLS enforcement on outbound peer calls

Server outbound transport follows server TLS mode:

- `required`: fails if TLS client config is unavailable or handshake fails
- `optional`: attempts TLS first, can fall back to plaintext
- `off`: plaintext only

## 3) CA-root and certificate issuance enforcement

Enrollment signing is explicitly gated:

- If a node receives `TlsCertEnrollRequest` and `ca_root` is not enabled,
  it returns a rejection response.
- Only CA-root nodes should issue certs for enrolling peers.

## 4) CA uniqueness and race safety

Auto-TLS generation in `serverlib`:

- Reuses existing CA cert/key when present
- Uses lock-file coordination to avoid concurrent CA creation races
- Waits for CA material if another process is initializing it

This enforces one CA per shared `p2p-tls` storage location.

## 5) CA distribution and trust bootstrap

Two trust bootstrap paths are implemented:

1. Service-level CA distribution (`TlsCaDistribution`) for peer propagation
2. Lightweight CA bootstrap wire protocol (`CACB`) for connector auto-discovery

CA material transferred is public CA cert only. Private keys are never transported.

## 6) CSR enrollment flow enforcement

When a non-CA-root server needs cert material:

1. Generates private key + CSR locally
2. Sends `TlsCertEnrollRequest` to peers
3. Receives signed cert + CA cert from issuer
4. Installs local key/cert/CA and proceeds with TLS

If enrollment fails, server can fall back to local generation (depending on runtime path).

## 7) Client certificate validation

Connector TLS client builds a rustls root store from:

- `tls_ca` file if provided, or
- auto-discovered CA PEM from bootstrap path

Server identity is validated using hostname/IP-derived `ServerName`.

## 8) Rustls crypto provider enforcement

Process startup installs rustls crypto provider explicitly (`ring`) before TLS use.
This avoids runtime ambiguity and panics from implicit provider selection.

## Operational Recommendations

1. Production clusters: use `ca_root=1` on a designated issuer node.
2. Prefer `tls=required` in production once all clients are configured.
3. Keep `tls=optional` only during staged rollout or mixed compatibility phases.
4. Use explicit `tls_san` entries for all expected IP/DNS dial targets.
5. Protect CA key files and shared `p2p-tls` storage with strict filesystem permissions.

## Current Limitations

1. In `tls=optional`, plaintext fallback is still possible when TLS negotiation fails.
2. CA scope is storage-root based; independent storage roots can form separate trust domains.
3. Service announcements are informational and not yet policy-authoritative access controls.

