# Installation Advisories

This note captures operational advisories discovered during real deployment runs.

## 1. `tls_server` Must Be A Dial Target

When starting `server`, `tls_server` is a client-style outbound target used to fetch TLS material.
It must be a reachable host/port, for example:

- `tls_server=127.0.0.1:5443`
- `tls_server=localhost:5443`

Do not use `tls_server=0.0.0.0:5443` for outbound dialing.

`0.0.0.0` is appropriate as a bind/listen address, but it is not a reliable target for connect calls.

## 2. Bootstrap Memory Advisory (Large Datasets)

Large WAL replay plus runtime-index bootstrap can consume multiple GB of memory.
On low-memory hosts with no swap, Linux may OOM-kill `server`, which causes connector port flapping
(`4001` starts as reachable, then switches to `connection refused` after process death).

Recommended baseline:

- Memory: 8GB+ preferred for larger catalogs/WAL sets.
- Swap: enabled (for example 8GB) to avoid abrupt OOM termination during bootstrap.

## 3. Swap Provisioning Example (Linux)

```bash
fallocate -l 8G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab
```

Verify:

```bash
swapon --show
free -h
```

## 4. Quick Health Checks

Check listeners:

```bash
ss -ltnp | egrep ':4001|:4002|:5443' || netstat -ltnp | egrep ':4001|:4002|:5443'
```

Check process state:

```bash
ps -ef | egrep 'target/release/server|target/debug/server|tlsserver' | grep -v grep
```

Check local connector reachability:

```bash
nc -vz 127.0.0.1 4001
```

Check kernel OOM events:

```bash
dmesg -T | egrep -i 'oom|out of memory|killed process' | tail -n 50
```

## 5. Startup Recommendation (Current)

For remote hosts where `tlsserver` runs on the same machine, prefer:

```bash
./target/release/server datadir=./data -- \
  wss=on \
  tls_san=localhost,provision.distdb.com \
  tls_server=127.0.0.1:5443
```

If the deployment should issue a local-only certificate, set `TLS_SANS=localhost` in the startup script or pass `tls_san=localhost` explicitly.

Runtime-index bootstrap builds every declared non-temporary index. Profiles and
allowlists must not gate index generation, because doing so can make valid
indexed queries incomplete. `DISTDB_RUNTIME_INDEX_BUILD_WORKERS`,
`DISTDB_RUNTIME_INDEX_PRELOAD_ACCESSORS_ON_BOOTSTRAP`,
`DISTDB_RUNTIME_INDEX_BACKGROUND_PREWARM_SKIPPED_ACCESSORS`, and
`DISTDB_RUNTIME_INDEX_PARALLEL_BUILD_MIN_ROWS` tune resource use and startup
time only; they do not change index coverage.

These flags are stabilization levers, not long-term replacements for adequate memory sizing.
