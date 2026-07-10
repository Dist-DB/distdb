# Running A DistDB Service Instance

This guide shows how to run one local DistDB server instance and connect to it with the console client.

## Prerequisites

- Rust toolchain installed (`cargo`, `rustc`)
- macOS/Linux shell (commands below use `bash`/`zsh`)
- workspace checked out locally

All commands below are run from the repository root unless noted.

## 1. Build The Server And Console

```bash
cargo build --manifest-path server/Cargo.toml
cargo build --manifest-path console/Cargo.toml
```

## 2. Start One Server Instance

Option A (recommended for local development):

```bash
cd server
./debug.sh
```

Option B (explicit runtime flags):

```bash
cd server
cargo run -- \
	node_id=server-node-01 \
	datadir=./data/server-node-01 \
	listen_addr=127.0.0.1 \
	port=9400 \
	tls=off
```

Notes:

- `node_id` identifies this server instance.
- `datadir` is where local state and WAL data are stored.
- server default TLS mode is `required` when `tls=` is omitted.
- `tls=off` in the example above is an explicit local-dev override for smoke testing.
- when `tls=required` is used, the server does not fall back to plaintext.

## 3. Connect With Console

In a second terminal:

```bash
cd console
./debug.sh
```

Or explicitly point console to your server address:

```bash
cd console
cargo run -- 127.0.0.1:9400 tls=off user=root@server-node-01
```

For TLS-required runs, configure console with `tls=required` and a trust root:

```bash
cd console
cargo run -- 127.0.0.1:9400 tls=required tls_ca=/path/to/ca.pem user=root@server-node-01
```

## 4. Authenticate And Run A Quick Check

In the console session:

```sql
password root;
create database main;
use main;
create table users (id uint64 primary key, email text);
create index idx_users_email on users(email);
show indexes from users;
insert into users (id, email) values (1, 'sam@example.com');
select count(*) as c_all from users;
quit;
```

Expected result: `c_all` should return `1`.

`show indexes from users;` should include at least:

- the primary-key-derived index for `id`,
- the user-defined `idx_users_email` index.

If you restart the server with the same `datadir`, both indexes should still appear in `SHOW INDEXES` output.

## 5. Stop The Service

- If running in foreground (`cargo run` or `./debug.sh`): press `Ctrl+C` in the server terminal.
- If started in background: terminate the process with `kill <pid>`.

## Troubleshooting

- If console cannot connect, confirm server is listening on the same `listen_addr`/`port`.
- If authentication fails, use `password root;` for bootstrap root access.
- If you changed ports or node id, pass matching values in the console `user=root@<node_id>` and target address.
