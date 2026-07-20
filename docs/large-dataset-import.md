# Large Dataset Import Runbook

This runbook documents a safe, high-throughput path for loading multi-million row SQL datasets into DistDB using the console import pipeline.

## Why this path

The console import path is optimized for large files:

- Streams SQL from disk instead of loading it all into memory.
- Splits very large `INSERT ... VALUES` statements into bounded chunks.
- Batches DML into explicit transaction windows for lower commit overhead.
- Retries transient transport failures.

## Prerequisites

- A reachable server peer address.
- A prepared target database and table schema.
- A SQL file with row data (typically many `INSERT ... VALUES` statements).
- The server should be started with non-primary runtime index materialization disabled unless you explicitly need selected secondary indexes during ingest:

```bash
DISTDB_RUNTIME_INDEX_MATERIALIZE_NON_PRIMARY=0
```

## Fast-start command

Use the helper script:

```bash
bash scripts/import_large_dataset.sh \
  --server 127.0.0.1:3316 \
  --database appdb \
  --file /absolute/path/to/dataset.sql
```

Optional auth/TLS examples:

```bash
bash scripts/import_large_dataset.sh \
  --server 10.0.0.20:3316 \
  --database appdb \
  --file /data/seed/big.sql \
  --user root@12D3KooW... \
  --password 'your-password' \
  --tls required \
  --tls-ca /path/to/ca.pem
```

## Throughput tuning knobs

The import engine supports runtime knobs via environment variables:

- `IMPORT_INSERT_CHUNK_MAX_TUPLES`: max tuples per split INSERT chunk.
- `IMPORT_INSERT_CHUNK_BYTES`: target bytes per split INSERT chunk.
- `IMPORT_TX_BATCH_SIZE`: DML statements per transaction batch.
- `IMPORT_TX_BATCH_MAX_AGE_MS`: max transaction age before commit.

Recommended starting points:

- Balanced profile (default in helper script):
  - `IMPORT_INSERT_CHUNK_MAX_TUPLES=1024`
  - `IMPORT_INSERT_CHUNK_BYTES=524288`
  - `IMPORT_TX_BATCH_SIZE=1200`
  - `IMPORT_TX_BATCH_MAX_AGE_MS=1500`
- Conservative profile (more stable on constrained hosts):
  - `IMPORT_INSERT_CHUNK_MAX_TUPLES=512`
  - `IMPORT_INSERT_CHUNK_BYTES=262144`
  - `IMPORT_TX_BATCH_SIZE=500`
  - `IMPORT_TX_BATCH_MAX_AGE_MS=1000`
- Aggressive profile (only after validation):
  - `IMPORT_INSERT_CHUNK_MAX_TUPLES=2000`
  - `IMPORT_INSERT_CHUNK_BYTES=1048576`
  - `IMPORT_TX_BATCH_SIZE=2000`
  - `IMPORT_TX_BATCH_MAX_AGE_MS=2000`

## Operational guidance for 4M+ rows

- Keep statements simple to stay on the fast insert path:
  - Prefer plain `INSERT INTO ... VALUES ...`.
  - Avoid `ON DUPLICATE KEY UPDATE`, `REPLACE`, and `RETURNING` for initial bulk load.
- Confirm the server boot log reports `materialize_non_primary=false` before starting the import.
- Disable or defer non-essential secondary indexes during initial load when possible.
- Run imports from a stable host close to the server node to reduce transport jitter.
- Start with the balanced profile, then increase only one knob at a time.

## Validation checklist

1. Run pre-flight count in source file if available.
2. Import dataset.
3. Run target `COUNT(*)` and spot checks by key ranges.
4. Inspect logs for repeated transport retries or duplicate-key skips.
5. If throughput degrades over time, lower chunk size first, then batch size.

## Failure handling

- Duplicate key rows are skippable by the import pipeline.
- Transient transport issues are retried automatically.
- If import stalls or repeatedly retries:
  1. Reduce `IMPORT_INSERT_CHUNK_BYTES`.
  2. Reduce `IMPORT_TX_BATCH_SIZE`.
  3. Re-run from last known checkpoint boundary in your SQL file.
