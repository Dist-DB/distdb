# OLAP Views

This page documents DistDB's OLAP (Online Analytical Processing) view feature, which provides memory-resident, coordinate-addressed analysis of committed table data.

## Overview

An **OLAP view** is a named, catalog-persisted definition that describes how to organize table data into a **hypercube** — a multi-dimensional structure suitable for rapid analytical queries.

Syntax:
```sql
CREATE OLAPVIEW <name> USING <col1>, <col2>, ... AS <select_sql>
```

Example:
```sql
CREATE OLAPVIEW sales_by_region_product 
USING region, product 
AS SELECT id, region, product, quantity, revenue FROM orders
```

Key characteristics:
- **Definition persists**: The OLAPVIEW SQL definition is WAL-backed and survives restarts.
- **Cube is memory-resident**: The hypercube is built from committed live rows at startup or on demand, never persisted to disk.
- **Multi-dimensional**: You can pivot on multiple columns (e.g., region, product, year).
- **Fast aggregation**: Each cube cell pre-computes measures (SUM, COUNT, MIN, MAX, AVG) over coordinate groups.

---

## Design Philosophy

### Lessons from Gentia (1996)

Gentia pioneered OLAP cubes but faced a critical pain point: **cube generation was a manual, batch process** decoupled from the data store. This meant:
- Cubes were stale relative to fresh data
- Rebuilds were expensive and scheduled offline
- Users couldn't ask ad-hoc questions about analytical structure

DistDB eliminates this via **definition-based derivation**. When you create an OLAPVIEW:

1. The definition is immediately registered in the catalog
2. The hypercube is built from current committed data
3. On table commits, the cube can be invalidated/refreshed automatically
4. On restart, the definition persists; the cube is rebuilt from warm live rows

Result: **The cube is always current or explicitly marked stale.**

### Memory-Resident, Not Persisted

The hypercube is a **derived structure**, not a new storage tier:
- It holds references (row ids) back to the source `RuntimeIndex`, not duplicate row data
- It is rebuilt from the source rows, not replayed from a separate WAL
- It is cache-like: stale cubes are invalidated and rebuilt on commit, or on query if the query layer chooses to serve stale with a watermark

This design avoids:
- Dual write paths (one to table, one to cube WAL)
- Replication complexity (cube state needn't cross affinity boundaries)
- Storage bloat (no cube snapshots on disk)

---

## Hypercube Concept

### Coordinate-Addressed Cells

A hypercube is a coordinate-indexed map where:

```
Key:   (DimensionCoordinate, DimensionCoordinate, ...)  
Value: HypercubeCell { source_row_ids, measures }
```

**Example** — OLAPVIEW with z-dimensions `region` and `product`:

```
Coordinate                   | Cell Data
─────────────────────────────┼───────────────────────────
("EU", "Widget-A")           | rows: [1, 3, 7], measures: [sum=1245, count=3, avg=415]
("EU", "Widget-B")           | rows: [2, 5], measures: [sum=892, count=2, avg=446]
("US", "Widget-A")           | rows: [4, 6, 8], measures: [sum=2104, count=3, avg=701.3]
("US", "Widget-B")           | rows: [9], measures: [sum=567, count=1, avg=567]
```

Each cell preserves:
- **source_row_ids**: Ids that reference rows in `RuntimeIndex`, enabling drill-through queries
- **measures**: Pre-aggregated values (one per measure column in the OLAPVIEW schema)

### Dimensions vs. Measures

**Dimensions** are the pivot axes:
- Declare via `USING <col1>, <col2>, ...` 
- Must be comparable and hashable (text, integer, boolean, NULL)
- Float columns should not be dimensions (use bucketing or materialized text representations instead)
- Index 0 is the **primary z-dimension** (historical name from Gentia)
- Index 1+ are secondary dimensions for multi-dimensional slicing

**Measures** are numeric aggregates:
- Any numeric column in the SELECT can be a measure
- Currently supported aggregations: SUM, COUNT, MIN, MAX, AVG
- Each cell pre-computes the aggregate over all rows contributing to that coordinate

---

## Creating OLAPVIEW

### Single Dimension

```sql
CREATE OLAPVIEW sales_by_region 
USING region 
AS SELECT id, region, quantity, revenue FROM orders
```

Hypercube shape: 1D — one row per unique region value.

### Multiple Dimensions

```sql
CREATE OLAPVIEW sales_by_region_product_year
USING region, product, year
AS SELECT id, region, product, year, quantity, revenue FROM orders
```

Hypercube shape: 3D — cells indexed by (region, product, year) tuples.

### Validation

- The SELECT schema must include all columns listed in `USING`
- Column names are case-insensitive and normalized to lower-case
- At execution time, the hypercube builder validates that all dimension columns are present in live rows
- Rows with NULL in a dimension coordinate contribute to a `NULL` cell (hashable in Rust, valid coordinate)

---

## Querying Slices

### SHOW SLICES

Return metadata about all cells (slices) in an OLAPVIEW:

Current runtime status:
- `SHOW SLICES FROM <olapview>` is recognized by SQL classification.
- Slice materialization is wired for a first-pass implementation that returns dimension coordinates, `row_count`, and numeric per-slice aggregates (`sum_<field>`, `min_<field>`, `max_<field>`, `avg_<field>`) for non-dimension numeric fields projected by the OLAPVIEW SELECT.
- Richer measure selection semantics (for example explicit aggregate configuration) remain follow-up work.

```sql
SHOW SLICES FROM sales_by_region_product_year
```

Result set:
```
┌────────┬─────────────┬──────┬──────────┬─────────────┬─────────────────────┐
│ region │ product     │ year │ row_count│ sum_quantity│ max_revenue         │
├────────┼─────────────┼──────┼──────────┼─────────────┼─────────────────────┤
│ EU     │ Widget-A    │ 2024 │    1245  │       5100  │        12500.50     │
│ EU     │ Widget-B    │ 2024 │     892  │       3200  │         8900.25     │
│ US     │ Widget-A    │ 2025 │    2104  │       7500  │        18200.00     │
│ US     │ Widget-B    │ 2025 │     567  │       1800  │         4200.75     │
└────────┴─────────────┴──────┴──────────┴─────────────┴─────────────────────┘
```

Columns returned:
- Each z-dimension column value
- `row_count`: number of source rows in this cell
- Pre-computed measures (SUM, COUNT, etc.)
- Optional: `last_updated_tx_id`, `stale_status`

Ordering behavior (current):
- SHOW SLICES rows are returned in deterministic coordinate order.
- For each dimension position, `NULL` coordinates sort before non-`NULL` values.
- SHOW SLICES supports first-pass post-processing with `WHERE` (simple `AND` conjunctions over emitted columns), a single `ORDER BY <column> [ASC|DESC]`, and `LIMIT <n>` over the emitted slice result columns.

### Drill-Through (Future)

Once `SHOW SLICES` returns cell metadata, a future enhancement could allow:

```sql
SELECT * FROM sales WHERE (region = 'EU' AND product = 'Widget-A' AND year = 2024)
```

to retrieve the original rows that contributed to the cell (using the cached row ids from the hypercube).

---

## Multi-Dimensional Analysis

### Slicing

Query a subset of the hypercube by filtering dimensions:

```sql
SHOW SLICES FROM sales_by_region_product_year
WHERE region = 'EU'
```

Returns all cells where region is 'EU', regardless of product or year.

### Dicing

Future support for combining multiple dimension filters:

```sql
SHOW SLICES FROM sales_by_region_product_year
WHERE region IN ('EU', 'US') AND year = 2024
```

### Rollup / Drill-Down

Currently explicit via separate OLAPVIEW definitions:

```sql
-- High-level view
CREATE OLAPVIEW sales_by_region USING region AS SELECT ...

-- Detailed view
CREATE OLAPVIEW sales_by_region_product USING region, product AS SELECT ...
```

Future: computed hierarchies via view derivation (e.g., automatically aggregate regional cubes into a global total).

---

## Performance Considerations

### Cube Build Time

Hypercube construction is O(n × d) where:
- n = number of live rows in the source table
- d = number of dimensions

Bottleneck: repeated HashMap lookups per row. Optimization strategies for large tables:
- Parallel cube building (future)
- Streaming/incremental updates (future)
- Bucketing float dimensions to reduce cardinality

### Memory Footprint

Each cell stores:
- Coordinate key: `Vec<DimensionCoordinate>` (O(d) per cell)
- Row ids: `Vec<u64>` (O(r) where r = avg rows per cell)
- Measures: `Vec<Option<f64>>` (O(m) where m = # measures)

Total: roughly `O(cells × (d + avg_rows_per_cell + measures))`.

For wide dimensions (high cardinality), cell count can grow explosively. Mitigation:
- Use coarse dimensions (e.g., month, not day)
- Bucket continuous values (e.g., age ranges)
- Use secondary views for detailed drill-down

### Cache Invalidation

When a table commits DML:
- Option A: **Lazy**: Mark cube stale, rebuild on next query
- Option B: **Eager**: Invalidate immediately, rebuild post-commit
- Option C: **Incremental**: Update affected cells in-place (complex, future)

Current: Lazy invalidation (Option A) minimizes commit latency.

---

## Current Limitations

1. **No WHERE clause in OLAPVIEW**
   - The SELECT must define the full data scope
   - Future: support `CREATE OLAPVIEW ... AS SELECT ... WHERE ...` with predicate pushdown

2. **Float dimensions not supported**
   - Float coordinates are not hashable without bit-casting
   - Workaround: bucket floats to bucketed integers or text ranges upfront in SELECT

3. **Computed measures only**
   - No user-defined measure functions yet
   - Roadmap: user-defined aggregations via Rust UDF registration

4. **Single table source**
   - OLAPVIEW SELECT can only reference one table
   - Future: joins and multi-table cubes

5. **No incremental updates**
   - Cube rebuild is full recomputation, not delta
   - Roadmap: stream-based delta application for write-heavy tables

6. **Read-only**
   - Cubes cannot be written to directly
   - All updates go through the source table

---

## Roadmap

### Near-term (Beta)
- [ ] Parallel hypercube building for large tables
- [ ] Explicit cube refresh schedules (e.g., post-commit or on-demand)
- [ ] Extended `SHOW SLICES` filtering beyond current first-pass `WHERE`/`ORDER BY`/`LIMIT` semantics

### Medium-term
- [ ] Incremental cube updates (delta application on DML)
- [ ] User-defined measure functions (UDAFs)
- [ ] Multi-table cubes (joins in OLAPVIEW SELECT)
- [ ] Computed hierarchies (automatic rollup dimensions)

### Long-term
- [ ] OLAP-aware query optimizer (e.g., route aggregates to cube instead of re-scanning table)
- [ ] Materialized views integration (shared cube infrastructure)
- [ ] Real-time OLAP (streaming updates via Kafka/event sources)

---

## Examples

### Example 1: Sales Dashboard

```sql
-- Create a regional sales cube
CREATE OLAPVIEW regional_sales 
USING region, quarter 
AS SELECT order_id, region, quarter, amount FROM sales_transactions;

-- View all slices
SHOW SLICES FROM regional_sales;

-- Specific region
SHOW SLICES FROM regional_sales WHERE region = 'EU';
```

### Example 2: Product Analytics

```sql
-- Multi-dimensional product performance
CREATE OLAPVIEW product_performance 
USING category, product, market 
AS SELECT 
    id, category, product, market, 
    units_sold, revenue, margin 
FROM product_sales;

-- See all product-market combinations
SHOW SLICES FROM product_performance;
```

### Example 3: Time-Series Aggregation

```sql
-- Sales over time by region
CREATE OLAPVIEW sales_timeseries 
USING region, date 
AS SELECT order_id, region, date(created_at) as date, total FROM orders;

-- Drill down by region and day
SHOW SLICES FROM sales_timeseries WHERE region = 'US';
```

---

## Related Pages

- [sql-compliance.md](sql-compliance.md) — Feature coverage overview
- [using.md](using.md) — How to use DistDB operationally
- [select-architecture.md](select-architecture.md) — SELECT execution design
