# Design: Migrate to Polars Pipeline

## Context

This PoC compares two data processing approaches:
1. **Baseline**: Full reprocessing (no database state, process entire dataset)
2. **Optimized**: Incremental processing (with database state management, filter based on last processed date)

### Current Problem

The current implementation has a **methodological flaw**:
- Baseline uses **Pandas** for full reprocessing
- Optimized uses **qsv** (Rust tool) for filtering + **Pandas** for aggregation

This creates an **apples-to-oranges comparison** where we're testing:
1. Full vs incremental strategy **AND**
2. Pandas vs qsv performance **simultaneously**

We cannot isolate which factor contributes more to performance improvements.

### Current Architecture

```
Baseline Pipeline (Current):
1. Unzip CSV to disk
2. Load entire CSV with Pandas read_csv
3. Aggregate with Pandas groupby

Optimized Pipeline (Current):
1. Query DB → last_processed_date
2. Unzip CSV to disk
3. Spawn qsv subprocess → filter rows → write filtered CSV
4. Load filtered CSV with Pandas
5. Aggregate with Pandas groupby
6. Write to SQLite
```

### Constraints

- Must provide apples-to-apples comparison (isolate the incremental strategy variable)
- Must maintain measurement granularity (separate timing for each stage)
- PoC scope: Focus on performance, not production features

## Goals / Non-Goals

### Goals

- **Provide apples-to-apples comparison**: Use the same data processing library (Polars) for both pipelines
- **Isolate the incremental strategy variable**: Ensure performance differences reflect the incremental approach, not library differences
- Eliminate external binary dependency (qsv)
- Reduce subprocess and disk I/O overhead
- Leverage Polars' lazy evaluation for optimal performance
- Maintain clear separation of timing measurements

### Non-Goals

- Not comparing Pandas vs Polars (already benchmarked separately in test.py)
- Not adding parallel processing or advanced optimization features
- Not implementing production error handling or logging

## Decisions

### Decision 1: Use Polars for Both Baseline and Optimized Pipelines

**Rationale**: To provide a scientifically valid comparison, both pipelines must use the same underlying data processing library. This isolates the performance impact of the **incremental strategy** (database state + filtering) from library performance differences.

**Why not keep Pandas in baseline?**
- We already proved Polars is 10x faster than Pandas in separate benchmarks (test.py)
- Comparing Pandas (baseline) vs Polars+qsv (optimized) conflates two variables
- The PoC goal is to demonstrate incremental processing benefits, not library performance

**Implementation**:
- **Baseline**: Polars full reprocessing (scan entire CSV, groupby, no filtering)
- **Optimized**: Polars incremental (scan CSV, filter by date, groupby)

### Decision 2: Use Polars LazyFrame API

**Rationale**: Polars' lazy evaluation (`scan_csv`) enables query optimization without loading the full dataset into memory. The query plan is optimized before execution, allowing predicate pushdown and efficient filtering.

**Implementation**:
```python
import polars as pl

# Baseline: Full processing (no filter)
lazy_df = (
    pl.scan_csv(temp_csv)
    .group_by(["sku_id", "transaction_date"])
    .agg([
        pl.col("quantity_sold").sum().alias("total_quantity"),
        pl.col("revenue_usd").sum().alias("total_revenue")
    ])
)

# Optimized: Incremental processing (with filter)
lazy_df = (
    pl.scan_csv(temp_csv)
    .filter(pl.col("transaction_date") > last_processed_date)
    .group_by(["sku_id", "transaction_date"])
    .agg([
        pl.col("quantity_sold").sum().alias("total_quantity"),
        pl.col("revenue_usd").sum().alias("total_revenue")
    ])
)

# Execute optimized query plan
result_df = lazy_df.collect()
```

**Alternatives considered**:
- **DuckDB**: Excellent for SQL workflows, but benchmark showed 9x slower than Polars
- **Keep Pandas baseline + qsv optimized**: Creates apples-to-oranges comparison (rejected)
- **Polars eager API**: Would load full CSV; lazy API is more performant

### Decision 3: Combine Filter and Aggregation in Single Pipeline

**Rationale**: Polars allows chaining filter → group_by → agg in a single optimized query plan. This eliminates intermediate CSV serialization that qsv required.

**Trade-off**: Slightly less granular timing (filter + aggregate combined), but more accurately reflects real-world usage and provides cleaner code.

### Decision 4: Keep Pandas for Database Writing

**Rationale**: Pandas' `.to_sql()` is well-tested for SQLite bulk inserts. Converting Polars DataFrame → Pandas is cheap and avoids rewriting database logic.

**Alternative**: Use Polars' native database connectors (via SQLAlchemy), but adds unnecessary complexity for PoC scope.

## Risks / Trade-offs

### Risk: Polars API Stability

- **Mitigation**: Pin Polars version in requirements; Polars is production-ready (v1.x released)

### Risk: Benchmark Comparison Validity

- **Concern**: Changing the optimized implementation might invalidate comparisons
- **Mitigation**: The baseline Pandas pipeline remains unchanged, providing consistent comparison baseline. Polars migration only improves the optimized path.

### Trade-off: Combined Timing

- **Before**: Separate timings for "qsv filter" and "pandas aggregate"
- **After**: Combined timing for "polars process" (filter + aggregate)
- **Justification**: More representative of end-to-end performance; granular subprocess timing was less meaningful

## Migration Plan

1. **Add Polars dependency**: `pip install polars`
2. **Update main.py**:
   - Replace qsv subprocess code with Polars LazyFrame
   - Update timing labels and result keys
   - Remove QSV_PATH configuration
3. **Update documentation**:
   - README.md tech stack section (already done by user)
   - project.md prerequisites
4. **Test with existing data**: Run benchmarks on generated data to verify correctness
5. **Update results.md template**: Adjust labels from "qsv filter" to "Polars process"

### Rollback

If issues arise: Revert main.py changes and restore qsv dependency documentation. No database schema or data changes required.

## Open Questions

None. Implementation path is clear based on benchmark results and Polars' well-documented API.
