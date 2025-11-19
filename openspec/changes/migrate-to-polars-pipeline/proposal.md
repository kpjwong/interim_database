# Proposal: Migrate to Polars Pipeline

## Why

The current implementation has two issues that compromise benchmark validity:

1. **Optimized pipeline uses qsv**: A Rust-based CSV command-line tool for filtering incremental data, which introduces:
   - External dependency requiring platform-specific binary installation
   - Subprocess overhead adding latency and complexity
   - Data serialization (write intermediate CSV files to disk)
   - Limited Python integration

2. **Apples-to-oranges comparison**: The baseline uses **Pandas** while the optimized pipeline uses **qsv**, making it impossible to isolate whether performance gains come from:
   - The incremental processing strategy (database state + filtering)
   - The underlying data processing library differences
   - Or both

Recent benchmarks demonstrate that **Polars** (a high-performance DataFrame library with lazy evaluation) is significantly faster than both Pandas and DuckDB:
- **9x faster** than DuckDB (0.29s vs 2.59s on 10M rows)
- **10x faster** than Pandas (0.29s vs 2.81s on 10M rows)

**Solution**: Migrate **both baseline and optimized pipelines** to use Polars. This provides an apples-to-apples comparison where the **only variable** is the incremental vs full reprocessing strategy.

## What Changes

- **REPLACE** baseline Pandas implementation with Polars (full reprocessing, no filtering)
- **REPLACE** optimized qsv-based filtering with Polars lazy evaluation (with filtering)
- **UPDATE** both benchmark functions to use Polars LazyFrame API
- **UPDATE** project documentation to reflect Polars as the core technology for both pipelines
- **REMOVE** qsv installation requirements and subprocess handling code
- **ADD** Polars library to project dependencies

### Breaking Changes

None. This is an internal implementation change that improves benchmark validity without altering the methodology or output format. Results will change numerically, but comparison structure remains the same.

## Impact

- **Affected specs**: `data-processing` (new capability being specified)
- **Affected code**:
  - `main.py`: `run_optimized_benchmark()` function (~70 lines)
  - `openspec/project.md`: Tech stack section
  - Dependencies: Remove qsv requirement, add Polars
- **Performance expectations**:
  - Expected speed improvement: Maintain or exceed current optimized performance (5-10x faster than baseline)
  - Reduced complexity: Eliminate subprocess calls and intermediate file I/O
