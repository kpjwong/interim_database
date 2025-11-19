# Incremental Database Pipeline - Performance PoC

A comprehensive benchmark comparing **5 different approaches** to processing large-scale transaction data, demonstrating the trade-offs between library choice (Pandas vs Polars), execution strategy (eager vs lazy), and processing approach (full vs incremental).

## Overview

This proof-of-concept explores optimal strategies for processing millions of daily transaction records, comparing:

1. **Traditional Pandas** - The baseline approach most teams start with
2. **Eager Polars** - Modern columnar processing with immediate execution
3. **Lazy Polars** - Query optimization with deferred execution
4. **Eager Incremental** - Incremental updates using eager evaluation
5. **Lazy Incremental** - Incremental updates using lazy evaluation

## Key Results

**Processing 10 days of ~5M rows/day:**

| Approach | Total Time | Speedup vs Pandas | Best For |
|----------|------------|-------------------|----------|
| Pandas Baseline | 70.74s | 1.0x | Legacy systems |
| **Eager Polars Baseline** | **17.31s** | **4.1x** | Batch processing |
| Lazy Polars Baseline | 25.98s | 2.7x | Complex queries |
| Eager Incremental | 19.80s | 3.6x | Fast incremental updates |
| **Lazy Incremental** | **17.81s** | **4.0x** | **Production pipelines** |

See [results.md](results.md) for detailed analysis and day-by-day timing breakdowns.

## Architecture

### Data Flow

```
Raw Transaction Data (Compressed CSVs)
    ↓
Unzip & Load
    ↓
Clean & Validate
    ↓
Enrich (Join SKU & ZIP lookups)
    ↓
Transform & Calculate Metrics
    ↓
Aggregate by Date & SKU Group
    ↓
Write to SQLite Database
```

### Incremental Processing Strategy

```
┌─────────────────────────────────────────┐
│  SQLite Database (processed_sales)      │
│  - Stores last processed date per client│
│  - Tracks aggregated metrics            │
└─────────────────────────────────────────┘
           ↑                 ↓
    Write Results      Read Last Date
           ↑                 ↓
┌─────────────────────────────────────────┐
│  Polars Pipeline                        │
│  - Filter: WHERE date > last_date       │
│  - Process only new/changed records     │
│  - Lazy evaluation optimizes query      │
└─────────────────────────────────────────┘
           ↑
    Load from disk
           ↑
┌─────────────────────────────────────────┐
│  Compressed Transaction Files           │
│  transactions_day_N.zip                 │
└─────────────────────────────────────────┘
```

### File Structure

```
interim_database/
├── main.py                    # Main benchmark script (5 scenarios)
├── generate_data.py           # Synthetic data generator
├── poc_database.db           # SQLite database (created on first run)
├── data/                     # Generated transaction data (not in repo)
│   └── client_1/
│       ├── transactions_day_1.zip
│       ├── transactions_day_2.zip
│       └── ...
├── results.md                # Detailed benchmark results
├── README.md                 # This file
└── .gitignore               # Excludes data/, *.csv, *.db
```

## Requirements

### Python Dependencies

```bash
pip install polars pandas memory-profiler
```

**Version Requirements:**
- Python 3.8+
- polars >= 0.19.0
- pandas >= 2.0.0
- memory-profiler >= 0.61.0

### System Requirements

- **RAM:** 2GB+ available (benchmarks use ~850MB peak)
- **Disk:** 5GB+ free space for generated data
- **OS:** Windows, macOS, or Linux

## Setup & Usage

### 1. Generate Synthetic Data

```bash
python generate_data.py
```

This creates:
- 2 clients (client_1, client_2)
- 10 days per client
- ~5M rows per day (growing by 2,000 rows daily)
- 20 compressed CSV files total (~4GB uncompressed)

**Data Schema:**
- `transaction_id`: Unique identifier
- `transaction_date`: Date of transaction (YYYY-MM-DD)
- `sku_id`: Product SKU (1000 unique SKUs)
- `quantity_sold`: Quantity sold (1-10 units)
- `revenue_usd`: Revenue in USD
- `maturity_date`: Optional maturity date for certain products
- `zip_code_raw`: Customer ZIP code

### 2. Run Benchmark

```bash
python main.py
```

This executes all 5 scenarios sequentially:

1. **Scenario 1:** Pandas baseline (all 10 days)
2. **Scenario 2:** Eager Polars baseline (all 10 days)
3. **Scenario 3:** Lazy Polars baseline (all 10 days)
4. **Scenario 4:** Eager incremental (all 10 days)
5. **Scenario 5:** Lazy incremental (all 10 days)

**Expected Runtime:** 2-3 minutes for full benchmark

**Memory Profiling:** The script uses `@profile` decorators to track memory usage. Results are printed to console with line-by-line memory allocation details.

### 3. View Results

Results are printed to console and can be analyzed in detail in [results.md](results.md).

## Technical Deep Dive

### Why Eager Polars Beats Lazy for Full Scans?

When processing entire datasets without filtering:
- **Eager mode** reads data directly into memory and processes it
- **Lazy mode** adds query planning overhead without benefiting from predicate pushdown
- No rows are skipped, so lazy optimization provides no value
- Eager's simpler execution path wins

### Why Lazy Polars Shines for Incremental Updates?

When filtering to only new records:
- **Lazy mode** can push predicates down to the CSV reader
- Skips reading irrelevant rows entirely
- Optimizes the entire query plan before execution
- Streaming execution minimizes memory footprint

### Database Trade-offs

Incremental approaches add database overhead:
- Querying for last processed date (~5-10ms)
- Deleting and inserting aggregated results (~10-50ms)
- This overhead is offset by processing fewer rows
- Benefits increase exponentially with dataset growth

### Memory Usage

All approaches use similar peak memory (~850MB for 5M rows):
- Polars' columnar format is memory-efficient
- Lazy evaluation doesn't reduce peak memory for full scans
- Incremental processing reduces working set size

## Use Cases

### When to Use Each Approach

**Eager Polars Baseline (Scenario 2):**
- One-time batch processing
- Historical data recomputation
- Fast ad-hoc analysis
- No existing database state

**Lazy Incremental (Scenario 5):**
- Daily ETL pipelines
- Streaming data ingestion
- Long-running production systems
- Memory-constrained environments
- Datasets that grow over time

**Eager Incremental (Scenario 4):**
- Lower latency requirements
- Simpler query patterns
- When lazy optimization overhead exceeds benefits

**Pandas Baseline (Scenario 1):**
- Legacy codebases
- Small datasets (<1M rows)
- Pandas-specific functionality needed

## Key Learnings

### 1. Library Choice Matters Most
Switching from Pandas to Polars provides 2.7-4.1x speedup regardless of strategy. This is the highest-ROI optimization for most teams.

### 2. Incremental Processing is for Scale
Incremental gains are modest in this benchmark (1.3-1.5x), but the real value emerges when:
- Historical data grows to 100M+ rows
- Full reprocessing becomes prohibitively expensive
- You need predictable, consistent performance

### 3. Context Determines Eager vs Lazy
- **Full dataset scans:** Eager wins (simpler execution)
- **Filtered queries:** Lazy wins (predicate pushdown)
- **Memory constraints:** Lazy wins (streaming)
- **Low latency:** Eager wins (no planning overhead)

### 4. Database State Enables Incremental
SQLite provides:
- Persistent state tracking (last processed date)
- Efficient aggregated storage
- ACID guarantees for production reliability

## Future Enhancements

Potential areas for exploration:

1. **Partitioning:** Partition data by date for faster incremental queries
2. **Parallel Processing:** Process multiple days concurrently
3. **Streaming:** Process data as it arrives (no batch windows)
4. **DuckDB:** Compare against DuckDB's analytical query engine
5. **Arrow:** Use Apache Arrow for zero-copy data sharing
6. **Compression:** Test different compression algorithms (zstd, lz4)

## Contributing

This is a proof-of-concept for benchmarking purposes. Feel free to:
- Run benchmarks on your hardware and share results
- Test with different data volumes or schemas
- Propose alternative approaches
- Report issues or inconsistencies

## License

This project is provided as-is for educational and benchmarking purposes.

## Acknowledgments

- **Polars:** High-performance DataFrame library in Rust
- **Pandas:** The foundational Python data analysis library
- **memory-profiler:** Line-by-line memory profiling tool

---

**Questions or feedback?** Open an issue or submit a pull request.
