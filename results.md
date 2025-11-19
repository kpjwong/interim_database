# Incremental Database Pipeline - Benchmark Results

## Executive Summary

This benchmark compares **4 different approaches** to processing transaction data over 10 days, demonstrating the evolution from traditional full reprocessing to optimized incremental processing with hybrid and lazy evaluation strategies.

**Key Findings:**
- **Eager Incremental (Hybrid)** is the fastest at 17.41s (4.0x faster than Pandas)
- **Lazy Incremental** achieves 17.88s, nearly matching hybrid performance
- **Hybrid eager approach** eliminates query planning overhead while retaining predicate pushdown benefits
- Incremental approaches provide consistent, predictable performance for ongoing updates

---

## Benchmark Configuration

- **Client ID:** client_1
- **Number of Days:** 10 days of transaction data
- **Data Volume:** ~5M rows per day (growing by 2,000 rows daily)
- **Test Date:** 2025-11-19
- **Processing Library:** Polars (both eager and lazy modes) + Pandas baseline
- **Database:** SQLite with `processed_sales` table

---

## Results Summary: 10-Day Cumulative Performance

### Scenario 1: Pandas Baseline (Full Reprocessing)
**Total Time: 69.14 seconds (1.15 minutes)**

Traditional pandas-based processing, reprocessing entire dataset each day.

- Uses pandas for all data operations
- Reprocesses all historical data every day
- Simple implementation but scales poorly

### Scenario 2: Lazy Polars Baseline (Full Reprocessing)
**Total Time: 24.96 seconds (0.42 minutes)**

Full reprocessing using Polars with lazy evaluation (`scan_csv`).

- Uses lazy Polars evaluation (query optimization)
- Reprocesses all historical data every day
- **2.8x faster than Pandas**
- Query planning overhead limits performance on full scans

### Scenario 3: Eager Incremental (Hybrid) - Database State + Filtering
**Total Time: 17.41 seconds (0.29 minutes)**

Incremental processing using hybrid eager/lazy Polars with database state tracking.

- **Hybrid approach:** Lazy scan + filter (predicate pushdown), then eager execution
- Processes only new/changed data
- **4.0x faster than Pandas**
- **1.4x faster than Lazy Polars Baseline**
- **Fastest overall** - eliminates query planning overhead after filtering

### Scenario 4: Lazy Incremental (Database State + Filtering)
**Total Time: 17.88 seconds (0.30 minutes)**

Incremental processing using lazy Polars with database state tracking.

- Uses fully lazy Polars with database state
- Processes only new/changed data
- **3.9x faster than Pandas**
- **1.4x faster than Lazy Polars Baseline**
- Slightly slower than hybrid due to continued query planning overhead

---

## Performance Comparison Matrix

| Scenario | Total Time | vs Pandas | vs Scenario 2 | Strategy |
|----------|------------|-----------|---------------|----------|
| 1. Pandas Baseline | 69.14s | 1.0x | 2.8x slower | Full reprocessing, pandas |
| 2. Lazy Polars Baseline | 24.96s | 2.8x | 1.0x | Full reprocessing, lazy Polars |
| 3. **Eager Incremental (Hybrid)** | **17.41s** | **4.0x** | **1.4x faster** | Incremental, hybrid approach |
| 4. Lazy Incremental | 17.88s | 3.9x | 1.4x faster | Incremental, lazy Polars |

---

## Key Insights

### 1. Library Performance Matters Most
The jump from Pandas to Polars provides the largest speedup (2.8-4.0x). Polars' columnar architecture and Rust implementation deliver exceptional performance.

### 2. Hybrid Eager Approach Wins
The hybrid eager incremental approach achieves the best performance (17.41s):
- **Predicate pushdown benefits** from lazy scan and filter
- **Eliminates query planning overhead** after filtering with eager execution
- **2.6% faster** than fully lazy incremental
- Best of both worlds for incremental processing

### 3. Incremental Strategy is Critical at Scale
Incremental processing provides strong speedups (1.4x over lazy baseline). The real value emerges when:
- Historical data grows to 100M+ rows
- Full reprocessing becomes prohibitively expensive
- You need predictable, consistent performance

### 4. When to Use Each Approach

**Pandas Baseline (Scenario 1):**
- Legacy codebases requiring pandas
- Small datasets (<1M rows)
- Pandas-specific functionality needed

**Lazy Polars Baseline (Scenario 2):**
- One-time batch processing without state
- Complex query patterns
- When memory constraints require streaming

**Eager Incremental / Hybrid (Scenario 3) - RECOMMENDED:**
- Daily ETL pipelines
- Production incremental updates
- When predictable performance matters
- Best overall performance for incremental workloads

**Lazy Incremental (Scenario 4):**
- Extreme memory constraints
- When query planning benefits outweigh overhead
- Alternative to hybrid when fully lazy pipeline preferred

---

## Timing Breakdown by Day

### Scenario 1: Pandas Baseline
| Day | Time |
|-----|------|
| Day 1 | 7.41s |
| Day 2 | 6.98s |
| Day 3 | 6.77s |
| Day 4 | 6.87s |
| Day 5 | 6.71s |
| Day 6 | 6.90s |
| Day 7 | 6.84s |
| Day 8 | 6.88s |
| Day 9 | 6.88s |
| Day 10 | 6.88s |

### Scenario 2: Lazy Polars Baseline
| Day | Time |
|-----|------|
| Day 1 | 2.31s |
| Day 2 | 2.53s |
| Day 3 | 2.58s |
| Day 4 | 2.86s |
| Day 5 | 2.45s |
| Day 6 | 2.47s |
| Day 7 | 2.42s |
| Day 8 | 2.36s |
| Day 9 | 2.49s |
| Day 10 | 2.50s |

### Scenario 3: Eager Incremental (Hybrid)
| Day | Time |
|-----|------|
| Day 1 | 2.55s |
| Day 2 | 1.68s |
| Day 3 | 1.66s |
| Day 4 | 1.60s |
| Day 5 | 1.60s |
| Day 6 | 1.65s |
| Day 7 | 1.63s |
| Day 8 | 1.66s |
| Day 9 | 1.70s |
| Day 10 | 1.67s |

### Scenario 4: Lazy Incremental
| Day | Time |
|-----|------|
| Day 1 | 2.42s |
| Day 2 | 1.67s |
| Day 3 | 1.66s |
| Day 4 | 1.77s |
| Day 5 | 1.83s |
| Day 6 | 1.77s |
| Day 7 | 1.81s |
| Day 8 | 1.81s |
| Day 9 | 1.86s |
| Day 10 | 1.89s |

---

## Methodology

All scenarios process the same synthetic transaction data:
- **5 million rows per day** (base) + 2,000 additional rows each subsequent day
- **10 days of data** for client_1
- Data includes transaction details, SKU information, ZIP codes, dates, quantities, and revenue

**Processing Steps:**
1. Unzip compressed transaction data
2. Load and parse CSV data
3. Clean and validate transactions
4. Join with static lookup tables (SKU categories, ZIP code regions)
5. Apply business logic (revenue calculations, date transformations)
6. Aggregate by transaction date and SKU group
7. Compute quality metrics and locality flags
8. Write results to database or CSV

**Incremental Processing (Scenarios 3 & 4):**
- Query database for last processed date
- Filter source data to only new transactions
- Process only incremental data
- Update database with new aggregations

**Hybrid Eager Approach (Scenario 3):**
- Uses `scan_csv()` for lazy loading
- Applies date filter lazily (predicate pushdown for efficient I/O)
- Calls `.collect()` to materialize filtered data
- Executes all subsequent operations eagerly (no query planning overhead)

**Fully Lazy Approach (Scenario 4):**
- Uses `scan_csv()` for lazy loading
- All operations remain lazy until final collection
- Query optimizer plans entire pipeline
- Single materialization at end

---

## Reproducing Results

```bash
# Generate synthetic data (only needed once)
python generate_data.py

# Run full benchmark (4 scenarios)
python main.py
```

Results are automatically written to this file after benchmark completion.
