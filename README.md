# PoC Implementation Plan: Incremental Data Processing Pipeline

### 1. Objective

The goal of this project is to create a proof-of-concept (PoC) that demonstrates the performance and resource-efficiency gains of an incremental data processing pipeline compared to a full-reprocessing pipeline. The PoC simulates a daily data ingestion workflow, processes the data using Polars for both pipelines, and provides clear, measurable benchmarks comparing the two approaches.

**Key Methodology:** Both pipelines use the same data processing library (Polars) to provide an apples-to-apples comparison, isolating the performance benefit of the incremental strategy.

### 2. Tech Stack

- **Language:** Python 3.9+
- **Core Libraries:**
  - `polars`: High-performance engine for **both** baseline and optimized pipelines.
  - `pandas`: Used only for its convenient `.to_sql()` database interface.
  - `sqlite3`: Proof-of-concept database backend.
  - `memory-profiler`: Peak memory capture via `@profile` decorators.
  - `zipfile`: Handling input archives.
- **Database:** SQLite

### 3. Project Structure

```
.
├── data/
│   ├── static/
│   │   ├── sku_groups.csv
│   │   └── us_zip_codes.csv
│   ├── client_1/
│   │   ├── transactions_day_1.zip
│   │   └── ...
│   └── client_2/
│       ├── transactions_day_1.zip
│       └── ...
├── generate_data.py
├── main.py
├── poc_database.db
└── results.md
```

### 4. Phase 1: Data Generation (`generate_data.py`)

`generate_data.py` creates realistic benchmark data and the supporting lookup tables consumed by the pipelines.

- **Static assets (written to `data/static/`):**
  - `sku_groups.csv`: Deterministic mapping of every `sku_id` to `sku_group`, `category`, and `is_high_priority`.
  - `us_zip_codes.csv`: Lightweight ZIP dimension with `zip_code`, `city`, `state`, and `is_us` indicators (includes an `00000` “international” code).
- **Synthetic feed controls:**
  - `CLIENTS = ["client_1", "client_2"]`
  - `NUM_DAYS = 10`
  - `BASE_ROWS_DAY_1 = 1_000_000`
  - `INCREMENTAL_ROWS_PER_DAY = 100_000`
  - `START_DATE = "2025-10-01"`
  - `NUM_SKUS = 5_000` with a Pareto-distributed long tail (≈80/20 transaction split).
- **CSV schema per archive:**
  - `transaction_id` (string)
  - `transaction_date` (string `YYYY-MM-DD`)
  - `sku_id` (string)
  - `quantity_sold` (int 1–10)
  - `revenue_usd` (float 5.00–500.00)
  - `maturity_date` (string) – realistic offsets including early/late/missing cases
  - `zip_code_raw` (string or null) – intentionally noisy ZIP data (ZIP+4, embedded spaces, alphanumeric prefixes, international markers)
- **Workflow:**
  1. Load ZIP reference data and bias sampling toward California ZIPs so “local” metrics have signal.
  2. Generate incremental rows for each client/day and append to a cumulative DataFrame; every day _N_ file contains all previous rows plus the new increment.
  3. Inject `maturity_date` and `zip_code_raw` noise per batch.
  4. Write the cumulative CSV to a temporary file, compress it to `transactions_day_{N}.zip`, then remove the temporary file.
  5. Repeat for both clients (20 ZIP archives) and emit the static lookup files.

### 5. Phase 2: Benchmarking Script (`main.py`)

`main.py` owns ingestion, enrichment, aggregation, incremental state management, parity validation, and reporting.

- **Database schema (`processed_sales`):** composite primary key `(client_id, transaction_date, maturity_date, sku_group, local)` with the following columns: `total_quantity`, `total_revenue`, `order_count`, `avg_quantity_per_order`, `avg_order_value`, `days_to_maturity`, `revenue_share_of_day`, `revenue_rank`. The initializer detects legacy schemas and recreates the table when needed.
- **Shared helper (`process_transactions`):**
  - Loads static lookups once (cached).
  - Parses dates, normalizes `zip_code_raw` into a clean `zip_code`, imputes missing maturity dates (+3 days), clamps negative maturity deltas, and tracks diagnostics counters.
  - Joins SKU group and ZIP metadata, adds a boolean `local` flag, and aggregates Polars metrics over `(transaction_date, maturity_date, sku_group, local)` including window-derived revenue share and dense ranking.
  - Returns a `PipelineArtifacts` bundle (aggregated frame, top-five slice, data-quality counters, latest processed date).

#### 5.1 Baseline pipeline (`run_baseline_benchmark`)

- Decorated with `@profile` to capture peak memory.
- Unzips the requested day’s archive, processes the entire file through `process_transactions`, prints timing and data-quality metrics, and returns the timing dict plus artifacts for reporting reuse.

#### 5.2 Optimized pipeline (`run_optimized_benchmark`)

- Decorated with `@profile` as well.
- Reads the last processed date from SQLite, filters the archive lazily to only new rows, and executes the same enrichment/aggregation flow.
- Recomputes a baseline aggregate for the same file and uses `polars.testing.assert_frame_equal` to verify parity; differences are logged and bubbled to the CLI if detected.
- Deletes overlapping rows in SQLite, inserts the enriched aggregates, and reports granular timings alongside data-quality counters.

#### 5.3 Historical warm-up (`populate_historical_data`)

Replays days 1 through `BENCHMARK_DAY - 1` through the incremental path so the database reflects a realistic prior state before benchmarking.

#### 5.4 Report generation

`generate_report` combines both artifacts, timing dictionaries, parity status, and memory metrics. The Markdown report contains:

- Benchmark conditions (client, day, benchmark date, estimated row counts, parity status)
- Timing/memory summaries for both pipelines with associated data-quality counters
- A “Top 5 SKU Groups by Revenue” leaderboard
- A California-local versus non-local summary table
- Methodology notes reaffirming the apples-to-apples comparison

The report body is assembled from a list of ASCII strings (`"\n".join(...)`) to avoid encoding issues.

#### 5.5 Main execution flow

1. Initialize or migrate the SQLite database.
2. Run the baseline benchmark while capturing profiler output.
3. Populate historical data for the incremental pipeline (days `< BENCHMARK_DAY`).
4. Execute the incremental benchmark (with parity validation) and log timings.
5. Generate `results.md` and print a timing/memory summary to stdout.

### 6. Phase 3: Reporting (`results.md`)

`results.md` is generated automatically after the benchmarks complete. The report captures:

- Benchmark conditions (client, day, benchmark date, estimated row counts, parity status)
- Baseline vs incremental timing and memory metrics, each accompanied by data-quality counters
- A “Top 5 SKU Groups by Revenue” leaderboard
- A California-local vs non-local performance summary
- A conclusion articulating speed/memory deltas and a brief methodology reminder

All content is assembled from ASCII strings so the file remains portable across environments.

