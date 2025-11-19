# Project Context

## Purpose

This project is a proof-of-concept (PoC) that demonstrates the performance and resource-efficiency gains of an incremental data processing pipeline compared to a full-reprocessing pipeline. It simulates daily transaction data ingestion, processes the data, and provides measurable benchmarks comparing both approaches.

## Tech Stack

- **Language:** Python 3.9+
- **Data Processing:** polars (high-performance DataFrame library with lazy evaluation)
- **Database:** SQLite3 (built-in with Python)
- **Database Interface:** pandas (for `.to_sql()` convenience)
- **Performance Profiling:** memory-profiler
- **Standard Libraries:** zipfile, time, os, datetime

## Project Structure

```
.
├── data/                          # Generated transaction data (ZIP files)
│   ├── client_1/
│   │   └── transactions_day_*.zip
│   └── client_2/
│       └── transactions_day_*.zip
├── generate_data.py               # Data generation script
├── main.py                        # Main benchmark script
├── poc_database.db                # SQLite database (created at runtime)
├── results.md                     # Benchmark results report (generated)
└── openspec/                      # OpenSpec change proposals
```

## Prerequisites

1. **Python 3.9 or higher**

2. **Python packages:**
   ```bash
   pip install polars pandas memory-profiler
   ```

   - **polars**: High-performance data processing (both pipelines)
   - **pandas**: Database interface via `.to_sql()`
   - **memory-profiler**: Peak memory measurement

## Running the PoC

### Step 1: Generate synthetic data
```bash
python generate_data.py
```

This creates 10 days of transaction data for 2 clients (~1.9M rows by day 10).

### Step 2: Run benchmarks
```bash
python main.py
```

This runs both baseline and optimized pipelines and generates `results.md`.

## Project Conventions

### Code Style
- Use descriptive function and variable names
- Include docstrings for all functions
- Follow PEP 8 guidelines
- Use type hints where appropriate

### Architecture Patterns
- **Separation of Concerns:** Data generation, benchmarking, and reporting are independent
- **State Management:** Database maintains incremental processing state
- **Profiling:** Use `@profile` decorator for memory profiling
- **Timing:** Manual timers for granular performance measurement

### Testing Strategy
- Validation points built into each task
- Use reduced data size for quick testing (configurable constants)
- End-to-end test verifies complete workflow

## Domain Context

### Data Model
- **Transaction Data:** Realistic e-commerce transaction records with long-tail SKU distribution
- **Aggregation:** Group by SKU and date, summing quantities and revenues
- **Cumulative Datasets:** Each day contains all previous data plus new incremental rows

### Performance Expectations
- **Baseline:** Process all ~1.9M rows on day 10 (full reprocessing with Polars)
- **Optimized:** Process only ~200K new rows (days 9-10) (incremental with Polars + database state)
- **Expected Improvements:** 2-5x faster (isolated benefit of incremental strategy)
- **Methodology:** Both pipelines use Polars for apples-to-apples comparison

## Important Constraints

- **PoC Scope:** Minimal error handling (assumes well-formed inputs)
- **Single Client Benchmark:** Focuses on client_1, day 10
- **No Parallel Processing:** Sequential execution for simplicity
- **No Production Features:** No logging, monitoring, or error recovery
- **Apples-to-Apples Comparison:** Both pipelines use identical data processing library (Polars)
