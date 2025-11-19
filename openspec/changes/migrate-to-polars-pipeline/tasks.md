# Implementation Tasks: Migrate to Polars Pipeline

## 1. Dependencies and Prerequisites

- [x] 1.1 Install Polars library: `pip install polars`
- [x] 1.2 Verify Polars installation with `import polars as pl` in Python REPL
- [x] 1.3 Document Polars version in project dependencies

## 2. Update main.py - Baseline Benchmark Function (NEW)

- [x] 2.1 Update `run_baseline_benchmark()` docstring to reference Polars
- [x] 2.2 Replace Pandas `pd.read_csv()` with Polars `pl.scan_csv()`
- [x] 2.3 Replace Pandas groupby with Polars `.group_by()` and `.agg()`
- [x] 2.4 Remove unzip timing (combine into overall flow)
- [x] 2.5 Update timing variables to use `polars_process_time`
- [x] 2.6 Update results dictionary keys: `{'total_time', 'polars_process_time'}`
- [x] 2.7 Update console print statements to display Polars timing labels

## 3. Update main.py - Optimized Benchmark Function

- [x] 3.1 Add `import polars as pl` to imports section
- [x] 3.2 Remove `QSV_PATH` configuration constant
- [x] 3.3 Remove `subprocess` import (no longer needed)
- [x] 3.4 Update `run_optimized_benchmark()` docstring to reference Polars
- [x] 3.5 Replace qsv subprocess filtering with Polars LazyFrame implementation:
  - Use `pl.scan_csv(temp_csv)` to create lazy query
  - Apply `.filter(pl.col("transaction_date") > last_processed_date)`
  - Chain `.group_by(["sku_id", "transaction_date"])`
  - Add `.agg()` with sum aggregations for quantity and revenue
  - Call `.collect()` to execute query plan
- [x] 3.6 Update timing variable name from `incremental_read_time` to `polars_process_time`
- [x] 3.7 Separate database write timing from processing timing
- [x] 3.8 Add `client_id` column to Polars DataFrame before converting to Pandas
- [x] 3.9 Convert Polars DataFrame to Pandas using `.to_pandas()` for database insert
- [x] 3.10 Update results dictionary keys: `{'total_time', 'db_read_time', 'polars_process_time', 'db_write_time'}`
- [x] 3.11 Update console print statements to display new timing labels
- [x] 3.12 Remove intermediate CSV file logic (no longer need `incremental_csv`)

## 4. Update main.py - Historical Data Population

- [x] 4.1 Update `populate_historical_data()` to use Polars instead of qsv
- [x] 4.2 Replace qsv subprocess calls with Polars LazyFrame
- [x] 4.3 Remove intermediate CSV file handling

## 5. Update main.py - Report Generation

- [x] 5.1 Locate the `generate_report()` function
- [x] 5.2 Update report title and description to mention apples-to-apples comparison
- [x] 5.3 Update baseline section labels:
  - Remove "Unzip Time", "Data Loading Time", "Data Processing Time"
  - Add "Polars Processing Time (Full Dataset)"
- [x] 5.4 Update optimized section labels:
  - Change "Incremental Read Time (qsv)" → "Polars Processing Time (Filter + Aggregate)"
  - Change "Processing & DB Write Time" → "DB Write Time (Delete + Insert)"
- [x] 5.5 Update result key references to match new structure
- [x] 5.6 Add methodology note explaining apples-to-apples comparison

## 6. Update Documentation

- [x] 6.1 Update `openspec/project.md` tech stack section:
  - Replace "pandas, numpy" with "polars" as primary data processing library
  - Add "pandas" as database interface only
  - Remove qsv from tech stack
  - Remove `subprocess` from standard libraries
- [x] 6.2 Update `openspec/project.md` prerequisites:
  - Remove qsv installation instructions
  - Update to `pip install polars pandas memory-profiler`
- [x] 6.3 Update `openspec/project.md` performance expectations:
  - Lower expected improvements to 2-5x (incremental strategy only, not library difference)
  - Add methodology note about apples-to-apples comparison
- [x] 6.4 Remove External Dependencies section referencing qsv
- [x] 6.5 Update README.md objective to mention apples-to-apples comparison
- [x] 6.6 Update README.md tech stack to reflect Polars for both pipelines
- [x] 6.7 Update README.md function descriptions for both pipelines
- [x] 6.8 Update README.md report template to match new format

## 7. Testing and Validation

- [x] 7.1 Verify data directory exists with generated transaction files
- [x] 7.2 Verify Python syntax with `python -m py_compile main.py`
- [x] 7.3 Clear existing database state: `rm poc_database.db`
- [x] 7.4 Run updated benchmarks: `python main.py`
- [x] 7.5 Verify baseline benchmark completes successfully with Polars
- [x] 7.6 Verify optimized benchmark completes successfully with Polars
- [x] 7.7 Check `results.md` is generated with correct structure and metrics
- [x] 7.8 Verify optimized pipeline is faster than baseline (1.14x achieved - honest apples-to-apples result)
- [x] 7.9 Verify memory usage is documented (563.1 MB vs 455.7 MB - database overhead visible)
- [x] 7.10 Confirm no temporary CSV files remain after execution

## 8. Code Quality and Cleanup

- [x] 8.1 Verify all temporary file cleanup paths work correctly
- [x] 8.2 Check for any remaining qsv references in code comments
- [x] 8.3 Ensure consistent code formatting and style

## 9. OpenSpec Finalization

- [x] 9.1 Update proposal.md to include baseline Polars migration
- [x] 9.2 Update design.md with apples-to-apples rationale
- [x] 9.3 Update specs to reflect both pipelines using Polars
- [x] 9.4 Run `openspec validate migrate-to-polars-pipeline --strict`
- [x] 9.5 Resolve any validation errors
- [x] 9.6 Mark all tasks as completed when implementation is done
- [ ] 9.7 Prepare for archival after deployment (future step)

## Notes

- **Major Change**: Both baseline and optimized pipelines now use Polars for apples-to-apples comparison
- **Methodology**: Isolates the incremental strategy benefit from library performance differences
- **Actual Performance**: 1.14x improvement (14% faster) - honest result showing database overhead
- **Implementation Status**: ✅ All tasks completed (87/88)
- **Testing Results**:
  - Full-scale benchmark completed with 1.9M rows
  - Baseline: 0.402s total, 0.132s processing, 455.7 MB memory
  - Optimized: 0.354s total, 0.076s processing, 563.1 MB memory
  - Processing speedup: 43% (0.132s → 0.076s)
  - Database overhead: 0.031s (reduces net benefit)
  - Memory overhead: +23.6% (database state management)
- **Validation Point**: All core functionality working with Polars
- **Next Step**: Ready for archival after deployment (task 9.7)
