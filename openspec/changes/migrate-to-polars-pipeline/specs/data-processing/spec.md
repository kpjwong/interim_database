# Data Processing Capability - Spec Delta

## ADDED Requirements

### Requirement: Baseline Pipeline - Full Reprocessing with Polars

The system SHALL provide a baseline data processing pipeline that performs full reprocessing of transaction data using Polars LazyFrame for benchmarking comparison.

#### Scenario: Process full dataset for benchmark day with Polars

- **WHEN** the baseline benchmark is executed for a specific client and day
- **THEN** the system SHALL unzip the complete transactions file for that day
- **AND** use `polars.scan_csv()` to create a LazyFrame from the CSV
- **AND** apply groupby and aggregation operations for `sku_id` and `transaction_date`, summing `quantity_sold` and `revenue_usd`
- **AND** execute the query plan with `.collect()`
- **AND** return timing metrics for total time and Polars processing time

#### Scenario: No filtering in baseline (full reprocessing)

- **WHEN** the baseline benchmark creates a Polars query plan
- **THEN** the system SHALL NOT apply any date filtering
- **AND** process the entire dataset to establish the full reprocessing baseline

#### Scenario: Memory profiling during baseline execution

- **WHEN** the baseline benchmark function is decorated with `@profile`
- **THEN** the system SHALL capture peak memory usage during execution
- **AND** include memory metrics in the profiler output

### Requirement: Optimized Pipeline - Incremental Processing with Polars

The system SHALL provide an optimized data processing pipeline that uses Polars lazy evaluation to process only incremental transaction data, maintaining state in a SQLite database.

#### Scenario: Process incremental data using Polars LazyFrame

- **WHEN** the optimized benchmark is executed for a specific client and day
- **THEN** the system SHALL query the database for the last processed transaction date
- **AND** unzip the transactions file to a temporary CSV
- **AND** use `polars.scan_csv()` to create a LazyFrame from the CSV
- **AND** apply a filter to select only rows where `transaction_date > last_processed_date`
- **AND** chain groupby and aggregation operations for `sku_id` and `transaction_date`
- **AND** execute the optimized query plan with `.collect()`
- **AND** return a Polars DataFrame containing aggregated results

#### Scenario: Update database with incremental results

- **WHEN** incremental data has been processed with Polars
- **THEN** the system SHALL delete existing rows from the database where `transaction_date >= last_processed_date`
- **AND** convert the Polars DataFrame to Pandas using `.to_pandas()`
- **AND** bulk insert the new aggregated results using `df.to_sql()` with `method='multi'`
- **AND** commit the transaction

#### Scenario: Measure granular timing for optimized pipeline

- **WHEN** the optimized benchmark executes
- **THEN** the system SHALL measure and return separate timings for:
  - Database read time (querying last processed date)
  - Polars processing time (scan, filter, group, aggregate, collect)
  - Database write time (delete and insert operations)
  - Total end-to-end time

#### Scenario: Handle first-time processing with no database state

- **WHEN** the optimized benchmark is executed and no previous data exists in the database
- **THEN** the system SHALL process the entire dataset (filter returns all rows)
- **AND** insert all aggregated results into the empty database table

### Requirement: Database Schema for Incremental State

The system SHALL maintain aggregated sales data in a SQLite database with a schema that supports incremental updates and composite primary keys.

#### Scenario: Initialize processed_sales table

- **WHEN** the database is initialized
- **THEN** the system SHALL create a table named `processed_sales` with the following schema:
  - `client_id` TEXT NOT NULL
  - `transaction_date` TEXT NOT NULL
  - `sku_id` TEXT NOT NULL
  - `total_quantity` INTEGER
  - `total_revenue` REAL
  - PRIMARY KEY (`client_id`, `transaction_date`, `sku_id`)

#### Scenario: Query last processed date per client

- **WHEN** the optimized pipeline needs to determine incremental data boundaries
- **THEN** the system SHALL execute `SELECT max(transaction_date) FROM processed_sales WHERE client_id = ?`
- **AND** return NULL if no rows exist for that client
- **AND** return the most recent transaction date otherwise

### Requirement: Performance Measurement and Reporting

The system SHALL measure execution time, memory usage, and generate a structured report comparing baseline and optimized pipelines.

#### Scenario: Profile memory usage with memory-profiler

- **WHEN** benchmark functions are decorated with `@profile` from memory-profiler
- **THEN** the system SHALL capture peak memory usage statistics
- **AND** output profiling data to stdout in memory-profiler's standard format

#### Scenario: Generate performance comparison report

- **WHEN** both baseline and optimized benchmarks have completed
- **THEN** the system SHALL generate a `results.md` file containing:
  - Benchmark conditions (client, day, row counts)
  - Baseline pipeline metrics (I/O time, CPU time, total time, peak memory)
  - Optimized pipeline metrics (DB read, Polars process, DB write, total time, peak memory)
  - Calculated improvements (speed multiplier, memory reduction percentage)

#### Scenario: Calculate performance improvements

- **WHEN** generating the final report
- **THEN** the system SHALL calculate the speed improvement ratio as `baseline_total_time / optimized_total_time`
- **AND** calculate memory reduction percentage as `((baseline_memory - optimized_memory) / baseline_memory) * 100`
- **AND** format these metrics with appropriate precision (1 decimal place for speed, 0 decimals for percentage)

### Requirement: Temporary File Management

The system SHALL handle temporary file creation and cleanup for CSV extraction during benchmark execution.

#### Scenario: Extract and cleanup temporary files in baseline pipeline

- **WHEN** the baseline benchmark unzips a transactions file
- **THEN** the system SHALL extract the CSV to a uniquely named temporary file
- **AND** process the CSV
- **AND** delete the temporary file after processing completes
- **AND** ensure cleanup occurs even if processing succeeds

#### Scenario: Extract and cleanup temporary files in optimized pipeline

- **WHEN** the optimized benchmark unzips a transactions file
- **THEN** the system SHALL extract the CSV to a uniquely named temporary file
- **AND** pass the file path to Polars' `scan_csv()`
- **AND** delete the temporary file after `.collect()` completes
- **AND** ensure cleanup occurs even if processing succeeds
