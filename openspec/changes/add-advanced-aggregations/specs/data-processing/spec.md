# Data Processing Capability - Spec Delta

## MODIFIED Requirements

### Requirement: Baseline Pipeline - Full Reprocessing with Polars

The baseline pipeline SHALL return an enriched aggregate dataset that captures additional dimensional columns and analytics beyond simple sums so stakeholders can evaluate maturity and locality behavior.

#### Scenario: Aggregate per (transaction_date, maturity_date, sku_group, local)

- **WHEN** the baseline benchmark executes the Polars LazyFrame plan
- **THEN** it SHALL group raw transactions by `transaction_date`, `maturity_date`, `sku_group`, and `local`
- **AND** it SHALL calculate for each group:
  - `total_quantity` = SUM of `quantity_sold`
  - `total_revenue` = SUM of `revenue_usd`
  - `order_count` = COUNT of rows
  - `avg_quantity_per_order` = MEAN of `quantity_sold`
  - `avg_order_value` = MEAN of `revenue_usd`
  - `days_to_maturity` = MEAN of the row-level maturity deltas
- **AND** it SHALL materialize the aggregates in a Polars DataFrame before any downstream processing.

#### Scenario: Derive revenue share and rank per day

- **WHEN** baseline aggregates are materialized
- **THEN** the pipeline SHALL compute `revenue_share_of_day = total_revenue / SUM(total_revenue)` using a Polars window partitioned by `transaction_date`
- **AND** it SHALL compute a dense `revenue_rank` where 1 represents the highest `total_revenue` for that transaction date and locality combination
- **AND** it SHALL include both derived columns in the returned baseline results.

### Requirement: Optimized Pipeline - Incremental Processing with Polars

The optimized incremental pipeline SHALL maintain the same metric and dimension shape as the baseline while only processing new or changed data.

#### Scenario: Maintain enriched metrics during incremental updates

- **WHEN** the optimized benchmark processes new incremental data
- **THEN** the Polars lazy plan SHALL yield the same columns defined for the baseline requirement, including `maturity_date`, `sku_group`, `local`, and all derived metrics
- **AND** it SHALL recompute window-based fields (`revenue_share_of_day`, `revenue_rank`) using only the rows eligible for insertion
- **AND** it SHALL upsert the enriched aggregates into the database so every stored row contains the complete metric set.

#### Scenario: Validate incremental parity with baseline

- **WHEN** the optimized pipeline finishes processing a day
- **THEN** it SHALL recompute the baseline aggregates for that day in-memory and compare the metric set (value equality or hash) against the incremental output
- **AND** if discrepancies arise, it SHALL emit a warning containing the affected keys and metric names before proceeding.

### Requirement: Database Schema for Incremental State

The processed sales table SHALL evolve to store the richer metric set and additional dimensional columns required by the new analytics.

#### Scenario: Expand processed_sales schema for enriched metrics

- **WHEN** the database initializes the `processed_sales` table
- **THEN** it SHALL define the following columns in addition to the existing composite key (`client_id`, `transaction_date`, `maturity_date`, `sku_group`, `local`):
  - `total_quantity` INTEGER NOT NULL
  - `total_revenue` REAL NOT NULL
  - `order_count` INTEGER NOT NULL
  - `avg_quantity_per_order` REAL NOT NULL
  - `avg_order_value` REAL NOT NULL
  - `days_to_maturity` REAL NOT NULL
  - `revenue_share_of_day` REAL NOT NULL
  - `revenue_rank` INTEGER NOT NULL
- **AND** it SHALL recreate the table when the legacy schema (without these columns or dimensions) is detected.

### Requirement: Performance Measurement and Reporting

Benchmark reporting SHALL surface the new analytics, locality breakdowns, and data-quality counters so the richer processing is transparent.

#### Scenario: Surface enriched metrics in benchmark report

- **WHEN** `results.md` is generated
- **THEN** it SHALL include the expanded metrics for both pipelines, explicitly listing California-local vs non-local totals
- **AND** it SHALL render a “Top 5 SKUs by Revenue” section that lists `sku_group`, `total_revenue`, `order_count`, `avg_order_value`, and `revenue_share_of_day` for the benchmark day
- **AND** it SHALL summarize data-quality counters (invalid ZIPs sanitized, defaulted maturity dates, lookup misses).

## ADDED Requirements

### Requirement: Raw Data Normalization and Enrichment

The system SHALL sanitize noisy transactional attributes and enrich them with static reference data before aggregation.

#### Scenario: Normalize and validate ZIP code data

- **WHEN** raw transactions contain a `zip_code_raw` field
- **THEN** the pipeline SHALL strip whitespace, uppercase letters, extract the leading five digits, and drop rows that lack a valid US ZIP (logging a warning counter)
- **AND** it SHALL join the cleaned ZIP to the static US ZIP dimension to fetch `city`, `state`, and `is_us`
- **AND** it SHALL set `local = (state == "CA")` for domestic rows and `local = False` for non-US records.

#### Scenario: Enrich transactions with SKU group metadata

- **WHEN** the cleaned dataset is prepared for aggregation
- **THEN** it SHALL join each `sku_id` to the static SKU group lookup
- **AND** it SHALL populate `sku_group` and related descriptive attributes (e.g., `category`, `is_high_priority`)
- **AND** it SHALL replace missing lookup results with a sentinel group (e.g., `UNKNOWN`) and increment a data-quality counter.

#### Scenario: Compute maturity deltas

- **WHEN** both `transaction_date` and `maturity_date` are present
- **THEN** the pipeline SHALL cast them to the DATE type, compute `days_to_maturity = maturity_date - transaction_date`, and clamp negative values to zero
- **AND** it SHALL impute missing maturity dates with a configurable fallback (e.g., `transaction_date + 3 days`) while recording the number of imputations.

### Requirement: Static Reference Data Management

The processing environment SHALL ship with reusable static datasets that power the enrichments and ensure deterministic joins.

#### Scenario: Provide SKU group lookup file

- **WHEN** the project is initialized
- **THEN** it SHALL include `data/static/sku_groups.csv` containing SKU-to-group mappings with at least the fields `sku_id`, `sku_group`, `category`, and `is_high_priority`
- **AND** the data-generation script SHALL reference this file when synthesizing transactions to guarantee coverage.

#### Scenario: Provide US ZIP code dimension

- **WHEN** ZIP enrichment runs
- **THEN** the project SHALL load a static file (CSV or Parquet) containing US ZIP metadata with fields `zip_code`, `city`, `state`, and `is_us`
- **AND** the enrichment pipeline SHALL use the file without mutating it, allowing repeated benchmark runs to rely on the same source of truth.

### Requirement: Daily Revenue Leaderboard Export

The pipelines SHALL expose a reusable leaderboard slice so downstream reporting does not need to repeat heavy aggregations.

#### Scenario: Provide reusable top-SKU slice

- **WHEN** either pipeline finishes processing a benchmark day
- **THEN** it SHALL expose a data structure (in-memory or serialized) containing the top 5 `(transaction_date, sku_group)` combinations ranked by `total_revenue`
- **AND** the slice SHALL include `order_count`, `avg_order_value`, `revenue_share_of_day`, and `local` status for each entry so consumers can reuse it without re-running Polars scans.
