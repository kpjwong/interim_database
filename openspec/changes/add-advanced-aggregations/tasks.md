# Implementation Tasks: Add Advanced Aggregations

## 1. Data Assets & Generation

- [x] 1.1 Extend `generate_data.py` to synthesize `maturity_date`, noisy `zip_code_raw`, and realistic SKU/order attributes.
- [x] 1.2 Add deterministic noise helpers (ZIP+4, lowercase, whitespace, alphanumeric contamination) and unit-testable sanitization fixtures.
- [x] 1.3 Create static lookup files (`data/static/sku_groups.csv`, `data/static/us_zip_codes.csv`) with representative coverage and README documentation.
- [x] 1.4 Update data-generation CLI output to mention new columns and lookup dependencies; ensure regeneration is idempotent.

## 2. Schema & Setup

- [x] 2.1 Update database initialization to create the revised `processed_sales` table with new metric and dimension columns.
- [x] 2.2 Provide migration guidance: drop or recreate the existing PoC database before running benchmarks.
- [x] 2.3 Document new columns, types, and meaning in README/project instructions.

## 3. Raw Data Cleaning & Enrichment

- [x] 3.1 Normalize `zip_code_raw` to `zip_code` (5-digit) using Polars string operations, handling nulls, ZIP+4, non-US markers, and logging invalids.
- [x] 3.2 Derive `state`, `city`, and `is_us` by joining to the static ZIP dimension; quarantine unmatched ZIPs into diagnostics counters.
- [x] 3.3 Join SKU metadata (`sku_group`, `category`, `is_high_priority`) via the static lookup, filling missing groups with a sentinel value.
- [x] 3.4 Cast and validate `transaction_date` and `maturity_date` as Polars `Date`, compute `days_to_maturity`, and clamp negatives/long tails per business rule.
- [x] 3.5 Flag `local` transactions where `state == "CA"`, preserving explicit boolean columns in the cleaned dataset.

## 4. Baseline Pipeline Enhancements

- [x] 4.1 Expand the Polars aggregation to include order count, quantity/revenue averages, and the new dimensional columns (`maturity_date`, `sku_group`, `local`).
- [x] 4.2 Add derived columns for `revenue_share_of_day` and `revenue_rank` using Polars window expressions scoped per `(transaction_date, local)`.
- [x] 4.3 Ensure the baseline run exposes a reusable “Top 5 SKU groups by revenue” slice with cleaned metadata for reporting.

## 5. Optimized Pipeline Enhancements

- [x] 5.1 Mirror baseline metric expressions within the incremental Polars query (both filtered and full-pass branches).
- [x] 5.2 Update database write path to persist the additional columns and maintain correct ordering when deleting/reinserting rows.
- [x] 5.3 Introduce a verification step comparing optimized output against a baseline recomputation for overlapping days.
- [x] 5.4 Track data-quality counters (rows dropped, unmatched ZIPs, defaulted maturity dates) and surface them in benchmark output.

## 6. Historical Data Population

- [x] 6.1 Update `populate_historical_data()` to compute and store the expanded metric set with the new dimensions.
- [x] 6.2 Confirm that per-day revenue share, rank, and `days_to_maturity` remain consistent when days are replayed sequentially.

## 7. Reporting

- [x] 7.1 Extend `generate_report()` to display the new metrics alongside totals and include a California-local vs non-local comparison.
- [x] 7.2 Add a “Top 5 SKUs by Revenue” section pulling data from the enriched aggregates.
- [x] 7.3 Surface data-quality counters and lookup miss summaries in the report.
- [ ] 7.4 Refresh `results.md` template examples if checked into the repo.

## 8. Validation & Testing

- [x] 8.1 Add automated assertions ensuring baseline and optimized outputs match for a sample slice.
- [ ] 8.2 Create focused tests for ZIP normalization, maturity date calculations, and lookup joins using small Polars DataFrames.
- [x] 8.3 Run `python -m py_compile main.py` after changes.
- [x] 8.4 Execute full benchmark run (`python main.py`) and capture outputs for the report.

## 9. Performance Impact

- [x] 9.1 Record timing deltas introduced by the richer aggregations for both pipelines.
- [x] 9.2 Note any changes to memory usage attributable to wider tables or additional Polars operations.

## 10. OpenSpec Finalization

- [ ] 10.1 Update proposal and design decisions (if a design doc is created) to reflect implementation details.
- [x] 10.2 Run `openspec validate add-advanced-aggregations --strict` and resolve issues.
- [ ] 10.3 Mark tasks complete once work is merged; prepare for archival when deployed.
