# Proposal: Add Advanced Aggregations

## Why

- The current Polars pipelines only produce total quantity and revenue per SKU/day, which limits benchmark insights to a single aggregation.
- Product stakeholders want to understand order volume, pricing dispersion, and top-performing SKUs without reprocessing raw transactions.
- Without richer metrics, it is impossible to validate whether the incremental pipeline preserves distributional characteristics beyond totals.

## What Changes

- Extend both baseline and optimized Polars pipelines to compute additional per-SKU/day metrics: order count, average quantity per order, average order value, revenue share of the day, and revenue rank.
- Introduce Polars window expressions so each run also calculates per-day totals needed for share and ranking.
- Enrich the raw transaction feed with new domain columns: `maturity_date`, Polars-derived `days_to_maturity`, a noisy `zip_code_raw` that is normalized to `zip_code`, and downstream geo metadata (`state`, `city`, `is_us`, `local`).
- Join static reference files (`sku_groups.csv`, `us_zip_codes.parquet`) into the processing pipeline; quarantine and log any missing lookups.
- Persist the expanded metrics set plus new dimensional columns (`maturity_date`, `sku_group`, `local`) in the `processed_sales` table and propagate them through incremental updates.
- Update the markdown report to surface the richer metrics, highlight the top 5 SKUs by revenue for the benchmark day, and call out California (“local”) performance.
- Add validation safeguards (unit-style checks) ensuring baseline and optimized metrics align for overlapping days and that data-quality counters stay within thresholds.

### Breaking Changes

- The `processed_sales` schema gains new columns and existing databases must be regenerated (task runner will document the reset step).
- Historical data files must be re-generated to include `maturity_date`, raw ZIP noise, and SKU grouping metadata.

## Impact

- **Specs:** `data-processing`
- **Code:** `main.py` (baseline, optimized, historical population, report generation), schema initialization helpers, potential lightweight validation utilities.
- **Data Assets:** `generate_data.py`, new static lookup files under `data/static/`.
- **Tooling/Docs:** README, results template (if needed) to mention the richer metrics and data-quality instrumentation.
- **Risks:** Increased Polars workload, joins, and wider rows could marginally affect performance; proposal includes measurement tasks and data-quality monitoring to capture the delta.
