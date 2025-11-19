"""
Data Generation Script for Incremental Pipeline PoC

This script generates synthetic transaction data for benchmarking purposes.
It creates cumulative daily datasets where each day contains all previous data
plus new incremental rows, simulating real-world batch data exports.
"""

import pandas as pd
import numpy as np
import zipfile
import os
from datetime import datetime, timedelta

# Static data paths
STATIC_DATA_DIR = os.path.join("data", "static")
SKU_GROUPS_PATH = os.path.join(STATIC_DATA_DIR, "sku_groups.csv")
US_ZIP_CODES_PATH = os.path.join(STATIC_DATA_DIR, "us_zip_codes.csv")

# Configuration Constants
CLIENTS = ["client_1", "client_2"]
NUM_DAYS = 10
BASE_ROWS_DAY_1 = 5_000_000
INCREMENTAL_ROWS_PER_DAY = 2_000
START_DATE = "2025-10-01"
NUM_SKUS = 5000

# Noise ratios (tune to simulate realistic dirty ZIP inputs)
ZIP_NOISE_BINS = {
    "zip4": 0.20,
    "leading_trailing_spaces": 0.15,
    "embedded_space": 0.10,
    "alpha_prefix": 0.07,
    "lowercase": 0.06,
    "intl": 0.05,
    "invalid": 0.04,
    "null": 0.03,
    "clean": 0.30  # Remaining probability mass
}

def load_zip_reference():
    """Load static ZIP metadata for sampling during data generation."""
    if not os.path.exists(US_ZIP_CODES_PATH):
        raise FileNotFoundError(
            f"Static ZIP reference not found at {US_ZIP_CODES_PATH}. "
            "Ensure static lookup files are present before generating transactions."
        )

    zip_df = pd.read_csv(US_ZIP_CODES_PATH, dtype={'zip_code': str, 'state': str})
    base_codes = zip_df['zip_code'].tolist()

    weights = []
    for _, row in zip_df.iterrows():
        zip_code = row['zip_code']
        state = row['state']

        if zip_code == "00000":
            weight = 0.5  # Rare international marker used for non-US orders
        elif state == "CA":
            weight = 3.0  # Bias towards California for locality analytics
        else:
            weight = 1.0

        weights.append(weight)

    weights = np.array(weights, dtype=float)
    weights = weights / weights.sum()

    return base_codes, weights


def generate_noisy_zip_codes(base_codes, weights, size):
    """
    Generate noisy ZIP code strings that simulate real-world data quality issues.
    """
    base_choices = np.random.choice(base_codes, size=size, p=weights)
    zip_series = pd.Series(base_choices, dtype="object")
    noise = np.random.rand(size)

    cumulative = 0.0
    masks = {}
    for key, prob in ZIP_NOISE_BINS.items():
        upper = cumulative + prob
        if key == "clean":
            masks[key] = noise >= cumulative
        else:
            masks[key] = (noise >= cumulative) & (noise < upper)
        cumulative = upper

    # ZIP+4 variants
    mask = masks.get("zip4")
    if mask is not None and mask.any():
        zip4_extras = np.random.randint(0, 10000, size=mask.sum())
        zip_series.loc[mask] = (
            zip_series.loc[mask].astype(str) + "-" +
            pd.Series(zip4_extras, index=zip_series.index[mask]).astype(str).str.zfill(4)
        )

    # Leading/trailing spaces
    mask = masks.get("leading_trailing_spaces")
    if mask is not None and mask.any():
        zip_series.loc[mask] = " " + zip_series.loc[mask].astype(str) + "  "

    # Embedded spaces
    mask = masks.get("embedded_space")
    if mask is not None and mask.any():
        zip_series.loc[mask] = (
            zip_series.loc[mask].astype(str).str[:3]
            + " "
            + zip_series.loc[mask].astype(str).str[3:]
        )

    # Alpha prefixes (e.g., "ZIP12345")
    mask = masks.get("alpha_prefix")
    if mask is not None and mask.any():
        zip_series.loc[mask] = "ZIP" + zip_series.loc[mask].astype(str)

    # Lowercase representations
    mask = masks.get("lowercase")
    if mask is not None and mask.any():
        zip_series.loc[mask] = zip_series.loc[mask].astype(str).str.lower()

    # International markers routed to "00000"
    mask = masks.get("intl")
    if mask is not None and mask.any():
        zip_series.loc[mask] = "INTL 00000"

    # Completely invalid strings
    mask = masks.get("invalid")
    if mask is not None and mask.any():
        zip_series.loc[mask] = "INVALID_ZIP"

    # Nulls (missing data)
    mask = masks.get("null")
    if mask is not None and mask.any():
        zip_series.loc[mask] = None

    return zip_series.tolist()


def generate_maturity_dates(transaction_date_str, num_rows):
    """
    Generate maturity dates with realistic variation (same-day, early, delayed, missing).
    """
    base_date = datetime.strptime(transaction_date_str, "%Y-%m-%d")

    offsets = np.random.randint(1, 45, size=num_rows).astype(int)

    # Same-day maturity for a portion
    same_day_mask = np.random.rand(num_rows) < 0.05
    offsets[same_day_mask] = 0

    # Early maturities (negative days)
    negative_mask = np.random.rand(num_rows) < 0.03
    if negative_mask.any():
        offsets[negative_mask] = -np.random.randint(1, 5, size=negative_mask.sum())

    # Long-tail maturities (delayed beyond 45 days)
    long_tail_mask = np.random.rand(num_rows) < 0.04
    if long_tail_mask.any():
        offsets[long_tail_mask] = np.random.randint(45, 120, size=long_tail_mask.sum())

    maturity_dates = (base_date + pd.to_timedelta(offsets, unit="D")).strftime("%Y-%m-%d")
    maturity_series = pd.Series(maturity_dates, dtype="object")

    # Introduce missing maturity dates (forces pipeline imputation)
    missing_mask = np.random.rand(num_rows) < 0.04
    if missing_mask.any():
        maturity_series.loc[missing_mask] = None

    return maturity_series


# For quick testing, uncomment these lines:
# BASE_ROWS_DAY_1 = 10_000
# INCREMENTAL_ROWS_PER_DAY = 1_000

def generate_sku_distribution():
    """
    Generate a long-tail SKU distribution where 20% of SKUs account for 80% of transactions.

    Returns:
        tuple: (sku_list, sku_weights) where sku_list contains SKU IDs and
               sku_weights contains normalized probabilities for sampling
    """
    # Create SKU IDs
    sku_list = [f"SKU_{i:05d}" for i in range(1, NUM_SKUS + 1)]

    # Generate Pareto distribution (power law)
    # Lower alpha creates stronger long-tail effect
    alpha = 1.16  # This approximates 80/20 rule
    raw_weights = np.random.pareto(alpha, NUM_SKUS)

    # Normalize to create probability distribution
    sku_weights = raw_weights / raw_weights.sum()

    return sku_list, sku_weights


def generate_transactions(num_rows, date, sku_list, sku_weights, zip_codes, zip_weights, start_id=0):
    """
    Generate transaction data for a given number of rows and date.

    Args:
        num_rows: Number of transactions to generate
        date: Transaction date as string (YYYY-MM-DD)
        sku_list: List of SKU IDs to sample from
        sku_weights: Probability weights for SKU sampling
        zip_codes: List of clean ZIP codes to sample from
        zip_weights: Probability weights for ZIP sampling
        start_id: Starting transaction ID number

    Returns:
        DataFrame with transaction data
    """
    # Generate transaction IDs
    transaction_ids = [f"TXN_{start_id + i:010d}" for i in range(num_rows)]

    # Sample SKUs with weighted probability
    skus = np.random.choice(sku_list, size=num_rows, p=sku_weights)

    # Generate quantities (1-10)
    quantities = np.random.randint(1, 11, size=num_rows)

    # Generate revenue (5.00-500.00, rounded to 2 decimals)
    revenues = np.round(np.random.uniform(5.0, 500.0, size=num_rows), 2)

    # Create DataFrame
    df = pd.DataFrame({
        'transaction_id': transaction_ids,
        'transaction_date': date,
        'sku_id': skus,
        'quantity_sold': quantities,
        'revenue_usd': revenues
    })

    # Add maturity and ZIP noise
    df['maturity_date'] = generate_maturity_dates(date, num_rows)
    df['zip_code_raw'] = generate_noisy_zip_codes(zip_codes, zip_weights, num_rows)

    return df


def generate_day_data(client_id, day, cumulative_df, sku_list, sku_weights, zip_codes, zip_weights):
    """
    Generate and save data for a specific day.

    Args:
        client_id: Client identifier (e.g., 'client_1')
        day: Day number (1-based)
        cumulative_df: DataFrame with all previous days' data (None for day 1)
        sku_list: List of SKU IDs
        sku_weights: Probability weights for SKU sampling
        zip_codes: Clean ZIP codes sampled for raw data noise
        zip_weights: Sampling weights for ZIP codes

    Returns:
        DataFrame with cumulative data through this day
    """
    # Calculate date for this day
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    current_date = (start_dt + timedelta(days=day - 1)).strftime("%Y-%m-%d")

    # Determine number of rows to generate
    if day == 1:
        num_new_rows = BASE_ROWS_DAY_1
        start_id = 0
    else:
        num_new_rows = INCREMENTAL_ROWS_PER_DAY
        start_id = len(cumulative_df)

    # Generate new transactions
    new_df = generate_transactions(
        num_new_rows,
        current_date,
        sku_list,
        sku_weights,
        zip_codes,
        zip_weights,
        start_id
    )

    # Combine with previous data
    if cumulative_df is None:
        cumulative_df = new_df
    else:
        cumulative_df = pd.concat([cumulative_df, new_df], ignore_index=True)

    # Create client directory if it doesn't exist
    client_dir = os.path.join("data", client_id)
    os.makedirs(client_dir, exist_ok=True)

    # Save to temporary CSV
    temp_csv = f"temp_day_{day}.csv"
    cumulative_df.to_csv(temp_csv, index=False)

    # Compress to ZIP
    zip_path = os.path.join(client_dir, f"transactions_day_{day}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(temp_csv)

    # Remove temporary CSV
    os.remove(temp_csv)

    return cumulative_df


def main():
    """Main execution function for data generation."""
    print("Starting data generation...")
    print(f"Configuration:")
    print(f"  Clients: {CLIENTS}")
    print(f"  Days: {NUM_DAYS}")
    print(f"  Base rows (day 1): {BASE_ROWS_DAY_1:,}")
    print(f"  Incremental rows per day: {INCREMENTAL_ROWS_PER_DAY:,}")
    print(f"  Start date: {START_DATE}")
    print(f"  Number of SKUs: {NUM_SKUS:,}")
    print()

    # Generate SKU distribution (reusable for all clients and days)
    print("Generating SKU distribution...")
    sku_list, sku_weights = generate_sku_distribution()

    # Validate distribution (verify ~80/20 rule)
    top_20_percent = int(NUM_SKUS * 0.2)
    sorted_weights = sorted(sku_weights, reverse=True)
    top_20_weight = sum(sorted_weights[:top_20_percent])
    print(f"  Top 20% of SKUs account for {top_20_weight*100:.1f}% of transactions")
    print()

    print("Loading ZIP reference data...")
    zip_codes, zip_weights = load_zip_reference()
    print(f"  ZIP codes available: {len(zip_codes)} (weights favor California for local metrics)")
    print()

    # Generate data for each client
    for client_id in CLIENTS:
        print(f"Generating data for {client_id}...")
        cumulative_df = None

        for day in range(1, NUM_DAYS + 1):
            cumulative_df = generate_day_data(
                client_id,
                day,
                cumulative_df,
                sku_list,
                sku_weights,
                zip_codes,
                zip_weights
            )
            total_rows = len(cumulative_df)
            print(f"  Day {day:2d}: {total_rows:,} total rows (ZIP created)")

        print()

    print("Data generation complete!")
    print(f"Generated {NUM_DAYS} days of data for {len(CLIENTS)} clients")
    print(f"Total files created: {NUM_DAYS * len(CLIENTS)} ZIP files")
    print(f"Data directory structure: data/{{client_id}}/transactions_day_{{N}}.zip")


if __name__ == "__main__":
    main()
