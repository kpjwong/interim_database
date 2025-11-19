#!/usr/bin/env python3
"""
A comprehensive speed test to compare CSV filtering performance across five tools:
1. Pandas (default C engine)
2. Polars (lazy execution)
3. DuckDB (in-process analytical DB)
4. gawk (command-line streaming)
5. qsv (command-line streaming)

Installation:
pip install pandas numpy polars duckdb
- gawk and qsv must be installed and available in the system's PATH.
"""

import os
import subprocess
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import polars as pl
import duckdb

# --- Configuration ---
CSV_FILENAME = "very_large_transactions.csv"
GAWK_OUTPUT_FILENAME = "gawk_filtered_output.csv"
QSV_OUTPUT_FILENAME = "qsv_filtered_output.csv"
NUM_ROWS = 10_000_000
FILTER_DATE = "2025-10-05"
# Column index for date (1-based for gawk, name-based for others)
DATE_COLUMN_INDEX = 2 
DATE_COLUMN_NAME = "transaction_date"


def generate_fake_data():
    """Creates a very large CSV file for testing if it doesn't exist."""
    if os.path.exists(CSV_FILENAME):
        print(f"'{CSV_FILENAME}' already exists. Skipping generation.")
        return
    print(f"Generating a test file with {NUM_ROWS:,} rows...")
    start_date = datetime(2025, 10, 1)
    date_range = [start_date + timedelta(days=x) for x in range(10)]

    data = {
        'transaction_id': np.arange(NUM_ROWS),
        'transaction_date': np.random.choice([d.strftime('%Y-%m-%d') for d in date_range], size=NUM_ROWS),
        'sku_id': [f"SKU_{i}" for i in np.random.randint(1, 5000, size=NUM_ROWS)],
        'quantity': np.random.randint(1, 10, size=NUM_ROWS)
    }
    df = pd.DataFrame(data)
    df.to_csv(CSV_FILENAME, index=False)
    print(f"'{CSV_FILENAME}' created successfully.\n")


def test_pandas():
    """Measures the time to read the full file and filter using pandas."""
    print("--- Testing Pandas (Default C Engine) ---")
    start_time = time.perf_counter()
    df = pd.read_csv(CSV_FILENAME)
    _ = df[df[DATE_COLUMN_NAME] > FILTER_DATE]
    end_time = time.perf_counter()
    return end_time - start_time


def test_polars():
    """Measures the time to scan, filter, and collect using polars."""
    print("--- Testing Polars ---")
    start_time = time.perf_counter()
    q = pl.scan_csv(CSV_FILENAME)
    q = q.filter(pl.col(DATE_COLUMN_NAME) > FILTER_DATE)
    _ = q.collect()
    end_time = time.perf_counter()
    return end_time - start_time


def test_duckdb():
    """Measures the time to query the CSV directly using DuckDB."""
    print("--- Testing DuckDB ---")
    start_time = time.perf_counter()
    query = f"""
        SELECT *
        FROM read_csv_auto('{CSV_FILENAME}')
        WHERE {DATE_COLUMN_NAME} > '{FILTER_DATE}'
    """
    _ = duckdb.sql(query).fetchall()
    end_time = time.perf_counter()
    return end_time - start_time


def test_gawk():
    """Measures a disk-to-disk filter using gawk."""
    print("--- Testing gawk (disk-to-disk) ---")
    # Windows-compatible quoting for the command
    gawk_command = (
        f'gawk -F, "NR > 1 && ${DATE_COLUMN_INDEX} > \\"{FILTER_DATE}\\"" '
        f'"{CSV_FILENAME}" > "{GAWK_OUTPUT_FILENAME}"'
    )
    start_time = time.perf_counter()
    try:
        subprocess.run(gawk_command, shell=True, check=True, capture_output=True)
    except FileNotFoundError:
        print("ERROR: 'gawk' command not found. Skipping test.")
        return None
    except subprocess.CalledProcessError as e:
        print(f"ERROR: gawk command failed with exit code {e.returncode}")
        print(f"Stderr: {e.stderr.decode()}")
        return None
    end_time = time.perf_counter()
    return end_time - start_time

def test_qsv():
    """Measures a disk-to-disk filter using qsv."""
    print("--- Testing qsv (disk-to-disk) ---")
    qsv_command = (
        f'qsv luau . "return row.{DATE_COLUMN_NAME} > \'{FILTER_DATE}\'" '
        f'"{CSV_FILENAME}" > "{QSV_OUTPUT_FILENAME}"'
    )
    # --- END CORRECTION ---

    start_time = time.perf_counter()
    try:
        subprocess.run(qsv_command, shell=True, check=True, capture_output=True)
    except FileNotFoundError:
        print("ERROR: 'qsv' command not found. Skipping test.")
        return None
    except subprocess.CalledProcessError as e:
        print(f"ERROR: qsv command failed with exit code {e.returncode}")
        print(f"Stderr: {e.stderr.decode()}")
        return None
    end_time = time.perf_counter()
    return end_time - start_time

def main():
    """Main execution function to run all tests and print results."""
    generate_fake_data()
    
    results = []
    results.append(("Pandas", test_pandas()))
    results.append(("Polars", test_polars()))
    results.append(("DuckDB", test_duckdb()))
    results.append(("gawk", test_gawk()))
    results.append(("qsv", test_qsv()))

    # Filter out any failed tests and sort by runtime
    results = sorted(
        [(name, runtime) for name, runtime in results if runtime is not None],
        key=lambda x: x[1]
    )

    print("\n--- Final Speed Test Results (10 Million Rows) ---")
    print("-" * 55)
    for i, (name, runtime) in enumerate(results):
        print(f"#{i+1}: {name:<10} | Runtime: {runtime:.4f} seconds")
    print("-" * 55)

    if len(results) > 1:
        fastest_name, fastest_time = results[0]
        slowest_name, slowest_time = results[-1]
        if fastest_time > 0:
            times_faster = slowest_time / fastest_time
            print(f"\nConclusion: {fastest_name} was the fastest, beating {slowest_name} by {times_faster:.2f}x.")

    # Clean up all generated files
    print("\nCleaning up generated files...")
    for filename in [CSV_FILENAME, GAWK_OUTPUT_FILENAME, QSV_OUTPUT_FILENAME]:
        if os.path.exists(filename):
            os.remove(filename)

if __name__ == "__main__":
    main()
    """
    -----------------------------------------------
    #1: Polars     | Runtime: 0.1900 seconds
    #2: DuckDB     | Runtime: 2.3305 seconds
    #3: Pandas     | Runtime: 2.5889 seconds
    #4: gawk       | Runtime: 6.0486 seconds
    -----------------------------------------------
    """