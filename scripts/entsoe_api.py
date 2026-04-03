"""
entsoe_api.py
=============
Pulls day-ahead prices, cross-border physical flows, and generation by type
from the ENTSO-E Transparency Platform using the entsoe-py library.

Outputs:
  data/processed/entsoe/prices_YYYY_YYYY.parquet
  data/processed/entsoe/crossborder_flows_YYYY_YYYY.parquet
  data/processed/entsoe/generation_{zone}_YYYY_YYYY.parquet

Usage:
  python scripts/entsoe_api.py                    # fetch all data types
  python scripts/entsoe_api.py prices             # prices only
  python scripts/entsoe_api.py flows              # cross-border flows only
  python scripts/entsoe_api.py generation         # generation only

Requires:
  pip install entsoe-py pandas pyarrow
  Environment variable ENTSOE_API_KEY set to your security token

  Alternatively, create a .env file in the project root:
    ENTSOE_API_KEY=your-token-here
"""

import os
import sys
import time
import pandas as pd
from pathlib import Path
from entsoe import EntsoePandasClient

# -- Configuration -----------------------------------------------------------

OUTPUT_DIR = Path("data/processed/entsoe")

# Read API key from environment or .env file
API_KEY = os.environ.get("ENTSOE_API_KEY")
if not API_KEY:
    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("ENTSOE_API_KEY="):
                API_KEY = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

if not API_KEY:
    print("ERROR: ENTSOE_API_KEY not found.")
    print("Set it as an environment variable or in a .env file.")
    sys.exit(1)

# Date range
START_YEAR = 2020
END_YEAR = 2025

# Bidding zones for day-ahead prices and generation
PRICE_ZONES = [
    "DE_LU",    # Germany/Luxembourg
    "FR",       # France
    "NL",       # Netherlands
    "BE",       # Belgium
    "AT",       # Austria
    "DK_1",     # Denmark West
    "DK_2",     # Denmark East
    "NO_1",     # Norway South
    "NO_2",     # Norway Southwest
    "NO_3",     # Norway Central
    "NO_4",     # Norway North
    "NO_5",     # Norway West
    "SE_1",     # Sweden North
    "SE_2",     # Sweden Central-North
    "SE_3",     # Sweden Central-South
    "SE_4",     # Sweden South
    "FI",       # Finland
]

# Generation queries — subset of zones (most relevant for the analysis)
GENERATION_ZONES = ["DK_1", "DK_2", 
                    "FR",   "NL", 
                    "BE",   "AT",
                    "NO_1", "NO_2", 
                    "NO_3", "NO_4", 
                    "NO_5", "SE_1", 
                    "SE_2", "SE_3", 
                    "SE_4", "FI"]

# Cross-border flow pairs (from, to) — DE-LU borders
FLOW_PAIRS = [
    ("DE_LU", "FR"),
    ("FR", "DE_LU"),
    ("DE_LU", "NL"),
    ("NL", "DE_LU"),
    ("DE_LU", "DK_1"),
    ("DK_1", "DE_LU"),
    ("DE_LU", "DK_2"),
    ("DK_2", "DE_LU"),
    ("DE_LU", "AT"),
    ("AT", "DE_LU"),
    ("DE_LU", "PL"),
    ("PL", "DE_LU"),
    ("DE_LU", "CZ"),
    ("CZ", "DE_LU"),
    ("DE_LU", "CH"),
    ("CH", "DE_LU"),
    # Nordic interconnections
    ("NO_2", "NL"),
    ("NL", "NO_2"),
    ("DK_1", "NO_2"),
    ("NO_2", "DK_1"),
    ("SE_4", "DE_LU"),
    ("DE_LU", "SE_4"),
]

# Delay between API requests (seconds) to respect rate limits
REQUEST_DELAY = 1.0


# -- Helpers -----------------------------------------------------------------

def fetch_yearly(fetch_fn, years, delay=REQUEST_DELAY, **kwargs):
    """
    Call fetch_fn for each year, concatenate results.
    Handles NoMatchingDataError gracefully.
    """
    results = []
    for year in years:
        start = pd.Timestamp(f"{year}0101", tz="UTC")
        end = pd.Timestamp(f"{year + 1}0101", tz="UTC")
        try:
            data = fetch_fn(start=start, end=end, **kwargs)
            if data is not None and len(data) > 0:
                results.append(data)
                print(f"      {year}: {len(data)} rows")
            else:
                print(f"      {year}: no data")
        except Exception as e:
            print(f"      {year}: ERROR - {e}")
        time.sleep(delay)

    if results:
        combined = pd.concat(results)
        combined = combined[~combined.index.duplicated(keep="first")]
        return combined.sort_index()
    return None


# -- Task 1: Day-ahead prices -----------------------------------------------

def fetch_prices(client, years):
    """Fetch day-ahead prices for all bidding zones."""

    print("\n=== Day-Ahead Prices ===")

    output_path = OUTPUT_DIR / f"prices_{min(years)}_{max(years)}.parquet"
    if output_path.exists():
        print(f"  [SKIP] {output_path.name} already exists. Delete to re-fetch.")
        return

    all_series = {}

    for zone in PRICE_ZONES:
        print(f"  {zone}:")
        series = fetch_yearly(
            client.query_day_ahead_prices,
            years,
            country_code=zone,
        )
        if series is not None:
            all_series[zone] = series

    if not all_series:
        print("  No price data retrieved.")
        return

    # Combine into a single DataFrame (zones as columns)
    df = pd.DataFrame(all_series)
    df.index.name = "datetime_utc"

    # Convert timezone-aware index to UTC
    if df.index.tz is not None:
        df.index = df.index.tz_convert("UTC")

    output_path = OUTPUT_DIR / f"prices_{min(years)}_{max(years)}.parquet"
    df.to_parquet(output_path)
    print(f"\n  Written: {output_path}")
    print(f"  Shape: {df.shape}, {df.index.min()} to {df.index.max()}")


# -- Task 2: Cross-border physical flows ------------------------------------

def fetch_flows(client, years):
    """Fetch cross-border physical flows for all defined pairs."""

    print("\n=== Cross-Border Physical Flows ===")

    output_path = OUTPUT_DIR / f"crossborder_flows_{min(years)}_{max(years)}.parquet"
    if output_path.exists():
        print(f"  [SKIP] {output_path.name} already exists. Delete to re-fetch.")
        return

    all_series = {}

    for from_zone, to_zone in FLOW_PAIRS:
        label = f"{from_zone}->{to_zone}"
        print(f"  {label}:")
        series = fetch_yearly(
            client.query_crossborder_flows,
            years,
            country_code_from=from_zone,
            country_code_to=to_zone,
        )
        if series is not None:
            all_series[label] = series

    if not all_series:
        print("  No flow data retrieved.")
        return

    df = pd.DataFrame(all_series)
    df.index.name = "datetime_utc"

    if df.index.tz is not None:
        df.index = df.index.tz_convert("UTC")

    output_path = OUTPUT_DIR / f"crossborder_flows_{min(years)}_{max(years)}.parquet"
    df.to_parquet(output_path)
    print(f"\n  Written: {output_path}")
    print(f"  Shape: {df.shape}, {df.index.min()} to {df.index.max()}")


# -- Task 3: Generation by type ---------------------------------------------

def fetch_generation_month(client, zone, start, end, max_retries=3):
    """
    Fetch generation for a single zone and single month.
    Manual retry with increasing backoff on failure.
    """
    for attempt in range(1, max_retries + 1):
        try:
            df = client.query_generation(zone, start=start, end=end)
            if df is not None and len(df) > 0:
                return df
            return None
        except Exception as e:
            wait = 10 * attempt
            print(f"        Attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                print(f"        Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"        Giving up on this month.")
                return None


def fetch_generation(client, years):
    """Fetch actual generation by type for selected zones, chunked monthly."""

    print("\n=== Generation by Type ===")
    print("  (Fetching monthly to avoid API timeouts)")

    for zone in GENERATION_ZONES:
        output_path = OUTPUT_DIR / f"generation_{zone}_{min(years)}_{max(years)}.parquet"
        if output_path.exists():
            print(f"  {zone}: [SKIP] already exists. Delete to re-fetch.")
            continue

        print(f"\n  {zone}:")
        results = []
        failed_months = []

        for year in years:
            for month in range(1, 13):
                # Build month start/end
                start = pd.Timestamp(f"{year}-{month:02d}-01", tz="UTC")
                if month == 12:
                    end = pd.Timestamp(f"{year + 1}-01-01", tz="UTC")
                else:
                    end = pd.Timestamp(f"{year}-{month + 1:02d}-01", tz="UTC")

                label = f"{year}-{month:02d}"
                print(f"    {label}...", end=" ", flush=True)

                df = fetch_generation_month(client, zone, start, end)

                if df is not None:
                    results.append(df)
                    print(f"OK ({df.shape[0]} rows, {df.shape[1]} cols)")
                else:
                    failed_months.append(label)
                    print("no data")

                time.sleep(REQUEST_DELAY)

        if failed_months:
            print(f"    Failed months: {failed_months}")

        if not results:
            print(f"    No generation data for {zone}")
            continue

        combined = pd.concat(results)
        combined = combined[~combined.index.duplicated(keep="first")]
        combined = combined.sort_index()
        combined.index.name = "datetime_utc"

        if combined.index.tz is not None:
            combined.index = combined.index.tz_convert("UTC")

        combined.to_parquet(output_path)
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"    Written: {output_path.name} ({combined.shape}, {size_mb:.1f} MB)")


# -- Main --------------------------------------------------------------------

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    client = EntsoePandasClient(api_key=API_KEY)
    years = list(range(START_YEAR, END_YEAR + 1))

    print(f"ENTSO-E Data Ingestion")
    print(f"  Years: {min(years)}-{max(years)}")
    print(f"  Price zones: {len(PRICE_ZONES)}")
    print(f"  Flow pairs: {len(FLOW_PAIRS)}")
    print(f"  Generation zones: {len(GENERATION_ZONES)}")

    # Parse optional task filter
    tasks = sys.argv[1:] if len(sys.argv) > 1 else ["prices", "flows", "generation"]

    if "prices" in tasks:
        fetch_prices(client, years)

    if "flows" in tasks:
        fetch_flows(client, years)

    if "generation" in tasks:
        fetch_generation(client, years)

    print("\nDone.")


if __name__ == "__main__":
    main()
