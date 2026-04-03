"""
Fetch actual total load for multiple ENTSO-E bidding zones.
Saves hourly data to data/processed/entsoe/load_{zone}_2020_2025.parquet

Usage (from project root, in terminal — NOT RStudio console):
    .venv\Scripts\activate
    python scripts/entsoe_load_zones.py

Skips zones that already have a load parquet file.
To re-download a zone, delete its file first.
"""

import os
import sys
import time
import pandas as pd
from pathlib import Path
from entsoe import EntsoePandasClient

# ── Configuration ─────────────────────────────────────────────────────
# ZONES = ["DK_1", "DK_2", "FR", "NL", "BE", "AT"]
ZONES = ["DK_1", "DK_2", "FR", "NL", "BE", "AT",
         "NO_1", "NO_2", "NO_3", "NO_4", "NO_5",
         "SE_1", "SE_2", "SE_3", "SE_4", "FI"]
START_YEAR = 2020
END_YEAR = 2025
OUTPUT_DIR = Path("data/processed/entsoe")
SLEEP_BETWEEN_YEARS = 1   # seconds — avoid API rate limits

# ── API key (matches entsoe_api.py pattern) ───────────────────────────
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

client = EntsoePandasClient(api_key=API_KEY)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Fetch each zone ──────────────────────────────────────────────────
for zone in ZONES:
    output_file = OUTPUT_DIR / f"load_{zone}_{START_YEAR}_{END_YEAR}.parquet"

    if output_file.exists():
        print(f"SKIP {zone} — {output_file} already exists")
        continue

    print(f"\n{'='*60}")
    print(f"  Fetching load: {zone}")
    print(f"{'='*60}")

    frames = []

    for year in range(START_YEAR, END_YEAR + 1):
        start = pd.Timestamp(f"{year}-01-01", tz="UTC")
        end = pd.Timestamp(f"{year + 1}-01-01", tz="UTC")

        print(f"  {zone} {year} ... ", end="", flush=True)

        try:
            df = client.query_load(zone, start=start, end=end)
        except Exception as e:
            print(f"FAILED: {e}")
            continue

        # query_load returns a Series or DataFrame depending on version
        if isinstance(df, pd.Series):
            df = df.to_frame(name="total_load")
        elif isinstance(df, pd.DataFrame):
            if len(df.columns) == 1:
                df.columns = ["total_load"]
            else:
                actual_cols = [c for c in df.columns if "Actual" in str(c)]
                if actual_cols:
                    df = df[actual_cols[0]].to_frame(name="total_load")
                else:
                    df = df.iloc[:, 0].to_frame(name="total_load")

        # Resample to hourly mean (in case of sub-hourly resolution)
        df = df.resample("1h").mean()

        rows = len(df)
        nas = df["total_load"].isna().sum()
        print(f"{rows} rows, {nas} NAs")

        frames.append(df)

        # Be polite to the API
        if year < END_YEAR:
            time.sleep(SLEEP_BETWEEN_YEARS)

    if not frames:
        print(f"  WARNING: No data fetched for {zone} — skipping")
        continue

    load_df = pd.concat(frames).sort_index()
    load_df.index.name = "datetime_utc"
    load_df = load_df[~load_df.index.duplicated(keep="first")]

    print(f"  Combined: {len(load_df)} rows, "
          f"{load_df['total_load'].isna().sum()} NAs")
    print(f"  Range: {load_df.index.min()} -> {load_df.index.max()}")

    load_df.to_parquet(output_file, engine="pyarrow")
    print(f"  Saved to {output_file}")

    # Pause between zones
    time.sleep(SLEEP_BETWEEN_YEARS)

print("\nDone.")
