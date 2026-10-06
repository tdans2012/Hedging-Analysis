"""
entsoe_api.py
=============
Pulls day-ahead prices, cross-border physical flows, and generation by type
from the ENTSO-E Transparency Platform using the entsoe-py library, and
provides maintenance tasks for the generation files.

Outputs:
  data/processed/entsoe/prices_YYYY_YYYY.parquet
  data/processed/entsoe/crossborder_flows_YYYY_YYYY.parquet
  data/processed/entsoe/generation_{zone}_YYYY_YYYY.parquet

Usage - fetch tasks (unchanged):
  python scripts/entsoe_api.py                    # fetch all data types
  python scripts/entsoe_api.py prices             # prices only
  python scripts/entsoe_api.py flows              # cross-border flows only
  python scripts/entsoe_api.py generation         # generation only

Usage - generation file maintenance:
  python scripts/entsoe_api.py audit                  # report problems (no API calls)
  python scripts/entsoe_api.py audit --zone NO_5 DK_1
  python scripts/entsoe_api.py repair                 # fix mixed-type columns (no API calls)
  python scripts/entsoe_api.py repair --zone NO_5
  python scripts/entsoe_api.py refetch --zone NO_5 --months 2025-09 2025-10
  python scripts/entsoe_api.py refetch --from-audit   # refetch all missing/partial months
  python scripts/entsoe_api.py refetch --from-audit --zone NO_5

  audit   - For every generation_*.parquet file, reports:
              * mixed-type columns (two-level labels stringified by parquet)
                and the months whose values live only in those columns
              * months with no data at all
              * partial months (more than PARTIAL_TOLERANCE_HOURS hours short)
  repair  - Folds mixed-type columns into the standard column names and
            rewrites the file. No API calls. Backs up the original first.
  refetch - Re-downloads specific zone-months and merges them into the
            existing file (also repairs mixed-type columns in that file).
            Backs up the original first.

  Backups are written as <file>.parquet.bak. An existing backup is never
  overwritten, so the .bak file is always the oldest (pre-maintenance) copy.

Generation column convention:
  For months where a zone also reports consumption (e.g. pumped storage),
  entsoe-py returns two-level columns. These are flattened as:
    (type, "Actual Aggregated")  -> "type"               e.g. "Hydro Water Reservoir"
    (type, "Actual Consumption") -> "type Consumption"   e.g. "Hydro Pumped Storage Consumption"
  All other column names are unchanged.

Requires:
  pip install entsoe-py pandas pyarrow
  Environment variable ENTSOE_API_KEY set to your security token

  Alternatively, create a .env file in the project root:
    ENTSOE_API_KEY=your-token-here
"""

import argparse
import ast
import os
import shutil
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError

# -- Configuration -----------------------------------------------------------

OUTPUT_DIR = Path("data/processed/entsoe")

# Date range (months after the current UTC hour are never requested)
START_YEAR = 2020
END_YEAR = 2026

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

# Generation queries - subset of zones (most relevant for the analysis)
GENERATION_ZONES = ["DK_1", "DK_2",
                    "FR",   "NL",
                    "BE",   "AT",
                    "NO_1", "NO_2",
                    "NO_3", "NO_4",
                    "NO_5", "SE_1",
                    "SE_2", "SE_3",
                    "SE_4", "FI"]

# Cross-border flow pairs (from, to) - DE-LU borders
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

# Audit: a month counts as partial if it is short by more than this many hours
PARTIAL_TOLERANCE_HOURS = 24

# Second-level labels returned by entsoe-py for generation
AGGREGATED = "Actual Aggregated"
CONSUMPTION = "Actual Consumption"

FETCH_TASKS = ("prices", "flows", "generation")
MAINTENANCE_TASKS = ("audit", "repair", "refetch")


# -- API key and client -------------------------------------------------------

def load_api_key():
    """Read API key from environment or .env file (only needed for API tasks)."""
    api_key = os.environ.get("ENTSOE_API_KEY")
    if not api_key:
        env_file = Path(".env")
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("ENTSOE_API_KEY="):
                    api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break

    if not api_key:
        print("ERROR: ENTSOE_API_KEY not found.")
        print("Set it as an environment variable or in a .env file.")
        sys.exit(1)
    return api_key


def make_client():
    return EntsoePandasClient(api_key=load_api_key())


# -- Time helpers -------------------------------------------------------------

def fetch_cap():
    """Latest timestamp ever requested: the start of the current UTC hour."""
    return pd.Timestamp.now(tz="UTC").floor("h")


def month_window(period, cap):
    """(start, end) in UTC for a monthly Period, clipped at cap. None if in the future."""
    start = period.start_time.tz_localize("UTC")
    if start >= cap:
        return None
    end = min((period + 1).start_time.tz_localize("UTC"), cap)
    return start, end


def month_windows(years, cap):
    """List of (label, start, end) for every month in years up to cap."""
    windows = []
    for period in pd.period_range(f"{min(years)}-01", f"{max(years)}-12", freq="M"):
        window = month_window(period, cap)
        if window is None:
            break
        windows.append((str(period), *window))
    return windows


def compress_months(months):
    """['2025-09', '2025-10', '2025-11', '2026-01'] -> '2025-09..2025-11, 2026-01'."""
    if not months:
        return "none"
    periods = sorted(pd.Period(m, freq="M") for m in months)
    ranges = []
    first = prev = periods[0]
    for p in periods[1:]:
        if p == prev + 1:
            prev = p
            continue
        ranges.append((first, prev))
        first = prev = p
    ranges.append((first, prev))
    return ", ".join(str(a) if a == b else f"{a}..{b}" for a, b in ranges)


def to_utc_index(df):
    """Ensure a UTC DatetimeIndex named datetime_utc."""
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    df.index.name = "datetime_utc"
    return df


# -- Generation column handling -----------------------------------------------

def parse_label(col):
    """
    Return (type, kind) if col is a two-level label - either a real tuple
    (MultiIndex) or a tuple that parquet stringified, e.g.
    "('Hydro Water Reservoir', 'Actual Aggregated')". Otherwise None.
    """
    if isinstance(col, tuple):
        parts = col
    elif isinstance(col, str) and col.startswith("(") and col.endswith(")"):
        try:
            parts = ast.literal_eval(col)
        except (ValueError, SyntaxError):
            return None
        if not isinstance(parts, tuple):
            return None
    else:
        return None

    if len(parts) == 0:
        return None
    if len(parts) == 1:
        return str(parts[0]), AGGREGATED
    return str(parts[0]), str(parts[1])


def flat_name(col):
    """Standard flat column name for any label."""
    parsed = parse_label(col)
    if parsed is None:
        return str(col)
    gen_type, kind = parsed
    return f"{gen_type} Consumption" if kind == CONSUMPTION else gen_type


def tuple_columns(df):
    """Columns that are two-level labels (real or stringified)."""
    return [c for c in df.columns if parse_label(c) is not None]


def normalise_generation_columns(df):
    """
    Flatten two-level labels to standard names, then coalesce any columns
    that now share a name (first non-null value wins, left to right).
    """
    out = df.copy()
    out.columns = pd.Index([flat_name(c) for c in df.columns])

    if out.columns.has_duplicates:
        merged = {}
        for name in pd.unique(out.columns):
            block = out.loc[:, out.columns == name]
            merged[name] = block.bfill(axis=1).iloc[:, 0]
        out = pd.DataFrame(merged, index=out.index)

    return out


def combine_frames(frames):
    """Concatenate generation frames; later frames win on duplicate timestamps."""
    combined = pd.concat(frames, sort=False)
    combined = normalise_generation_columns(combined)
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined.sort_index()


# -- Generation file helpers --------------------------------------------------

def generation_path(zone, years):
    return OUTPUT_DIR / f"generation_{zone}_{min(years)}_{max(years)}.parquet"


def parse_generation_filename(path):
    """generation_NO_5_2020_2026.parquet -> ('NO_5', 2020, 2026)."""
    stem = path.stem
    if not stem.startswith("generation_"):
        return None
    try:
        zone, y0, y1 = stem[len("generation_"):].rsplit("_", 2)
        return zone, int(y0), int(y1)
    except ValueError:
        return None


def find_generation_files(zones=None, years=None):
    """List (path, zone, y0, y1), optionally filtered by zone and year range."""
    files = []
    for path in sorted(OUTPUT_DIR.glob("generation_*.parquet")):
        parsed = parse_generation_filename(path)
        if parsed is None:
            continue
        zone, y0, y1 = parsed
        if zones and zone not in zones:
            continue
        if years and (y0, y1) != (min(years), max(years)):
            continue
        files.append((path, zone, y0, y1))
    return files


def backup_file(path):
    backup = path.with_name(path.name + ".bak")
    if backup.exists():
        print(f"    Backup exists, left untouched: {backup.name}")
    else:
        shutil.copy2(path, backup)
        print(f"    Backup: {backup.name}")


def write_generation(df, path):
    df = df.sort_index()
    df.index.name = "datetime_utc"
    bad = [c for c in df.columns if not isinstance(c, str)]
    if bad:
        raise ValueError(f"Non-string column names would be written: {bad}")
    df.to_parquet(path)
    size_mb = path.stat().st_size / (1024 * 1024)
    print(f"    Written: {path.name} ({df.shape}, {size_mb:.1f} MB)")


# -- Shared yearly fetch (prices, flows) --------------------------------------

def fetch_yearly(fetch_fn, years, cap, delay=REQUEST_DELAY, **kwargs):
    """
    Call fetch_fn for each year up to cap, concatenate results.
    Handles NoMatchingDataError without retrying.
    """
    results = []
    for year in years:
        start = pd.Timestamp(f"{year}-01-01", tz="UTC")
        if start >= cap:
            print(f"      {year}: skipped (future)")
            continue
        end = min(pd.Timestamp(f"{year + 1}-01-01", tz="UTC"), cap)
        try:
            data = fetch_fn(start=start, end=end, **kwargs)
            if data is not None and len(data) > 0:
                results.append(data)
                print(f"      {year}: {len(data)} rows")
            else:
                print(f"      {year}: no data")
        except NoMatchingDataError:
            print(f"      {year}: no data")
        except Exception as e:
            print(f"      {year}: ERROR - {e}")
        time.sleep(delay)

    if results:
        combined = pd.concat(results)
        combined = combined[~combined.index.duplicated(keep="first")]
        return combined.sort_index()
    return None


# -- Task 1: Day-ahead prices -------------------------------------------------

def fetch_prices(client, years, cap):
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
            cap,
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

    if df.index.tz is not None:
        df.index = df.index.tz_convert("UTC")

    df.to_parquet(output_path)
    print(f"\n  Written: {output_path}")
    print(f"  Shape: {df.shape}, {df.index.min()} to {df.index.max()}")


# -- Task 2: Cross-border physical flows --------------------------------------

def fetch_flows(client, years, cap):
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
            cap,
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

    df.to_parquet(output_path)
    print(f"\n  Written: {output_path}")
    print(f"  Shape: {df.shape}, {df.index.min()} to {df.index.max()}")


# -- Task 3: Generation by type -----------------------------------------------

def fetch_generation_month(client, zone, start, end, max_retries=3):
    """
    Fetch generation for a single zone and single month, with flattened
    column names and a UTC index. No retry when ENTSO-E reports no data;
    increasing backoff on other failures.
    """
    for attempt in range(1, max_retries + 1):
        try:
            df = client.query_generation(zone, start=start, end=end)
        except NoMatchingDataError:
            return None
        except Exception as e:
            wait = 10 * attempt
            print(f"        Attempt {attempt}/{max_retries} failed: {e!r}")
            if attempt < max_retries:
                print(f"        Retrying in {wait}s...")
                time.sleep(wait)
                continue
            print("        Giving up on this month.")
            return None

        if df is None or len(df) == 0:
            return None
        if isinstance(df, pd.Series):
            df = df.to_frame()
        df = normalise_generation_columns(df)
        return to_utc_index(df)

    return None


def fetch_generation(client, years, cap):
    """Fetch actual generation by type for selected zones, chunked monthly."""

    print("\n=== Generation by Type ===")
    print("  (Fetching monthly to avoid API timeouts)")

    windows = month_windows(years, cap)

    for zone in GENERATION_ZONES:
        output_path = generation_path(zone, years)
        if output_path.exists():
            print(f"  {zone}: [SKIP] already exists. "
                  f"Use 'refetch' to update months, or delete to re-fetch.")
            continue

        print(f"\n  {zone}:")
        results = []
        failed_months = []

        for label, start, end in windows:
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
            print(f"    Failed months: {compress_months(failed_months)}")

        if not results:
            print(f"    No generation data for {zone}")
            continue

        write_generation(combine_frames(results), output_path)


# -- Task 4: Audit generation files -------------------------------------------

def audit_file(path, zone, y0, y1, cap):
    """Inspect one generation file; returns a dict of findings."""
    df = to_utc_index(pd.read_parquet(path))

    tcols = tuple_columns(df)
    if tcols:
        has_tuple_values = df[tcols].notna().any(axis=1).to_numpy()
        mixed_months = sorted(set(df.index[has_tuple_values].strftime("%Y-%m")))
    else:
        mixed_months = []

    hours_per_month = pd.Index(
        df.index.floor("h").unique().strftime("%Y-%m")
    ).value_counts()

    missing, partial = [], []
    for period in pd.period_range(f"{y0}-01", f"{y1}-12", freq="M"):
        window = month_window(period, cap)
        if window is None:
            break
        start, end = window
        expected = int((end - start) / pd.Timedelta(hours=1))
        covered = int(hours_per_month.get(str(period), 0))
        if covered == 0:
            missing.append(str(period))
        elif expected - covered > PARTIAL_TOLERANCE_HOURS:
            partial.append(str(period))

    return {
        "path": path,
        "zone": zone,
        "rows": len(df),
        "cols": df.shape[1],
        "mixed_columns": sorted({flat_name(c) for c in tcols}),
        "mixed_months": mixed_months,
        "missing": missing,
        "partial": partial,
    }


def run_audit(zones=None, years=None, cap=None, verbose=True):
    """Audit generation files; returns list of findings dicts."""
    cap = cap or fetch_cap()
    files = find_generation_files(zones, years)

    if verbose:
        print("\n=== Generation File Audit ===")
        print(f"  Checked up to {cap:%Y-%m-%d %H:%M} UTC")

    if not files:
        if verbose:
            print("  No matching generation files found.")
        return []

    findings = []
    for path, zone, y0, y1 in files:
        result = audit_file(path, zone, y0, y1, cap)
        findings.append(result)
        if not verbose:
            continue

        clean = not (result["mixed_columns"] or result["missing"] or result["partial"])
        status = "OK" if clean else "ISSUES"
        print(f"\n  {path.name}  ({result['rows']:,} rows, {result['cols']} cols)  [{status}]")
        if result["mixed_columns"]:
            print(f"    Mixed-type columns ({len(result['mixed_columns'])}): "
                  f"{', '.join(result['mixed_columns'])}")
            print(f"      Months with values only in these columns: "
                  f"{compress_months(result['mixed_months'])}")
        print(f"    Missing months: {compress_months(result['missing'])}")
        print(f"    Partial months: {compress_months(result['partial'])}")

    if verbose:
        n_mixed = sum(bool(r["mixed_columns"]) for r in findings)
        n_gaps = sum(bool(r["missing"] or r["partial"]) for r in findings)
        print(f"\n  Summary: {len(findings)} files, {n_mixed} with mixed-type columns, "
              f"{n_gaps} with missing/partial months.")
        if n_mixed:
            print("    -> 'repair' fixes mixed-type columns without API calls.")
        if n_gaps:
            print("    -> 'refetch --from-audit' re-downloads missing/partial months.")
            print("       (Months ENTSO-E genuinely has no data for will stay flagged.)")

    return findings


# -- Task 5: Repair generation files ------------------------------------------

def repair_generation(zones=None):
    """Fold mixed-type columns into standard names; no API calls."""
    print("\n=== Repair Generation Files ===")

    files = find_generation_files(zones)
    if not files:
        print("  No matching generation files found.")
        return

    for path, zone, _, _ in files:
        df = to_utc_index(pd.read_parquet(path))
        tcols = tuple_columns(df)

        if not tcols and not df.columns.has_duplicates:
            print(f"  {path.name}: clean, skipped")
            continue

        print(f"\n  {path.name}:")
        moved = int(df[tcols].notna().sum().sum())
        before = int(df.notna().sum().sum())

        fixed = normalise_generation_columns(df)
        after = int(fixed.notna().sum().sum())

        print(f"    Folded {len(tcols)} mixed-type columns ({moved:,} values) "
              f"into standard names")
        if after < before:
            print(f"    WARNING: {before - after:,} timestamps had values in both "
                  f"forms; kept the standard-name value")

        new_cols = [c for c in fixed.columns if c not in df.columns]
        if new_cols:
            print(f"    New columns: {', '.join(new_cols)}")

        backup_file(path)
        write_generation(fixed, path)


# -- Task 6: Targeted refetch -------------------------------------------------

def refetch_generation(client, targets, years, cap):
    """
    targets: dict {zone: [YYYY-MM, ...]}. Re-downloads those months and
    merges them into the existing file for the current year range.
    """
    print("\n=== Refetch Generation Months ===")

    if not targets:
        print("  Nothing to refetch.")
        return

    for zone, months in targets.items():
        path = generation_path(zone, years)
        print(f"\n  {zone} ({path.name}): {compress_months(months)}")

        if not path.exists():
            print("    File not found - run the 'generation' task for this zone first.")
            continue

        existing = normalise_generation_columns(to_utc_index(pd.read_parquet(path)))

        new_frames, fetched_windows, failed = [], [], []
        for month in sorted(set(months)):
            period = pd.Period(month, freq="M")
            if not (min(years) <= period.year <= max(years)):
                print(f"    {month}... outside {min(years)}-{max(years)}, skipped")
                continue
            window = month_window(period, cap)
            if window is None:
                print(f"    {month}... future, skipped")
                continue

            start, end = window
            print(f"    {month}...", end=" ", flush=True)
            df = fetch_generation_month(client, zone, start, end)
            if df is not None:
                new_frames.append(df)
                fetched_windows.append((start, end))
                print(f"OK ({df.shape[0]} rows, {df.shape[1]} cols)")
            else:
                failed.append(month)
                print("no data")
            time.sleep(REQUEST_DELAY)

        if failed:
            print(f"    No data for: {compress_months(failed)} (existing rows kept)")

        if not new_frames:
            print("    Nothing fetched; file unchanged.")
            continue

        # Replace only the months that were successfully fetched
        drop = np.zeros(len(existing), dtype=bool)
        for start, end in fetched_windows:
            drop |= (existing.index >= start) & (existing.index < end)

        combined = combine_frames([existing.loc[~drop]] + new_frames)
        backup_file(path)
        write_generation(combined, path)


def targets_from_audit(zones, years, cap):
    """Missing and partial months per zone, from current-range files."""
    findings = run_audit(zones=zones, years=years, cap=cap, verbose=False)
    targets = {}
    for r in findings:
        months = r["missing"] + r["partial"]
        if months:
            targets[r["zone"]] = months
    return targets


# -- Main ---------------------------------------------------------------------

def month_arg(value):
    try:
        return str(pd.Period(value, freq="M"))
    except Exception:
        raise argparse.ArgumentTypeError(f"invalid month '{value}', expected YYYY-MM")


def build_parser():
    parser = argparse.ArgumentParser(
        prog="entsoe_api.py",
        description="Generation file maintenance tasks.",
    )
    sub = parser.add_subparsers(dest="task", required=True)

    p_audit = sub.add_parser("audit", help="Report mixed-type columns and missing/partial months")
    p_audit.add_argument("--zone", nargs="+", help="Limit to these zones")

    p_repair = sub.add_parser("repair", help="Fold mixed-type columns into standard names")
    p_repair.add_argument("--zone", nargs="+", help="Limit to these zones")

    p_refetch = sub.add_parser("refetch", help="Re-download specific zone-months")
    p_refetch.add_argument("--zone", nargs="+", choices=GENERATION_ZONES,
                           help="Zones to refetch (required with --months)")
    p_refetch.add_argument("--months", nargs="+", type=month_arg,
                           help="Months as YYYY-MM")
    p_refetch.add_argument("--from-audit", action="store_true",
                           help="Refetch all missing/partial months found by audit")
    return parser


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    years = list(range(START_YEAR, END_YEAR + 1))
    cap = fetch_cap()
    argv = sys.argv[1:]

    # Maintenance tasks
    if argv and argv[0] in MAINTENANCE_TASKS:
        parser = build_parser()
        args = parser.parse_args(argv)

        if args.task == "audit":
            run_audit(zones=args.zone, cap=cap)

        elif args.task == "repair":
            repair_generation(zones=args.zone)

        elif args.task == "refetch":
            if args.from_audit and args.months:
                parser.error("use either --months or --from-audit, not both")
            if args.from_audit:
                targets = targets_from_audit(args.zone, years, cap)
            elif args.zone and args.months:
                targets = {zone: args.months for zone in args.zone}
            else:
                parser.error("refetch needs --zone and --months, or --from-audit")
            client = make_client()
            refetch_generation(client, targets, years, cap)

        print("\nDone.")
        return

    # Fetch tasks
    tasks = argv if argv else list(FETCH_TASKS)
    unknown = [t for t in tasks if t not in FETCH_TASKS]
    if unknown:
        print(f"ERROR: unknown task(s): {', '.join(unknown)}")
        print(f"Fetch tasks: {', '.join(FETCH_TASKS)}")
        print(f"Maintenance tasks: {', '.join(MAINTENANCE_TASKS)} (use -h after the task for options)")
        sys.exit(2)

    client = make_client()

    print("ENTSO-E Data Ingestion")
    print(f"  Years: {min(years)}-{max(years)} (up to {cap:%Y-%m-%d %H:%M} UTC)")
    print(f"  Price zones: {len(PRICE_ZONES)}")
    print(f"  Flow pairs: {len(FLOW_PAIRS)}")
    print(f"  Generation zones: {len(GENERATION_ZONES)}")

    if "prices" in tasks:
        fetch_prices(client, years, cap)

    if "flows" in tasks:
        fetch_flows(client, years, cap)

    if "generation" in tasks:
        fetch_generation(client, years, cap)

    print("\nDone.")


if __name__ == "__main__":
    main()
