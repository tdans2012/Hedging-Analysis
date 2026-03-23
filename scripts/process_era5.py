"""
process_era5.py  (v2)
=====================
Processes raw ERA5 NetCDF files into bidding-zone-level hourly time series.

ERA5 downloads produce two files per month:
  - era5_YYYY_MM_instant.nc  (u10, v10, u100, v100, t2m)
  - era5_YYYY_MM_accum.nc    (ssrd)

Steps:
  1. Load Natural Earth country shapefile
  2. Build bidding zone polygons (merge DE+LU for DE-LU, etc.)
  3. For each month pair:
     - Identify ERA5 grid cells inside each zone polygon
     - Compute cosine-latitude-weighted spatial averages
     - Derive wind speed, convert units
  4. Write one parquet file per bidding zone to data/processed/era5/

Usage:
  python scripts/process_era5.py               # process all zones
  python scripts/process_era5.py DE-LU         # process one zone

Requires:
  pip install xarray netCDF4 geopandas shapely numpy pandas pyarrow
"""

import sys
import re
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path

# -- Configuration -----------------------------------------------------------

RAW_DIR = Path("data/raw/era5")
SHAPEFILE = Path("data/raw/shapefiles/ne_10m_admin_0_countries.shp")
OUTPUT_DIR = Path("data/processed/era5")

BIDDING_ZONES = {
    "DE-LU": ["DEU", "LUX"],
    "FR":    ["FRA"],
    "NL":    ["NLD"],
    "BE":    ["BEL"],
    "AT":    ["AUT"],
    "DK":    ["DNK"],
    "NO":    ["NOR"],
    "SE":    ["SWE"],
    "FI":    ["FIN"],
}


# -- Step 1: Build bidding zone polygons ------------------------------------

def load_bidding_zone_polygons(shapefile_path: Path) -> dict:
    """Load country shapefile and merge into bidding zone polygons."""

    print("Loading shapefile...")
    world = gpd.read_file(shapefile_path)

    iso_col = "ISO_A3_EH" if "ISO_A3_EH" in world.columns else "ISO_A3"

    zones = {}
    for zone_id, iso_codes in BIDDING_ZONES.items():
        subset = world[world[iso_col].isin(iso_codes)]
        if subset.empty:
            print(f"  [WARN] No polygons found for {zone_id} ({iso_codes})")
            continue
        merged = subset.dissolve()
        zones[zone_id] = merged.geometry.iloc[0]
        print(f"  {zone_id}: merged {len(iso_codes)} polygon(s)")

    return zones


# -- Step 2: Build grid masks -----------------------------------------------

def build_zone_masks(ds: xr.Dataset, zones: dict) -> dict:
    """
    For each bidding zone, identify which ERA5 grid cells fall inside
    the polygon and compute cosine-latitude weights.
    """

    lats = ds.latitude.values
    lons = ds.longitude.values
    lon_grid, lat_grid = np.meshgrid(lons, lats)

    cos_weights = np.cos(np.deg2rad(lat_grid))

    masks = {}
    for zone_id, polygon in zones.items():
        inside = np.zeros_like(lat_grid, dtype=bool)
        for i in range(len(lats)):
            for j in range(len(lons)):
                inside[i, j] = polygon.contains(Point(lons[j], lats[i]))

        if not inside.any():
            print(f"  [WARN] No grid cells found inside {zone_id}")
            continue

        weighted = np.where(inside, cos_weights, 0.0)
        weighted = weighted / weighted.sum()

        masks[zone_id] = xr.DataArray(
            weighted,
            dims=["latitude", "longitude"],
            coords={"latitude": lats, "longitude": lons},
        )

        n_cells = inside.sum()
        print(f"  {zone_id}: {n_cells} grid cells")

    return masks


# -- Step 3: Process one month (instant + accum pair) ------------------------

def get_time_dim(ds: xr.Dataset) -> str:
    """Return the name of the time dimension."""
    for name in ["valid_time", "time"]:
        if name in ds.dims:
            return name
    raise ValueError(f"No time dimension found. Dims: {list(ds.dims)}")


def process_one_month(instant_path: Path, accum_path: Path, masks: dict) -> dict:
    """
    Read instant + accum NetCDF files for one month and compute
    zone-level hourly averages.

    Returns dict of {zone_id: pd.DataFrame}.
    """

    ds_inst = xr.open_dataset(instant_path, engine="netcdf4")
    ds_accum = xr.open_dataset(accum_path, engine="netcdf4")

    time_dim = get_time_dim(ds_inst)
    times = pd.to_datetime(ds_inst[time_dim].values)

    results = {}
    for zone_id, mask in masks.items():
        records = {}

        # -- Instantaneous variables (from instant file) --

        # Wind speed at 10m
        u10 = ds_inst["u10"]
        v10 = ds_inst["v10"]
        ws10 = np.sqrt(u10**2 + v10**2)
        records["wind_speed_10m"] = (ws10 * mask).sum(dim=["latitude", "longitude"]).values

        # Wind speed at 100m
        u100 = ds_inst["u100"]
        v100 = ds_inst["v100"]
        ws100 = np.sqrt(u100**2 + v100**2)
        records["wind_speed_100m"] = (ws100 * mask).sum(dim=["latitude", "longitude"]).values

        # 2m temperature: Kelvin -> Celsius
        t2m = ds_inst["t2m"] - 273.15
        records["temperature_2m"] = (t2m * mask).sum(dim=["latitude", "longitude"]).values

        # -- Accumulated variables (from accum file) --

        # SSRD: accumulated since 00:00 each day in J/m²
        # Differencing gives hourly irradiance
        time_dim_accum = get_time_dim(ds_accum)
        ssrd_raw = ds_accum["ssrd"]

        # Spatial average first, then diff (much faster + avoids alignment issues)
        ssrd_zone = (ssrd_raw * mask).sum(dim=["latitude", "longitude"])
        ssrd_values = ssrd_zone.values

        # Diff to get hourly values; first timestep gets NaN
        ssrd_hourly = np.empty_like(ssrd_values)
        ssrd_hourly[0] = np.nan
        ssrd_hourly[1:] = np.diff(ssrd_values)

        # Where diff is negative (daily accumulation reset), use the raw value
        # because after reset the accumulated value IS the hourly total
        neg_mask = ssrd_hourly < 0
        ssrd_hourly[neg_mask] = ssrd_values[neg_mask]

        # Convert J/m² to W/m²
        records["ssrd_wm2"] = ssrd_hourly / 3600.0

        # Build DataFrame
        df = pd.DataFrame(records, index=times)
        df.index.name = "datetime_utc"
        results[zone_id] = df

    ds_inst.close()
    ds_accum.close()
    return results


# -- Step 4: Discover month pairs --------------------------------------------

def find_month_pairs() -> list:
    """
    Find all (instant, accum) file pairs in RAW_DIR.
    Returns list of (year_month_str, instant_path, accum_path) sorted.
    """

    instant_files = sorted(RAW_DIR.glob("era5_*_instant.nc"))
    pairs = []

    for inst_path in instant_files:
        # Extract YYYY_MM from filename like era5_2020_01_instant.nc
        match = re.search(r"era5_(\d{4}_\d{2})_instant\.nc", inst_path.name)
        if not match:
            continue
        ym = match.group(1)
        accum_path = RAW_DIR / f"era5_{ym}_accum.nc"

        if accum_path.exists():
            pairs.append((ym, inst_path, accum_path))
        else:
            print(f"  [WARN] Missing accum file for {ym}, skipping")

    return pairs


# -- Step 5: Main pipeline ---------------------------------------------------

def main() -> None:
    zone_filter = None
    if len(sys.argv) > 1:
        zone_filter = sys.argv[1:]
        print(f"Filtering to zones: {zone_filter}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load zone polygons
    zones = load_bidding_zone_polygons(SHAPEFILE)

    if zone_filter:
        zones = {k: v for k, v in zones.items() if k in zone_filter}

    if not zones:
        print("No valid zones to process. Exiting.")
        return

    # Find month pairs
    pairs = find_month_pairs()
    if not pairs:
        print(f"No ERA5 file pairs found in {RAW_DIR}. Exiting.")
        print(f"Expected pattern: era5_YYYY_MM_instant.nc + era5_YYYY_MM_accum.nc")
        return

    print(f"\nFound {len(pairs)} month pairs")

    # Build masks from the first instant file
    print("\nBuilding spatial masks...")
    ds_sample = xr.open_dataset(pairs[0][1], engine="netcdf4")
    masks = build_zone_masks(ds_sample, zones)
    ds_sample.close()

    if not masks:
        print("No valid masks. Exiting.")
        return

    # Process all months
    print(f"\nProcessing {len(pairs)} months across {len(masks)} zones...")
    all_results = {zone_id: [] for zone_id in masks}

    for i, (ym, inst_path, accum_path) in enumerate(pairs):
        print(f"  [{i+1}/{len(pairs)}] {ym}")
        month_results = process_one_month(inst_path, accum_path, masks)

        for zone_id, df in month_results.items():
            all_results[zone_id].append(df)

    # Concatenate and write parquet
    print("\nWriting parquet files...")
    for zone_id, dfs in all_results.items():
        combined = pd.concat(dfs).sort_index()
        combined = combined[~combined.index.duplicated(keep="first")]

        output_path = OUTPUT_DIR / f"{zone_id}.parquet"
        combined.to_parquet(output_path)

        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  {zone_id}: {len(combined)} rows, "
              f"{combined.index.min()} to {combined.index.max()} "
              f"({size_mb:.1f} MB)")

    print("\nDone.")


if __name__ == "__main__":
    main()
