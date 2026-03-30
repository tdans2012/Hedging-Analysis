"""
download_era5.py  (v3)
=====================
Downloads ERA5 reanalysis data and extracts/renames the resulting files.

The CDS API returns ZIP archives containing two NetCDF files:
  - data_stream-oper_stepType-instant.nc  (wind, temperature)
  - data_stream-oper_stepType-accum.nc    (SSRD)

This script renames them to:
  - era5_YYYY_MM_instant.nc
  - era5_YYYY_MM_accum.nc

Usage:
  python scripts/download_era5.py                # download all years
  python scripts/download_era5.py 2022           # download a single year
  python scripts/download_era5.py 2022 2023      # download specific years
"""

import cdsapi
import sys
import zipfile
from pathlib import Path

# -- Configuration -----------------------------------------------------------

OUTPUT_DIR = Path("data/raw/era5")
TEMP_ZIP = OUTPUT_DIR / "_temp_download.zip"

ALL_YEARS = list(range(2020, 2026))  # 2020-2025

# CWE + Nordic bounding box: [North, West, South, East]
AREA = [72, -5, 45, 32]

VARIABLES = [
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "100m_u_component_of_wind",
    "100m_v_component_of_wind",
    "surface_solar_radiation_downwards",
    "2m_temperature",
]

HOURS = [f"{h:02d}:00" for h in range(24)]
DAYS = [f"{d:02d}" for d in range(1, 32)]
DATASET = "reanalysis-era5-single-levels"


# -- Download logic ----------------------------------------------------------

def download_month(client: cdsapi.Client, year: int, month: int) -> None:
    """Download, extract, and rename ERA5 data for a single year-month."""

    # Check if already processed
    instant_file = OUTPUT_DIR / f"era5_{year}_{month:02d}_instant.nc"
    accum_file = OUTPUT_DIR / f"era5_{year}_{month:02d}_accum.nc"

    if instant_file.exists() and accum_file.exists():
        print(f"  [SKIP] {year}-{month:02d} already exists.")
        return

    print(f"  [QUEUE] Requesting {year}-{month:02d}...")

    # Download to a temp zip file
    client.retrieve(
        DATASET,
        {
            "product_type": "reanalysis",
            "variable": VARIABLES,
            "year": str(year),
            "month": f"{month:02d}",
            "day": DAYS,
            "time": HOURS,
            "area": AREA,
            "format": "netcdf",
        },
        str(TEMP_ZIP),
    )

    # Extract and rename
    if zipfile.is_zipfile(TEMP_ZIP):
        with zipfile.ZipFile(TEMP_ZIP, "r") as z:
            for name in z.namelist():
                data = z.read(name)
                if "instant" in name:
                    dest = instant_file
                elif "accum" in name:
                    dest = accum_file
                else:
                    # Unexpected file — save with generic name
                    dest = OUTPUT_DIR / f"era5_{year}_{month:02d}_{name}"
                dest.write_bytes(data)
                size_mb = len(data) / (1024 * 1024)
                print(f"         -> {dest.name} ({size_mb:.1f} MB)")
        TEMP_ZIP.unlink()
    else:
        # Not a zip — just rename directly
        TEMP_ZIP.rename(OUTPUT_DIR / f"era5_{year}_{month:02d}.nc")
        size_mb = (OUTPUT_DIR / f"era5_{year}_{month:02d}.nc").stat().st_size / (1024 * 1024)
        print(f"         -> era5_{year}_{month:02d}.nc ({size_mb:.1f} MB)")

    print(f"  [DONE] {year}-{month:02d}")


def main() -> None:
    if len(sys.argv) > 1:
        years = [int(y) for y in sys.argv[1:]]
    else:
        years = ALL_YEARS

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"ERA5 Download")
    print(f"  Years:     {years}")
    print(f"  Variables: {len(VARIABLES)}")
    print(f"  Region:    CWE + Nordic {AREA}")
    print(f"  Output:    {OUTPUT_DIR.resolve()}")
    print()

    client = cdsapi.Client()

    for year in years:
        print(f"--- {year} ---")
        for month in range(1, 13):
            download_month(client, year, month)
        print()

    print("All downloads complete.")


if __name__ == "__main__":
    main()
