# Renewable Variability & Electricity Price Risk Decomposition

A four-stage quantitative case study analysing how renewable generation 
variability drives day-ahead electricity price risk in Central Western Europe.
Built as a Quarto portfolio project targeting energy trading roles in the 
CWE/Nordic markets.

**Live site:** https://tdans2012.github.io/Hedging-Analysis/

---

## Analytical Question

How much of the variance in German day-ahead electricity prices can be 
attributed to renewable generation variability, and what does this imply 
for hedging demand across bidding zones?

---

## Structure

| Notebook | Topic |
|---|---|
| `01_data.qmd` | Data sources & QA — SMARD, ENTSO-E, ERA5, TTF |
| `02_weather.qmd` | Weather → generation capacity factor modelling |
| `03_prices.qmd` | Price decomposition — elastic net, variance attribution |
| `04_risk.qmd` | Monte Carlo revenue risk & hedging implications |

---

## Data Sources

- **SMARD (Bundesnetzagentur)** — DE-LU hourly generation, load, day-ahead price
- **ENTSO-E Transparency Platform** — day-ahead prices (17 zones), cross-border 
  flows, generation by type
- **Copernicus ERA5** — hourly wind speed, solar irradiance, temperature 
  (spatial averages per bidding zone)
- **Yahoo Finance** — TTF natural gas front-month futures (via tidyquant)

---

## Stack

**R:** tidyverse, tidymodels, arrow, plotly, gt, httr2, tidyquant, imputeTS  
**Python:** entsoe-py, cdsapi, xarray, geopandas, pyarrow  
**Publishing:** Quarto → GitHub Pages

---

## Reproducing the Analysis

### R environment
```r
renv::restore()
```

### Python environment
```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### Data ingestion
Python ingestion scripts are in `scripts/` and must be run from the terminal 
(not via RStudio) due to a DLL conflict between reticulate and pyarrow on 
Windows:
```bash
python scripts/entsoe_api.py
python scripts/entsoe_load_zones.py
```

ERA5 data requires a [Copernicus CDS API key](https://cds.climate.copernicus.eu/).  
ENTSO-E data requires an [ENTSO-E Transparency Platform API key](https://transparency.entsoe.eu/).  
Store both in a project-level `.Renviron` file (not committed to the repo).

### Rendering
```bash
quarto render
```

Pre-rendered output is committed to `docs/` and served via GitHub Actions.

---

## Data Attribution

- SMARD: Bundesnetzagentur | SMARD.de, CC BY 4.0
- ENTSO-E Transparency Platform data used per ENTSO-E terms of service
- TTF gas price: Yahoo Finance (ICE Dutch TTF front-month futures)
- ERA5: Hersbach et al. (2020), doi:10.24381/cds.adbb2d47