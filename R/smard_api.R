# smard_api.R
# ============================================================================
# Pulls hourly generation, price, and load data from the SMARD API
# (Bundesnetzagentur) and writes cleaned parquet files to data/processed/.
#
# API docs: https://smard.api.bund.dev/openapi.yaml
# Base URL: https://www.smard.de/app
#
# Pattern:
#   1. GET index endpoint  -> list of available timestamps (week boundaries)
#   2. GET data endpoint   -> time series for each timestamp chunk
#   3. Bind, clean, write parquet
#
# Usage:
#   source("R/smard_api.R")
#
#   # Fetch a single zone (backwards-compatible)
#   smard_fetch_all()                          # DE-LU, 2020-2024
#   smard_fetch_all(years = 2023:2024)         # narrower range
#   smard_fetch_all(region = "NL")             # single non-default zone
#
#   # Fetch all configured zones
#   smard_fetch_all_zones()                    # all 9 zones, 2020-2024
#   smard_fetch_all_zones(years = 2023:2024)   # narrower range
#
#   # Fetch a subset of zones
#   smard_fetch_all_zones(zones = c("NL", "BE", "FR"))
# ============================================================================

library(tidyverse)
library(httr2)
library(arrow)

# -- Configuration -----------------------------------------------------------

SMARD_BASE       <- "https://www.smard.de/app"
SMARD_RESOLUTION <- "hour"
OUTPUT_DIR       <- "data/processed/smard"

# ── Zone configuration ──────────────────────────────────────────────────────
# Maps project bidding zones to SMARD API region codes.
#
# Nordic notes:
#   SMARD splits DK, NO, SE into sub-zones (DK1/DK2, NO1-NO5, SE1-SE4).
#   We use the largest / most representative sub-zone as default.
#   Adjust smard_region below if your analysis targets a different sub-zone.

SMARD_ZONES <- tribble(
  ~bidding_zone,  ~smard_region,
  "DE_LU",        "DE-LU",
  "FR",           "FR",
  "NL",           "NL",
  "BE",           "BE",
  "AT",           "AT",
  "DK",           "DK1",       # DK1 = West Denmark (Jylland/Fyn)
  "NO",           "NO2",       # NO2 = Southern Norway (largest by load)
  "SE",           "SE4",       # SE4 = Southern Sweden (Malmö region)
  "FI",           "FI",
)


# ── Filter IDs ──────────────────────────────────────────────────────────────
# Generation and consumption filters use the same IDs across all SMARD
# regions — only the region parameter in the URL changes.
# Price filter IDs are region-specific.

SMARD_FILTERS_BASE <- tribble(
  ~filter_id, ~name,                ~category,
  4067L,      "wind_onshore",       "generation",
  1225L,      "wind_offshore",      "generation",
  4068L,      "solar",              "generation",
  4066L,      "biomass",            "generation",
  4071L,      "gas",                "generation",
  4069L,      "hard_coal",          "generation",
  1223L,      "lignite",            "generation",
  1224L,      "nuclear",            "generation",
  410L,       "total_load",         "consumption",
)

# Price filter IDs per SMARD region
# Source: https://smard.api.bund.dev — verify if any return 404
SMARD_PRICE_FILTERS <- tribble(
  ~smard_region,  ~filter_id, ~name,     ~category,
  "DE-LU",        4169L,      "price",   "price",
  "FR",           4045L,      "price",   "price",
  "NL",           4176L,      "price",   "price",
  "BE",           5078L,      "price",   "price",
  "AT",           4170L,      "price",   "price",
  "DK1",          252L,       "price",   "price",
  "DK2",          253L,       "price",   "price",
  "NO2",          260L,       "price",   "price",
  "SE4",          267L,       "price",   "price",
  "FI",           268L,       "price",   "price",
)


# -- Helper: fetch index timestamps ------------------------------------------

smard_get_index <- function(filter_id,
                            region,
                            resolution = SMARD_RESOLUTION) {
  url <- glue::glue(
    "{SMARD_BASE}/chart_data/{filter_id}/{region}/index_{resolution}.json"
  )
  
  resp <- request(url) |>
    req_retry(max_tries = 3, backoff = ~2) |>
    req_perform()
  
  resp |>
    resp_body_json() |>
    pluck("timestamps") |>
    unlist()
}


# -- Helper: fetch one chunk of time series data -----------------------------

smard_get_chunk <- function(filter_id,
                            timestamp,
                            region,
                            resolution = SMARD_RESOLUTION) {
  
  url <- glue::glue(
    "{SMARD_BASE}/chart_data/{filter_id}/{region}/",
    "{filter_id}_{region}_{resolution}_{format(timestamp, scientific = FALSE)}.json"
  )
  
  # Some index timestamps don't have data — return NULL on 404
  resp <- tryCatch(
    request(url) |>
      req_retry(max_tries = 3, backoff = ~2) |>
      req_perform(),
    error = function(e) {
      if (grepl("404", conditionMessage(e))) {
        return(NULL)
      }
      stop(e)  # re-throw non-404 errors
    }
  )
  
  if (is.null(resp)) return(NULL)
  
  raw <- resp |> resp_body_json()
  
  # series is a list of [timestamp_ms, value] pairs
  tibble(
    timestamp_ms = map_dbl(raw$series, ~ .x[[1]]),
    value        = map_dbl(raw$series, ~ .x[[2]] %||% NA_real_)
  )
}


# -- Core: fetch full time series for one filter -----------------------------

smard_fetch_filter <- function(filter_id,
                               filter_name,
                               years,
                               region,
                               resolution = SMARD_RESOLUTION) {
  
  cli::cli_alert_info("Fetching index for {filter_name} ({filter_id})...")
  
  # If the index call itself 404s, the filter doesn't exist for this region
  timestamps <- tryCatch(
    smard_get_index(filter_id, region, resolution),
    error = function(e) {
      if (grepl("404", conditionMessage(e))) {
        cli::cli_alert_warning(
          "  Filter {filter_name} ({filter_id}) not available for {region} — skipping"
        )
        return(NULL)
      }
      stop(e)
    }
  )
  
  if (is.null(timestamps)) return(NULL)
  
  # Filter to requested year range
  ts_years <- as.integer(
    format(as.POSIXct(timestamps / 1000, origin = "1970-01-01", tz = "UTC"), "%Y")
  )
  timestamps <- timestamps[ts_years >= min(years) & ts_years <= max(years)]
  
  if (length(timestamps) == 0) {
    cli::cli_alert_warning(
      "  No data for {filter_name} in {min(years)}-{max(years)} — skipping"
    )
    return(NULL)
  }
  
  cli::cli_alert_info(
    "  {length(timestamps)} chunks to fetch for {min(years)}-{max(years)}"
  )
  
  # Fetch all chunks with progress (NULLs from 404s are dropped)
  series <- map(
    timestamps,
    \(ts) smard_get_chunk(filter_id, ts, region, resolution),
    .progress = glue::glue("  {filter_name}")
  ) |>
    compact() |>
    list_rbind()
  
  if (nrow(series) == 0) {
    cli::cli_alert_warning("  No rows returned for {filter_name} — skipping")
    return(NULL)
  }
  
  # Clean up
  series |>
    mutate(
      datetime_utc = as.POSIXct(timestamp_ms / 1000,
                                origin = "1970-01-01",
                                tz = "UTC")
    ) |>
    select(datetime_utc, value) |>
    rename(!!filter_name := value) |>
    distinct(datetime_utc, .keep_all = TRUE) |>
    arrange(datetime_utc)
}


# -- Fetch all filters for a single zone ------------------------------------

smard_fetch_all <- function(years  = 2020:2024,
                            region = "DE-LU") {
  
  dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
  
  # Build filter table: base filters + zone-specific price filter
  price_row <- SMARD_PRICE_FILTERS |> filter(smard_region == region)
  
  filters <- SMARD_FILTERS_BASE
  if (nrow(price_row) == 1) {
    filters <- bind_rows(filters, price_row |> select(filter_id, name, category))
  } else {
    cli::cli_alert_warning("No price filter configured for {region} — skipping price")
  }
  
  cli::cli_h1("SMARD Data Ingestion")
  cli::cli_alert_info("Region: {region}")
  cli::cli_alert_info("Years:  {min(years)}-{max(years)}")
  cli::cli_alert_info("Filters: {nrow(filters)}")
  
  # Fetch each filter as a separate tibble (NULLs for unavailable filters)
  results <- pmap(
    filters,
    \(filter_id, name, category) {
      smard_fetch_filter(
        filter_id   = filter_id,
        filter_name = name,
        years       = years,
        region      = region
      )
    }
  ) |>
    compact()
  
  if (length(results) == 0) {
    cli::cli_alert_danger("No data returned for {region} — nothing to write")
    return(invisible(NULL))
  }
  
  # Join all series on datetime_utc
  combined <- results |>
    reduce(full_join, by = "datetime_utc") |>
    arrange(datetime_utc)
  
  # Quick summary
  cli::cli_h2("Summary")
  cli::cli_alert_success("Rows: {nrow(combined)}")
  cli::cli_alert_success(
    "Date range: {min(combined$datetime_utc)} to {max(combined$datetime_utc)}"
  )
  cli::cli_alert_success(
    "Missing values per column:\n{paste(
      names(combined),
      map_int(combined, ~sum(is.na(.x))),
      sep = ': ', collapse = '\n'
    )}"
  )
  
  # Write parquet
  output_path <- file.path(
    OUTPUT_DIR,
    glue::glue("smard_{region}_{min(years)}_{max(years)}.parquet")
  )
  
  write_parquet(combined, output_path)
  cli::cli_alert_success("Written to {output_path}")
  
  invisible(combined)
}


# -- Orchestrator: iterate across all zones ----------------------------------

smard_fetch_all_zones <- function(zones = SMARD_ZONES$bidding_zone,
                                  years = 2020:2024) {
  
  # Resolve bidding zone names to SMARD region codes
  zone_cfg <- SMARD_ZONES |> filter(bidding_zone %in% zones)
  
  unmatched <- setdiff(zones, zone_cfg$bidding_zone)
  if (length(unmatched) > 0) {
    cli::cli_alert_danger(
      "Unknown bidding zone(s): {paste(unmatched, collapse = ', ')}. ",
      "Add them to SMARD_ZONES first."
    )
  }
  
  cli::cli_h1("SMARD Multi-Zone Ingestion")
  cli::cli_alert_info("Zones: {paste(zone_cfg$bidding_zone, collapse = ', ')}")
  cli::cli_alert_info("SMARD regions: {paste(zone_cfg$smard_region, collapse = ', ')}")
  cli::cli_alert_info("Years: {min(years)}-{max(years)}")
  
  results <- list()
  
  for (i in seq_len(nrow(zone_cfg))) {
    bz     <- zone_cfg$bidding_zone[i]
    region <- zone_cfg$smard_region[i]
    
    cli::cli_rule()
    cli::cli_h2("Zone {i}/{nrow(zone_cfg)}: {bz} (SMARD region: {region})")
    
    results[[bz]] <- tryCatch(
      smard_fetch_all(years = years, region = region),
      error = function(e) {
        cli::cli_alert_danger("Failed for {bz}: {conditionMessage(e)}")
        NULL
      }
    )
  }
  
  # Report
  cli::cli_rule()
  cli::cli_h1("Ingestion complete")
  ok   <- names(results)[!map_lgl(results, is.null)]
  fail <- names(results)[map_lgl(results, is.null)]
  if (length(ok) > 0)   cli::cli_alert_success("Succeeded: {paste(ok, collapse = ', ')}")
  if (length(fail) > 0) cli::cli_alert_danger("Failed:    {paste(fail, collapse = ', ')}")
  
  invisible(results)
}