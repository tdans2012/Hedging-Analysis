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
#   smard_fetch_all()                          # fetch all filters, 2020-2024
#   smard_fetch_all(years = 2023:2024)         # narrower range
# ============================================================================

library(tidyverse)
library(httr2)
library(arrow)

# -- Configuration -----------------------------------------------------------

SMARD_BASE <- "https://www.smard.de/app"

# Filters relevant to the analysis
SMARD_FILTERS <- tribble(
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
  4169L,      "price_de_lu",        "price",
)

SMARD_REGION     <- "DE-LU"
SMARD_RESOLUTION <- "hour"

OUTPUT_DIR <- "data/processed/smard"


# -- Helper: fetch index timestamps ------------------------------------------

smard_get_index <- function(filter_id,
                            region     = SMARD_REGION,
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
                            region     = SMARD_REGION,
                            resolution = SMARD_RESOLUTION) {
#  url <- glue::glue(
#    "{SMARD_BASE}/chart_data/{filter_id}/{region}/",
#    "{filter_id}_{region}_{resolution}_{timestamp}.json"
#  )


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
                               years       = 2020:2024,
                               region      = SMARD_REGION,
                               resolution  = SMARD_RESOLUTION) {
  
  cli::cli_alert_info("Fetching index for {filter_name} ({filter_id})...")
  
  timestamps <- smard_get_index(filter_id, region, resolution)
  
  # Filter to requested year range
  # Timestamps are ms since epoch — convert to year for filtering
  ts_years <- as.integer(
    format(as.POSIXct(timestamps / 1000, origin = "1970-01-01", tz = "UTC"), "%Y")
  )
  timestamps <- timestamps[ts_years >= min(years) & ts_years <= max(years)]
  
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


# -- Main: fetch all filters and join into a single table --------------------

smard_fetch_all <- function(years  = 2020:2024,
                            region = SMARD_REGION) {
  
  dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
  
  cli::cli_h1("SMARD Data Ingestion")
  cli::cli_alert_info("Region: {region}")
  cli::cli_alert_info("Years:  {min(years)}-{max(years)}")
  cli::cli_alert_info("Filters: {nrow(SMARD_FILTERS)}")
  
  # Fetch each filter as a separate tibble
  results <- pmap(
    SMARD_FILTERS,
    \(filter_id, name, category) {
      smard_fetch_filter(
        filter_id   = filter_id,
        filter_name = name,
        years       = years,
        region      = region
      )
    }
  )
  
  # Join all series on datetime_utc
  combined <- results |>
    reduce(full_join, by = "datetime_utc") |>
    arrange(datetime_utc)
  
  # Quick summary
  cli::cli_h2("Summary")
  cli::cli_alert_success("Rows: {nrow(combined)}")
  cli::cli_alert_success("Date range: {min(combined$datetime_utc)} to {max(combined$datetime_utc)}")
  cli::cli_alert_success(
    "Missing values per column:\n{paste(
      names(combined),
      map_int(combined, ~sum(is.na(.x))),
      sep = ': ', collapse = '\n'
    )}"
  )
  
  # Write parquet
  output_path <- file.path(OUTPUT_DIR, glue::glue("smard_{region}_{min(years)}_{max(years)}.parquet"))
  
  write_parquet(combined, output_path)
  cli::cli_alert_success("Written to {output_path}")
  
  invisible(combined)
}