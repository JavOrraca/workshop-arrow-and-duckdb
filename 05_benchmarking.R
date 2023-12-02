
# Load Libraries ----------------------------------------------------------

require(here)
library(arrow)
library(dplyr)
library(stringr)
library(tictoc)
library(duckplyr)
library(polars)
library(ggplot2)


# Download Data -----------------------------------------------------------

# Download 40GB (1.1 billion rows) of NYC Taxi rides
# NOTE: This may take several hours
data_path <- here::here("data/nyc-taxi")

open_dataset("s3://voltrondata-labs-datasets/nyc-taxi") |>
  filter(year %in% 2012:2021) |>
  write_dataset(data_path, partitioning = c("year", "month"))


# Subset & Benchmark ------------------------------------------------------

# Manually iterated over the code below to benchmark
# and compare performance on 1 million, 10 million, 100
# million, and 500 million rows
nyc_taxi_tibble <- open_dataset("data/nyc-taxi") |> 
  dplyr::select(year, passenger_count) |>
  dplyr::collect() |> 
  dplyr::slice_sample(n = 1000000)

nyc_taxi <- nyc_taxi_tibble |>
  arrow::as_arrow_table()

nyc_taxi_duckplyr_df <- nyc_taxi_tibble |>
  duckplyr::as_duckplyr_df()

nyc_taxi_polars <- pl$DataFrame(nyc_taxi_tibble)$lazy()

tic()
bnch <- bench::mark(
  min_iterations = 50,
  tibble_to_arrow = nyc_taxi_tibble |>
    arrow::as_arrow_table() |>
    dplyr::group_by(year) |>
    dplyr::summarise(all_trips = n(),
                     shared_trips = sum(passenger_count > 1, na.rm = T)) |>
    dplyr::mutate(pct_shared = shared_trips / all_trips * 100) |>
    dplyr::collect(),
  tibble_to_duckplyr = nyc_taxi_tibble |>
    duckplyr::as_duckplyr_df() |>
    duckplyr::mutate(all_trips = n(), .by = year) |>
    duckplyr::filter(passenger_count > 1) |>
    duckplyr::summarise(shared_trips = n(),
                        .by = c(year, all_trips)) |>
    duckplyr::mutate(pct_shared = shared_trips / all_trips * 100) |>
    duckplyr::collect(),
  tibble_to_dplyr = nyc_taxi_tibble |>
    dplyr::group_by(year) |>
    dplyr::summarise(all_trips = n(),
                     shared_trips = sum(passenger_count > 1, na.rm = T)) |>
    dplyr::mutate(pct_shared = shared_trips / all_trips * 100),
  arrow_table = nyc_taxi |>
    dplyr::group_by(year) |>
    dplyr::summarise(all_trips = n(),
                     shared_trips = sum(passenger_count > 1, na.rm = T)) |>
    dplyr::mutate(pct_shared = shared_trips / all_trips * 100) |>
    dplyr::collect(),
  arrow_to_duckdb = nyc_taxi |>
    arrow::to_duckdb() |>
    dplyr::mutate(all_trips = n(), .by = year) |>
    dplyr::filter(passenger_count > 1) |>
    dplyr::group_by(year, all_trips) |>
    dplyr::summarise(shared_trips = n(),
                     .groups = "drop") |>
    dplyr::mutate(pct_shared = shared_trips / all_trips * 100) |>
    dplyr::collect(),
  duckplyr_df = nyc_taxi_duckplyr_df |>
    duckplyr::mutate(all_trips = n(), .by = year) |>
    duckplyr::filter(passenger_count > 1) |>
    duckplyr::summarise(shared_trips = n(),
                        .by = c(year, all_trips)) |>
    duckplyr::mutate(pct_shared = shared_trips / all_trips * 100) |>
    duckplyr::collect(),
  polars = nyc_taxi_polars$
    select(pl$col(c("year", "passenger_count")))$
    with_columns(
      pl$count()$
        over("year")$
        alias("all_trips")
    )$
    filter(pl$col("passenger_count") > 1)$
    group_by(c("year", "all_trips"))$
    agg(
      pl$count()$alias("shared_trips")
    )$
    collect()$
    to_data_frame() |>
    mutate(pct_shared = shared_trips / all_trips * 100),
  check = FALSE
)
toc()

autoplot(bnch)


# Session Info ------------------------------------------------------------

sessionInfo()
