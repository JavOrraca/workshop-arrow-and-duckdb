library(arrow)
library(dplyr)
library(stringr)
library(tictoc)
library(duckplyr)
library(ggplot2)

nyc_taxi_tibble <- open_dataset("data/nyc-taxi") |> 
  dplyr::select(year, passenger_count) |>
  dplyr::collect() |> 
  dplyr::slice_sample(n = 500000000)

nyc_taxi <- nyc_taxi_tibble |>
  arrow::as_arrow_table()

nyc_taxi_duckplyr_df <- nyc_taxi_tibble |> 
  duckplyr::as_duckplyr_df()

tic()
bnch <- bench::mark(
  min_iterations = 50,
  tibble_to_arrow = nyc_taxi_tibble |> 
    arrow::as_arrow_table() |> 
    dplyr::filter(passenger_count > 1) |> 
    dplyr::group_by(year) |> 
    dplyr::summarise(
      all_trips = n(),
      shared_trips = sum(passenger_count, na.rm = T)
    ) |>
    dplyr::mutate(pct_shared = shared_trips / all_trips * 100) |> 
    dplyr::collect(),
  tibble_to_duckplyr = nyc_taxi_tibble |> 
    duckplyr::as_duckplyr_df() |> 
    duckplyr::filter(passenger_count > 1) |> 
    duckplyr::summarise(
      all_trips = n(),
      shared_trips = sum(passenger_count, na.rm = T),
      .by = year
      ) |>
    duckplyr::mutate(pct_shared = shared_trips / all_trips * 100),
  tibble_to_dplyr = nyc_taxi_tibble |> 
    dplyr::filter(passenger_count > 1) |> 
    dplyr::group_by(year) |> 
    dplyr::summarise(all_trips = n(),
                     shared_trips = sum(passenger_count, na.rm = T)) |>
    dplyr::mutate(pct_shared = shared_trips / all_trips * 100),
  arrow_table = nyc_taxi |> 
    dplyr::filter(passenger_count > 1) |> 
    dplyr::group_by(year) |> 
    dplyr::summarise(
      all_trips = n(),
      shared_trips = sum(passenger_count, na.rm = T)
    ) |>
    dplyr::mutate(pct_shared = shared_trips / all_trips * 100) |> 
    dplyr::collect(),
  arrow_to_duckdb = nyc_taxi |> 
    arrow::to_duckdb() |> 
    dplyr::filter(passenger_count > 1) |> 
    dplyr::group_by(year) |> 
    dplyr::summarise(
      all_trips = n(),
      shared_trips = sum(passenger_count, na.rm = T)
    ) |>
    dplyr::mutate(pct_shared = shared_trips / all_trips * 100) |> 
    dplyr::collect(),
  duckplyr_df = nyc_taxi_duckplyr_df |> 
    duckplyr::filter(passenger_count > 1) |> 
    duckplyr::summarise(
      all_trips = n(),
      shared_trips = sum(passenger_count, na.rm = T),
      .by = year
    ) |>
    duckplyr::mutate(pct_shared = shared_trips / all_trips * 100),
  check = FALSE
)
toc()

autoplot(bnch)

