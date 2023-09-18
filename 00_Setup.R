# 0. Load Libraries to Download Data --------------------------------------

library(here)
library(arrow)
library(dplyr)


# 1. NYC Taxi Data download (40 GB) ---------------------------------------

data_path <- here::here("data/nyc-taxi")

open_dataset("s3://voltrondata-labs-datasets/nyc-taxi") |>
  filter(year %in% 2012:2021) |> 
  write_dataset(data_path, partitioning = c("year", "month"))

# If downloaded correctly, check number of rows
open_dataset(data_path) |>
  nrow()

# 2. Seattle Checkouts by Title Data (9 GB) -------------------------------

options(timeout = 1800)
download.file(
  url = "https://r4ds.s3.us-west-2.amazonaws.com/seattle-library-checkouts.csv",
  destfile = here::here("data/seattle-library-checkouts.csv")
)


# 3. Taxi Zone Lookups & Shapefiles ---------------------------------------

options(timeout = 1800)
download.file(
  url = "https://github.com/posit-conf-2023/arrow/releases/download/v0.1.0/taxi_zone_lookup.csv",
  destfile = here::here("data/taxi_zone_lookup.csv")
)

download.file(
  url = "https://github.com/posit-conf-2023/arrow/releases/download/v0.1.0/taxi_zones.zip",
  destfile = here::here("data/taxi_zones.zip")
)

# Extract the spatial files from the zip folder:
unzip(
  zipfile = here::here("data/taxi_zones.zip"), 
  exdir = here::here("data/taxi_zones")
)
