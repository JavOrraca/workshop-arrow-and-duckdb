library(arrow)
library(dplyr)
library(stringr)
library(tictoc)

nyc_taxi <- open_dataset("data/nyc-taxi")

time_diff_minutes <- function(dropoff, pickup) {
  difftime(dropoff, pickup, units = "mins") |> 
    round() |> 
    as.integer()
}

nyc_taxi |> 
  mutate(duration_minutes = time_diff_minutes(dropoff_datetime, pickup_datetime)) |> 
  select(pickup_datetime, dropoff_datetime, duration_minutes) |> 
  head() |> 
  collect()

register_scalar_function(
  name = "time_diff_minutes",
  # Note: the first argument must always be context
  function(context, dropoff, pickup) {
    difftime(dropoff, pickup, units = "mins") |> 
      round() |> 
      as.integer()
  },
  in_type = schema(
    pickup = timestamp(unit = "ms"),
    dropoff = timestamp(unit = "ms")
  ),
  out_type = int32(),
  auto_convert = T
)

nyc_taxi |> 
  mutate(duration_minutes = time_diff_minutes(dropoff_datetime, pickup_datetime)) |> 
  select(pickup_datetime, dropoff_datetime, duration_minutes) |> 
  head() |> 
  collect()

# Now, try a user-defined function to wrap str_replace_na

nyc_taxi |> 
  distinct(vendor_name) |> 
  collect()

replace_arrow_nas <- function(x, replacement) {
  stringr::str_replace_na(x, replacement)
}

register_scalar_function(
  name = "replace_arrow_nas",
  # Note: the first argument must always be context
  function(context, x, replacement) {
    stringr::str_replace_na(x, replacement)
  },
  in_type = schema(
    x = string(),
    replacement = string()
  ),
  out_type = string(),
  auto_convert = T
)

nyc_taxi |> 
  filter(is.na(vendor_name)) |> 
  mutate(vendor_name = replace_arrow_nas(vendor_name, "No vendor")) |> 
  distinct(vendor_name) |> 
  head() |> 
  collect()

# Joining a reference table
vendors <- tibble::tibble(
  code = c("VTS", "CMT", "DDS"),
  full_name = c(
    "Verifone Transportation Systems",
    "Creative Mobile Technologies",
    "Digital Dispatch Systems"
  )
)

# Joining 
nyc_taxi |>
  left_join(vendors, by = c("vendor_name" = "code")) |>
  select(vendor_name, full_name, pickup_datetime) |>
  head(3) |>
  collect()

# Now try another example of joining and troubleshoot the complexities
nyc_taxi_zones <- 
  read_csv_arrow(here::here("data/taxi_zone_lookup.csv")) |>
  select(location_id = LocationID,
         borough = Borough)

# Troubleshoot Joining Complexities ---------------------------------------

nyc_taxi_zones

nyc_taxi |>
  left_join(nyc_taxi_zones, by = c("pickup_location_id" = "location_id")) |>
  collect()

arrow::schema(nyc_taxi)

nyc_taxi_zones_arrow <- arrow_table(nyc_taxi_zones)

# Review schema of the taxi zones
schema(nyc_taxi_zones_arrow)

# Change the schema types
nyc_taxi_zones_arrow <- arrow_table(
  nyc_taxi_zones, 
  schema = schema(location_id = int64(), borough = utf8())
)

# Prepare the auxiliary tables
pickup <- nyc_taxi_zones_arrow |>
  select(pickup_location_id = location_id,
         pickup_borough = borough)

dropoff <- nyc_taxi_zones_arrow |>
  select(dropoff_location_id = location_id,
         dropoff_borough = borough)

# Join and cross-tabulate
### Note: 2-3 minutes to join twice and cross-tabulate on non-partition 
###       variables, with 1.15 billion rows of data 🙂
tic()
borough_counts <- nyc_taxi |> 
  left_join(pickup) |>
  left_join(dropoff) |>
  count(pickup_borough, dropoff_borough) |>
  arrange(desc(n)) |>
  collect()
toc()

View(borough_counts)

# Exercise
### How many taxi pickups were recorded in 2019 from the three major airports 
### covered by the NYC Taxis data set (JFK, LaGuardia, Newark)? Hint: you can 
### use stringr::str_detect() to help you find pickup zones with the word 
### “Airport” in them.
pickup_location <- read_csv_arrow(here::here("data/taxi_zone_lookup.csv"))

pickup_location <- pickup_location |>
  select(
    pickup_location_id = LocationID,
    borough = Borough,
    pickup_zone = Zone
  ) 

pickup_location_arrow <- arrow_table(
  pickup_location, 
  schema = schema(
    pickup_location_id = int64(),
    borough = utf8(),
    pickup_zone = utf8()
  ))

nyc_taxi |>
  filter(year == 2019) |>
  left_join(pickup_location_arrow) |>
  filter(str_detect(pickup_zone, "Airport")) |>
  count(pickup_zone) |>
  collect()
