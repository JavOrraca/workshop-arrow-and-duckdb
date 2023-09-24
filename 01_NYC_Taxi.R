library(arrow)
library(dplyr)
library(tictoc)

nyc_taxi <- open_dataset(here::here("data/nyc-taxi"))

glimpse(nyc_taxi)

nyc_taxi |> 
  nrow()

bnch <- bench::mark(
  min_iterations = 10,
  arrow = nyc_taxi |> 
    dplyr::group_by(year) |> 
    dplyr::summarise(all_trips = n(),
                     shared_trips = sum(passenger_count > 1, na.rm = T)) |>
    dplyr::mutate(pct_shared = shared_trips / all_trips * 100) |> 
    dplyr::collect()
)

library(ggplot2)

autoplot(bnch)

tic()
nyc_taxi |> 
  group_by(year) |> 
  summarise(all_trips = n(),
    shared_trips = sum(passenger_count > 1, na.rm = T)) |>
  mutate(pct_shared = shared_trips / all_trips * 100) |> 
  collect() 
### Custom binding request below
# |> mutate(pct_shared = scales::label_percent(accuracy = 0.1)(shared_trips / all_trips)) |>  
toc()

# longest trip distance for every month in 2019
tic()
nyc_taxi |> 
  filter(year == 2019) |> 
  group_by(month) |> 
  summarise(max_trip_dist = max(trip_distance, na.rm = T)) |> 
  arrange(month) |>
  nrow()
  
  collect()
toc()

# longest trip duration for every month in 2019
tic()
nyc_taxi |> 
  filter(year == 2019) |> 
  group_by(month) |> 
  ### Custom binding request below
  mutate(duration_minutes = as.numeric(dropoff_datetime - pickup_datetime) / 60) |> 
  arrange(month) |> 
  collect()
toc()

# How many taxi fares in the dataset had a total amount greater than $100?
glimpse(nyc_taxi)

nyc_taxi |> 
  filter(total_amount > 100) |> 
  nrow()


# How many distinct pickup locations (distinct combinations of the 
# pickup_latitude and pickup_longitude columns) are in the dataset since 2016?
  # using pickup and dropoff location IDs
nyc_taxi |> 
  filter(year >= 2016) |> 
  distinct(pickup_location_id, dropoff_location_id) |> 
  compute() |>
  nrow()

nyc_taxi |> 
  filter(year >= 2016) |> 
  # using pickup and dropoff location lat/long pairs
  distinct(pickup_latitude, pickup_longitude) |> 
  collect() |> 
  nrow()

# Adjust in diff currencies
taxi_gbp <- nyc_taxi |> 
  # Question: Why does ~.x work below but not an anonymous function syntax \(x)
  # Note on list(), below: This is the .fns arg and the output is named by 
  # combining the function name and the column name using the glue specification 
  # in .names.
  mutate(across(ends_with("amount"), list(pounds = ~.x * 0.79))) |> 
  head() |> 
  select(contains("amount")) |>
  collect()

# Look at na_if function as an error when Arrow hasn't implemented a function binding
nyc_taxi |> 
  mutate(vendor_name = na_if(vendor_name, "CMT")) |> 
  head() |> 
  collect()

nyc_taxi |> 
  mutate(vendor_name = if_else(vendor_name == "CMT", NA, vendor_name)) |> 
  head() |> 
  collect()

# Use the dplyr::filter() and stringr::str_ends() functions to return a subset 
# of the data which is a) from September 2020, and b) the value in vendor_name 
# ends with the letter “S”.
nyc_taxi |> 
  filter(year == 2020,
         month == 9,
         stringr::str_ends(vendor_name, "S")) |> 
  collect()

# Try to use the stringr function str_replace_na() to replace any NA values in 
# the vendor_name column with the string “No vendor” instead. What happens, and 
# why?
nyc_taxi |> 
  mutate(vendor_name = stringr::str_replace_na(vendor_name, "No vendor"))
# The console says the followubg for the above:
# Error: Expression stringr::str_replace_na(vendor_name, "No vendor") not supported in Arrow
# Call collect() first to pull data into R.

nyc_taxi |> 
  mutate(vendor_name = if_else(is.na(vendor_name), "No vendor", vendor_name))

# Count number of NAs
nyc_taxi |>
  summarise(across(everything(), ~sum(is.na(.)))) |> 
  collect()
