library(arrow)
library(dplyr)

seattle_csv <- open_dataset(here::here("data/seattle-library-checkouts.csv"),
                            format = "csv")

seattle_csv

schema(seattle_csv)

# If you needed to determine the estimated schema for all columns, you could run 
# the following to change out the class of a certain column:
seattle_csv$schema$code()

seattle_csv <- open_dataset(
  sources = here::here("data/seattle-library-checkouts.csv"),
  format = "csv",
  skip = 1,
  schema = schema(
    UsageClass = utf8(),
    CheckoutType = utf8(),
    MaterialType = utf8(),
    CheckoutYear = int64(),
    CheckoutMonth = int64(),
    Checkouts = int64(),
    Title = utf8(),
    ISBN = string(), #utf8() was replaced since this field is a string()
    Creator = utf8(),
    Subjects = utf8(),
    Publisher = utf8(),
    PublicationYear = utf8()
  )
)

seattle_csv |> 
  group_by(CheckoutYear) |> 
  count() |>  
  arrange(CheckoutYear) |> 
  collect() |> 
  system.time()

seattle_parquet <- here::here("data/seattle-library-checkouts-parquet")

seattle_csv |>
  write_dataset(path = seattle_parquet,
                format = "parquet")

seattle_parquet_obj <- open_dataset(seattle_parquet)

seattle_parquet_obj |> 
  group_by(CheckoutYear) |> 
  count() |> 
  arrange(CheckoutYear) |> 
  collect() |> 
  system.time()

# Write out partitioned data set
seattle_parquet_part <- here::here("data/seattle-library-checkouts")

seattle_csv |>
  group_by(CheckoutYear) |>
  write_dataset(path = seattle_parquet_part,
                format = "parquet")

seattle_parquet_part_obj <- open_dataset(seattle_parquet_part)

seattle_parquet_part_obj |> 
  group_by(CheckoutYear) |> 
  count() |> 
  arrange(CheckoutYear) |> 
  collect() |> 
  system.time()

open_dataset(here::here("data/seattle-library-checkouts")) |> 
  filter(CheckoutYear == 2019, CheckoutMonth == 9) |>
  group_by(CheckoutMonth) |>
  summarize(TotalCheckouts = sum(Checkouts, na.rm = T)) |>
  arrange(desc(CheckoutMonth)) |>
  collect() |> 
  system.time()

# use the read_parquet() function
parquet_file <- here::here("data/nyc-taxi/year=2019/month=9/part-0.parquet")

taxi_df <- read_parquet(parquet_file) |> 
  tibble()

object.size(taxi_df)
