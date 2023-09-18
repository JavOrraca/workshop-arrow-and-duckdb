library(arrow)
library(testthat)

# Define Custom R Functions -----------------------------------------------

custom_scales_pct <- function(x, accuracy = 0.1) {
  scales::label_percent(accuracy = accuracy)(x)
}

custom_str_replace_na <- function(x, y) {
  stringr::str_replace_na(x, y)
}



# Perform Unit Tests ------------------------------------------------------

test_that("custom_scales_pct behaves identically in dplyr and Arrow", {
  df <- tibble(x = c(0.1, 0.2, 0.75, 0.9))
  compare_dplyr_binding(
    .input |> 
      filter(custom_scales_pct(x, "b")) |> 
      collect(),
    df
  )
})



# Map R Function to C++ Kernel --------------------------------------------

arrow::register_binding(
  fun_name = "arrow::custom_scales_pct", 
  fun = function(x, ...) {
    Expression$create(
      "custom_scales_pct",
      x,
      options = list(...)
    )
})








test_that("startsWith behaves identically in dplyr and Arrow", {
  df <- tibble(x = c("Foo", "bar", "baz", "qux"))
  compare_dplyr_binding(
    .input %>%
      filter(startsWith(x, "b")) %>%
      collect(),
    df
  )
})

reprex::reprex({
  library(testthat)
  library(dplyr)
  library(arrow)
  test_that("startsWith behaves identically in dplyr and Arrow", {
    df <- tibble(x = c("Foo", "bar", "baz", "qux"))
    compare_dplyr_binding(
      .input %>%
        filter(startsWith(x, "b")) %>%
        collect(),
      df
    )
  })
})
