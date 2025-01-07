#---- --- ---- --- ---- --- ---- --- ----#
# Load libraries
library(arrow)   # for creating, reading,
                 # writing parquet files
library(dplyr)   # for data wrangling
library(tidyr)   # for more data wrangling
library(here)    # for working with dir/files
library(readr)   # for reading csv files)
library(stringr) # for string manipulation
#---- --- ---- --- ---- --- ---- --- ----#

atlas_path <- here::here(".data","DiabetesAtlasData.csv")
atlas_data <- read_csv(atlas_path,
                       col_types = cols(.default = "c")) |>
  slice(-3076) # remove last row containing site info

# Checking class of each column in data
peek(atlas_data)

# Just learned `\()` this is short-hand for anonymous lambda function
atlas_data <- atlas_data |>
  rename_with(\(x) x |>
                tolower() |>
                str_replace_all(" ", "_") |>
                str_replace_all("[()]", "")) |>
  mutate(across(
    c(year,
      diagnosed_diabetes_percentage,
      obesity_percentage),
    as.numeric
  ))

tf1 <- tempfile(tmpdir = "data",fileext = ".parquet")
write_dataset(atlas_data, tf1)

#---- --- ---- --- ---- --- ---- --- ----#
# US Chronic Disease Indicators CDI 2023 Release
chronic_path <- here::here(".data","USCDI2023.csv")
chronic_data_messy <- read_csv(chronic_path,
                       col_types = cols(.default = "c"))

peek(chronic_data_messy)

count_na(chronic_data_messy) # [1] 10626701 NAs

na_count_df <- chronic_data_messy |>
  summarise(across(everything(), ~count_na(.))) |>
  pivot_longer(cols = everything(),
               names_to = "column",
               values_to = "na_count") |>
  filter(na_count > 0)

cols <- na_count_df$column[na_count_df$na_count ==1185676]

# A tibble: 14 × 2
# column                  na_count
# <chr>                      <int>
#   1 Response                 1185676
# 2 DataValueUnit             152123
# 3 DataValue                 378734
# 4 DataValueAlt              381098
# 5 DataValueFootnoteSymbol   791966
# 6 DatavalueFootnote         791966
# 7 LowConfidenceLimit        503296
# 8 HighConfidenceLimit       503296
# 9 StratificationCategory2  1185676
# 10 Stratification2          1185676
# 11 StratificationCategory3  1185676
# 12 Stratification3          1185676
# 13 GeoLocation                10166
# 14 ResponseID               1185676

chronic_data <- chronic_data_messy |>
  select(-c(cols,TopicID,
            LocationID,
            QuestionID,
            DataValueTypeID,
            DataValueFootnoteSymbol,
            StratificationCategoryID1,
            StratificationID1)) |>
  rename_with(tolower) |>
  rename(
    year_start = yearstart,
    year_end = yearend,
    loc = locationabbr,
    loc_name = locationdesc,
    source = datasource,
    data_units = datavalueunit,
    data_type = datavaluetype,
    value = datavalue,
    alt_value = datavaluealt,
    footnote = datavaluefootnote,
    lower_ci = lowconfidencelimit,
    upper_ci = highconfidencelimit,
    strata_cat = stratificationcategory1,
    strata = stratification1
  ) |>
  mutate(geolocation = str_remove_all(geolocation,
                                      "POINT \\(|\\)")) |>
  separate(geolocation,
           into = c("longitude", "latitude"),
           sep = " ",
           convert = TRUE)

peek(chronic_data)

tf2 <- tempfile(tmpdir = "data",fileext = ".parquet")
write_dataset(chronic_data, tf2)
