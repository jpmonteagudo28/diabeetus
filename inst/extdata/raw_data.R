# DO NOT RUN THIS SCRIPT
# THE SCRIPT IS FOR CREATING DATASETS
# AVAILABLE IN THE DIABEETUS PACKAGE
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

#---- --- ---- --- ---- --- ---- --- ----#
# Diabetes mellitus and treatment
diabetes_mellitus <- merge_into_frame("Diabetes-Data",starts_with = "data-") |>
  dplyr::rename(date = V1,
         time = V2,
         measurement = V3,
         value = V4) |>
  dplyr::mutate(value = as.numeric(value),
                measurement = factor(measurement))

peek(diabetes_mellitus)

# Unknown values(5) in the data
# [1] "0Hi" "0Lo" ""  "3A" "0''"

is.na((as.numeric(diabetes_mellitus$value))) -> x
unique(subset(diabetes_mellitus,x,select = measurement))
# Measurements for which there are missing values
#         measurement
# 1113           60
# 1147           62
# 1165           48
# 9961            0
# 9962           33
# 16467          57

# Getting the total rows per data file
root_dir <- getwd()
folder <- as.character("Diabetes-Data")
dirs <- omit_folders(root_dir)
target_dirs <- grep(paste0("/", folder, "$"), dirs, value = TRUE)

files <- list.files(target_dirs, full.names = TRUE)
row_counts <- numeric(length(files))
for (i in seq_along(files)) {
  row_counts[i] <- nrow(read.delim(files[i], header = FALSE))
}
file_row_counts <- data.frame(
  file = basename(files), # Get only the file names, not full paths
  rows = row_counts
)
print(file_row_counts)

participants <- paste0("P_", sprintf("%02d", 1:70))
data_rows <- file_row_counts$rows[1:70]
participant_labels <- rep(participants, times = data_rows)
# Add a new column to diabetes_mellitus
diabetes_mellitus$participant <- participant_labels

tf3 <- tempfile(tmpdir = "data",fileext = ".parquet")
write_dataset(diabetes_mellitus, tf3)
