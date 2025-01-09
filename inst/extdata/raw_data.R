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

write_dataset(chronic_data, tf1)

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


write_dataset(diabetes_mellitus, tf1)

#---- --- ---- --- ---- --- ---- ---- ---- --- ----#
# Hospitalized patient record diagnosed with diabetes
# in 130 hospitals across the US from 1999–2008
# https://onlinelibrary.wiley.com/doi/10.1155/2014/781670

# The dataset represents ten years (1999-2008) of clinical
# care at 130 US hospitals and integrated delivery networks.
# It includes over 50 features representing patient and hospital
# outcomes. Information was extracted from the database for
# encounters that satisfied the following criteria.

# (1)	It is an inpatient encounter (a hospital admission).
# (2)	It is a diabetic encounter, that is, one during which
# any kind of diabetes was entered into the system as a diagnosis.
# (3)	The length of stay was at least 1 day and at most 14 days.
# (4)	Laboratory tests were performed during the encounter.
# (5)	Medications were administered during the encounter.

# The data contains such attributes as patient number, race, gender,
# age, admission type, time in hospital, medical specialty of admitting
# physician, number of lab tests performed, HbA1c test result, diagnosis,
# number of medications, diabetic medications, number of outpatient,
# inpatient, and emergency visits in the year before the hospitalization, etc.

hospital_path <- here::here(".data","diab_hosp_99","diabetic_data.csv")
hospital_records <- read.csv(hospital_path,
                             sep = ",",
                             header = TRUE,
                             stringsAsFactors = FALSE) |>
  as.data.frame()

hospital_data <- hospital_records |>
  rename_with(~ str_replace_all(., "\\.", "_")) |>
  rename(a1c_result = A1Cresult,
         diabetes_med = diabetesMed) |>
  mutate(across(where(is.character),
           ~ na_if(., "?")),
         race = as.factor(race),
         gender = as.factor(gender),
         age = as.factor(age),
         payer_code = as.factor(payer_code),
         medical_specialty = as.factor(medical_specialty),
         diag_1 = as.factor(diag_1),
         diag_2 = as.factor(diag_2),
         diag_3 = as.factor(diag_3),
         across(23:50,as.factor),
         )

write_dataset(hospital_data, tf1)

