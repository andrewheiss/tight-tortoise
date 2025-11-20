library(tidyverse)
library(here)
library(dbplyr)
library(duckdb)

# Here's what the huge .zip file looks like inside:
#
# unzip -l data/data-raw/GODAD.csv.zip
# Archive:  data/data-raw/GODAD Workshop Pre-releases.zip
#   Length       Date    Time    Name
# ---------   ---------- -----   ----
#      1037   04-09-2025 04:28   CHANGELOG.txt
#      3363   04-09-2025 04:28   GODAD error list.txt
# 192914945   04-09-2025 04:28   GODAD_adm1.csv
# 370855033   04-09-2025 04:28   GODAD_adm1.dta
# 2359698131  04-09-2025 04:27   GODAD_adm2.csv
# 4810447763  04-09-2025 04:27   GODAD_adm2.dta
#    381828   04-09-2025 04:27   GODAD_codebook.pdf
# 779890143   04-09-2025 04:27   GODAD_projectlevel.csv
# 1619179816  04-09-2025 04:26   GODAD_projectlevel.dta
# ---------                      -------
# 10133372059                    9 files

# We can extract parts of this without unzipping the whole massive thing, like
# the codebook:
#
# unzip(
#   zipfile = here("data", "data-raw", "GODAD Workshop Pre-releases.zip"),
#   files = "GODAD_codebook.pdf",
# )

# But more importantly, we need to move these huge CSV files into a DuckDB
# database because it's far faster and more efficient to work with

db_file <- here::here("data/godad.duckdb")

con <- DBI::dbConnect(duckdb::duckdb(), db_file)

connections::connection_view(con)


# I could use duckdb::duckdb_read_csv() to read this into the database natively,
# but it complains when auto-detecting column types and I trust
# readr::read_csv() to do the right thing
adm1 <- read_csv(
  unz(
    here("data", "data-raw", "GODAD Workshop Pre-releases.zip"),
    "GODAD_adm1.csv"
  )
)

copy_to(
  con,
  adm1,
  name = "adm1_raw",
  overwrite = TRUE,
  temporary = FALSE
)

# The project-level data has weird quoting issues that DuckDB's native CSV
# reader chokes on, but reader::read_csv() handles it just fine
projectlevel <- read_csv(
  unz(
    here("data", "data-raw", "GODAD Workshop Pre-releases.zip"),
    "GODAD_projectlevel.csv"
  )
)

copy_to(
  con,
  projectlevel,
  name = "projectlevel_raw",
  overwrite = TRUE,
  temporary = FALSE
)

# However, readr::read_csv() chokes on adm2 because it's so big, so we have to
# do the native approach, which requires an actual CSV file and not an unz()
# connection, so we unzip that one file to a temporary file, read it, write it
# to the DB, and clean up
tmp_dir <- tempdir()
unzip(
  zipfile = here("data", "data-raw", "GODAD Workshop Pre-releases.zip"),
  files = "GODAD_adm2.csv",
  exdir = tmp_dir
)

duckdb_read_csv(
  con,
  "adm2_raw",
  file.path(tmp_dir, "GODAD_adm2.csv"),
  nrow.check = 150000
)

# Delete that huge CSV
unlink(tmp_dir, recursive = TRUE)

# All done with the database!
dbDisconnect(con)
