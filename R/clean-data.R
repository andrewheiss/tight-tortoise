library(tidyverse)
library(sf)
library(dbplyr)
library(here)
library(rnaturalearth)
library(vdemdata)
library(countrycode)


db_file <- here::here("data/godad.duckdb")

con <- DBI::dbConnect(duckdb::duckdb(), db_file)

# OECD DAC ----
dac_eligible <- read_csv(here::here("data", "data-raw", "oecd_dac_countries.csv")) |> 
  # Ignore High Income Countries and More Advanced Developing Countries
  filter(!(dac_abbr %in% c("HIC", "ADC")))

# V-Dem ----
vdem_clean <- vdem |>
  select(
    country_name,
    country_text_id,
    year,
    population = e_pop,
    ccsi = v2xcs_ccsi,
    polyarchy = v2x_polyarchy,
    corruption = v2x_corr,
    rule_law = v2x_rule,
    civil_liberties = v2x_civlib,
    physical_violence = v2x_clphy
  ) |>
  filter(year >= 1973, year <= 2020) |> 
  mutate(population = population * 10000)


# ------------------------------------------------------------------------------
# Deal with adm1
# ------------------------------------------------------------------------------
adm1_raw <- tbl(con, I("adm1_raw")) |> 
  collect()

# After the first few identifying columns + the year, all other columns follow this pattern:
#
# {country}_{type}_{sector}_{financing}
#
# ...with these possible values:
#
# - {country}: 
#     AUT, BEL, CHE, DEN, ESP, FIN, FRA, GER, GRE, ICE, IRE, 
#     ITA, LUX, NED, NOR, POR, SWE, UK, USA
# - {type}: 
#     “comm” (for commitments in constant USD), “comm_nominal” (for commitments 
#     in current USD), “disb” (for disbursements in constant USD), 
#     “disb_nominal” (for disbursements in current USD)
# - {sector}: 
#     “eco” (for “Economic Infrastructure and Services”), “soc” (for “Social 
#     Infrastructure and Services”), “prod” (for “Production Sectors”), or 
#     “other” for other sectors
# - {financing}: 
#     “ibrd” or “ida”
#
# We don't need all these different things---just commitments and project counts
adm1_long <- adm1_raw |>
  pivot_longer(
    cols = -c(gid_0, gid_1, name_0, name_1, year),
    names_to = "column",
    values_to = "value"
  ) |> 
  filter(
    (str_ends(column, "comm") | str_ends(column, "projectscount")),
    !str_detect(column, "dummy"),
    !str_detect(column, "_aims")
  )

# Further complicating things, not all these components are always present!
#
# This is tricky!
#
# The `column` column follows the pattern `COUNTRY_measure` when there are two
# elements and `COUNTRY_type_measure` when there are three. Ordinarily I'd use
# something like `separate(column into = c("country", "type", "measure")) `but I
# can't do that here because `type` is optional and either appears as the 2nd or
# 3rd element
#
# So instead I separate it into country, temp1, and temp2, and then set the
# components to their proper type based on whether temp2 is NA (which means it
# was originally `COUNTRY_measure`)
adm1_long_clean <- adm1_long |>
  # AFD does weird things with the AFD_projectscount variable, using AFD_projectscount_comm. China and the WB also do weird things
  mutate(
    column = case_match(
      column,
      "AFD_projectscount_comm" ~ "AFD_projectscount",
      "CHN_loccount_comm" ~ "CHN_loccount",
      "WB_loccount_comm" ~ "WB_loccount",
      .default = column
    )
  ) |>
  separate(
    column,
    into = c("country", "temp1", "temp2"),
    sep = "_",
    extra = "merge",
    fill = "right"
  ) |>
  mutate(
    type = case_when(
      is.na(temp2) ~ NA_character_, # Only 2 parts: country_measure
      TRUE ~ temp1 # 3 parts: country_type_measure
    ),
    measure = case_when(
      is.na(temp2) ~ temp1, # Only 2 parts: temp1 is measure
      TRUE ~ temp2 # 3 parts: temp2 is measure
    )
  ) |>
  select(-temp1, -temp2, -type) |> 
  mutate(measure = if_else(str_detect(measure, "count"), "count", measure))

# Save this cleaned data as a qs file for now until I get {targets} all hooked
# up and do it from there
qs2::qs_save(adm1_long_clean, here::here("data/data-processed/adm1_long.qs2"))

aid_aggregates <- adm1_long_clean |>
  filter(year >= 1973, year <= 2020) |>
  group_by(iso3 = gid_0, year, measure) |>
  summarize(total = sum(value, na.rm = TRUE)) |>
  mutate(
    measure = case_match(
      measure,
      "comm" ~ "aid_committed",
      "count" ~ "aid_projects"
    )
  ) |>
  ungroup() |>
  pivot_wider(names_from = measure, values_from = total)


# ------------------------------------------------------------------------------
# Deal with project-level stuff
# ------------------------------------------------------------------------------

projectlevel_raw <- tbl(con, I("projectlevel_raw")) |> 
  collect()

# Switch to Equal Earth since it's in meters and will make buffer calculations easier
world <- ne_countries(scale = 50) |> 
  st_transform(crs = st_crs("EPSG:8857"))

# Country capitals from https://www.kaggle.com/datasets/nikitagrec/world-capitals-gps
# {WDI} has them from the World Bank, but they're often like 100s of miles off
capitals <- read_csv(here::here("data", "data-raw", "concap.csv")) |>
  filter(CountryCode != "NULL") |>
  mutate(
    iso3 = countrycode(
      CountryCode,
      origin = "iso2c",
      destination = "iso3c",
      custom_match = c("KO" = "XKO")
    )
  ) |> 
  select(
    iso3,
    capital_name = CapitalName,
    capital_lon = CapitalLongitude,
    capital_lat = CapitalLatitude
  )

projects <- projectlevel_raw |>
  # Make geometry column in projects data
  filter(!is.na(longitude), !is.na(latitude)) |>
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) |>
  st_transform(crs = st_crs(world)) |> 
  # Handle start and end years
  # This is tricky!
  mutate(
    # If the start year is missing and the payment year is after the closing year, use the closing year
    # If the payment year is missing and the start year is *not* missing, use the start year
    # Otherwise use start year, then payment year if it's missing
    year_start = case_when(
      is.na(startyear) & paymentyear > closingyear ~ closingyear,
      is.na(paymentyear) & !is.na(startyear) ~ startyear,
      .default = coalesce(startyear, paymentyear)
    ),
    # If the closing year is missing and the payment year is after the start year, use the payment year
    # If the closing year is missing and the start year is missing, use the payment year
    # If the closing year is missing and the payment year is missing, use the start year
    # If the start year is *not* missing and the closing year is missing, use the payment year
    # Otherwise use the closing year
    year_close = case_when(
      is.na(closingyear) & paymentyear >= startyear ~ paymentyear,
      is.na(closingyear) & is.na(startyear) ~ paymentyear,
      is.na(closingyear) & is.na(paymentyear) ~ startyear,
      !is.na(startyear) & is.na(closingyear) ~ paymentyear,
      .default = closingyear
    )
  ) |>
  # Some of these break time and have closing years that come before the starting year
  # For simplicity (and based on some cursory googling), it seems that this is 
  # generally a data entry error, so we flip the start and closing years
  mutate(breaks_time = year_close < year_start) |>
  mutate(
    year_start_good = case_when(
      breaks_time ~ year_close,
      .default = year_start
    ),
    year_close_good = case_when(
      breaks_time ~ year_start,
      .default = year_close
    )
  ) |>
  select(-c(year_start, year_close, breaks_time)) |>
  rename(year_start = year_start_good, year_close = year_close_good) |> 
  # Add capitals
  left_join(capitals, by = join_by("gid_0" == "iso3")) |> 
  mutate(
    # Create capital geometry as a regular column (not the active geometry)
    capital_geom = st_sfc(
      map2(capital_lon, capital_lat, ~st_point(c(.x, .y))),
      crs = 4326
    ),
    capital_geom = st_transform(capital_geom, crs = st_crs(world)),

    # Find the distance between each project and the capital
    dist_to_capital = st_distance(geometry, capital_geom, by_element = TRUE),
    dist_to_capital_km = as.numeric(dist_to_capital) / 1000
  ) |>
  # Find the standardized distance between projects and the capital
  group_by(gid_0) |>
  mutate(
    dist_to_capital_z = scale(dist_to_capital_km)[,1],
  ) |>
  ungroup() 


calc_projects <- function(iso3, world, projects, buffer_km = 100) {
  country <- world |> filter(iso_a3 == iso3)

  projects_in_country <- projects |> 
    filter(gid_0 == iso3)

  # Expand the number of years so that projects repeat across the duration of
  # the time between start and end years (e.g. a project with a start year of
  # 2008 and and end year of 2010 will have years for 2008, 2009, and 2010)
  projects_in_country_long <- projects_in_country |>
    mutate(year = map2(year_start, year_close, \(start, end) start:end)) |>
    unnest(year)

  # Create buffer zone
  border_buffer <- st_boundary(country) |>
    st_buffer(dist = buffer_km * 1000) |> 
    select(iso_a3, name)

  # Find all the projects that fall inside the border buffer
  projects_outside_border <- projects |>
    filter(gid_0 != iso3) |> 
    st_filter(border_buffer)

  # Expand the number of years so that projects repeat across the duration of
  # the time between start and end years (e.g. a project with a start year of
  # 2008 and and end year of 2010 will have years for 2008, 2009, and 2010)
  projects_outside_border_long <- projects_outside_border |>
    mutate(year = map2(year_start, year_close, \(start, end) start:end)) |>
    unnest(year)

  return(lst(
    projects_in_country,
    projects_in_country_long,
    border_buffer,
    projects_outside_border,
    projects_outside_border_long
  ))
}


# Clean and parse project details and neighbors ----

# All aid recipients
all_recipients <- expand_grid(gid_0 = unique(projects$gid_0)) |> 
  drop_na(gid_0) |> 
  filter(gid_0 != "unspecified", !str_detect(gid_0, "\\|"))

# TODO: There are a couple countries from all_recipients that don't appear here
recipients_map <- world |> 
  filter(iso_a3 %in% all_recipients$gid_0)

library(furrr)
plan(multisession, workers = 8)
options(future.globals.maxSize = +Inf)

tictoc::tic()
recipients_projects <- recipients_map |>
  select(iso_a3, name) |>
  # slice(1:5) |>
  mutate(
    project_details = future_map(
      iso_a3,
      \(x) calc_projects(x, world, projects),
      .options = furrr_options(seed = 1234)
    )
  )
tictoc::toc()

qs2::qs_save(recipients_projects, here::here("data/data-processed/recipients_projects.qs2"))


# Aggregated country-year project details ----

calc_spatial_entropy <- function(projects_sf, country_boundary, grid_size_km = 50) {
  # Handle empty projects or empty country boundary
  if (nrow(projects_sf) == 0 || 
      all(st_is_empty(projects_sf)) || 
      st_is_empty(country_boundary)) {
    return(tibble(
      shannon_entropy = NA_real_,
      normalized_entropy = NA_real_,
      n_grid_cells = NA_integer_
    ))
  }

  # Convert single geometry to sf object
  country_sf <- st_sf(geometry = st_sfc(country_boundary, crs = st_crs(projects_sf)))

  # Create grid over country
  grid <- st_make_grid(country_boundary, cellsize = grid_size_km * 1000, square = TRUE)
  grid_sf <- st_sf(grid_id = 1:length(grid), geometry = grid) |>
    st_set_crs(st_crs(projects_sf)) |> 
    st_filter(country_boundary)

  # Count projects per grid cell - FIX: projects join to grid
  project_counts <- st_join(projects_sf, grid_sf) |>
    st_drop_geometry() |>
    count(grid_id) |>
    complete(grid_id = 1:nrow(grid_sf), fill = list(n = 0))

  # Shannon entropy
  shannon_ent <- entropy::entropy(project_counts$n, method = "ML")

  # Max entropy is when all grid cells have equal counts
  max_entropy <- log(nrow(grid_sf))
  normalized_ent <- pmin(shannon_ent / max_entropy, 1)

  return(tibble(
    shannon_entropy = shannon_ent,
    normalized_entropy = normalized_ent,
    n_grid_cells = nrow(grid_sf)
  ))
}


neighbor_project_counts <- recipients_projects |> 
  unnest_wider(project_details) |>
  mutate(
    neighbor_counts = map(projects_outside_border_long, \(x) {
      if (is.null(x)) {
        NULL
      } else {
        x |>
          filter(year >= 1973, year <= 2020) |> 
          group_by(year) |>
          summarize(n_neighbor_projects = n()) |>
          st_drop_geometry()
      }
    })
  ) |>
  st_drop_geometry() |>
  unnest(neighbor_counts) |> 
  select(iso_a3, year, n_neighbor_projects)


country_project_stuff <- recipients_projects |>
  unnest_wider(project_details) |>
  mutate(
    country_details = map(projects_in_country_long, \(x) {
      if (is.null(x)) {
        NULL
      } else {
        x |>
          filter(year >= 1973, year <= 2020) |>
          group_by(year) |>
          summarize(
            n_country_projects = n(),
            mean_dist_z = mean(dist_to_capital_z, na.rm = TRUE),
            median_dist_z = median(dist_to_capital_z, na.rm = TRUE),
          ) |>
          st_drop_geometry()
      }
    })) |> 
  mutate(
    entropy_details = map2(
      projects_in_country_long,
      geometry,
      \(projects, country_geom) {
        if (is.null(projects)) {
          NULL
        } else {
          projects |>
            filter(year >= 1973, year <= 2020) |>
            group_by(year) |>
            group_nest() |> # Create nested data by year
            mutate(
              entropy_stats = map(data, \(year_projects) {
                calc_spatial_entropy(year_projects, country_geom)
              })
            ) |>
            unnest(entropy_stats) |>
            select(-data)
        }
      })) |>
  st_drop_geometry() |>
  # unnest(country_details) |> 
  # select(iso_a3, year, n_country_projects, mean_dist_z, median_dist_z)
  unnest(c(country_details, entropy_details), names_sep = "_") |>
  select(
    iso_a3,
    year = country_details_year,
    n_country_projects = country_details_n_country_projects,
    mean_dist_z = country_details_mean_dist_z,
    median_dist_z = country_details_median_dist_z,
    shannon_entropy = entropy_details_shannon_entropy,
    normalized_entropy = entropy_details_normalized_entropy,
    n_grid_cells = entropy_details_n_grid_cells
  )


# Chaudhry laws ----
chaudhry_raw <- readxl::read_excel("data/data-raw/Updated NGO data.xlsx") |>
  select(1:15) |>
  mutate(
    ccode = case_match(
      ccode,
      315 ~ 316,
      342 ~ 345,
      348 ~ 341,
      818 ~ 816,
      525 ~ 626,
      .default = ccode
    )
  ) |>
  mutate(
    iso3 = countrycode(
      ccode,
      origin = "cown",
      destination = "iso3c",
      custom_match = c("345" = "SRB")
    )
  )

  regulations <- tribble(
    ~question, ~barrier,       ~question_clean,                  ~ignore_in_index, ~question_display,
    "q1a",     "association",  "const_assoc",                    TRUE,             "Constitutional associational rights",
    "q1b",     "association",  "political_parties",              TRUE,             "Citizens form political parties",
    "q2a",     "entry",        "ngo_register",                   TRUE,             "NGO registration required",
    "q2b",     "entry",        "ngo_register_burden",            FALSE,            "NGO registration burdensome",
    "q2c",     "entry",        "ngo_register_appeal",            FALSE,            "NGO registration appealXXXnot allowed",
    "q2d",     "entry",        "ngo_barrier_foreign_funds",      FALSE,            "Registration barriers differentXXXif foreign funds involved",
    "q3a",     "funding",      "ngo_disclose_funds",             TRUE,             "Funds must be disclosed",
    "q3b",     "funding",      "ngo_foreign_fund_approval",      FALSE,            "Prior approval requiredXXXfor foreign funds",
    "q3c",     "funding",      "ngo_foreign_fund_channel",       FALSE,            "Foreign funds channeledXXXthrough government",
    "q3d",     "funding",      "ngo_foreign_fund_restrict",      FALSE,            "Foreign funds restricted",
    "q3e",     "funding",      "ngo_foreign_fund_prohibit",      FALSE,            "Foreign funds prohibited",
    "q3f",     "funding",      "ngo_type_foreign_fund_prohibit", FALSE,            "Foreign funds prohibitedXXXfor some types of NGOs",
    "q4a",     "advocacy",     "ngo_politics",                   FALSE,            "NGOs restricted from politics",
    "q4b",     "advocacy",     "ngo_politics_intimidation",      TRUE,             "NGOs intimidated from politics",
    "q4c",     "advocacy",     "ngo_politics_foreign_fund",      FALSE,            "Political barriers differentXXXif foreign funds involved"
  )

library(scales)
chaudhry_long <- chaudhry_raw %>%
    # Ethiopia and Czech Republic have duplicate rows in 1993 and 1994 respectively, 
    # but the values are identical, so just keep the first of the two
    group_by(iso3, year) %>%
    slice(1) %>%
    ungroup() %>%
    arrange(iso3, year) %>%
    # Reverse values for q2c
    mutate(q2c = 1 - q2c) %>%
    # Rescale 2-point questions to 0-1 scale
    mutate_at(vars(q3e, q3f, q4a), ~rescale(., to = c(0, 1), from = c(0, 2))) %>%
    # q2d and q4c use -1 to indicate less restriction/burdensomeness. Since we're
    # concerned with an index of restriction, we make the negative values zero
    mutate_at(vars(q2d, q4c), ~ifelse(. == -1, 0, .)) %>%
    pivot_longer(cols = starts_with("q"), names_to = "question") %>%
    left_join(regulations, by = "question") %>%
    group_by(iso3) %>%
    mutate(all_missing = all(is.na(value))) %>%
    group_by(iso3, question) %>%
    # Bring most recent legislation forward in time
    fill(value) %>%
    # For older NA legislation that can't be brought forward, set sensible
    # defaults. Leave countries that are 100% 0 as NA.
    mutate(value = ifelse(!all_missing & is.na(value), 0, value)) %>%
    ungroup()

  chaudhry_registration <- chaudhry_long %>%
    select(iso3, year, question_clean, value) %>%
    pivot_wider(names_from = "question_clean", values_from = "value")

  chaudhry_summed <- chaudhry_long %>%
    filter(!ignore_in_index) %>%
    group_by(iso3, year, barrier) %>%
    summarize(total = sum(value)) %>%
    ungroup()

  chaudhry_clean <- chaudhry_summed %>%
    pivot_wider(names_from = barrier, values_from = total) %>%
    mutate_at(vars(entry, funding, advocacy),
              list(std = ~. / max(., na.rm = TRUE))) %>%
    mutate(barriers_total = advocacy + entry + funding,
           barriers_total_std = advocacy_std + entry_std + funding_std) %>%
    left_join(chaudhry_registration, by = c("iso3", "year"))



# Finally(!) build a panel
# Combine everything into a panel dataset ---
countries_in_data <- list(
  unique(dac_eligible$iso3),
  unique(adm1_long_clean$gid_0),
  unique(vdem_clean$country_text_id)
) |>
  reduce(intersect)

skeleton <- expand_grid(
  iso3 = countries_in_data,
  year = 1990:2020
)

panel <- skeleton |> 
  left_join(aid_aggregates, by = join_by(iso3, year)) |> 
  left_join(neighbor_project_counts, by = join_by(iso3 == iso_a3, year)) |> 
  left_join(country_project_stuff, by = join_by(iso3 == iso_a3, year)) |> 
  left_join(vdem_clean, by = join_by(iso3 == country_text_id, year)) |> 
  left_join(chaudhry_clean, by = join_by(iso3, year)) |> 
  relocate(country_name, .after = iso3) |> 
  mutate(aid_per_capita = aid_committed / population)

qs2::qs_save(panel, here::here("data/data-processed/panel.qs2"))
