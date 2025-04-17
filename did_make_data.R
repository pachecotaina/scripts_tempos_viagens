arrow::set_cpu_count(8)  # Use multiple threads
arrow::default_memory_pool()

# ############################################################################ #
####        DEFINIR STATUS DE CONTROLE OU TRATAMENTO PARA AS ROTAS          ####
# ############################################################################ #
# definir intervalo para análise
df_days <- tibble(
  "data_vigor" = seq.Date(
    as.Date("2015-01-01", "%Y-%m-%d"), 
    as.Date("2016-12-31", "%Y-%m-%d"), 
    by = 1)) %>% 
  mutate(
    weekday = weekdays(data_vigor),
    date_running = row_number()) %>% 
  filter(weekday %notin% c("Saturday", "Sunday") & data_vigor %notin% bank_holiday$date)

vec_days <- df_days %>% 
  mutate(data_vigor = str_replace_all(as.character(data_vigor), "-", "")) %>% 
  pull(data_vigor)

# abrir rotas de interesse
df_routes <- read_parquet(
  "data/intermediate/osrm/pairs_od_sample.parquet")

# abrir vias com velocidades alteradas
shp_vias <- read_sf(
  "data/input/vias_vel_reduz/ViasVelocidadeReduzida_Bloomberg_SIRGAS200023S.shp") %>% 
  st_transform(4326)

# unir bancos
df_vias <- shp_vias %>% 
  st_set_geometry(NULL) %>% 
  filter(ano_vigor > 2014) %>% 
  distinct(codlog, veloc_aps, data_vigor, ano_vigor) %>% 
  rename(cvc_codlog = codlog) %>% 
  add_count(cvc_codlog) %>% 
  na.omit() %>% 
  group_by(cvc_codlog, data_vigor, ano_vigor) %>% 
  summarize(veloc_aps = max(veloc_aps), .groups = "drop") %>% 
  group_by(cvc_codlog) %>% 
  summarize(
    veloc_aps = max(veloc_aps), 
    data_vigor = min(data_vigor),
    .groups = "drop")

df_routes <- df_routes %>% 
  left_join(df_vias, by = "cvc_codlog") %>% 
  mutate(
    status = ifelse(is.na(data_vigor), "Control", status))

# verificar quantas rotas temos de cada tipo
table(df_routes$status)
# Control Treated 
# 2849    2496

vec_od <- unique(df_routes$pair_od)
vec_treated <- unique(df_routes$pair_od[df_routes$status == "Treated"])

# ############################################################################ #
####        LOAD DATA          ####
# ############################################################################ #
# Use your available 9 cores
plan(multisession, workers = 9)

vec_dep_hours <- 0:23  # Or a custom subset

# Minimal info needed
df_routes_info <- df_routes %>% 
  distinct(pair_od, duration, distance)

df_dates <- df_routes %>% 
  group_by(pair_od) %>% 
  summarise(data_vigor = min(data_vigor), .groups = "drop")

# h <- 6
process_hour <- function(h) {
  message("Processing hour: ", h)
  
  # Read and filter by dep_hour
  df_hour <- open_dataset(
    paste0("/Users/tainasouzapacheco/Downloads/matrix_grouped_osrm/", vec_days, ".parquet")) %>%
    filter(dep_hour == h) %>%
    mutate(pair_od = paste0(from_id, "-", to_id)) %>%
    filter(pair_od %in% vec_od) %>%
    select(pair_od, data, dep_hour, dur_obs_mean, dur_obs_sd, n_vehicles) %>%
    collect() %>%
    mutate(data = as.Date(data, "%Y%m%d")) %>% 
    left_join(df_routes_info, by = "pair_od") %>%
    left_join(df_dates, by = "pair_od") %>%
    left_join(df_days, by = "data_vigor") %>%
    mutate(
      treat = as.integer(pair_od %in% vec_treated),
      treat_day = ifelse(is.na(date_running), 0, date_running),
      data = as.Date(data, format = "%Y%m%d"),
      post = as.integer(data >= data_vigor)
    )
  
  # Return ready-to-model dataset
  return(df_hour)
}

list_hourly_data <- future_map(vec_dep_hours, process_hour)
names(list_hourly_data) <- paste0("hour_", vec_dep_hours)

df_days <- tibble(
  "data" = seq.Date(
    as.Date("2015-01-01", "%Y-%m-%d"), 
    as.Date("2016-12-31", "%Y-%m-%d"), 
    by = 1)) %>% 
  mutate(
    date_running = row_number())

df_id <- df_dates %>% 
  select(pair_od) %>% 
  mutate(id_num = row_number())

for (i in seq_along(vec_dep_hours)) {
  list_hourly_data[[i]] <- list_hourly_data[[i]] %>% 
    select(-date_running) %>% 
    left_join(df_days, by = "data") %>% 
    left_join(df_id, by = "pair_od") %>% 
    mutate(
      year = as.numeric(year(data)),
      year_treat = as.numeric(year(data_vigor)),
      month = ifelse(year == 2015, month(data), month(data) + 12),
      month_treat = ifelse(year_treat == 2015, month(data_vigor), month(data_vigor) + 12),
      month_treat = ifelse(is.na(month_treat), 0, month_treat)) %>% 
    filter(month_treat != 3)
}

walk2(list_hourly_data, names(list_hourly_data), ~ {
  write_parquet(.x, paste0("data/output/df_dep_hour_", .y, ".parquet"))
})
