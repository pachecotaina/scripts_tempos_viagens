df_hypc <- read_parquet(
  "data/output/hypercongestion_by_id.parquet") %>% 
  filter(problems == "ok")

df_class <- read_parquet(
  "data/intermediate/radares_5min_vol_week_averages.parquet") %>% 
  filter(id %in% df_hypc$id) %>% 
  distinct(
    id, data_ativacao, data_vigor, mes_vigor, ano_vigor, mes_ano, treat, id_num)

table(df_class$treat)

df_hypc <- df_hypc %>% 
  left_join(df_class, by = "id")

# ############################################################################ #
####                PROCESS HYPC FOR EACH HOUR - CONTROL                    ####
# ############################################################################ #
df_hypc_control <- df_hypc %>% 
  filter(treat == 0) %>% 
  distinct(id, max_speed)

if (!file.exists("data/intermediate/radares_hypc_hour_control.parquet")) {
  # List all files with correct pattern and date range
  folder_path <- "/Users/tainasouzapacheco/Library/CloudStorage/Dropbox/Academico/UAB/tese/ch_overpass/data/intermediate/parquet/"
  
  # get files
  all_files <- list.files(
    folder_path,
    pattern = "\\d{8}_05\\.parquet$",
    full.names = TRUE
  )
  
  # Extract date from file names
  valid_files <- all_files %>%
    keep(~ {
      date_part <- str_extract(basename(.x), "^\\d{8}")  # extract "YYYYMMDD"
      date_val <- as.Date(date_part, format = "%Y%m%d")
      date_val >= as.Date("2015-01-01") & date_val <= as.Date("2016-12-31")
    })
  
  #  Get list of radar IDs
  ids_radares <- unique(df_hypc_control$id)
  
  # Function to process each file
  process_file_df <- function(path) {
    df <- open_dataset(path) %>%
      filter(id %in% ids_radares & vel_p > 0 & vel_p < 150) %>%
      collect() %>% 
      mutate(data = as.Date(as.character(data), format = "%Y%m%d")) %>%
      left_join(df_hypc_control, by = "id") %>% 
      mutate(hypc = ifelse(max_speed > vel_p, 1, 0)) %>% 
      group_by(id, data, hora) %>%
      summarise(
        n_hypc = sum(hypc, na.rm = TRUE),
        n_hours = n(),
        .groups = "drop"
      ) %>% 
      mutate(pct_hyc = n_hypc/n_hours)
  }
  
  # Plan parallelism
  plan(multisession, workers = 8)  
  
  # Run parallel version of map_dfr
  df_result <- future_map_dfr(valid_files, process_file_df, .progress = TRUE)
  
  write_parquet(
    df_result,
    "data/intermediate/radares_hypc_hour_control.parquet")
} else{
  df_result <- read_parquet("data/intermediate/radares_hypc_hour_control.parquet")
}

df_result %>% 
  group_by(hora) %>% 
  summarise(
    pct_mean = mean(pct_hyc, na.rm = TRUE)) %>% 
  ggplot(aes(x = hora, y = pct_mean)) +
  geom_bar(
    stat = "identity")


# ############################################################################ #
####                PROCESS HYPC FOR EACH HOUR - TREATED                    ####
# ############################################################################ #
df_hypc_treat <- df_hypc %>% 
  filter(treat == 1) %>% 
  distinct(id, max_speed, period, data_vigor) %>% 
  pivot_wider(
    names_from = period,
    values_from = max_speed,
    names_prefix = "max_speed_") %>% 
  na.omit()

if (!file.exists("data/intermediate/radares_hypc_hour_treat.parquet")) {
  # List all files with correct pattern and date range
  folder_path <- "/Users/tainasouzapacheco/Library/CloudStorage/Dropbox/Academico/UAB/tese/ch_overpass/data/intermediate/parquet/"
  
  # get files
  all_files <- list.files(
    folder_path,
    pattern = "\\d{8}_05\\.parquet$",
    full.names = TRUE
  )
  
  # Extract date from file names
  valid_files <- all_files %>%
    keep(~ {
      date_part <- str_extract(basename(.x), "^\\d{8}")  # extract "YYYYMMDD"
      date_val <- as.Date(date_part, format = "%Y%m%d")
      date_val >= as.Date("2015-01-01") & date_val <= as.Date("2016-12-31")
    })
  
  #  Get list of radar IDs
  ids_radares <- unique(df_hypc_treat$id)
  
  # Function to process each file
  process_file_df <- function(path) {
    df <- open_dataset(path) %>%
      filter(id %in% ids_radares & vel_p > 0 & vel_p < 150) %>%
      collect() %>% 
      mutate(data = as.Date(as.character(data), format = "%Y%m%d")) %>%
      left_join(df_hypc_treat, by = "id") %>% 
      mutate(
        max_speed = ifelse(data >= data_vigor, max_speed_after, max_speed_before),
        hypc = ifelse(max_speed > vel_p, 1, 0)) %>% 
      group_by(id, data, hora) %>%
      summarise(
        n_hypc = sum(hypc, na.rm = TRUE),
        n_hours = n(),
        .groups = "drop"
      ) %>% 
      mutate(pct_hyc = n_hypc/n_hours)
  }
  
  # Plan parallelism
  plan(multisession, workers = 8)  
  
  # Run parallel version of map_dfr
  df_result_treat <- future_map_dfr(valid_files, process_file_df, .progress = TRUE)
  
  write_parquet(
    df_result_treat,
    "data/intermediate/radares_hypc_hour_treat.parquet")
} else{
  df_result_treat <- read_parquet("data/intermediate/radares_hypc_hour_treat.parquet")
}

# ############################################################################ #
####                      COMBINE CONTROL AND TREATED                       ####
# ############################################################################ #
df_result <- df_result %>% 
  rbind(df_result_treat) %>% 
  left_join(df_class, by = "id")

write_parquet(
  df_result,
  "data/intermediate/radares_hypc_hour.parquet")

remove(df_result, df_result_treat)

# ############################################################################ #
####                 PROCESS HYPC FOR EACH DAY - CONTROL                    ####
# ############################################################################ #
df_hypc_control <- df_hypc %>% 
  filter(treat == 0) %>% 
  distinct(id, max_speed)

if (!file.exists("data/intermediate/radares_hypc_day_control.parquet")) {
  # List all files with correct pattern and date range
  folder_path <- "/Users/tainasouzapacheco/Library/CloudStorage/Dropbox/Academico/UAB/tese/ch_overpass/data/intermediate/parquet/"
  
  # get files
  all_files <- list.files(
    folder_path,
    pattern = "\\d{8}_05\\.parquet$",
    full.names = TRUE
  )
  
  # Extract date from file names
  valid_files <- all_files %>%
    keep(~ {
      date_part <- str_extract(basename(.x), "^\\d{8}")  # extract "YYYYMMDD"
      date_val <- as.Date(date_part, format = "%Y%m%d")
      date_val >= as.Date("2015-01-01") & date_val <= as.Date("2016-12-31")
    })
  
  #  Get list of radar IDs
  ids_radares <- unique(df_hypc_control$id)
  
  # Function to process each file
  process_file_df <- function(path) {
    df <- open_dataset(path) %>%
      filter(id %in% ids_radares & vel_p > 0 & vel_p < 150) %>%
      collect() %>% 
      mutate(data = as.Date(as.character(data), format = "%Y%m%d")) %>%
      left_join(df_hypc_control, by = "id") %>% 
      mutate(hypc = ifelse(max_speed > vel_p, 1, 0)) %>% 
      group_by(id, data) %>%
      summarise(
        n_hypc = sum(hypc, na.rm = TRUE),
        n_hours = n(),
        .groups = "drop"
      ) %>% 
      mutate(pct_hyc = n_hypc/n_hours)
  }
  
  # Plan parallelism
  plan(multisession, workers = 8)  
  
  # Run parallel version of map_dfr
  df_result <- future_map_dfr(valid_files, process_file_df, .progress = TRUE)
  
  write_parquet(
    df_result,
    "data/intermediate/radares_hypc_day_control.parquet")
} else{
  df_result <- read_parquet("data/intermediate/radares_hypc_day_control.parquet")
}

# df_result %>% 
#   group_by(hora) %>% 
#   summarise(
#     pct_mean = mean(pct_hyc, na.rm = TRUE)) %>% 
#   ggplot(aes(x = hora, y = pct_mean)) +
#   geom_bar(
#     stat = "identity")

# ############################################################################ #
####                PROCESS HYPC FOR EACH DAY - TREATED                     ####
# ############################################################################ #
df_hypc_treat <- df_hypc %>% 
  filter(treat == 1) %>% 
  distinct(id, max_speed, period, data_vigor) %>% 
  pivot_wider(
    names_from = period,
    values_from = max_speed,
    names_prefix = "max_speed_") %>% 
  na.omit()

if (!file.exists("data/intermediate/radares_hypc_day_treat.parquet")) {
  # List all files with correct pattern and date range
  folder_path <- "/Users/tainasouzapacheco/Library/CloudStorage/Dropbox/Academico/UAB/tese/ch_overpass/data/intermediate/parquet/"
  
  # get files
  all_files <- list.files(
    folder_path,
    pattern = "\\d{8}_05\\.parquet$",
    full.names = TRUE
  )
  
  # Extract date from file names
  valid_files <- all_files %>%
    keep(~ {
      date_part <- str_extract(basename(.x), "^\\d{8}")  # extract "YYYYMMDD"
      date_val <- as.Date(date_part, format = "%Y%m%d")
      date_val >= as.Date("2015-01-01") & date_val <= as.Date("2016-12-31")
    })
  
  #  Get list of radar IDs
  ids_radares <- unique(df_hypc_treat$id)
  
  # Function to process each file
  process_file_df <- function(path) {
    df <- open_dataset(path) %>%
      filter(id %in% ids_radares & vel_p > 0 & vel_p < 150) %>%
      collect() %>% 
      mutate(data = as.Date(as.character(data), format = "%Y%m%d")) %>%
      left_join(df_hypc_treat, by = "id") %>% 
      mutate(
        max_speed = ifelse(data >= data_vigor, max_speed_after, max_speed_before),
        hypc = ifelse(max_speed > vel_p, 1, 0)) %>% 
      group_by(id, data) %>%
      summarise(
        n_hypc = sum(hypc, na.rm = TRUE),
        n_hours = n(),
        .groups = "drop"
      ) %>% 
      mutate(pct_hyc = n_hypc/n_hours)
  }
  
  # Plan parallelism
  plan(multisession, workers = 8)  
  
  # Run parallel version of map_dfr
  df_result_treat <- future_map_dfr(valid_files, process_file_df, .progress = TRUE)
  
  write_parquet(
    df_result_treat,
    "data/intermediate/radares_hypc_day_treat.parquet")
} else{
  df_result_treat <- read_parquet("data/intermediate/radares_hypc_day_treat.parquet")
}

# ############################################################################ #
####                  COMBINE CONTROL AND TREATED - DAY                     ####
# ############################################################################ #
df_result <- df_result %>% 
  rbind(df_result_treat) %>% 
  left_join(df_class, by = "id")

write_parquet(
  df_result,
  "data/intermediate/radares_hypc_day.parquet")

# df_result %>% 
#   filter(data <= as.Date("2015-07-19")) %>% 
#   group_by(hora, treat) %>% 
#   summarise(
#     pct_mean = mean(pct_hyc, na.rm = TRUE)) %>% 
#   ggplot(aes(x = hora, y = pct_mean)) +
#   geom_bar(
#     stat = "identity") +
#   facet_wrap(~treat)
