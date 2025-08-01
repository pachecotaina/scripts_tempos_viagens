df_result_week <- read_parquet(
  "data/intermediate/radares_5min_vol_week_averages.parquet")

# ############################################################################ #
####                    MAKE FHP VOL E VEL EVERY 15 MINUTES                 ####
# ############################################################################ #
if (!file.exists("data/intermediate/radares_fhp_15min.parquet")) {
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
  ids_radares <- unique(df_result_week$id)
  
  # Function to process each file
  process_file_df <- function(path) {
    df <- open_dataset(path) %>%
      filter(id %in% ids_radares) %>%
      collect() %>% 
      mutate(
        data = as.Date(as.character(data), format = "%Y%m%d"),
        vel_tot = vel_p * volume) %>%
      group_by(id, data, hora, min) %>%
      summarise(
        volume = sum(volume, na.rm = TRUE),
        vel_tot = sum(vel_tot, na.rm = TRUE),
        .groups = "drop") %>% 
      mutate(vel_p = vel_tot/volume) %>% 
      mutate(
        min15 = case_when(
          min %in% c(0, 5, 10) ~ "00-15",
          min %in% c(15, 20, 25) ~ "15-30",
          min %in% c(30, 35, 40) ~ "30-45",
          min %in% c(45, 50, 55) ~ "45-60"),
        vel_tot = vel_p * volume) %>% 
      group_by(id, data, hora, min15) %>%
      summarise(
        volume = sum(volume, na.rm = TRUE),
        vel_tot = sum(vel_tot, na.rm = TRUE),
        .groups = "drop") %>% 
      mutate(vel_p = ifelse(volume == 0, 0, vel_tot/volume)) %>% 
      group_by(id, data, hora) %>% 
      summarise(
        vol_tot = sum(volume, na.rm = TRUE),
        vol_max = max(volume, na.rm = TRUE),
        vel_mean = mean(vel_p, na.rm = TRUE),
        vel_max = max(vel_p, na.rm = TRUE),
        .groups = "drop") %>% 
      mutate(
        vol_15min_x4 = vol_max * 4,
        fhp = vol_tot/vol_15min_x4,
        fhp_vel = vel_mean/vel_max)
  }
  
  # Plan parallelism
  plan(multisession, workers = 8)  
  
  # Run parallel version of map_dfr
  df_result_fhp15 <- future_map_dfr(valid_files, process_file_df, .progress = TRUE)
  
  write_parquet(
    df_result_fhp15,
    "data/intermediate/radares_fhp_15min.parquet")
} #else{
#   df_result_fhp15 <- read_parquet("data/intermediate/radares_fhp_15min.parquet")
# }

# ############################################################################ #
####                    MAKE FHP VOL E VEL EVERY 5 MINUTES                  ####
# ############################################################################ #
if (!file.exists("data/intermediate/radares_fhp_5min.parquet")) {
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
  ids_radares <- unique(df_result_week$id)
  
  # Function to process each file
  process_file_df <- function(path) {
    df <- open_dataset(path) %>%
      filter(id %in% ids_radares) %>%
      collect() %>% 
      mutate(
        data = as.Date(as.character(data), format = "%Y%m%d"),
        vel_tot = vel_p * volume) %>%
      group_by(id, data, hora, min) %>%
      summarise(
        volume = sum(volume, na.rm = TRUE),
        vel_tot = sum(vel_tot, na.rm = TRUE),
        .groups = "drop")  %>% 
      mutate(vel_p = ifelse(volume == 0, 0, vel_tot/volume)) %>% 
      group_by(id, data, hora) %>% 
      summarise(
        vol_tot = sum(volume, na.rm = TRUE),
        vol_max = max(volume, na.rm = TRUE),
        vel_mean = mean(vel_p, na.rm = TRUE),
        vel_max = max(vel_p, na.rm = TRUE),
        .groups = "drop") %>% 
      mutate(
        vol_5min_x12 = vol_max * 12,
        fhp_5min = vol_tot/vol_5min_x12,
        fhp_vel_5min = vel_mean/vel_max)
  }
  
  # Plan parallelism
  plan(multisession, workers = 8)  
  
  # Run parallel version of map_dfr
  df_result_fhp5 <- future_map_dfr(valid_files, process_file_df, .progress = TRUE)
  
  write_parquet(
    df_result_fhp5,
    "data/intermediate/radares_fhp_5min.parquet")
} #else{
#   df_result_fhp5 <- read_parquet("data/intermediate/radares_fhp_5min.parquet")
# }
