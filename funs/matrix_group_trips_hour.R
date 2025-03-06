matrix_group_trips_hour <- function(file_path = NULL,
                                    output_folder = NULL,
                                    max_trip_duration = 1) {
  # Read the parquet 
  # df <- fread(file_path)
  df <- read_parquet(file_path) %>% collect()
  
  setDT(df)
  
  if ("from" %in% names(df)) {
    df <- df[, .(placa, from, dep_time, vel_from, to, arr_time, vel_to)]
    # Rename columns and calculate 'dur_obs' (from seconds to minutes)
    df[, `:=`(from_id = as.character(from), 
              to_id = as.character(to),
              pair_od = paste0(from_id, "-", to_id),
              dur_obs = as.numeric(arr_time - dep_time) / 60)]
  }else{
    df <- df[, .(placa, from_id, dep_time, vel_from, to_id, arr_time, vel_to)]
    # Rename columns and calculate 'dur_obs' (from seconds to minutes)
    df[, `:=`(from_id = as.character(from_id), 
              to_id = as.character(to_id),
              pair_od = paste0(from_id, "-", to_id),
              dur_obs = as.numeric(arr_time - dep_time) / 60)] 
  }
  
  if("pair_od" %in% names(df_id)) {
    # Filter for valid pair_od
    df <- df[pair_od %in% df_id$pair_od]
  }
  
  # Filter rows where arr_time is greater than dep_time (only valid trips)
  df <- df[arr_time > dep_time]
  
  # Return NULL if no valid data after filtering
  if (nrow(df) == 0) return(NULL)
  
  # Join with df_id efficiently using data.table's merge
  df <- merge(df, df_id, by = c("from_id", "to_id"), all.x = TRUE)
  
  # Return NULL if 'duration' column is missing
  if (!"duration" %in% colnames(df)) {
    warning(paste("Duration column missing in file:", file_path))
    return(NULL)
  }
  
  # Filter for valid trips where observed duration is below max trip duration
  df <- df[dur_obs <= max_trip_duration*60]
  
  # Precompute the 90th percentile of 'dur_ratio'
  df[, dur_ratio := ifelse(duration == 0, NA_real_, dur_obs / duration)]
  dur_ratio_quantile <- quantile(df$dur_ratio, probs = 0.90, na.rm = TRUE)
  
  # Filter for valid trips where dur_ratio <= 85th percentile
  df <- df[dur_ratio <= dur_ratio_quantile]
  
  # Return NULL if no valid trips after filtering
  if (nrow(df) == 0) return(NULL)
  
  # Add 'dep_hour' column and summarize efficiently
  df[, dep_hour := hour(dep_time)]
  summary_df <- df[, .(
    n_vehicles = .N,
    dur_obs_mean = mean(dur_obs, na.rm = TRUE),
    dur_obs_sd = sd(dur_obs, na.rm = TRUE)
  ), by = .(from_id, to_id, dep_hour)]
  
  summary_df[, data := tools::file_path_sans_ext(basename(file_path))]
  
  # Save the result as a Parquet file
  output_file_parquet <- paste0(output_folder, tools::file_path_sans_ext(basename(file_path)), ".parquet")
  write_parquet(summary_df, output_file_parquet)
  
  # Return the path of the saved Parquet file (optional)
  # return(output_file_parquet)
}
