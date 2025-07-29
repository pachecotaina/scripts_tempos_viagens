# list files to process
processed_files <- list.files(
  "data/intermediate/matrix_grouped_osrm", 
  pattern = "*.parquet", 
  full.names = FALSE)

to_process_files <- list.files(
  "data/intermediate/matrix", 
  pattern = "*.parquet", 
  full.names = FALSE)

to_process_files <- to_process_files[to_process_files %notin% processed_files]

# open free-flow travel time matrix
df_id <- read_sf("data/intermediate/osrm/routes_od_streets_2015.gpkg") %>% 
  st_set_geometry(NULL) %>% 
  rename(from_id = src, to_id = dst) %>%
  mutate(pair_od = paste0(from_id, "-", to_id)) %>% 
  distinct(from_id, to_id, pair_od, duration) %>% 
  mutate(from_id = str_sub(pair_od, 1, 4),
         to_id = str_sub(pair_od, 6, 10))

# length(unique(df_id$pair_od))

setDT(df_id)

if (length(to_process_files)>0) {
  to_process_files <- paste(
    "data/intermediate/matrix/",
    to_process_files[to_process_files %notin% processed_files],
    sep = "")
  
  # process files
  walk(to_process_files, 
       matrix_group_trips_hour, output_folder = "data/intermediate/matrix_grouped_osrm/") 
} else {
  print("Grouped matrix data already processed")
}
