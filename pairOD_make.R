
# Using osrmRoute
sc_points <- helper_geo %>% 
  count(id) %>% 
  select(-n)

vec_origin <- sc_points$id

files_matrix <- list.files(
  "data/intermediate/matrix_grouped",
  full.names = TRUE,
  pattern = ".parquet")

list_result <- list()
list_routes <- list()
for (i in seq_along(vec_origin)) {
  df_aux <- open_dataset(files_matrix) %>% 
    filter(from_id == vec_origin[i]) %>%        # Filter for origin
    select(from_id, to_id, n_vehicles) %>%      # Select relevant columns
    group_by(from_id, to_id) %>%
    summarize(n_vehicles = sum(n_vehicles),
              .groups = "drop") %>%  # Summarize across all files
    arrange(desc(n_vehicles)) %>%    # Sort by descending n_vehicles
    slice_head(n = 15) %>%           # Select the top 15 rows
    collect()
  
  df_aux_pairs <- open_dataset(files_matrix) %>% 
    filter(from_id %in% df_aux$from_id & to_id %in% df_aux$to_id) %>% 
    distinct(from_id, to_id, data) %>% 
    mutate(data = as.Date(data, "%Y%m%d")) %>% 
    # collect() %>% 
    group_by(from_id, to_id) %>% 
    summarize(n_date = n(),
              min_date = min(data, na.rm = TRUE),
              max_date = max(data, na.rm = TRUE),
              .groups = "drop") %>%
    collect()
  
  list_result[[i]] <- df_aux %>% 
    left_join(df_aux_pairs, by = c("from_id", "to_id"))
  
  point_o <- sc_points %>% 
    filter(id == vec_origin[i])
  
  vec_dest <- df_aux$to_id
  
  points_d <- sc_points %>% 
    filter(id %in% vec_dest)
  
  list_aux <- list()
  for (j in seq_along(vec_dest)) {
    list_aux[[j]] <- osrmRoute(
      src = point_o,
      dst = points_d[j,],
      overview = "full") %>% 
      mutate(src = point_o$id,
             dst = points_d[j,]$id)
  }
  
  list_routes[[i]] <- bind_rows(list_aux)
  
  print(i)
}

saveRDS(list_routes,
        "data/intermediate/osrm/list_routes.RDS")

saveRDS(list_result,
        "data/intermediate/osrm/list_pairs_od.RDS")
