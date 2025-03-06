list_routes <- readRDS("data/intermediate/osrm/list_routes.RDS")

list_result <- readRDS("data/intermediate/osrm/list_pairs_od.RDS")

# Filter out data frames with zero rows or zero columns
filtered_dfs <- list_routes %>%
  keep(~ nrow(.x) > 0 && ncol(.x) > 0)
df_routes <- bind_rows(filtered_dfs)

filtered_dfs <- list_result %>%
  keep(~ nrow(.x) > 0 && ncol(.x) > 0)
df_pairs <- bind_rows(filtered_dfs)

df_pairs <- df_pairs %>% 
  mutate(n_per_day = n_vehicles/n_date)

df_pairs %>% 
  filter(n_per_day < quantile(n_per_day, 0.95)) %>% 
  ggplot(aes(y = n_per_day)) +
  geom_boxplot()

df_pairs %>% 
  filter(n_per_day >= quantile(n_per_day, 0.95)) %>% 
  ggplot(aes(y = n_per_day)) +
  geom_boxplot()

write_sf(
  df_routes,
  "data/intermediate/osrm/routes_od.gpkg")

write_parquet(
  df_pairs,
  "data/intermediate/osrm/pairs_od.parquet")
