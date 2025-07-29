list_routes <- readRDS("data/intermediate/osrm/list_routes.RDS")
df_routes <- bind_rows(list_routes)

list_result <- readRDS("data/intermediate/osrm/list_pairs_od.RDS")
df_pairs <- bind_rows(list_result)

# length(df_pairs %>% mutate(pair_od = paste0(from_id, "-", to_id)) %>% 
#   count(pair_od) %>% pull(pair_od)) # 16005

# Filter out data frames with zero rows or zero columns
filtered_dfs <- list_routes %>%
  keep(~ nrow(.x) > 0 && ncol(.x) > 0)
df_routes <- bind_rows(filtered_dfs)

filtered_dfs <- list_result %>%
  keep(~ nrow(.x) > 0 && ncol(.x) > 0)
df_pairs <- bind_rows(filtered_dfs)

df_pairs <- df_pairs %>% 
  mutate(n_per_day = n_vehicles/n_date)

summary(df_pairs$n_per_day)
summary(df_routes$distance)
summary(df_routes$duration)

df_pairs %>% 
  filter(n_per_day < quantile(n_per_day, 0.95)) %>% 
  ggplot(aes(y = n_per_day)) +
  geom_boxplot()

df_pairs %>% 
  filter(n_per_day >= quantile(n_per_day, 0.95)) %>% 
  ggplot(aes(y = n_per_day)) +
  geom_boxplot()

df_pairs %>% 
  filter(n_per_day < quantile(n_per_day, 0.90)) %>% 
  ggplot(aes(x = n_per_day)) +
  geom_histogram()

write_sf(
  df_routes,
  "data/intermediate/osrm/routes_od.gpkg")

write_parquet(
  df_pairs,
  "data/intermediate/osrm/pairs_od.parquet")
