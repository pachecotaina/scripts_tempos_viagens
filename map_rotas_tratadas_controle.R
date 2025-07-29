shp_rotas <- read_sf(
  "data/intermediate/osrm/routes_od_streets_2015.gpkg") %>% 
  mutate(
    pair_od = paste0(src, "-", dst))

df_pairod <- read_parquet(
  "data/intermediate/osrm/pairs_od_sample.parquet")

df_map <- shp_rotas %>% 
  filter(pair_od %in% df_pairod$pair_od) %>% 
  left_join(
    df_pairod %>% distinct(pair_od, status),
    by = "pair_od")

df_map %>% 
  mutate(status = ifelse(status == "Control", "Controle", "Tratada")) %>% 
  # distinct(cvc_codlog, status) %>% 
  ggplot() +
  geom_sf(aes(color = status), alpha = 0.5) +
  labs(
    color = "Tipo de rota:"
  ) +
  # geom_sf(
  #   data = helper_geo %>% filter(id %in% c(df_map$from_id, df_map$to_id)),
  #   color = "black") +
  my_theme_map +
  my_scale +
  my_north_arrow

ggsave(
  paste0("figures/map_vias_tratadas_controle.png"), 
  plot = last_plot(), 
  width = 6, 
  height = 6)
