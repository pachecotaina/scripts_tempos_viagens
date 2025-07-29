# ############################################################################ #
####                                LOAD DATA                               ####
# ############################################################################ #
vec_dep_hours <- 0:23  # Or a custom subset

list_hourly_data <- list()

for (h in seq_along(vec_dep_hours)) {
  list_hourly_data[[h]] <- read_parquet(
    paste0("data/output/df_dep_hour_hour_", vec_dep_hours[h], ".parquet")
  ) %>% 
    distinct(pair_od, month_treat, treat)
}

df_all <- bind_rows(list_hourly_data) %>% 
  distinct(pair_od, month_treat, treat)

df_all %>% count(month_treat)

# ############################################################################ #
####                                 MAPA                                   ####
# ############################################################################ #
vec_meses <- df_all %>% 
  count(month_treat) %>% 
  filter(month_treat != 0) %>% 
  pull(month_treat)

shp_rotas <- read_sf(
  "data/intermediate/osrm/routes_od_streets_2015.gpkg") %>% 
  mutate(
    pair_od = paste0(src, "-", dst))

df_pairod <- read_parquet(
  "data/intermediate/osrm/pairs_od_sample.parquet")

list_map <- list()
for (i in seq_along(vec_meses)) {
  df_aux <- df_all %>% 
    filter(month_treat == 0 | month_treat == vec_meses[i])
  
  df_map <- shp_rotas %>% 
    filter(pair_od %in% df_aux$pair_od) %>% 
    left_join(
      df_aux %>% distinct(pair_od, treat),
      by = "pair_od") %>% 
    mutate(treat = ifelse(treat == 0, "Controle", "Tratada"))
  
  df_map  %>% 
    ggplot() +
    geom_sf(aes(color = treat), alpha = 0.5) +
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
    paste0("figures/map_vias_tratadas_controle_grupo", vec_meses[i],".png"), 
    plot = last_plot(), 
    width = 6, 
    height = 6)
  
  list_map[[i]] <- df_map %>% 
    mutate(grupo = vec_meses[i])
}

df_map_all <- bind_rows(list_map)

df_map_all  %>% 
  ggplot() +
  geom_sf(aes(color = treat), alpha = 0.5) +
  labs(
    color = "Tipo de rota:"
  ) +
  facet_wrap(~grupo, ncol = 2) +
  # geom_sf(
  #   data = helper_geo %>% filter(id %in% c(df_map$from_id, df_map$to_id)),
  #   color = "black") +
  my_theme_map +
  my_scale +
  my_north_arrow

ggsave(
  paste0("figures/map_vias_tratadas_controle_grupoALL.png"), 
  plot = last_plot(), 
  width = 12, 
  height = 12)

# ############################################################################ #
####                           MAPA: 07 versus other                        ####
# ############################################################################ #
# df control
df_aux <- df_all %>% 
  filter(month_treat == 0)

df_map_control <- shp_rotas %>% 
  filter(pair_od %in% df_aux$pair_od) %>% 
  left_join(
    df_aux %>% distinct(pair_od, treat, month_treat),
    by = "pair_od") %>% 
  mutate(
    treat = ifelse(treat == 0, "Controle", "Tratada"),
    group = "Controle")

# df control
df_aux <- df_all %>% 
  filter(month_treat != 0)

df_map_07others <- shp_rotas %>% 
  filter(pair_od %in% df_aux$pair_od) %>% 
  left_join(
    df_aux %>% distinct(pair_od, treat, month_treat),
    by = "pair_od") %>% 
  mutate(
    treat = ifelse(treat == 0, "Controle", "Tratada"),
    group = ifelse(month_treat == 7, "Julho/2015", "Demais meses"))

df_map_07others <- df_map_07others %>% 
  rbind(df_map_control)

df_map_07others  %>% 
  ggplot() +
  geom_sf(aes(color = group), alpha = 0.5) +
  labs(
    color = "Tipo de rota:"
  ) +
  my_theme_map +
  my_scale +
  my_north_arrow

ggsave(
  paste0("figures/map_vias_tratadas_controle_grupo_07others.png"), 
  plot = last_plot(), 
  width = 8, 
  height = 8)
