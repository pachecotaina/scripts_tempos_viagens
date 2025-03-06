# ############################################################################ #
####                FINAL DT: from_id, to_id, tp_from, tp_to                ####
# ############################################################################ #
#' data/input/SIRGAS_SHP_classeviariacet/SIRGAS_SHP_classeviariacet.shp
#' helper_geo
#' TO DO
#'  ook 1) Decidir qual base usar >>> classificacao viaria CET
#'  ook 2) Excluir vias muito pequenas (manter: arteriais, coletoras...)
#'  ook 3) Sobrepor rotas entre radares às vias
#'  

# ############################################################################ #
####                            Open street network                         ####
# ############################################################################ #
shp_class <- read_sf("data/input/SIRGAS_SHP_classeviariacet/SIRGAS_SHP_classeviariacet.shp") %>% 
  st_set_crs(31983) %>% 
  st_transform(4326)
names(shp_class)
table(shp_class$cvc_classe)

# Excluding LOCAL and VIA DE PEDESTRES streets
shp_class <- shp_class %>%
  filter(cvc_classe %in% c("ARTERIAL", "COLETORA", "RODOVIA", "VTR") |
           cvc_nomelg == "R   ANTONIO JOSE ANACLETO")

tm_shape(shp_class) +
  tm_lines()

# ############################################################################ #
####      Open street network - vias velocidade reduzida bloomberg          ####
# ############################################################################ #
shp_vias <- read_sf(
  "data/input/vias_vel_reduz/ViasVelocidadeReduzida_Bloomberg_SIRGAS200023S.shp") %>% 
  st_transform(4326)

# tm_shape(shp_vias) +
#   tm_lines() +
#   tm_shape(helper_geo) +
#   tm_dots(col = "red")

# ############################################################################ #
####                  Open routes OD between speed cameras                  ####
# ############################################################################ #
# looking at speed cameras that existed in 2015
df_pairs <- read_parquet("data/intermediate/osrm/pairs_od.parquet") %>% 
  filter(year(min_date) == 2015 & month(min_date) <= 7)

vec_origin <- unique(df_pairs$from_id)

shp_routes <- read_sf("data/intermediate/osrm/routes_od.gpkg")

# tm_shape(shp_vias) +
#   tm_lines() +
#   tm_shape(shp_routes) +
#   tm_lines(col = "red", alpha = 0.5)

list_result <- list()
for (i in seq_along(vec_origin)) {
  shp_aux <- shp_routes %>% 
    filter(src == vec_origin[[i]])
  
  shp_aux_buff <- shp_aux %>% 
    st_transform(31983) %>% 
    st_buffer(200) %>% 
    st_transform(4326) %>% 
    st_union() %>% 
    st_make_valid()
  
  shp_class_buff <- shp_class %>% 
    st_intersection(shp_aux_buff) %>% 
    st_make_valid()
  
  list_result[[i]] <- shp_aux %>% 
    st_transform(31983) %>% 
    st_buffer(7) %>% 
    st_transform(4326) %>% 
    st_intersection(shp_class_buff) %>% 
    st_make_valid()
  
  print(i)
}

df_result <- bind_rows(list_result) 

write_sf(
  df_result,
  "data/intermediate/osrm/routes_od_streets_2015.gpkg")

tm_shape(shp_aux_buff) +
  tm_polygons() +
  tm_shape(shp_class_buff) +
  tm_lines() +
  tm_shape(shp_aux) +
  tm_lines(col = "red")

tm_shape(shp_aux %>% filter(dst == "2419")) +
  tm_lines()+
  tm_shape(shp_aux_int %>% filter(dst == "2419")) +
  tm_lines(col = "red")

tm_shape(shp_class_buff) +
  tm_lines()+
  tm_shape(shp_aux) +
  tm_lines(col = "blue") +
  tm_shape(shp_aux_int) +
  tm_lines(col = "red")

# ############################################################################ #
####       CASE STUDY: Marginal Pinheiros (Pq Povo >> Cebolão)              ####
# ############################################################################ #
vec_sc_marginal <- c(
  "2445", "2895", "2700", "2699", "2631", "2878", "2876", "2446", "2447", "2632", 
  "2860")

teste <- df_result %>% 
  filter(src %in% vec_sc_marginal & dst %in% vec_sc_marginal)

teste_routes <- shp_routes %>% 
  filter(src %in% vec_sc_marginal & dst %in% vec_sc_marginal) %>% 
  mutate(pair_od = paste0(src, "-", dst))

vec_od <- unique(teste_routes$pair_od)

# puxar os dados de tempo de deslocamento entre os pares
vec_days <- tibble(
  "date" = seq.Date(
    as.Date("2015-01-01", "%Y-%m-%d"), 
    as.Date("2015-12-31", "%Y-%m-%d"), 
    by = 1)) %>% 
  mutate(weekday = weekdays(date)) %>% 
  filter(weekday %notin% c("Saturday", "Sunday") & date %notin% bank_holiday$date) %>% 
  mutate(date = str_replace_all(as.character(date), "-", "")) %>% 
  pull(date)

df_matrix <- open_dataset(
  paste0("data/intermediate/matrix_grouped/", vec_days, ".parquet")) %>% 
  mutate(pair_od = paste0(from_id, "-", to_id)) %>% 
  filter(pair_od %in% vec_od) %>% 
  collect() %>% 
  add_count(pair_od)

vec_df_matrix_from <- unique(df_matrix$from_id)
vec_df_matrix_to <- unique(df_matrix$to_id)
vec_df_matrix_od <- unique(df_matrix$pair_od)

df_pair_od <- df_matrix %>% 
  # mutate(date = as.Date(data, "%Y%m%d")) %>% 
  group_by(pair_od) %>% 
  summarize(min_date = min(data),
            max_date = max(data))

df_matrix_2631_2446 <- df_matrix %>% 
  filter(n == max(n) & dep_hour %in% c(17, 18, 19, 01, 02, 03, 04, 05)) %>% 
  mutate(time = ifelse(dep_hour %in% c(17, 18, 19), "Peak", "Off-peak"))
hist(df_matrix_2631_2446$dur_obs_mean)
tm_shape(teste_routes %>% filter(src == "2631" & dst == "2446")) +
  tm_lines(col = "blue") +
  tm_shape(helper_geo %>% filter(id %in% c("2631", "2446"))) +
  tm_dots(col = "red")

# codlog Marginal: 062383
df_matrix_2445_2699 <- df_matrix %>% 
  filter(pair_od == "2445-2699" & dep_hour %in% c(17, 18, 19))
hist(df_matrix_2445_2699$dur_obs_mean)
hist(df_matrix_2445_2699$dur_obs_mean[df_matrix_2445_2699$dur_obs_mean<=15])
tm_shape(teste_routes %>% filter(src == "2445" & dst == "2699")) +
  tm_lines(col = "blue") +
  tm_shape(helper_geo %>% filter(id %in% c("2445", "2699"))) +
  tm_dots(col = "red")

df_matrix_2445_2699 <- df_matrix_2445_2699 %>% 
  mutate(
    period = ifelse(
      as.Date(data, "%Y%m%d") > as.Date("2015-07-20"), "2.After", "1.Before"))
table(df_matrix_2445_2699$period)

df_matrix_2445_2699 %>% 
  ggplot(aes(x = dur_obs_mean)) +
  geom_histogram() +
  facet_wrap(~period)

df_matrix_2445_2699 %>% 
  group_by(period) %>% 
  summarize(
    dur_mean = mean(dur_obs_mean),
    n = n())

df_matrix_2445_2699 %>% 
  ggplot(aes(x = as.Date(data, "%Y%m%d"), y = dur_obs_mean)) +
  geom_point(alpha = 0.5) +
  geom_vline(xintercept = as.Date("2015-07-20"), col = "red", linetype = "dashed")

df_matrix_2631_2446 %>% 
  filter(dur_obs_mean <20) %>% 
  ggplot(aes(x = as.Date(data, "%Y%m%d"), y = dur_obs_mean)) +
  geom_point(alpha = 0.5) +
  geom_vline(xintercept = as.Date("2015-07-20"), col = "red", linetype = "dashed") +
  facet_wrap(~time)

# teste_vias <- shp_vias %>% 
#   filter(codlog %in% c(teste$cvc_codlog))
# teste <- teste %>% 
#   st_intersection(teste_vias)

# tm_shape(teste_vias) +
#   tm_lines() +
tm_shape(teste_routes %>% filter(src == "2631" & dst == "2446")) +
  tm_lines(col = "blue") +
  tm_shape(helper_geo %>% filter(id %in% c("2631", "2446"))) +
  tm_dots(col = "red")

# sc_points <- helper_geo %>% 
#   count(id) %>% 
#   select(-n) %>% 
#   filter(id %in% vec_sc_marginal) %>% 
#   st_transform(31983) %>% 
#   st_buffer(4) %>% 
#   st_transform(4326)
# 
# teste_routes2 <- teste_routes %>% 
#   st_intersection(sc_points)
# 
# teste_routes2 <- teste_routes2 %>% 
#   add_count(src, dst) %>% 
#   filter(n <= 2) %>% 
#   group_by(pair_od) %>% 
#   slice(1)
# 
# teste_routes <- teste_routes %>% 
#   filter(pair_od %in% teste_routes2$pair_od)
# 
# tm_shape(teste_routes) +
#   tm_lines(col = "pair_od") +
#   tm_shape(helper_geo %>% filter(id %in% vec_sc_marginal)) +
#   tm_dots(col = "red")

# ############################################################################ #
####                  Open routes OD between speed cameras                  ####
# ############################################################################ #
sc_points <- helper_geo %>% 
  count(id) %>% 
  select(-n) %>% 
  filter(!str_starts(id, "1")) %>% 
  st_transform(31983) %>% 
  st_buffer(4) %>% 
  st_transform(4326)

teste <- shp_routes %>% 
  slice(1:100) %>% 
  st_intersection(sc_points)

tm_shape(teste %>% filter(src == "2403" & dst == "2419")) +
  tm_lines() +
  tm_shape(shp_routes %>% filter(src == "2403" & dst == "2419"))+
  tm_lines() +
  tm_shape(helper_geo %>% filter(id %in% c("2403", "2419", "2672"))) +
  tm_dots(col = "red")
