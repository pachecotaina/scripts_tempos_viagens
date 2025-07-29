df_pairs <- read_parquet("data/intermediate/osrm/pairs_od.parquet") %>% 
  mutate(pair_od = paste0(from_id, "-", to_id))

vec_od <- unique(df_pairs$pair_od)

# puxar os dados de tempo de deslocamento entre os pares
vec_days <- tibble(
  "date" = seq.Date(
    as.Date("2015-01-01", "%Y-%m-%d"), 
    as.Date("2015-07-19", "%Y-%m-%d"), 
    by = 1)) %>% 
  mutate(weekday = weekdays(date)) %>% 
  filter(weekday %notin% c("Saturday", "Sunday") & date %notin% bank_holiday$date) %>% 
  mutate(date = str_replace_all(as.character(date), "-", "")) %>% 
  pull(date)

df_matrix <- open_dataset(
  paste0("/Users/tainasouzapacheco/Library/CloudStorage/Dropbox/Academico/UAB/tese/ch_overpass/data/intermediate/matrix_grouped/", vec_days, ".parquet")) %>% 
  mutate(pair_od = paste0(from_id, "-", to_id)) %>% 
  filter(pair_od %in% vec_od) %>% 
  group_by(pair_od, data) %>% 
  summarise(n = 1) %>% 
  count(pair_od) %>% 
  collect() 

# que apareçam pelo menos em 5 dias na base
length(unique(df_matrix$pair_od))
hist(df_matrix$n)
summary(df_matrix$n)
quantile(df_matrix$n, 0.25)

df_matrix <- df_matrix %>% 
  filter(n >= quantile(df_matrix$n, 0.25))
hist(df_matrix$n)

length(unique(df_matrix$pair_od))
length(unique(str_sub(df_matrix$pair_od, 1, 4)))
length(unique(str_sub(df_matrix$pair_od, 6, 10)))

# shp_routes <- read_sf("data/intermediate/osrm/routes_od.gpkg")  
shp_routes <- read_sf("data/intermediate/osrm/routes_od_streets_2015.gpkg") %>%
  rename(from_id = src, to_id = dst) %>% 
  mutate(pair_od = paste0(from_id, "-", to_id)) %>% 
  filter(pair_od %in% df_matrix$pair_od)

tm_shape(shp_routes) +
  tm_lines(col = "red", alpha = 0.1)

shp_vias <- read_sf(
  "data/input/vias_vel_reduz/ViasVelocidadeReduzida_Bloomberg_SIRGAS200023S.shp") %>% 
  st_transform(4326) %>% 
  filter(
    data_vigor >= as.Date("01-01-2015")) %>% 
  filter(data_vigor <= as.Date("2016-12-31"))

tm_shape(shp_vias) +
  tm_lines()

# ############################################################################ #
####              Classify routes between treated and control               ####
# ############################################################################ #
df_result <- read_sf("data/intermediate/osrm/routes_od_streets_2015.gpkg") %>% 
  rename(from_id = src, to_id = dst) %>%
  mutate(pair_od = paste0(from_id, "-", to_id)) %>% 
  filter(pair_od %in% df_matrix$pair_od)

length(unique(df_result$pair_od)) == length(unique(df_matrix$pair_od))

# Compute the length of each segment
df_result$length <- as.numeric(st_length(df_result))

# many segments are really small
hist(df_result$length)
summary(df_result$length)
hist(df_result$length[df_result$length<=8])
summary(df_result$length[df_result$length<=8])

df_result <- df_result %>% 
  filter(length >= 100)

# length(unique(df_result$pair_od)) # originally 5315
# 100m >> 5132 observações
#  80m >> 5149 observações
#  50m >> 5171 observações
#  40m >> 5177 observações
#  30m >> 5184 observações
#  20m >> 5194 observações
#  15m >> 5194 observações
#  10m >> 5309 observações

hist(df_result$distance)
summary(df_result$distance)

hist(df_result$distance[df_result$distance <=1])
summary(df_result$distance[df_result$distance <=1])

df_result <- df_result %>% 
  filter(distance >= 1)
length(unique(df_result$pair_od))

# ############################################################################ #
####            teste 1: filtrar pelo número de vias da rota                ####
# ############################################################################ #
# teste <- df_result %>% 
#   st_set_geometry(NULL) %>% 
#   group_by(pair_od, duration, distance, cvc_codlog, cvc_nmlogr) %>% 
#   summarize(
#     n = n(),
#     length = sum(length),
#     .groups = "drop") %>% 
#   add_count(pair_od, duration, distance) %>% 
#   filter(nn == 1)
# 
# length(unique(teste$pair_od))
# 
# tm_shape(df_result %>% filter(pair_od %in% unique(teste$pair_od)))+
#   tm_lines()
# 
# teste <- teste %>% 
#   mutate(
#     status = ifelse(cvc_codlog %in% shp_vias$codlog, 
#                     "Treated", 
#                     "Control"))
# 
# table(teste$status)
# 
# tm_shape(df_result %>% filter(pair_od %in% unique(teste$pair_od)) %>% 
#            left_join(teste %>% select(pair_od, status), by = "pair_od"))+
#   tm_lines(col = "status")

# ############################################################################ #
####      teste 2: filtrar rotas com status de tratamento homogêneo         ####
# ############################################################################ #
teste <- df_result %>% 
  st_set_geometry(NULL) %>% 
  group_by(pair_od, duration, distance, cvc_codlog, cvc_nmlogr) %>% 
  filter(!is.na(cvc_codlog)) %>% 
  summarize(
    n = n(),
    total_length = sum(.data[["length"]], na.rm = TRUE),
    .groups = "drop") %>% 
  add_count(pair_od, duration, distance)  %>% 
  mutate(status = ifelse(
    cvc_codlog %in% shp_vias$codlog,1, 0)) %>% 
  group_by(pair_od, duration, distance) %>% 
  mutate(
    status_sum = sum(status),
    status_length = sum(total_length/1000 * status)) %>% 
  mutate(
    status_n = status_sum/nn,
    status_pct = status_length/distance,
    status_pct = ifelse(status_pct > 1 | status_n == 1, 1, status_pct))

# hist(teste$status_n)
# hist(teste$status_pct)

teste %>% 
  distinct(pair_od, status_pct) %>% 
  ggplot(aes(x = status_pct)) +
  geom_histogram() +
  scale_x_continuous(labels = scales::percent_format()) +
  labs(
    x = "Proporção da rota em vias com velocidades alteradas",
    y = "Número de observações"
  )+
  my_theme

ggsave(
  paste0("figures/hist_pct_rota_tratadas.png"), 
  plot = last_plot(), 
  width = 7, 
  height = 4)

teste <- teste %>% 
  filter(status_n == 1 | status_n == 0) %>% 
  mutate(status = ifelse(status_n == 1, "Treated", "Control")) %>% 
  ungroup()

length(unique(teste$pair_od))
teste %>% distinct(pair_od, status) %>% count(status)

write_parquet(
  teste,
  "data/intermediate/osrm/pairs_od_sample.parquet")

# teste <- read_parquet("data/intermediate/osrm/pairs_od_sample.parquet")

# tm_shape(df_result %>% filter(pair_od %in% unique(teste$pair_od)) %>% 
#            left_join(teste %>% select(pair_od, status), by = "pair_od"))+
#   tm_lines(col = "status")

# número único de pares OD
# length(unique(teste$pair_od))

# número de tratados e de controle
# teste %>% 
#   distinct(pair_od, status) %>% 
#   count(status)

# teste %>% 
#   left_join(
#     shp_class %>% 
#       st_set_geometry(NULL) %>% 
#       distinct(cvc_codlog, cvc_classe),
#     by = "cvc_codlog") %>% 
#   distinct(pair_od, cvc_classe, status) %>% 
#   count(status, cvc_classe) %>% 
#   group_by(status) %>% 
#   mutate(nn = sum(n)) %>% 
#   ungroup() %>% 
#   mutate(pct = n/nn)