# ############################################################################ #
####               DEFINE TREATMENT STATUS FOR SPEED CAMERAS                ####
# ############################################################################ #
# abrir vias com velocidades alteradas
shp_vias <- read_sf(
  "data/input/vias_vel_reduz/ViasVelocidadeReduzida_Bloomberg_SIRGAS200023S.shp") %>% 
  st_transform(4326) %>% 
  filter(
    data_vigor >= as.Date("01-01-2015")) %>% 
  filter(data_vigor <= as.Date("2016-12-31")) %>% 
  filter(ano_vigor >= 2015 & ano_vigor <= 2016) %>% 
  st_transform(31983) %>% 
  st_buffer(30) %>% 
  st_transform(4326)

# limpar base de radares
shp_radares <- helper_geo %>% 
  distinct(id, .keep_all = TRUE) %>% 
  filter(str_sub(id, 1, 1) != "1")

df_radares_join <- shp_radares %>% 
  st_join(shp_vias) %>% 
  st_set_geometry(NULL) %>% 
  group_by(id) %>% 
  reframe(
    data_vigor = min(data_vigor),
    codlog = codlog,
    rua = Rua,
    data_ativacao = min(data_ativacao),
    vel_aps = mean(veloc_aps)) %>% 
  distinct(id, codlog, rua, data_vigor, data_ativacao, vel_aps) %>% 
  filter(!is.na(data_vigor)) %>% 
  add_count(id)
length(unique(df_radares_join$id))

#' em nenhum dos casos em que o join resultou em duas observações por radar a 
#' data_vigor de uma das vias é diferente da data_vigor da outra via. Assim,
#' vou dar um distinct e manter apenas uma data por radar.
df_radares_join <- df_radares_join %>% 
  distinct(id, data_vigor, data_ativacao, vel_aps)
length(unique(df_radares_join$id))

#' excluir as vias que tiveram alteração nos meses 3 e 6, pois nas outras 
#' análises elas geram muito ruído (tem pouca observação pré mudança)
df_radares_join <- df_radares_join %>% 
  mutate(
    mes_vigor = month(data_vigor),
    ano_vigor = year(data_vigor),
    mes_ano = floor_date(data_vigor, unit = "month"),
    ativo_before = as.numeric(data_vigor - data_ativacao),
    treat = 1) %>% 
  filter(mes_vigor %notin% c(3, 6))
length(unique(df_radares_join$id))

#' excluir os radares que foram instalados depois das mudanças de velocidade
df_radares_join <- df_radares_join %>% 
  filter(ativo_before > 0)
length(unique(df_radares_join$id))
table(df_radares_join$vel_aps)

#' excluir os radares que pegaram vias diferentes
# df_radares_join <- df_radares_join %>% 
#   filter(vel_aps %in% unique(shp_vias$veloc_aps))
# length(unique(df_radares_join$id))

#' fazer gráfico de barras
df_radares_join %>% 
  count(mes_ano) %>% 
  ggplot(aes(x = mes_ano, y = n)) +
  geom_col(fill = "grey30") +
  geom_text(aes(label = n), vjust = 1.5, size = 5, color = "white") +
  labs(
    x = "Mês/Ano",
    y = "Número de radares"
  ) +
  scale_x_date(date_labels = "%b %Y", date_breaks = "1 month") +
  my_theme +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave(
  paste0("figures/radares_data_vigor.png"), 
  plot = last_plot(), 
  width = 12, 
  height = 5)

##############
df_radares_join <- shp_radares %>% 
  st_join(shp_vias) %>% 
  st_set_geometry(NULL) %>% 
  group_by(id) %>% 
  reframe(
    data_vigor = min(data_vigor),
    codlog = codlog,
    rua = Rua,
    data_ativacao = min(data_ativacao),
    veloc_aps = mean(veloc_aps)) %>% 
  distinct(id, codlog, rua, data_vigor, data_ativacao, veloc_aps) %>% 
  filter(!is.na(data_vigor)) %>% 
  add_count(id) %>% 
  distinct(id, data_vigor, data_ativacao, veloc_aps) %>% 
  mutate(
    mes_vigor = month(data_vigor),
    ano_vigor = year(data_vigor),
    mes_ano = floor_date(data_vigor, unit = "month"),
    ativo_before = as.numeric(data_vigor - data_ativacao),
    treat = 1)

df_radares <- shp_radares %>% 
  st_set_geometry(NULL) %>% 
  distinct(id, data_ativacao) %>% 
  left_join(df_radares_join, by = c("id", "data_ativacao")) %>% 
  mutate(
    treat = ifelse(is.na(treat), 0, treat)) %>% 
  filter(mes_vigor %notin% c(3, 6)) %>% 
  filter(ativo_before > 0 | is.na(ativo_before)) %>% 
  filter(data_ativacao < as.Date("2016-12-31")) #%>% 
  # filter(veloc_aps %in% unique(shp_vias$veloc_aps) | is.na(veloc_aps))

summary(df_radares$ativo_before)
table(df_radares$treat)

# ############################################################################ #
####              OPEN PARQUET FILE WITH DATA EVERY 5 MINUTES               ####
# ############################################################################ #
if (!file.exists("data/intermediate/radares_5min_vol.parquet")) {
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
  ids_radares <- unique(df_radares$id)
  
  # Function to process each file
  process_file_df <- function(path) {
    df <- open_dataset(path) %>%
      filter(id %in% ids_radares) %>%
      collect() %>% 
      mutate(data = as.Date(as.character(data), format = "%Y%m%d")) %>%
      group_by(id, data) %>%
      summarise(
        volume = sum(volume, na.rm = TRUE),
        n_hours = n_distinct(hora),
        .groups = "drop"
      )
  }
  
  # Plan parallelism
  plan(multisession, workers = 8)  
  
  # Run parallel version of map_dfr
  df_result <- future_map_dfr(valid_files, process_file_df, .progress = TRUE)
  
  write_parquet(
    df_result,
    "data/intermediate/radares_5min_vol.parquet")
} else{
  df_result <- read_parquet("data/intermediate/radares_5min_vol.parquet")
}

# ############################################################################ #
####      CLEAN DATA BASED ON THE NUMBER OF OBS BEFORE JUL 20TH, 2015       ####
# ############################################################################ #
df_result <- df_result %>% 
  add_count(id)

summary(df_result$n)

df_result <- df_result %>% 
  group_by(id) %>% 
  mutate(
    data_min = min(data),
    data_max = max(data)) %>% 
  ungroup()

# # testes para definir corte
# df_result_filter <- df_result %>%
#   filter(data_min <= as.Date("2015-07-19")) %>%
#   filter(data <= as.Date("2015-07-19")) %>%
#   select(-n) %>%
#   add_count(id)
# 
# length(unique(df_result_filter$id))
# summary(df_result_filter$n)
# # Min. 1st Qu.  Median    Mean 3rd Qu.    Max.
# # 1.0   158.0   186.0   168.9   197.0   200.0
# 
# df_result_filter <- df_result_filter %>%
#   filter(n <= quantile(n, 0.25))
# summary(df_result_filter$n)
# # Min. 1st Qu.  Median    Mean 3rd Qu.    Max.
# # 1.0    96.0   119.0   113.8   139.0   158.0
# 
# df_result_filter <- df_result_filter %>%
#   filter(n <= quantile(n, 0.25))
# summary(df_result_filter$n)
# # Min. 1st Qu.  Median    Mean 3rd Qu.    Max.
# # 1.00   59.00   74.00   70.53   86.00   96.00

#########
df_result_filter <- df_result %>% 
  filter(data_min <= as.Date("2015-07-19")) %>% 
  filter(data <= as.Date("2015-07-19")) %>% 
  select(-n) %>% 
  add_count(id) 
length(unique(df_result_filter$id))

df_result_filter <- df_result_filter %>% 
  filter(n >= 60)
length(unique(df_result_filter$id))

df_result <- df_result %>% 
  filter(id %in% unique(df_result_filter$id)) %>% 
  filter(n > 100)

df_result %>% distinct(id, n) %>% 
  ggplot(aes(x = n)) +
  geom_histogram() +
  my_theme

summary(df_result$n)
length(unique(df_result$id))
# Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
# 350.0   649.0   700.0   681.1   719.0   731.0

# exclude weekends
df_result <- df_result %>% 
  mutate(
    weekday = weekdays(data)) %>% 
  filter(weekday %notin% c("Sábado", "Domingo", "Saturday", "Sunday"))

df_result <- df_result %>%
  mutate(
    week = isoweek(data), 
    year = isoyear(data)         
  )

max_week <- unique(
  df_result %>% filter(year == 2015) %>% filter(week == max(week)) %>% pull(week))

df_result <- df_result %>%
  mutate(
    week = ifelse(year == 2016, week + max_week, week))

unique(df_result$week)

df_result <- df_result %>%
  mutate(month = month(data)) %>% 
  group_by(week) %>% 
  mutate(
    month = min(month),
    month = ifelse(year == 2016, month + 12, month)) %>% 
  ungroup()

df_result_week <- df_result %>% 
  group_by(id, week, month) %>% 
  reframe(
    volume = mean(volume, na.rm = TRUE),
    n_hours = mean(n_hours, na.rm = TRUE),
    n = n()) %>% 
  mutate(
    volume_hour = volume/n_hours)

glimpse(df_radares)
df_radares <- df_radares %>% 
  select(-ativo_before) %>%
  filter(id %in% unique(df_result_week$id))

df_result_week <- df_result_week %>% 
  left_join(
    df_radares, by = join_by(id))

df_result_week <- df_result_week %>% 
  mutate(
    mes_vigor = ifelse(is.na(mes_vigor), 0, mes_vigor),
    ano_vigor = ifelse(is.na(ano_vigor), 0, ano_vigor),
    id_num = as.numeric(id))

table(df_result_week$mes_vigor)
table(df_result_week$month)
df_result_week %>% distinct(id, treat) %>% count(treat)
length(unique(df_result_week$id))

write_parquet(
  df_result_week,
  "data/intermediate/radares_5min_vol_week_averages.parquet")


######
df_result_day <- df_result %>% 
  mutate(
    volume_hour = volume/n_hours)

df_result_day <- df_result_day %>% 
  left_join(
    df_radares, by = join_by(id))

df_result_day <- df_result_day %>% 
  mutate(
    mes_vigor = ifelse(is.na(mes_vigor), 0, mes_vigor),
    ano_vigor = ifelse(is.na(ano_vigor), 0, ano_vigor),
    id_num = as.numeric(id))

table(df_result_day$mes_vigor)
table(df_result_day$month)
df_result_day %>% distinct(id, treat) %>% count(treat)
names(df_result_day)

write_parquet(
  df_result_day,
  "data/intermediate/radares_5min_vol_day_averages.parquet")

# ############################################################################ #
####                       MAKE MAP OF SPEED CAMERAS                        ####
# ############################################################################ #
shp_radares <- helper_geo %>% 
  filter(id %in% df_radares$id) %>% 
  distinct(id, .keep_all = TRUE) %>% 
  # left_join(df_result_day %>% distinct(id, treat), by = "id")
  left_join(df_radares %>% distinct(id, treat), by = "id") %>% 
  mutate(Grupo = ifelse(treat == 1, "Tratado", "Controle"))

tm_shape(shp_vias) +
  # tm_lines() +
  tm_polygons() +
  tm_shape(shp_radares) +
  tm_dots(col = "Grupo")
