arrow::set_cpu_count(8)  # Use multiple threads
arrow::default_memory_pool()


# ############################################################################ #
####        DEFINIR STATUS DE CONTROLE OU TRATAMENTO PARA AS ROTAS          ####
# ############################################################################ #
# definir intervalo para análise
vec_days <- tibble(
  "date" = seq.Date(
    as.Date("2015-01-01", "%Y-%m-%d"), 
    as.Date("2016-03-31", "%Y-%m-%d"), 
    by = 1)) %>% 
  mutate(weekday = weekdays(date)) %>% 
  filter(weekday %notin% c("Saturday", "Sunday") & date %notin% bank_holiday$date) %>% 
  mutate(date = str_replace_all(as.character(date), "-", "")) %>% 
  pull(date)

# abrir rotas de interesse
df_routes <- read_parquet(
  "data/intermediate/osrm/pairs_od_sample.parquet")

# abrir vias com velocidades alteradas
shp_vias <- read_sf(
  "data/input/vias_vel_reduz/ViasVelocidadeReduzida_Bloomberg_SIRGAS200023S.shp") %>% 
  st_transform(4326)

# unir bancos
df_vias <- shp_vias %>% 
  st_set_geometry(NULL) %>% 
  filter(ano_vigor > 2014) %>% 
  distinct(codlog, veloc_aps, data_vigor, ano_vigor) %>% 
  rename(cvc_codlog = codlog) %>% 
  add_count(cvc_codlog) %>% 
  na.omit() %>% 
  group_by(cvc_codlog, data_vigor, ano_vigor) %>% 
  summarize(veloc_aps = max(veloc_aps), .groups = "drop") %>% 
  group_by(cvc_codlog) %>% 
  summarize(
    veloc_aps = max(veloc_aps), 
    data_vigor = min(data_vigor),
    .groups = "drop")

df_routes <- df_routes %>% 
  left_join(df_vias, by = "cvc_codlog") %>% 
  mutate(
    status = ifelse(is.na(data_vigor), "Control", status))

# verificar quantas rotas temos de cada tipo
table(df_routes$status)
# Control Treated 
# 2849    2496

vec_od <- unique(df_routes$pair_od)
vec_treated <- unique(df_routes$pair_od[df_routes$status == "Treated"])

# ############################################################################ #
####           PUXAR DADOS DE TEMPO DE VIAGEM ENTRE OS PARES                ####
# ############################################################################ #
df_routes_info <- df_routes %>% distinct(pair_od, duration, distance)
df_infos <- df_routes %>% 
  group_by(pair_od) %>% 
  reframe(data_vigor = min(data_vigor)) %>% 
  left_join(df_routes_info, by = "pair_od")

# Step 2.1: Read only required columns and filter early
ds_matrix <- open_dataset(
  paste0("data/intermediate/matrix_grouped_osrm/", vec_days, ".parquet"))

df_matrix <- ds_matrix %>%
  filter(dep_hour == 7) %>% 
  mutate(pair_od = paste0(from_id, "-", to_id)) %>%
  filter(pair_od %in% vec_od) %>% # careful if vec_od is huge
  select(-from_id, -to_id)

object.size(df_matrix)
# system.time(df_matrix %>% collect())

# Step 2.2: Pull into memory after filtering
df_matrix_local <- df_matrix %>%
  collect()

df_matrix_local <- df_matrix_local %>%
  left_join(df_infos, by = "pair_od") %>%
  mutate(
    treat = as.integer(pair_od %in% vec_treated),
    data_vigor = ifelse(is.na(data_vigor), 0, data_vigor),
    data = as.Date(data, format = "%Y%m%d"))

# df_matrix <- open_dataset(
#   paste0("data/intermediate/matrix_grouped_osrm/", vec_days, ".parquet")) %>% 
#   mutate(
#     pair_od = paste0(from_id, "-", to_id)) %>% 
#   filter(pair_od %in% vec_od) %>%  
#   collect() 
# 
# df_matrix <- df_matrix %>% 
#   left_join(
#     df_routes %>% distinct(pair_od, duration, distance), 
#     by = "pair_od") %>% 
#   left_join(
#     df_dates,
#     by = "pair_od")
# 
# df_matrix <- df_matrix %>% 
#   collect() %>% 
#   mutate(
#     treat = ifelse(pair_od %in% vec_treated, 1, 0),
#     data_vigor = ifelse(is.na(data_vigor), 0, data_vigor),
#     data = as.Date(data, "%Y%m%d"))

# df_matrix <- df_matrix %>% 
#   # ungroup() %>% 
#   select(-dur_obs_sd) %>% 
#   mutate(
#     data = 
#       as.Date(data, "%Y%m%d"),
#     period =
#       ifelse(data > data_vigor, "1.After","0.Before"),
#     
#     status = 
#       ifelse(pair_od %in% vec_treated, "Treated", "Control"),
#     status = 
#       ifelse(!is.na(period), "Treated", "Control"),
#     status_d = 
#       ifelse(status == "Treated", 1, 0),
#     
#     period = case_when(
#       data > as.Date("2015-07-19") & status == "Control" ~ "1.After",
#       data <= as.Date("2015-07-19") & status == "Control" ~ "0.Before",
#       status == "Treated" ~ period),
#     period_d =
#       ifelse(period == "1.After", 1 , 0),
#     
#     day_time = case_when(
#       dep_hour %in% c(0, 1, 2, 3, 4) ~ "1.Off-peak",
#       dep_hour %in% c(7, 8, 9) ~ "2.Morning-peak",
#       dep_hour %in% c(17, 18, 19) ~ "3.Evening-peak",
#       TRUE ~ NA_character_),
#     running_var = ifelse(
#       !is.na(data_vigor), data - data_vigor, data - as.Date("2015-07-19")),
#     
#     day_hour = paste0(
#       str_replace_all(as.character(data), "-", ""), 
#       "-",
#       dep_hour))

vec_control <- unique(df_matrix$pair_od[df_matrix$status == "Control"])
vec_treat <- unique(df_matrix$pair_od[df_matrix$status == "Treated"])
vec_pairod <- unique(df_matrix$pair_od)

table(df_matrix$dep_hour)

teste <- df_matrix %>% 
  count(pair_od, day_hour) %>% 
  filter(n>1)

df_matrix_panel <- pdata.frame(
  df_matrix, 
  index = c("pair_od", "day_hour"))

df_matrix %>% 
  count(period, status)

df_matrix %>% 
  group_by(period, status) %>% 
  summarize(
    dur_mean = mean(dur_obs_mean))

# df_matrix %>%
#   ggplot(aes(x = data, y = dur_obs_mean)) +
#   geom_point(alpha = 0.5) +
#   geom_vline(xintercept = as.Date("2015-07-20"), col = "red", linetype = "dashed") +
#   facet_wrap(day_time~status)

model1 <- lm(dur_obs_mean ~ period*status, 
             data = df_matrix)
stargazer(
  model1,
  type = "text")

model2 <- plm(dur_obs_mean ~ period * status, 
              data = df_matrix, 
              model = "within")
stargazer(
  model2,
  type = "text")

library(fixest)

model2 <- feols(dur_obs_mean ~ period * status | pair_od, 
                data = df_matrix)

summary(model2)

stargazer(
  model2,
  type = "text")













df_matrix %>% 
  ungroup() %>% 
  count(pair_od, running_var, status) %>% 
  count(running_var, status) %>% 
  mutate(status = ifelse(status == "Control", "Controle", "Tratamento")) %>% 
  ggplot(aes(x = running_var, y = n, color = status, fill = status)) +
  geom_bar(stat = "identity") +
  facet_grid(status~.) +
  theme(legend.position = "none") +
  labs(
    title = "Todas as rotas - por dia",
    x = "Dias até ou desde a mudança de velocidade*", 
    y = "Frequência",
    caption = "*Para o grupo de controle, os valores do eixo X indicam dias até ou depois do dia 2015-07-20")

ggsave(
  plot = last_plot(),
  filename = 
    paste0("figures/tot_rotas_dia_all_all.png"),
  width = 18,
  height = 18*0.6,
  units = "cm")

# ############################################################################ #
####                      ANÁLISE PICO E FORA DE PICO                       ####
# ############################################################################ #
# filtrar para horários de pouco fluxo
df_matrix_off <- df_matrix %>% 
  ungroup() %>% 
  filter(dep_hour %in% c(0, 1, 2, 3, 4)) %>% 
  group_by(pair_od, period, status) %>% 
  mutate(
    dur_mean = mean(dur_obs_mean),
    dur_sd = sd(dur_obs_mean),
    dur_z = 
      (dur_obs_mean - mean(dur_mean, na.rm = TRUE)) / dur_sd) 

hist(df_matrix_off$dur_mean)
hist(df_matrix_off$duration)
hist(df_matrix_off$dur_z)
summary(df_matrix_off$dur_z)
sd(df_matrix_off$dur_z)
df_matrix_off %>% 
  group_by(period, status) %>% 
  summarize(
    dur_mean = mean(dur_obs_mean))

# filtrar para pico da manhã
df_matrix_mp <- df_matrix %>% 
  ungroup() %>% 
  filter(dep_hour %in% c(7, 8, 9)) %>% 
  group_by(pair_od, period, status) %>% 
  mutate(
    dur_mean = mean(dur_obs_mean),
    dur_sd = sd(dur_obs_mean),
    dur_z = 
      (dur_obs_mean - mean(dur_mean, na.rm = TRUE)) / dur_sd)

df_matrix_mp %>% 
  group_by(period, status) %>% 
  summarize(
    dur_mean = mean(dur_obs_mean))

df_matrix_ep <- df_matrix %>% 
  ungroup() %>% 
  filter(dep_hour %in% c(17, 18, 19)) %>% 
  group_by(pair_od, period, status) %>% 
  mutate(
    dur_mean = mean(dur_obs_mean),
    dur_sd = sd(dur_obs_mean),
    dur_z = 
      (dur_obs_mean - mean(dur_mean, na.rm = TRUE)) / dur_sd) 

df_matrix_ep %>% 
  group_by(period, status) %>% 
  summarize(
    dur_mean = mean(dur_obs_mean))

# filtrar para pico da manhã
df_matrix_ep <- df_matrix %>% 
  ungroup() %>% 
  filter(dep_hour %in% c(17, 18, 19)) %>% 
  group_by(running_var, status) %>% 
  reframe(
    dur_mean = mean(dur_obs_mean),
    dur_sd = sd(dur_obs_mean),
    # dur_z = 
    #   (dur_obs_mean - mean(dur_mean, na.rm = TRUE)) / dur_sd,
    n = n()) %>% 
  add_count(running_var) %>% 
  filter(nn == 2)

df_matrix_ep %>% 
  ggplot(aes(x = n, color = status, fill = status)) +
  geom_histogram() +
  facet_wrap(~status) +
  theme(legend.position = "none") +
  labs(
    x = "Número de rotas consideradas no dia",
    y = "Frequência")

df_matrix_ep %>% 
  ggplot(aes(x = running_var, y = dur_mean)) +
  geom_point() +
  facet_grid(status~.)

# ############################################################################ #
####                 WALD TEST PARA DADOS ANTES DO TRATAMENTO               ####
# ############################################################################ #
#' Para que um DiD funcione, precisamos de tendências paralelas no tempo de 
#' deslocamento entre os grupos de controle e tratamento.
#' Pelo que estimei, esse não é o caso. Isso pode ser por diversos fatores:
#'      1. A composição dos grupos muda ao longo do tempo.
df_wald <- df_matrix %>% 
  ungroup() %>% 
  filter(
    dep_hour %in% c(17, 18, 19) & 
      running_var <0 & 
      running_var %in% df_matrix_ep$running_var)

model <- lm(
  dur_obs_mean ~ running_var*status, 
  data = df_wald) 
vcov_multi <- vcovCL(model, cluster = data.frame(df_wald$pair_od))
wald_test <- coeftest(model, vcov = vcov_multi)

df_wald %>% 
  mutate(status = ifelse(status == "Control", "Controle", "Tratamento\n(rotas com\nmudança de\nvelocidade)")) %>% 
  ggplot(aes(x = running_var, y = dur_obs_mean, color = status)) +
  geom_line(stat = "summary", fun = "mean") +
  labs(
    title = "Todas as rotas - pico da tarde (17h, 18h e 19h)",
    x = "Dias até a mudança de velocidade*", 
    y = "Duração da rota (minutos)",
    caption = "*Para o grupo de controle, os valores do eixo X indicam dias até o dia 2015-07-20",
    color = "Grupo:") +
  # facet_wrap(~status) +
  theme_light() +
  # scale_x_date(date_labels = "%b %Y", date_breaks = "1 month") +
  scale_y_continuous(labels = scales::number_format())+
  geom_label(
    data = as.data.frame(wald_test),
    aes(
      x = -50,  # Position annotations near the middle of each facet
      y = 14,  # Adjust y as needed
      label = paste("Tendência temporal = ", round(wald_test[4,1], 4), 
                    "\nWald p.value =", round(wald_test[4,4], 4))),
    inherit.aes = FALSE,  # Prevent aes from lorenz_df from affecting annotations
    size = 3, color = "darkgrey") +
  theme(
    legend.position = "right",
    axis.text.x = element_text(angle = 0, vjust = 0.5, hjust = 0.5),
    plot.caption = element_text(hjust = 0),
    legend.margin = margin(0,0,0,0))

ggsave(
  plot = last_plot(),
  filename = 
    paste0("figures/wald_test_all_ep.png"),
  width = 18,
  height = 18*0.6,
  units = "cm")

# testando número de observações por dia
df_wald %>% 
  count(running_var, status) %>% 
  ggplot(aes(x = n, color = status, fill = status)) +
  geom_histogram() +
  facet_wrap(~status) +
  theme(legend.position = "none") +
  labs(
    x = "Número de rotas consideradas no dia",
    y = "Frequência")

df_wald %>% 
  count(pair_od, running_var, status) %>% 
  count(running_var, status) %>% 
  mutate(status = ifelse(status == "Control", "Controle", "Tratamento")) %>% 
  ggplot(aes(x = running_var, y = n, color = status, fill = status)) +
  geom_bar(stat = "identity") +
  facet_grid(status~.) +
  theme(legend.position = "none") +
  labs(
    title = "Todas as rotas - pico da tarde (17h, 18h e 19h)",
    x = "Dias até a mudança de velocidade*", 
    y = "Frequência",
    caption = "*Para o grupo de controle, os valores do eixo X indicam dias até o dia 2015-07-20") +
  theme(
    legend.position = "none",
    axis.text.x = element_text(angle = 0, vjust = 0.5, hjust = 0.5),
    plot.caption = element_text(hjust = 0),
    legend.margin = margin(0,0,0,0))

ggsave(
  plot = last_plot(),
  filename = 
    paste0("figures/tot_rotas_dia_all_ep.png"),
  width = 18,
  height = 18*0.6,
  units = "cm")
