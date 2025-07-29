# ############################################################################ #
####                                LOAD DATA                               ####
# ############################################################################ #
vec_dep_hours <- 0:23  # Or a custom subset

list_hourly_data <- list()

for (h in seq_along(vec_dep_hours)) {
  list_hourly_data[[h]] <- read_parquet(
    paste0("data/output/df_dep_hour_hour_", vec_dep_hours[h], ".parquet")
  ) %>% 
    mutate(
      weekday = weekdays(data),
      speed_obs_mean = dur_obs_mean/distance,
      speed_obs_mean = 60/speed_obs_mean) %>% 
    filter(speed_obs_mean <= 150)
}

# ############################################################################ #
####                               RUN PLOTS                                ####
# ############################################################################ #
for (i in seq_along(vec_dep_hours)) {
  message("Processing hour: ", vec_dep_hours[i])
  
  # Load hourly dataset
  df_hour <- list_hourly_data[[i]] %>% 
    filter(month_treat != 6)
  
  # GRÁFICO ROTAS NO DIA ----
  df_n <- df_hour %>% 
    count(data, month_treat)
  
  p_rotas <- ggplot() +
    geom_line(
      data = df_n %>% filter(month_treat != 0) %>% mutate(group = as.factor(month_treat)),
      aes(x = data, y = n, color = group, group = group),
      size = 1
    ) +
    scale_color_viridis_d() +
    scale_fill_grey() +
    scale_x_date(
      date_breaks = "2 months",
      date_labels = "%m/%Y"
    ) +
    labs(
      x = "Mês",
      y = "Número de rotas observadas",
      color = "Mês de tratamento",
      fill = "",  # optionally hide bar legend if confusing
      title = paste0("Número de rotas por grupo de tratamento - ", sprintf("%02d", vec_dep_hours[i]), "h"),
    ) +
    guides(color = guide_legend(nrow = 1)) +
    my_theme +
    theme(legend.position = "top")
  
  ggsave(
    paste0("figures/rotas_por_dia_", sprintf("%02d", vec_dep_hours[i]), "_sem_controle.png"), 
    p_rotas, 
    width = 10, 
    height = 6.25)
  
  p_rotas +
    geom_bar(
      data = df_n %>% filter(month_treat == 0) %>% mutate(group = "Controle"),
      aes(x = data, y = n, fill = group),
      stat = "identity",
      alpha = 0.3
    )
  
  ggsave(
    paste0("figures/rotas_por_dia_", sprintf("%02d", vec_dep_hours[i]), ".png"), 
    plot = last_plot(), 
    width = 10, 
    height = 6.25)
  
  # GRÁFICO ROTAS NO MÊS ----
  df_n_mes <- df_hour %>% 
    count(data, month, month_treat) %>% 
    group_by(month, month_treat) %>% 
    reframe(
      n = sum(n)) %>% 
    mutate(
      ano = if_else(month > 12, 2016, 2015),
      mes_real = if_else(month > 12, month - 12, month),
      data = as.Date(paste0(ano, "-", mes_real, "-01"))
    )
  
  p_rotas_mes <- ggplot() +
    geom_line(
      data = df_n_mes %>% filter(month_treat != 0) %>% mutate(group = as.factor(month_treat)),
      aes(x = data, y = n, color = group, group = group),
      size = 1
    ) +
    scale_color_viridis_d() +
    scale_fill_grey() +
    scale_x_date(
      date_breaks = "2 months",
      date_labels = "%m/%Y"
    ) +
    labs(
      x = "Mês",
      y = "Número de observações no mês",
      color = "Mês de tratamento",
      fill = "",  # optionally hide bar legend if confusing
      title = paste0("Número de observações por grupo de tratamento - ", sprintf("%02d", vec_dep_hours[i]), "h"),
    ) +
    guides(color = guide_legend(nrow = 1)) +
    my_theme +
    theme(legend.position = "top")
  
  ggsave(
    paste0("figures/rotas_por_mes_", sprintf("%02d", vec_dep_hours[i]), "_sem_controle.png"), 
    p_rotas_mes, 
    width = 10, 
    height = 6.25)
  
  p_rotas_mes +
    geom_bar(
      data = df_n_mes %>% filter(month_treat == 0) %>% mutate(group = "Controle"),
      aes(x = data, y = n, fill = group),
      stat = "identity",
      alpha = 0.3
    )
  
  ggsave(
    paste0("figures/rotas_por_mes_", sprintf("%02d", vec_dep_hours[i]), ".png"), 
    p_rotas, 
    width = 10, 
    height = 6.25)
  
  # GRÁFICO TEMPOS DE VIAGEM ----
  # Prepare untreated group (month_treat == 0)
  df_bar <- df_hour %>%
    filter(month_treat == 0) %>%
    mutate(group = "Controle") %>%  # label for legend
    group_by(group, month) %>%
    reframe(
      dur_obs = mean(dur_obs_mean),
      dur_sd = sd(dur_obs_mean)
    ) %>% 
    mutate(
      # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
      ano = if_else(month > 12, 2016, 2015),
      mes_real = if_else(month > 12, month - 12, month),
      data = as.Date(paste0(ano, "-", mes_real, "-01"))
    )
  
  # Prepare treated groups (month_treat != 0)
  df_line <- df_hour %>%
    filter(month_treat != 0) %>%
    mutate(group = as.factor(month_treat)) %>%
    group_by(group, month) %>%
    reframe(
      dur_obs = mean(dur_obs_mean),
      dur_sd = sd(dur_obs_mean)
    ) %>% 
    mutate(
      # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
      ano = if_else(month > 12, 2016, 2015),
      mes_real = if_else(month > 12, month - 12, month),
      data = as.Date(paste0(ano, "-", mes_real, "-01"))
    )
  
  # Combine both
  df_combined <- bind_rows(df_bar, df_line)
  
  # Plot
  p_tempo_viagem <- ggplot() +
    geom_bar(
      data = df_bar,
      aes(x = data, y = dur_obs, fill = group),
      stat = "identity",
      alpha = 0.3
    ) +
    geom_line(
      data = df_line,
      aes(x = data, y = dur_obs, color = group, group = group),
      size = 1
    ) +
    geom_point(
      data = df_line,
      aes(x = data, y = dur_obs, color = group, group = group),
      size = 2
    ) +
    scale_color_viridis_d() +
    scale_fill_grey() +
    scale_x_date(
      date_breaks = "2 months",
      date_labels = "%m/%Y"
    ) +
    # scale_x_continuous(breaks = min(df_hour$month):max(df_hour$month)) +
    labs(
      x = "Mês",
      y = "Tempo médio observado (min)",
      color = "Mês de tratamento",
      fill = "",  # optionally hide bar legend if confusing
      title = paste0("Tempo de viagem por grupo de tratamento - ", sprintf("%02d", vec_dep_hours[i]), "h"),
    ) +
    guides(color = guide_legend(nrow = 1)) +
    my_theme +
    theme(legend.position = "top")
  
  ggsave(
    paste0("figures/tempos_de_viagem_", sprintf("%02d", vec_dep_hours[i]), ".png"), 
    p_tempo_viagem, 
    width = 10, 
    height = 6.25)
  
  # GRÁFICO VELOCIDADES ----
  # Prepare untreated group (month_treat == 0)
  df_bar <- df_hour %>%
    filter(month_treat == 0) %>%
    mutate(group = "Controle") %>%  # label for legend
    group_by(group, month) %>%
    reframe(
      dur_obs = mean(speed_obs_mean),
      dur_sd = sd(speed_obs_mean)
    ) %>% 
    mutate(
      # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
      ano = if_else(month > 12, 2016, 2015),
      mes_real = if_else(month > 12, month - 12, month),
      data = as.Date(paste0(ano, "-", mes_real, "-01"))
    )
  
  # Prepare treated groups (month_treat != 0)
  df_line <- df_hour %>%
    filter(month_treat != 0) %>%
    mutate(group = as.factor(month_treat)) %>%
    group_by(group, month) %>%
    reframe(
      dur_obs = mean(speed_obs_mean),
      dur_sd = sd(speed_obs_mean)
    ) %>% 
    mutate(
      # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
      ano = if_else(month > 12, 2016, 2015),
      mes_real = if_else(month > 12, month - 12, month),
      data = as.Date(paste0(ano, "-", mes_real, "-01"))
    )
  
  # Plot
  p_speed_viagem <- ggplot() +
    geom_bar(
      data = df_bar,
      aes(x = data, y = dur_obs, fill = group),
      stat = "identity",
      alpha = 0.3
    ) +
    geom_line(
      data = df_line,
      aes(x = data, y = dur_obs, color = group, group = group),
      size = 1
    ) +
    geom_point(
      data = df_line,
      aes(x = data, y = dur_obs, color = group, group = group),
      size = 2
    ) +
    scale_color_viridis_d() +
    scale_fill_grey() +
    scale_x_date(
      date_breaks = "2 months",
      date_labels = "%m/%Y"
    ) +
    # scale_x_continuous(breaks = min(df_hour$month):max(df_hour$month)) +
    labs(
      x = "Mês",
      y = "Velocidade média observada (km)",
      color = "Mês de tratamento",
      fill = "",  # optionally hide bar legend if confusing
      title = paste0("Tempo de viagem por grupo de tratamento - ", sprintf("%02d", vec_dep_hours[i]), "h"),
    ) +
    guides(color = guide_legend(nrow = 1)) +
    my_theme +
    theme(legend.position = "top")
  
  ggsave(
    paste0("figures/velocidade_viagem_", sprintf("%02d", vec_dep_hours[i]), ".png"), 
    p_speed_viagem, 
    width = 10, 
    height = 6.25)
  
  # GRÁFICO DISTANCIAS ----
  # Prepare untreated group (month_treat == 0)
  df_bar <- df_hour %>%
    filter(month_treat == 0) %>%
    mutate(group = "Controle") %>%  # label for legend
    group_by(group, month) %>%
    reframe(
      dur_obs = mean(distance),
      dur_sd = sd(distance)
    ) %>% 
    mutate(
      # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
      ano = if_else(month > 12, 2016, 2015),
      mes_real = if_else(month > 12, month - 12, month),
      data = as.Date(paste0(ano, "-", mes_real, "-01"))
    )
  
  # Prepare treated groups (month_treat != 0)
  df_line <- df_hour %>%
    filter(month_treat != 0) %>%
    mutate(group = as.factor(month_treat)) %>%
    group_by(group, month) %>%
    reframe(
      dur_obs = mean(distance),
      dur_sd = sd(distance)
    ) %>% 
    mutate(
      # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
      ano = if_else(month > 12, 2016, 2015),
      mes_real = if_else(month > 12, month - 12, month),
      data = as.Date(paste0(ano, "-", mes_real, "-01"))
    )
  
  # Plot
  p_dist_viagem <- ggplot() +
    geom_bar(
      data = df_bar,
      aes(x = data, y = dur_obs, fill = group),
      stat = "identity",
      alpha = 0.3
    ) +
    geom_line(
      data = df_line,
      aes(x = data, y = dur_obs, color = group, group = group),
      size = 1
    ) +
    geom_point(
      data = df_line,
      aes(x = data, y = dur_obs, color = group, group = group),
      size = 2
    ) +
    scale_color_viridis_d() +
    scale_fill_grey() +
    scale_x_date(
      date_breaks = "2 months",
      date_labels = "%m/%Y"
    ) +
    # scale_x_continuous(breaks = min(df_hour$month):max(df_hour$month)) +
    labs(
      x = "Mês",
      y = "Distância média das rotas (km)",
      color = "Mês de tratamento",
      fill = "",  # optionally hide bar legend if confusing
      title = paste0("Distância da viagem por grupo de tratamento - ", sprintf("%02d", vec_dep_hours[i]), "h"),
    ) +
    guides(color = guide_legend(nrow = 1)) +
    my_theme +
    theme(legend.position = "top")
  
  ggsave(
    paste0("figures/distancia_viagem_", sprintf("%02d", vec_dep_hours[i]), ".png"), 
    p_dist_viagem, 
    width = 10, 
    height = 6.25)
  
  # GRÁFICO VOLUMES ----
  df_n_mes_veiculos <- df_hour %>% 
    count(data, month, month_treat) %>% 
    group_by(month, month_treat) %>% 
    reframe(
      n = sum(n)) %>% 
    left_join(
      df_hour %>% 
        group_by(month_treat, month) %>%
        reframe(
          n_vehicles = sum(n_vehicles, na.rm = TRUE)),
      by = c("month_treat", "month")) %>% 
    mutate(
      # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
      ano = if_else(month > 12, 2016, 2015),
      mes_real = if_else(month > 12, month - 12, month),
      data = as.Date(paste0(ano, "-", mes_real, "-01")),
      n_vehicles_mean = n_vehicles/n,
      n_vehicles = n_vehicles/1000
    )
  
  p_n_veic_mes <- ggplot() +
    geom_bar(
      data = df_n_mes_veiculos %>% filter(month_treat == 0) %>% mutate(group = "Controle"),
      aes(x = data, y = n_vehicles, fill = group),
      stat = "identity",
      alpha = 0.3
    ) +
    geom_line(
      data = df_n_mes_veiculos %>% filter(month_treat != 0) 
      %>% mutate(group = as.factor(month_treat)),
      aes(x = data, y = n_vehicles, color = group, group = group),
      size = 1
    ) +
    scale_color_viridis_d() +
    scale_fill_grey() +
    scale_x_date(
      date_breaks = "2 months",
      date_labels = "%m/%Y"
    ) +
    labs(
      x = "Mês",
      y = "Número de veículos registrados no mês \n(milhares)",
      color = "Mês de tratamento",
      fill = "",  # optionally hide bar legend if confusing
      title = paste0("Número de observações por grupo de tratamento - ", sprintf("%02d", vec_dep_hours[i]), "h"),
    ) +
    guides(color = guide_legend(nrow = 1)) +
    my_theme +
    theme(legend.position = "top")
  
  ggsave(
    paste0("figures/n_veiculos_mes_", sprintf("%02d", vec_dep_hours[i]), ".png"), 
    p_n_veic_mes, 
    width = 10, 
    height = 6.25)
  
  p_n_veic_mes_mean <- ggplot() +
    geom_bar(
      data = df_n_mes_veiculos %>% filter(month_treat == 0) %>% mutate(group = "Controle"),
      aes(x = data, y = n_vehicles_mean, fill = group),
      stat = "identity",
      alpha = 0.3
    ) +
    geom_line(
      data = df_n_mes_veiculos %>% filter(month_treat != 0) 
      %>% mutate(group = as.factor(month_treat)),
      aes(x = data, y = n_vehicles_mean, color = group, group = group),
      size = 1
    ) +
    scale_color_viridis_d() +
    scale_fill_grey() +
    scale_x_date(
      date_breaks = "2 months",
      date_labels = "%m/%Y"
    ) +
    labs(
      x = "Mês",
      y = "Número de veículos registrados no mês\n(média por rota)",
      color = "Mês de tratamento",
      fill = "",  # optionally hide bar legend if confusing
      title = paste0("Número de observações por grupo de tratamento - ", sprintf("%02d", vec_dep_hours[i]), "h"),
    ) +
    guides(color = guide_legend(nrow = 1)) +
    my_theme +
    theme(legend.position = "top")
  
  ggsave(
    paste0("figures/n_veiculos_mes_media", sprintf("%02d", vec_dep_hours[i]), ".png"), 
    p_n_veic_mes_mean, 
    width = 10, 
    height = 6.25)
}

# ############################################################################ #
####                 RUN PLOTS - DATA FOR THE WHOLE DAY                     ####
# ############################################################################ #
df_day <- bind_rows(list_hourly_data) %>% 
  filter(month_treat != 6)

# RADARES ATIVOS POR MÊS 
df_id_radares <- df_day %>% 
  mutate(
    id = str_sub(pair_od, 1, 4)
  ) %>% 
  distinct(data, id, month_treat) %>% 
  rbind(df_day %>% 
  mutate(
    id = str_sub(pair_od, 6, 9)
  ) %>% 
  distinct(data, id, month_treat)) %>% 
  mutate(
    month = month(data),
    ano = year(data)) %>% 
  distinct(month, ano, id, month_treat)  %>% 
  count(ano, month, month_treat) %>% 
  mutate(
    # ano = if_else(month > 12, 2016, 2015),
    # mes_real = if_else(month > 12, month - 12, month),
    data = as.Date(paste0(ano, "-", month, "-01")))

p_id_mes <- ggplot() +
  geom_line(
    data = df_id_radares %>% filter(month_treat != 0) %>% mutate(group = as.factor(month_treat)),
    aes(x = data, y = n, color = group, group = group),
    size = 1
  ) +
  scale_color_viridis_d() +
  scale_fill_grey() +
  scale_x_date(
    date_breaks = "2 months",
    date_labels = "%m/%Y"
  ) +
  labs(
    x = "Mês",
    y = "Número de radares ativos no mês",
    fill = "",
    color = "Mês de tratamento",
    title = paste0("Número de radares ativos por grupo de tratamento"),
  ) +
  guides(color = guide_legend(nrow = 1)) +
  my_theme +
  theme(legend.position = "top")

ggsave(
  paste0("figures/radares_por_mes_all_sem_controle.png"), 
  p_id_mes, 
  width = 10, 
  height = 6.25)

p_id_mes +
  geom_bar(
    data = df_id_radares %>% filter(month_treat == 0) %>% mutate(group = "Controle"),
    aes(x = data, y = n, fill = group),
    stat = "identity",
    alpha = 0.3
  ) 

ggsave(
  paste0("figures/radares_por_mes_all.png"), 
  plot = last_plot(), 
  width = 10, 
  height = 6.25)

# GRÁFICO ROTAS NO MÊS ----
df_n_mes <- df_day %>% 
  count(data, month, month_treat) %>% 
  group_by(month, month_treat) %>% 
  reframe(
    n = sum(n)) %>% 
  mutate(
    ano = if_else(month > 12, 2016, 2015),
    mes_real = if_else(month > 12, month - 12, month),
    data = as.Date(paste0(ano, "-", mes_real, "-01")),
    n = n/1000
  )

p_rotas_mes <- ggplot() +
  geom_line(
    data = df_n_mes %>% filter(month_treat != 0) %>% mutate(group = as.factor(month_treat)),
    aes(x = data, y = n, color = group, group = group),
    size = 1
  ) +
  scale_color_viridis_d() +
  scale_fill_grey() +
  scale_x_date(
    date_breaks = "2 months",
    date_labels = "%m/%Y"
  ) +
  labs(
    x = "Mês",
    y = "Número de observações no mês\n(milhares)",
    fill = "",
    color = "Mês de tratamento",
    title = paste0("Número de observações por grupo de tratamento - dia inteiro"),
  ) +
  guides(color = guide_legend(nrow = 1)) +
  my_theme +
  theme(legend.position = "top")

ggsave(
  paste0("figures/rotas_por_mes_all_sem_controle.png"), 
  p_rotas_mes, 
  width = 10, 
  height = 6.25)

p_rotas_mes +
  geom_bar(
    data = df_n_mes %>% filter(month_treat == 0) %>% mutate(group = "Controle"),
    aes(x = data, y = n, fill = group),
    stat = "identity",
    alpha = 0.3
  ) 

ggsave(
  paste0("figures/rotas_por_mes_all.png"), 
  plot = last_plot(), 
  width = 10, 
  height = 6.25)

# GRÁFICO VOLUMES ----
df_n_mes_veiculos <- df_day %>% 
  count(data, month, month_treat) %>% 
  group_by(month, month_treat) %>% 
  reframe(
    n = sum(n)) %>% 
  left_join(
    df_day %>% 
      group_by(month_treat, month) %>%
      reframe(
        n_vehicles = sum(n_vehicles, na.rm = TRUE)),
    by = c("month_treat", "month")) %>% 
  mutate(
    # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
    ano = if_else(month > 12, 2016, 2015),
    mes_real = if_else(month > 12, month - 12, month),
    data = as.Date(paste0(ano, "-", mes_real, "-01")),
    n_vehicles_mean = n_vehicles/n,
    n_vehicles = n_vehicles/1000000
  )

p_n_veic_mes <- ggplot() +
  geom_bar(
    data = df_n_mes_veiculos %>% filter(month_treat == 0) %>% mutate(group = "Controle"),
    aes(x = data, y = n_vehicles, fill = group),
    stat = "identity",
    alpha = 0.3
  ) +
  geom_line(
    data = df_n_mes_veiculos %>% filter(month_treat != 0) 
    %>% mutate(group = as.factor(month_treat)),
    aes(x = data, y = n_vehicles, color = group, group = group),
    size = 1
  ) +
  scale_color_viridis_d() +
  scale_fill_grey() +
  scale_x_date(
    date_breaks = "2 months",
    date_labels = "%m/%Y"
  ) +
  labs(
    x = "Mês",
    y = "Número de veículos registrados no mês \n(milhões)",
    color = "Mês de tratamento",
    fill = "",  # optionally hide bar legend if confusing
    title = paste0("Número de observações por grupo de tratamento - dia inteiro"),
  ) +
  guides(color = guide_legend(nrow = 1)) +
  my_theme +
  theme(legend.position = "top")

ggsave(
  paste0("figures/n_veiculos_mes_all.png"), 
  p_n_veic_mes, 
  width = 10, 
  height = 6.25)

p_n_veic_mes_mean <- ggplot() +
  geom_bar(
    data = df_n_mes_veiculos %>% filter(month_treat == 0) %>% mutate(group = "Controle"),
    aes(x = data, y = n_vehicles_mean, fill = group),
    stat = "identity",
    alpha = 0.3
  ) +
  geom_line(
    data = df_n_mes_veiculos %>% filter(month_treat != 0) 
    %>% mutate(group = as.factor(month_treat)),
    aes(x = data, y = n_vehicles_mean, color = group, group = group),
    size = 1
  ) +
  scale_color_viridis_d() +
  scale_fill_grey() +
  scale_x_date(
    date_breaks = "2 months",
    date_labels = "%m/%Y"
  ) +
  labs(
    x = "Mês",
    y = "Número de veículos registrados no mês \n(média por rota)",
    color = "Mês de tratamento",
    fill = "",  # optionally hide bar legend if confusing
    title = paste0("Número de observações por grupo de tratamento - dia inteiro"),
  ) +
  guides(color = guide_legend(nrow = 1)) +
  my_theme +
  theme(legend.position = "top")

ggsave(
  paste0("figures/n_veiculos_mes_mean_all.png"), 
  p_n_veic_mes_mean, 
  width = 10, 
  height = 6.25)

# GRÁFICO DISTANCIAS ----
# Prepare untreated group (month_treat == 0)
df_bar <- df_day %>%
  filter(month_treat == 0) %>%
  mutate(group = "Controle") %>%  # label for legend
  group_by(group, month) %>%
  reframe(
    dur_obs = mean(distance),
    dur_sd = sd(distance)
  ) %>% 
  mutate(
    # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
    ano = if_else(month > 12, 2016, 2015),
    mes_real = if_else(month > 12, month - 12, month),
    data = as.Date(paste0(ano, "-", mes_real, "-01"))
  )

# Prepare treated groups (month_treat != 0)
df_line <- df_day %>%
  filter(month_treat != 0) %>%
  mutate(group = as.factor(month_treat)) %>%
  group_by(group, month) %>%
  reframe(
    dur_obs = mean(distance),
    dur_sd = sd(distance)
  ) %>% 
  mutate(
    # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
    ano = if_else(month > 12, 2016, 2015),
    mes_real = if_else(month > 12, month - 12, month),
    data = as.Date(paste0(ano, "-", mes_real, "-01"))
  )

# Plot
p_dist_viagem <- ggplot() +
  geom_bar(
    data = df_bar,
    aes(x = data, y = dur_obs, fill = group),
    stat = "identity",
    alpha = 0.3
  ) +
  geom_line(
    data = df_line,
    aes(x = data, y = dur_obs, color = group, group = group),
    size = 1
  ) +
  geom_point(
    data = df_line,
    aes(x = data, y = dur_obs, color = group, group = group),
    size = 2
  ) +
  scale_color_viridis_d() +
  scale_fill_grey() +
  scale_x_date(
    date_breaks = "2 months",
    date_labels = "%m/%Y"
  ) +
  # scale_x_continuous(breaks = min(df_hour$month):max(df_hour$month)) +
  labs(
    x = "Mês",
    y = "Distância média das rotas (km)",
    color = "Mês de tratamento",
    fill = "",  # optionally hide bar legend if confusing
    title = paste0("Distância da viagem por grupo de tratamento - ", sprintf("%02d", vec_dep_hours[i]), "h"),
  ) +
  guides(color = guide_legend(nrow = 1)) +
  my_theme +
  theme(legend.position = "top")

ggsave(
  paste0("figures/distancia_viagem_all.png"), 
  p_dist_viagem, 
  width = 10, 
  height = 6.25)

