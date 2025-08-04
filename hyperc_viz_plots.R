# ############################################################################ #
####                              SETUP                                     ####
# ############################################################################ #
fig_folder <- "figures_hypc_viz/"

# ############################################################################ #
####                             LOAD DATA                                  ####
# ############################################################################ #
df_hypc <- read_parquet(
  "data/output/hypercongestion_by_id.parquet") %>% 
  filter(problems == "ok") %>% 
  mutate(period = ifelse(period == "all", "after", period)) %>% 
  select(-type, -problems, -max_flow) %>% 
  pivot_wider(
    names_from = period,
    values_from = max_speed,
    names_prefix = "hypc_speed_") %>% 
  filter(!is.na(hypc_speed_after)) %>% 
  mutate(
    hypc_speed_before = ifelse(is.na(hypc_speed_before), hypc_speed_after, hypc_speed_before),
    hypc_speed_after = ifelse(is.na(hypc_speed_after), hypc_speed_before, hypc_speed_after))

df_result_week <- open_dataset(
  "data/intermediate/radares_5min_vol_week_averages.parquet") %>% 
  distinct(
    id, data_ativacao, data_vigor, veloc_aps, mes_vigor, ano_vigor, 
    mes_ano, treat, id_num) %>% 
  collect()

df_result_fhp15 <- read_parquet(
  "data/intermediate/radares_fhp_15min.parquet") %>% 
  left_join(df_result_week, by = "id") %>% 
  left_join(df_hypc, by = "id") %>% 
  left_join(df_dicionario_dados %>% select(id, endereco, sentido), by = "id") %>% 
  mutate(
    month = month(data),
    month_treat = mes_vigor,
    year = year(data),
    month = ifelse(year == 2016, month + 12, month),
    weekday = weekdays(data),
    post = case_when(
      month_treat == 0 ~ "Controle - todo período",
      month < month_treat ~ "Antes da mudança de velocidade",
      month >= month_treat ~ "Depois da mudança de velocidade")) %>% 
  filter(weekday %notin% c("Sábado", "Domingo", "Saturday", "Sunday")) %>% 
  filter(!is.na(hypc_speed_before)) %>% 
  filter(!is.na(hypc_speed_after)) %>% 
  mutate(
    density = ifelse(vel_mean == 0, NA_integer_, vol_tot/vel_mean),
    hypc_speed = case_when(
      post == "Antes da mudança de velocidade" ~ hypc_speed_before,
      post == "Depois da mudança de velocidade" ~ hypc_speed_after,
      post == "Controle - todo período" ~ hypc_speed_after),
    hypc_dummy = ifelse(vel_mean < hypc_speed, "Supersaturado", "Normal"))

df_distinct <- df_result_fhp15 %>% 
  distinct(id, endereco, sentido)
vec_ids <- unique(df_distinct$id)
vec_endereco <- unique(df_distinct$endereco)
vec_sentido <- unique(df_distinct$sentido)
set.seed(05585040)

df_base <- df_result_fhp15 %>%
  filter(month %notin% c(1, 7, 13, 19),
         hora %in% 6:21)

df_meta <- df_distinct %>% arrange(id)

walk2(df_meta$id, seq_along(df_meta$id), function(id_i, i) {
  df_i <- df_base %>% filter(id == id_i)
  
  df_before <- df_i %>% filter(post == "Antes da mudança de velocidade")
  
  if (nrow(df_before) == 0) {
    df_aux <- df_i %>% filter(post == "Controle - todo período")
    n_obs <- nrow(df_aux)
  } else {
    n_obs <- nrow(df_before)
    df_after <- df_i %>%
      filter(post == "Depois da mudança de velocidade") %>%
      slice_sample(n = n_obs)
    df_aux <- bind_rows(df_before, df_after)
  }
  
  plot_caption <- glue(
    "Radar id: {id_i} em {df_meta$endereco[i]}, sentido {df_meta$sentido[i]}.\n",
    "Considerando observações entre 06h00 e 21h59, excluindo meses de férias (jan, jul).",
    "\nCada gráfico contém {n_obs} observações."
  )
  
  # plot list
  plots <- list(
    list(
      aes = aes(x = vol_tot, y = vel_mean, color = hypc_dummy),
      file = glue("{fig_folder}{id_i}_speed_flow_hypc_06_21.png"),
      facet = TRUE,
      color_label = "Tráfego:",
      xlab = "Volume de veículos por hora",
      ylab = "Velocidade média (km/h)"
    ),
    list(
      aes = aes(x = vol_tot, y = vel_mean, color = post),
      file = glue("{fig_folder}{id_i}_speed_flow_period_06_21.png"),
      facet = FALSE,
      color_label = "Período:",
      xlab = "Volume de veículos por hora",
      ylab = "Velocidade média (km/h)"
    ),
    list(
      aes = aes(x = density, y = vel_mean, color = hypc_dummy),
      file = glue("{fig_folder}{id_i}_speed_density_hypc_06_21.png"),
      facet = TRUE,
      color_label = "Tráfego:",
      xlab = "Densidade de veículos por km",
      ylab = "Velocidade média (km/h)"
    ),
    list(
      aes = aes(x = vol_tot, y = density, color = hypc_dummy),
      file = glue("{fig_folder}{id_i}_density_volume_hypc_06_21.png"),
      facet = TRUE,
      color_label = "Tráfego:",
      xlab = "Volume de veículos",
      ylab = "Densidade de veículos por km"
    ),
    list(
      aes = aes(x = vol_tot, y = density, color = post),
      file = glue("{fig_folder}{id_i}_density_volume_period_06_21.png"),
      facet = FALSE,
      color_label = "Período:",
      xlab = "Volume de veículos",
      ylab = "Densidade de veículos por km"
    )
  )
  
  walk(plots, function(p) {
    g <- ggplot(df_aux, p$aes) +
      geom_point(alpha = 0.2) +
      scale_x_continuous(labels = scales::label_number(big.mark = ".", decimal.mark = ",")) +
      scale_y_continuous(labels = scales::label_number(big.mark = ".", decimal.mark = ",")) +
      scale_color_viridis_d(end = 0.7) +
      labs(
        x = p$xlab,
        y = p$ylab,
        color = p$color_label,
        caption = plot_caption
      ) +
      guides(color = guide_legend(override.aes = list(alpha = 1))) +
      my_theme
    
    if (p$facet) g <- g + facet_wrap(~post)
    
    ggsave(g, filename = p$file, width = 19, height = 12, units = "cm")
  })
})
