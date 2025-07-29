# ############################################################################ #
####        LOAD DATA          ####
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
    filter(speed_obs_mean <= 150) %>% 
    mutate(
      dur_obs_mean = ifelse(
        dur_obs_mean > 0, 
        log(dur_obs_mean),
        NA_integer_))
}

# modelo com controle por tipo de via
# modelo com log(tempo de viagem)

# ############################################################################ #
####                                RUN ANALYSIS                            ####
# ############################################################################ #
# Initialize summary tables
att_summary_table <- data.frame()
aggte_combined_summary <- data.frame()
list_att <- list()
list_es <- list()
list_group <- list()

# i <- 5
# Loop over each hour
control_type <- c("notyettreated", "nevertreated")
caption_label = c(
  "Grupo de controle: rotas ainda não tratadas, mas que receberão tratamento no futuro.",
  "Grupo de controle: rotas que nunca foram tratadas.")
for (j in seq_along(control_type)) {
  for (i in seq_along(vec_dep_hours)) {
    message("Processing hour: ", vec_dep_hours[i])
    
    tryCatch({
      # Load hourly dataset
      df_hour <- list_hourly_data[[i]] %>% 
        filter(month_treat == 0 | month_treat == 7)
      
      # Estimate ATT
      att <- att_gt(
        yname = "dur_obs_mean",
        tname = "month",
        idname = "id_num",
        gname = "month_treat",
        xformla = ~1 + weekday + distance,
        data = df_hour,
        allow_unbalanced_panel = TRUE,
        est_method = "reg",
        control_group = control_type[j]
      )
      
      list_att[[i]] <- tidy(att) %>% 
        mutate(
          hour = vec_dep_hours[i],
          did_control_type = control_type[j],
          event_time = time - group)
      
      # Save ATT object to disk
      saveRDS(att, paste0("results/att_hour_", sprintf("%02d", vec_dep_hours[i]), ".rds"))
      
      # Make plot
      p_es_group <- list_att[[i]] %>% 
        mutate(
          estimate = ifelse(event_time == -1, 0, estimate),
          group = as.factor(group)) %>% 
        ggplot(aes(x = event_time, y = estimate, group = group, color = group)) +
        geom_point() +
        geom_line() +
        geom_vline(xintercept = -1, linetype = "dashed") +
        scale_color_viridis_d() +
        scale_x_continuous(breaks = min(list_att[[i]]$event_time):max(list_att[[i]]$event_time)) +
        labs(
          x = "Tempo desde a mudança de velocidade",
          y = "ATT (Δ tempo de viagem, em minutos)",
          color = "Mês de tratamento",
          title = paste0("Efeitos por grupo de tratamento - ", sprintf("%02d", vec_dep_hours[i]), "h"),
          caption = caption_label[j],
        ) +
        guides(color = guide_legend(nrow = 1))+
        my_theme
      
      ggsave(
        paste0("figures_log_only07/ES_per_group_hour_", sprintf("%02d", vec_dep_hours[i]), "_", control_type[j], ".png"), 
        p_es_group, 
        width = 10, 
        height = 6.25)
      
      # Summary of ATT
      simple_did <- aggte(
        att, 
        type = "simple", 
        min_e = -9,
        max_e = 12,
        na.rm = TRUE)
      #'simple: this just computes a weighted average of all group-time average 
      #' treatment effects with weights proportional to group size
      
      att_summary_table <- bind_rows(
        att_summary_table,
        tibble(
          hour = vec_dep_hours[i],
          n_treated_groups = length(unique(att$group)),
          n_time_periods = length(unique(att$t)),
          # n_controls = sum(att$gname == 0),
          mean_ATT = simple_did$overall.att,
          se_ATT = simple_did$overall.se,
          did_control_type = control_type[j]
        )
      )
      
      aggte_combined_summary <- bind_rows(
        aggte_combined_summary,
        tibble(
          hour = vec_dep_hours[i],
          type = "simple",
          att_avg = simple_did$overall.att,
          se_avg = simple_did$overall.se,
          did_control_type = control_type[j]
        ))
      
      # Plot statistically significant group effects
      att_tbl <- broom::tidy(att) %>%
        mutate(tvalue = estimate / std.error)
      
      sig_groups <- att_tbl %>%
        filter(abs(tvalue) >= 1.96) %>%
        distinct(group) %>%
        pull(group)
      
      if (length(sig_groups) > 0) {
        att_filtered <- att_tbl #%>%
        # filter(group %in% sig_groups)
        
        p_groups <- att_filtered %>%
          mutate(
            # ano base é 2015, mas ajustamos para 2016 nos valores maiores que 12
            ano = if_else(time > 12, 2016, 2015),
            mes_real = if_else(time > 12, time - 12, time),
            data = as.Date(paste0(ano, "-", mes_real, "-01"))
          ) %>% 
          filter(time >= 4) %>% 
          mutate(
            período = ifelse(time < group, "Antes da mudança", "Após a mudança de velocidade"),
            mês = as.factor(time),
            group = paste0("Rotas com mudança de velocidade no mês ", sprintf("%02d", group), "/2015")
          ) %>%
          ggplot(aes(x = data, y = estimate, color = período)) +
          geom_point() +
          geom_errorbar(aes(ymin = conf.low, ymax = conf.high), width = 0.2) +
          geom_hline(yintercept = 0, linetype = "dashed", color = "darkgray") +
          labs(
            x = "Mês",
            y = "ATT (Δ tempo de viagem, em minutos)",
            color = "Período:",
            title = paste0("Efeitos por grupo de tratamento - ", sprintf("%02d", vec_dep_hours[i]), "h"),
            caption = caption_label[j],
            # subtitle = paste0(
            #   "ATT médio geral para todos os grupos: ", 
            #   round(group_effects$overall.att, 2), 
            #   " [", round(group_effects$overall.att - 1.96*group_effects$overall.se, 2), ", ", 
            #   round(group_effects$overall.att + 1.96*group_effects$overall.se, 2), "]")
          ) +
          scale_x_date(
            date_breaks = "2 months",
            date_labels = "%m/%Y"
          ) +
          my_theme +
          theme(
            legend.position = "top", 
            axis.text.x = element_text(angle = 45, hjust = 1)) 
        
        p_groups_2col <- p_groups +
          facet_wrap(~group, ncol = 2)
        
        ggsave(
          paste0("figures_log_only07/siggroups_hour_", sprintf("%02d", vec_dep_hours[i]), "_", control_type[j], ".png"), 
          p_groups_2col, 
          width = 14, 
          height = 8)
        
        p_groups_1col <- p_groups +
          facet_wrap(~group, ncol = 1, scales = "free")
        
        ggsave(
          paste0("figures_log_only07/siggroups_hour_", sprintf("%02d", vec_dep_hours[i]), "_", control_type[j], "_1col.png"), 
          p_groups_1col, 
          width = 14, 
          height = 18)
        
        # Event-study aggregation
        es <- aggte(
          att, 
          type = "dynamic", 
          min_e = -9,
          max_e = 12,
          na.rm = TRUE)
        
        # Summary ES
        list_es[[i]] <- tidy(es) %>% 
          mutate(
            hour = vec_dep_hours[i],
            did_control_type = control_type[j])
        aggte_combined_summary <- bind_rows(
          aggte_combined_summary,
          tibble(
            hour = vec_dep_hours[i],
            type = "dynamic",
            att_avg = es$overall.att,
            se_avg = es$overall.se,
            did_control_type = control_type[j]
          )
        )
        
        # Group-level aggregation
        group_effects <- aggte(
          att, 
          type = "group", 
          min_e = -9,
          max_e = 12,
          na.rm = TRUE)
        
        # Summary GE
        list_group <- tidy(group_effects) %>% 
          mutate(hour = vec_dep_hours[i])
        aggte_combined_summary <- bind_rows(
          aggte_combined_summary,
          tibble(
            hour = vec_dep_hours[i],
            type = "group",
            att_avg = group_effects$overall.att,
            se_avg = group_effects$overall.se,
            did_control_type = control_type[j]
          )
        )
        
        # Save event study plot
        es_tbl <- broom::tidy(es)
        p_es <- es_tbl %>%
          mutate(periodo = ifelse(event.time <= 0, "Antes da mudança", "Após a mudança de velocidade")) %>%
          ggplot(aes(x = as.factor(event.time), y = estimate, color = periodo)) +
          geom_hline(yintercept = 0, linetype = "dashed", color = "darkgray") +
          geom_point() +
          geom_errorbar(aes(ymin = conf.low, ymax = conf.high), width = 0.2) +
          labs(
            x = "Número de meses de exposição à redução da velocidade máxima",
            y = "ATT (Δ tempo de viagem, em minutos)",
            color = "Período:",
            title = paste0("Efeito dinâmico - ", sprintf("%02d", vec_dep_hours[i]), "h"),
            subtitle = paste0(
              "ATT médio geral: ", 
              round(es$overall.att, 2), 
              " [", round(es$overall.att - 1.96*es$overall.se, 2), ", ", 
              round(es$overall.att + 1.96*es$overall.se, 2), "]"),
            caption = caption_label[j]
          ) +
          theme_minimal() +
          my_theme +
          theme(legend.position = c(0.5, 0.94), legend.direction = "horizontal")
        
        ggsave(
          paste0("figures_log_only07/eventstudy_hour_", sprintf("%02d", vec_dep_hours[i]), "_", control_type[j], ".png"), 
          p_es, 
          width = 10, 
          height = 6.25)
        
      }
      
      # Clean up
      rm(att, es, group_effects, df_hour, att_tbl, es_tbl)
      gc()
      
    }, error = function(e) {
      message("⚠️ Error in hour ", vec_dep_hours[i], ": ", e$message)
    })
  }
}

att_summary_table <- att_summary_table %>% 
  filter(!is.na(did_control_type))

att_summary_table %>% 
  mutate(controle = ifelse(did_control_type == "nevertreated", "Rotas nunca tratadas", "Rotas ainda não tratadas")) %>% 
  ggplot(aes(x = as.factor(sprintf("%02d", hour)), y = mean_ATT, color = controle)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "darkgray") +
  geom_point(position = position_dodge(width = 0.8)) +
  geom_errorbar(
    aes(ymin = mean_ATT - 1.96*se_ATT, ymax = mean_ATT + 1.96*se_ATT), width = 0.2,
    position = position_dodge(width = 0.8)) +
  labs(
    x = "Hora do dia\n(viagens que começam dentro dessa hora)",
    y = "ATT (Δ % no tempo de viagem)",
    color = "Grupo de controle:"
  ) + 
  coord_cartesian(ylim = c(0, 0.35))+
  scale_y_continuous(labels = scales::percent_format())+
  my_theme +
  theme(legend.position = c(0.5, 0.92), legend.direction = "horizontal")

ggsave(
  paste0("figures_log_only07/did_att_all_hours.png"), 
  plot = last_plot(), 
  width = 14, 
  height = 8)

att_summary_table %>% 
  mutate(
    mean_ATT_pct = (mean_ATT - 1)*100,
    controle = ifelse(did_control_type == "nevertreated", "Rotas nunca tratadas", "Rotas ainda não tratadas")) %>% 
  filter(controle == "Rotas nunca tratadas") %>% 
  ggplot(aes(x = as.factor(sprintf("%02d", hour)), y = mean_ATT, color = controle)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "darkgray") +
  geom_point(position = position_dodge(width = 0.8)) +
  geom_errorbar(
    aes(ymin = mean_ATT - 1.96*se_ATT, ymax = mean_ATT + 1.96*se_ATT), width = 0.2,
    position = position_dodge(width = 0.8)) +
  geom_label(
    aes(label = sprintf("%.2f", mean_ATT*100)),
    vjust = 0.5, # move label above the point
    size = 3,
    position = position_dodge(width = 0.8),
    color = "tomato"  # optional: keep text color consistent
  ) +
  labs(
    x = "Hora do dia\n(viagens que começam dentro dessa hora)",
    y = "ATT (Δ % no tempo de viagem)",
    color = "Grupo de controle:"
  ) + 
  coord_cartesian(ylim = c(0, 0.3))+
  scale_y_continuous(labels = scales::percent_format())+
  my_theme +
  theme(legend.position = "none", legend.direction = "horizontal")

ggsave(
  paste0("figures_log_only07/did_att_all_hours_nevertreated.png"), 
  plot = last_plot(), 
  width = 11, 
  height = 5)

aggte_combined_summary <- aggte_combined_summary %>% 
  filter(!is.na(did_control_type))

aggte_combined_summary %>% 
  mutate(
    controle = ifelse(did_control_type == "nevertreated", "Rotas nunca tratadas", "Rotas ainda não tratadas"),
    type = case_when(
      type == "dynamic" ~ "Média por tempo relativo ao tratamento (dynamic)",
      type == "group" ~ "Média por grupo de tratamento (group)",
      type == "simple" ~ "Média ponderada geral (simple)"),
    type = factor(type, levels = c(
      "Média ponderada geral (simple)", "Média por grupo de tratamento (group)", "Média por tempo relativo ao tratamento (dynamic)"
    ))) %>% 
  ggplot(aes(x = as.factor(sprintf("%02d", hour)), y = att_avg, color = controle)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "darkgray") +
  geom_point(position = position_dodge(width = 0.8)) +
  geom_errorbar(
    aes(ymin = att_avg - 1.96*se_avg, ymax = att_avg + 1.96*se_avg), width = 0.2,
    position = position_dodge(width = 0.8)) + 
  # geom_text(
  #   aes(label = ifelse(abs(att_avg/se_avg) > 1.96, "*", "")),
  #   vjust = -2, size = 3, position = position_dodge(width = 0.8))+
  labs(
    x = "Hora do dia\n(viagens que começam dentro dessa hora)",
    y = "ATT (Δ tempo de viagem, em minutos)",
    color = "Grupo de controle:"
  ) + 
  coord_cartesian(ylim = c(-0.05, 0.1))+
  facet_wrap(~type, ncol = 1)+
  my_theme +
  theme(
    legend.position = "bottom", legend.direction = "horizontal",
    legend.box.margin = margin(t =-5, r = 0, b = 0, l = 0))

ggsave(
  paste0("figures_log_only07/did_att_all_hours_3groups.png"), 
  plot = last_plot(), 
  width = 14, 
  height = 8)

write.csv(
  att_summary_table, "results/att_log_summary_table_only07.csv", row.names = FALSE)
write.csv(
  aggte_combined_summary, "results/aggte_log_combined_summary_only07.csv", row.names = FALSE)

