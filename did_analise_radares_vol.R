# ############################################################################ #
####        LOAD DATA          ####
# ############################################################################ #
df_result_week <- read_parquet(
  "data/intermediate/radares_5min_vol_week_averages.parquet")
vec_dep_hours <- c(1)
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
      df_hour <- df_result_week
      
      # Estimate ATT
      att <- att_gt(
        yname = "volume_hour",
        tname = "month",
        idname = "id_num",
        gname = "mes_vigor",
        xformla = ~1,
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
      saveRDS(att, paste0("results_radares_vol/att_hour_vol_dia.rds"))
      
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
          x = "Meses desde a mudança de velocidade",
          y = "ATT (volume de veículos, média por dia e hora)",
          color = "Mês de tratamento",
          title = paste0("Efeitos por grupo de tratamento - volume no dia, média por hora"),
          caption = caption_label[j],
        ) +
        guides(color = guide_legend(nrow = 1))+
        my_theme
      
      ggsave(
        paste0("figures_radares_vol/ES_per_group_hour_vol_dia_", control_type[j], ".png"), 
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
          # filter(time >= 4) %>% 
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
            y = "ATT (volume de veículos, média por dia e hora)",
            color = "Período:",
            title = paste0("Efeitos por grupo de tratamento - volume no dia, média por hora"),
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
          paste0("figures_radares_vol/siggroups_hour_vol_dia_", control_type[j], ".png"), 
          p_groups_2col, 
          width = 14, 
          height = 8)
        
        p_groups_1col <- p_groups +
          facet_wrap(~group, ncol = 2)
        
        ggsave(
          paste0("figures_radares_vol/siggroups_hour_vol_dia_", control_type[j], "_1col.png"), 
          p_groups_1col, 
          width = 20, 
          height = 27)
        
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
          filter(event.time >= -9 & event.time <= 12) %>% 
          mutate(periodo = ifelse(event.time <= 0, "Antes da mudança", "Após a mudança de velocidade")) %>%
          ggplot(aes(x = as.factor(event.time), y = estimate, color = periodo)) +
          geom_hline(yintercept = 0, linetype = "dashed", color = "darkgray") +
          geom_point() +
          geom_errorbar(aes(ymin = conf.low, ymax = conf.high), width = 0.2) +
          labs(
            x = "Número de meses de exposição à redução da velocidade máxima",
            y = "ATT (volume de veículos, média por dia e hora)",
            color = "Período:",
            title = paste0("Efeito dinâmico - volume no dia, média por hora"),
            subtitle = paste0(
              "ATT médio geral: ", 
              round(es$overall.att, 2), 
              " [", round(es$overall.att - 1.96*es$overall.se, 2), ", ", 
              round(es$overall.att + 1.96*es$overall.se, 2), "]")
            # caption = caption_label[j]
          ) +
          theme_minimal() +
          my_theme +
          theme(legend.position = c(0.5, 0.94), legend.direction = "horizontal")
        
        ggsave(
          paste0("figures_radares_vol/eventstudy_hour_vol_dia_", control_type[j], ".png"), 
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
    y = "ATT (volume de veículos, média por dia e hora)",
    color = "Grupo de controle:"
  ) + 
  coord_cartesian(ylim = c(-1, 2.4))+
  my_theme +
  theme(legend.position = c(0.5, 0.92), legend.direction = "horizontal")

ggsave(
  paste0("figures_radares_vol/did_att_all_vol_dia.png"), 
  plot = last_plot(), 
  width = 14, 
  height = 8)