gc()
df_result_week <- read_parquet(
  "data/intermediate/radares_5min_vol_week_averages.parquet")

vec_ids <- unique(df_result_week$id)

# needs to run the following code before, to process hourly data
# source("scripts_tempos_viagens/did_analise_radares_vol_hora.R")

# ############################################################################ #
####              OPEN PARQUET FILE WITH DATA EVERY HOUR                    ####
# ############################################################################ #
# Setup
output_dir <- "data/intermediate/radares_hour/"

res_folder <- "results_vel_hour_log_no07"
fig_folder <- "figures_vel_hour_log_no07"

if (!dir.exists(res_folder)) dir.create(res_folder, recursive = TRUE)
if (!dir.exists(fig_folder)) dir.create(fig_folder, recursive = TRUE)

plot_y_lab <-"ATT (Δ velocidade média - km/h)"
plot_y_lab_pct <-"ATT (Δ % na velocidade média)"

att_summary_table_name <- "/att_log_summary_table_vel_log_no07.csv"
aggte_combined_summary_name <-  "/aggte_log_combined_summary_vel_log_no07.csv"

# ############################################################################ #
####                                LOAD DATA                               ####
# ############################################################################ #
df_result_week <- read_parquet(
  "data/intermediate/radares_5min_vol_week_averages.parquet") %>% 
  distinct(
    id, data_ativacao, data_vigor, mes_vigor, ano_vigor, mes_ano, treat, id_num)

vec_dep_hours <- str_pad(0:23, 2, "left", "0")

list_hourly_data <- list()

for (h in seq_along(vec_dep_hours)) {
  list_hourly_data[[h]] <- read_parquet(
    paste0(output_dir, "radares_hora_", vec_dep_hours[h], ".parquet")) %>% 
    filter(vel_p <= 150) %>% 
    left_join(df_result_week, by = "id") %>% 
    mutate(
      month = month(data),
      month_treat = mes_vigor,
      year = year(data),
      month = ifelse(year == 2016, month + 12, month),
      weekday = weekdays(data),
      vel_p = as.numeric(vel_p)) %>% 
    filter(weekday %notin% c("Sábado", "Domingo", "Saturday", "Sunday")) %>% 
    mutate(
      vel_p_ln = ifelse(
        vel_p > 0, 
        log(vel_p),
        NA_integer_))
}

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
        filter(month_treat != 6 & month_treat != 7)
      
      # Estimate ATT
      att <- att_gt(
        yname = "vel_p_ln",
        tname = "month",
        idname = "id_num",
        gname = "month_treat",
        xformla = ~1 + weekday,
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
      saveRDS(att, paste0(res_folder, "/att_hour_", vec_dep_hours[i], ".rds"))
      
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
          y = plot_y_lab,
          color = "Mês de tratamento",
          title = paste0("Efeitos por grupo de tratamento - ", vec_dep_hours[i], "h"),
          caption = caption_label[j],
        ) +
        guides(color = guide_legend(nrow = 1))+
        my_theme
      
      ggsave(
        paste0(fig_folder, "/ES_per_group_hour_", vec_dep_hours[i], "_", control_type[j], ".png"), 
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
            y = plot_y_lab,
            color = "Período:",
            title = paste0("Efeitos por grupo de tratamento - ", vec_dep_hours[i], "h"),
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
          paste0(fig_folder, "/siggroups_hour_", vec_dep_hours[i], "_", control_type[j], ".png"), 
          p_groups_2col, 
          width = 14, 
          height = 8)
        
        p_groups_1col <- p_groups +
          facet_wrap(~group, ncol = 1, scales = "free")
        
        ggsave(
          paste0(fig_folder, "/siggroups_hour_", vec_dep_hours[i], "_", control_type[j], "_1col.png"), 
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
            y = plot_y_lab,
            color = "Período:",
            title = paste0("Efeito dinâmico - ", vec_dep_hours[i], "h"),
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
          paste0(fig_folder, "/eventstudy_hour_", vec_dep_hours[i], "_", control_type[j], ".png"), 
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
  ggplot(aes(x = as.factor(hour), y = mean_ATT, color = controle)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "darkgray") +
  geom_point(position = position_dodge(width = 0.8)) +
  geom_errorbar(
    aes(ymin = mean_ATT - 1.96*se_ATT, ymax = mean_ATT + 1.96*se_ATT), width = 0.2,
    position = position_dodge(width = 0.8)) +
  labs(
    x = "Hora do dia\n(viagens que começam dentro dessa hora)",
    y = plot_y_lab_pct,
    color = "Grupo de controle:"
  ) + 
  # coord_cartesian(ylim = c(-0.05, 0.1))+
  scale_y_continuous(labels = scales::percent_format())+
  my_theme +
  theme(legend.position = c(0.5, 0.92), legend.direction = "horizontal")

ggsave(
  paste0(fig_folder, "/did_att_all_hours.png"), 
  plot = last_plot(), 
  width = 14, 
  height = 8)

att_summary_table %>% 
  mutate(
    mean_ATT_pct = (mean_ATT - 1)*100,
    controle = ifelse(did_control_type == "nevertreated", "Rotas nunca tratadas", "Rotas ainda não tratadas")) %>% 
  filter(controle == "Rotas nunca tratadas") %>% 
  ggplot(aes(x = as.factor(hour), y = mean_ATT, color = controle)) +
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
    y = plot_y_lab_pct,
    color = "Grupo de controle:"
  ) + 
  # coord_cartesian(ylim = c(-0.05, 0.08))+
  scale_y_continuous(labels = scales::percent_format())+
  my_theme +
  theme(legend.position = "none", legend.direction = "horizontal")

ggsave(
  paste0(fig_folder, "/did_att_all_hours_nevertreated.png"), 
  plot = last_plot(), 
  width = 11, 
  height = 5)

att_summary_table %>% 
  mutate(
    se_ATT_log =100 * exp(mean_ATT) * log(se_ATT),
    perc_change = (exp(mean_ATT) - 1) * 100,
    ci_lower = (exp(mean_ATT - 1.96 * se_ATT_log) - 1) * 100,
    ci_upper = (exp(mean_ATT + 1.96 * se_ATT_log) - 1) * 100,
    controle = ifelse(did_control_type == "nevertreated", "Rotas nunca tratadas", "Rotas ainda não tratadas")) %>% 
  filter(controle == "Rotas nunca tratadas") %>% 
  ggplot(aes(x = as.factor(hour), y = perc_change, color = controle)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "darkgray") +
  geom_point(position = position_dodge(width = 0.8)) +
  geom_errorbar(
    aes(
      ymin = (exp(mean_ATT - 1.96*se_ATT)-1)*100, 
      ymax = (exp(mean_ATT + 1.96*se_ATT)-1)*100), 
    width = 0.2,
    position = position_dodge(width = 0.8)) +
  geom_label(
    aes(label = sprintf("%.2f", perc_change)),
    vjust = 0.5, # move label above the point
    size = 3,
    position = position_dodge(width = 0.8),
    color = "tomato"  # optional: keep text color consistent
  ) +
  labs(
    x = "Hora do dia\n(viagens que começam dentro dessa hora)",
    y = plot_y_lab_pct,
    color = "Grupo de controle:"
  ) + 
  # coord_cartesian(ylim = c(-0.05, 0.08))+
  # scale_y_continuous(labels = scales::percent_format())+
  my_theme +
  theme(legend.position = "none", legend.direction = "horizontal")

ggsave(
  paste0(fig_folder, "/did_att_all_hours_nevertreated.png"), 
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
  ggplot(aes(x = as.factor(hour), y = att_avg, color = controle)) +
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
    y = plot_y_lab,
    color = "Grupo de controle:"
  ) + 
  # coord_cartesian(ylim = c(-0.05, 0.1))+
  facet_wrap(~type, ncol = 1)+
  my_theme +
  theme(
    legend.position = "bottom", legend.direction = "horizontal",
    legend.box.margin = margin(t =-5, r = 0, b = 0, l = 0))

ggsave(
  paste0(fig_folder, "/did_att_all_hours_3groups.png"), 
  plot = last_plot(), 
  width = 14, 
  height = 8)

write.csv(
  att_summary_table, paste0(res_folder, att_summary_table_name), row.names = FALSE)
write.csv(
  aggte_combined_summary, paste0(res_folder, aggte_combined_summary_name), row.names = FALSE)
