# abrir os dados por radar por hora
# filtrar para os radares que aparecem na base de tempos de viagem
# gerar uma dummy para hypercongestionamento (velocidade média < 0.5 * vel_max)
# juntar com a base de tempos de viagem
# calcular a probabilidade do radar estar hypercongestionado no período anterior


# ############################################################################ #
####        LOAD DATA          ####
# ############################################################################ #
vec_dep_hours <- 0:23  # Or a custom subset

list_hourly_data <- list()

for (h in seq_along(vec_dep_hours)) {
  list_hourly_data[[h]] <- open_dataset(
    paste0("data/output/df_dep_hour_hour_", vec_dep_hours[h], ".parquet")
  )
}

df_aux <- open_dataset(
    paste0("data/output/df_dep_hour_hour_", vec_dep_hours, ".parquet")) %>% 
  count(data) %>% 
  collect()

min_date <- min(df_aux$data)
max_date <- max(df_aux$data)

df_aux <- open_dataset(
  paste0("data/output/df_dep_hour_hour_", vec_dep_hours, ".parquet")) %>% 
  count(pair_od) %>% 
  mutate(
    from = str_sub(pair_od, 1, 4),
    to = str_sub(pair_od, 6, 9))

vec_from <- df_aux %>% count(from) %>% collect() %>% pull(from) 
vec_to <- df_aux %>% count(to) %>% collect() %>% pull(to) 
vec_ids <- unique(vec_from, vec_to)

vec_dates <- seq.Date(
  from = min_date,
  to = max_date,
  by = 1) %>% 
  as.character() %>% 
  str_replace_all(., "-", "")

# Predefine dictionary subset
df_dic_subset <- df_dicionario_dados %>% 
  select(id, vel_carro_moto)

list_hour <- list()
for (k in seq_along(vec_dates)) {
  file_to_read <- paste0(
    "/Users/tainasouzapacheco/Library/CloudStorage/Dropbox/Academico/UAB/tese/ch_overpass/data/intermediate/parquet/",
    vec_dates[k],
    "_60.parquet"
  )
  file_size_mb <- file.info(file_to_read)$size
  
  if (file.exists(file_to_read) & file_size_mb > 0) {
    list_hour[[k]] <- open_dataset(file_to_read) %>% 
      filter(id %in% vec_ids) %>% 
      mutate(hora = as.numeric(hora)) %>%
      left_join(df_dic_subset, by = "id") %>% 
      mutate(
        vel_carro_moto = as.numeric(str_sub(vel_carro_moto, 1, 2)),
        hypercongestion = as.integer(vel_p < 0.5 * vel_carro_moto)
      ) %>%
      select(id, data, hora, hypercongestion) %>% 
      collect() %>%
      mutate(
        data = as.Date(as.character(data), format = "%Y%m%d"),
        dep_hour = hora
      ) %>%
      select(-hora)
  } 
  
  if (file.exists(file_to_read) & file_size_mb == 0) {
    print(vec_dates[k])
  }
  
  print(k)
}

df_hyper <- bind_rows(list_hour)
names(df_hyper)

df_hyper <- df_hyper %>% 
  mutate(from = id, to = id) %>% 
  select(-id)

# ############################################################################ #
####                                RUN ANALYSIS                            ####
# ############################################################################ #
gc()
# Initialize summary tables
att_summary_table <- data.frame()
aggte_combined_summary <- data.frame()
list_att <- list()
list_es <- list()
list_group <- list()

# i <- 9
# Loop over each hour
control_type <- c("notyettreated", "nevertreated")
caption_label = c(
  "Grupo de controle: rotas ainda não tratadas, mas que receberão tratamento no futuro.",
  "Grupo de controle: rotas que nunca foram tratadas.")

# control_type <- c("notyettreated")
# caption_label = c(
#   "Grupo de controle: rotas ainda não tratadas, mas que receberão tratamento no futuro.")

for (j in seq_along(control_type)) {
  for (i in seq_along(vec_dep_hours)) {
    message("Processing hour: ", vec_dep_hours[i])
    
    tryCatch({
      # Load hourly dataset
      df_hour <- list_hourly_data[[i]] %>% 
        mutate(
          from = str_sub(pair_od, 1, 4),
          to = str_sub(pair_od, 6, 9)) %>% 
        collect() %>% 
        left_join(
          df_hyper %>% select(-to) %>% rename(hypc_from = hypercongestion), 
          by = c("data", "dep_hour", "from"))%>% 
        left_join(
          df_hyper %>% select(-from) %>% rename(hypc_to = hypercongestion), 
          by = c("data", "dep_hour", "to")) %>% 
        mutate(
          hypc_from = ifelse(is.na(hypc_from), 0, hypc_from),
          hypc_to = ifelse(is.na(hypc_to), 0, hypc_to),
          hypc_pair = ifelse(hypc_from + hypc_to > 0, 1, 0)) #%>% 
        #filter(hypc_pair > 0)
      
      df_aux <- df_hour %>% 
        group_by(pair_od, post) %>% 
        reframe(
          n = n(),
          hypc_pair = sum(hypc_pair)
        ) %>% 
        mutate(hypc_pct = hypc_pair/n) %>% 
        filter(post == 0) %>% 
        select(pair_od, post, hypc_pct)
      
      df_hour <- df_hour %>% 
        left_join(df_aux, by = c("pair_od")) %>% 
        mutate(
          prob_group = ntile(hypc_pct, 2),
          prob_group = ifelse(hypc_pct == 0, 1, 2)
          )
      
      list_att_g <- list()
      for (g in 1:2) {
        df_hour_g <- df_hour %>% 
          filter(prob_group == g)
        
        # Estimate ATT
        att <- att_gt(
          yname = "dur_obs_mean",
          tname = "month",
          idname = "id_num",
          gname = "month_treat",
          xformla = ~1,
          data = df_hour_g,
          allow_unbalanced_panel = TRUE,
          est_method = "reg",
          control_group = control_type[j]
        )
        
        list_att_g[[i]] <- tidy(att) %>% 
          mutate(
            hour = vec_dep_hours[i],
            did_control_type = control_type[j])
        
        # Save ATT object to disk
        saveRDS(
          att, 
          paste0("results_hypc/att_hour_", sprintf("%02d", vec_dep_hours[i]), 
                 "_g", g, ".rds"))
        
        # Summary of ATT
        simple_did <- aggte(att, type = "simple", na.rm = TRUE)
        
        att_summary_table <- bind_rows(
          att_summary_table,
          tibble(
            hour = vec_dep_hours[i],
            n_treated_groups = length(att$group),
            n_time_periods = length(att$tlist),
            n_controls = sum(att$gname == 0),
            mean_ATT = simple_did$overall.att,
            se_ATT = simple_did$overall.se,
            did_control_type = control_type[j],
            group_hypc = g
          )
        )
        
        aggte_combined_summary <- bind_rows(
          aggte_combined_summary,
          tibble(
            hour = vec_dep_hours[i],
            type = "simple",
            att_avg = simple_did$overall.att,
            se_avg = simple_did$overall.se,
            did_control_type = control_type[j],
            group_hypc = g
          ))
        
        # Plot statistically significant group effects
        att_tbl <- broom::tidy(att) %>%
          mutate(tvalue = estimate / std.error)
        
        sig_groups <- att_tbl %>%
          filter(abs(tvalue) >= 1.96) %>%
          distinct(group) %>%
          pull(group)
        
        list_es_tbl <- list()
        if (length(sig_groups) > 0) {
          att_filtered <- att_tbl %>%
            filter(group %in% sig_groups)
          
          p_groups <- att_filtered %>%
            mutate(
              período = ifelse(time < group, "Antes da mudança", "Após a mudança de velocidade"),
              mês = as.factor(time),
              group = paste0("Rotas com mudança de velocidade no mês ", sprintf("%02d", group), "/2015")
            ) %>%
            ggplot(aes(x = mês, y = estimate, color = período)) +
            geom_point() +
            geom_errorbar(aes(ymin = conf.low, ymax = conf.high), width = 0.2) +
            geom_hline(yintercept = 0, linetype = "dashed", color = "darkgray") +
            labs(
              x = "Tempo (meses, 02 indica fevereiro de 2015)",
              y = "ATT (Δ tempo de viagem, em minutos)",
              color = "Período:",
              title = paste0("Efeitos por grupo de tratamento (apenas os significantes) - ", sprintf("%02d", vec_dep_hours[i]), "h"),
              caption = caption_label[j],
              # subtitle = paste0(
              #   "ATT médio geral para todos os grupos: ", 
              #   round(group_effects$overall.att, 2), 
              #   " [", round(group_effects$overall.att - 1.96*group_effects$overall.se, 2), ", ", 
              #   round(group_effects$overall.att + 1.96*group_effects$overall.se, 2), "]")
            ) +
            my_theme +
            theme(legend.position = "none") +
            facet_wrap(~group, ncol = 2)
          
          ggsave(
            paste0(
              "figures_hypc/siggroups_hour_", 
              sprintf("%02d", vec_dep_hours[i]), "_", 
              control_type[j], "_g", g, ".png"), 
            p_groups, 
            width = 14, 
            height = 8)
          
          # Event-study aggregation
          es <- aggte(att, type = "dynamic", na.rm = TRUE)
          
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
              did_control_type = control_type[j],
              group_hypc = g
            )
          )
          
          # Group-level aggregation
          group_effects <- aggte(att, type = "group", na.rm = TRUE)
          
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
              did_control_type = control_type[j],
              group_hypc = g
            )
          )
          
          # Save event study plot
          list_es_tbl[[g]] <- broom::tidy(es) %>% 
            mutate(group_hypc = g)
          p_es <- list_es_tbl[[g]] %>%
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
            paste0(
              "figures_hypc/eventstudy_hour_", 
              sprintf("%02d", vec_dep_hours[i]), 
              "_", control_type[j], 
              "_g", g, ".png"), 
            p_es, 
            width = 10, 
            height = 6.25)
        }
        
        p_es <- bind_rows(list_es_tbl) %>%
          mutate(
            group_hypc = ifelse(
              group_hypc == 1, "Rotas não congestionadas antes da mudança de velocidade",
              "Rotas congestionadas antes da mudança de velocidade"),
            periodo = ifelse(event.time <= 0, "Antes da mudança", "Após a mudança de velocidade")) %>%
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
          facet_wrap(group_hypc~., ncol = 1) +
          theme_minimal() +
          my_theme +
          theme(legend.position = c(0.5, 0.94), legend.direction = "horizontal")
        
        ggsave(
          paste0(
            "figures_hypc/eventstudy_hour_", 
            sprintf("%02d", vec_dep_hours[i]), 
            "_", control_type[j], 
            "_gall.png"), 
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
  filter(!is.na(did_control_type) & did_control_type == "notyettreated") %>% 
  # group_by(hour, did_control_type) %>% 
  # mutate(prob_group = row_number())
  mutate(
    group_hypc = ifelse(
      group_hypc == 1, "Rotas não congestionadas antes da mudança de velocidade",
      "Rotas congestionadas antes da mudança de velocidade"))

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
    y = "ATT (Δ tempo de viagem, em minutos)",
    color = "Grupo de controle:"
  ) + 
  # coord_cartesian(ylim = c(-1.2, 2.5))+
  facet_wrap(group_hypc~., ncol = 1) +
  my_theme +
  theme(legend.position = "bottom", legend.direction = "horizontal")  

ggsave(
  paste0("figures_hypc/did_att_all_hours.png"), 
  plot = last_plot(), 
  width = 14, 
  height = 8)

aggte_combined_summary <- aggte_combined_summary %>% 
  filter(!is.na(did_control_type) & did_control_type == "notyettreated") %>% 
  mutate(
    group_hypc = ifelse(
      group_hypc == 1, "Rotas não congestionadas",
      "Rotas congestionadas"))

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
  # coord_cartesian(ylim = c(-0.2, 2.5))+
  facet_wrap(group_hypc~type)+
  my_theme +
  theme(
    legend.position = "bottom", legend.direction = "horizontal",
    legend.box.margin = margin(t =-5, r = 0, b = 0, l = 0))

ggsave(
  paste0("figures_hypc/did_att_all_hours_3groups.png"), 
  plot = last_plot(), 
  width = 14, 
  height = 8)

write.csv(att_summary_table, "results_hypc/att_summary_table.csv", row.names = FALSE)
write.csv(aggte_combined_summary, "results_hypc/aggte_combined_summary.csv", row.names = FALSE)

#'Pode ter um efeito composição
#'Tentar olhar só para rotas que são sempre cpngestionadas
  