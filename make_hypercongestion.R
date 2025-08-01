# ############################################################################ #
####                                LOAD DATA                               ####
# ############################################################################ #
output_dir <- "data/intermediate/radares_hour/"

df_result_week <- read_parquet(
  "data/intermediate/radares_5min_vol_week_averages.parquet") %>% 
  distinct(
    id, data_ativacao, data_vigor, mes_vigor, ano_vigor, mes_ano, treat, id_num)

vec_dep_hours <- str_pad(6:22, 2, "left", "0")

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
      weekday = weekdays(data)) %>% 
    filter(weekday %notin% c("Sábado", "Domingo", "Saturday", "Sunday")) %>% 
    rename(vel = vel_p)
}

# ############################################################################ #
####                RUN MODEL FOR TREATED SPEED CAMERAS                     ####
# ############################################################################ #
list_results <- list()
vec_id <- unique(df_result_week %>% filter(treat == 1) %>% pull(id))
vec_period <- c("before", "after")
for (i in seq_along(vec_id)) {
  
  list_hour <- list()
  for (h in seq_along(vec_dep_hours)) {
    list_hour[[h]] <- list_hourly_data[[h]] %>% 
      filter(id == vec_id[i])
  }
  df_aux <- bind_rows(list_hour) %>% 
    mutate(period = ifelse(data > data_vigor, "after", "before"))
  
  if (nrow(df_aux) > 0) {
    list_aux <- list()
    for (j in seq_along(vec_period)) {
      df_model <- df_aux %>% 
        filter(period == vec_period[j] & vel >=5) %>% 
        mutate(vel2 = vel*vel)
      
      model <- lm(volume ~ vel + vel2,
                  data = df_model)
      
      if (summary(model)$coefficients["vel2","Pr(>|t|)"] < 0.1 & 
          summary(model)$coefficients["vel2","Estimate"] < 0) {
        max_speed <- -model$coefficients["vel"]/(2*model$coefficients["vel2"])
        
        max_flow <- summary(model)$coefficients["(Intercept)", "Estimate"] +
          max_speed * summary(model)$coefficients["vel","Estimate"] +
          max_speed * max_speed * summary(model)$coefficients["vel2","Estimate"] 
        
        list_aux[[j]] <- tibble(
          "id" = vec_id[i],
          "period" = vec_period[j],
          "max_speed" = max_speed,
          "max_flow" = max_flow,
          "type" = "hypercongestion")
      } else{
        max_speed <- min(df_model$vel)
        
        max_flow <- summary(model)$coefficients["(Intercept)", "Estimate"] +
          max_speed * summary(model)$coefficients["vel","Estimate"] +
          max_speed * max_speed * summary(model)$coefficients["vel2","Estimate"] 
        
        list_aux[[j]] <- tibble(
          "id" = vec_id[i],
          "period" = vec_period[j],
          "max_speed" = max_speed,
          "max_flow" = max_flow, 
          "type" = "no_hypercongestion")
      }
    }
    list_results[[i]] <- bind_rows(list_aux)
  }
}

df_results <- bind_rows(list_results) %>% 
  left_join(df_dicionario_dados %>% select(id, vel_carro_moto), by = "id") %>% 
  mutate(
    vel_carro_moto = as.numeric(str_sub(vel_carro_moto, 1, 2)),
    problems = ifelse(
      max_speed < 0 | max_speed > vel_carro_moto | max_flow < 0, "problem", "ok"),
    max_speed = ifelse(problems == "ok", max_speed, vel_carro_moto/2))

# ############################################################################ #
####                RUN MODEL FOR CONTROL SPEED CAMERAS                     ####
# ############################################################################ #
list_results <- list()
vec_id <- unique(df_result_week %>% filter(treat == 0) %>% pull(id))
vec_period <- c("all")
for (i in seq_along(vec_id)) {
  
  list_hour <- list()
  for (h in seq_along(vec_dep_hours)) {
    list_hour[[h]] <- list_hourly_data[[h]] %>% 
      filter(id == vec_id[i])
  }
  df_aux <- bind_rows(list_hour) %>% 
    mutate(period = "all")
  
  if (nrow(df_aux) > 0) {
    list_aux <- list()
    for (j in seq_along(vec_period)) {
      df_model <- df_aux %>% 
        filter(period == vec_period[j] & vel >=5) %>% 
        mutate(vel2 = vel*vel)
      
      model <- lm(volume ~ vel + vel2,
                  data = df_model)
      
      if (summary(model)$coefficients["vel2","Pr(>|t|)"] < 0.1 & 
          summary(model)$coefficients["vel2","Estimate"] < 0) {
        max_speed <- -model$coefficients["vel"]/(2*model$coefficients["vel2"])
        
        max_flow <- summary(model)$coefficients["(Intercept)", "Estimate"] +
          max_speed * summary(model)$coefficients["vel","Estimate"] +
          max_speed * max_speed * summary(model)$coefficients["vel2","Estimate"] 
        
        list_aux[[j]] <- tibble(
          "id" = vec_id[i],
          "period" = vec_period[j],
          "max_speed" = max_speed,
          "max_flow" = max_flow,
          "type" = "hypercongestion")
      } else{
        max_speed <- min(df_model$vel)
        
        max_flow <- summary(model)$coefficients["(Intercept)", "Estimate"] +
          max_speed * summary(model)$coefficients["vel","Estimate"] +
          max_speed * max_speed * summary(model)$coefficients["vel2","Estimate"] 
        
        list_aux[[j]] <- tibble(
          "id" = vec_id[i],
          "period" = vec_period[j],
          "max_speed" = max_speed,
          "max_flow" = max_flow, 
          "type" = "no_hypercongestion")
      }
    }
    list_results[[i]] <- bind_rows(list_aux)
  }
}

df_results_control <- bind_rows(list_results) %>% 
  left_join(df_dicionario_dados %>% select(id, vel_carro_moto), by = "id") %>% 
  mutate(
    vel_carro_moto = as.numeric(str_sub(vel_carro_moto, 1, 2)),
    problems = ifelse(
      max_speed < 0 | max_speed > vel_carro_moto | max_flow < 0, "problem", "ok"),
    max_speed = ifelse(problems == "ok", max_speed, vel_carro_moto/2))

# ############################################################################ #
####                          COMBINE RESULTS                               ####
# ############################################################################ #
df_results <- df_results %>% 
  rbind(df_results_control)

write_parquet(
  df_results,
  "data/output/hypercongestion_by_id.parquet")
