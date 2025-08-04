df_result_week <- open_dataset(
  "data/intermediate/radares_5min_vol_week_averages.parquet") %>% 
  distinct(
    id, data_ativacao, data_vigor, veloc_aps, mes_vigor, ano_vigor, 
    mes_ano, treat, id_num) %>% 
  collect()

table(df_result_week$mes_vigor)
