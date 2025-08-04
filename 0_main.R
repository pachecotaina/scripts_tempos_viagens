rm(list = ls())
gc()

options(scipen=999)

# set parameters
Sys.setlocale("LC_TIME", "pt_BR.UTF-8")

# Carregando os pacotes e instalando algum se necessário ----
load.lib <- c(
  "tidyverse", "sf", "xlsx", "readxl", "foreign", "stringr", "purrr",
  "stats", "data.table", "arrow", "lubridate", "ggspatial", #"ggsn",
  "spatstat", "raster", "geodist", #"slopes", 
  "tmap", "car", "scales",
  "sandwich",          #para vcoc()
  "lmtest",            #para coeftest()
  "huxtable", "broom", #para exportar regressoes em tabela
  "zoo",            # for moving averages
  "plm", "fixest",  # for FE
  "eventstudyr",    # for event study
  "stargazer",      # for regression table in Latex
  "pglm", "lme4",   # for logit panels
  "r5r", "rJava", "osrm",
  "furrr", "future",# for parallel computing
  "sfnetworks", "tidygraph", # for network analysis
  "tools", "stplanr",
  "osmextract", "ggridges",
  "gtfstools", "igraph", "tidygraph", "ggraph",
  "sfnetworks", "stplanr", "spDataLarge",
  "arrow", "did", "fs", "glue")
install.lib <- load.lib[!load.lib %in% installed.packages()]
for(lib in install.lib) install.packages(lib,dependencies=TRUE)
sapply(load.lib, require, character=TRUE)

remove(install.lib, lib, load.lib)

sf_use_s2()
select <- dplyr::select
read.xlsx <- openxlsx::read.xlsx
st_as_sf <- sf::st_as_sf
lag <- dplyr::lag
lead <- dplyr::lead

tmap_mode("view")

# import other codes ----
# source <- function(f, encoding = 'UTF-8') {
#   l <- readLines(f, encoding=encoding)
#   eval(parse(text=l),envir=.GlobalEnv)
# }

source("scripts_tempos_viagens/funs/helper_feriados.R")
source("scripts_tempos_viagens/funs/helper_ids_speedcameras.R")
source("scripts_tempos_viagens/0_setup_plots.R")


# ############################################################################ #
####         CREATE PAIR OD BASED ON LICENSE PLATES CONNECTIONS             ####
# ############################################################################ #
#' >> Para cada dia e cada radar, pega os 15 radares subsequentes mais comuns com
#' base nos dados identificados. Ou seja, olhamos todas as placas que saem de A
#' e listamos todos os radares de destino por ordem de número de veículos 
#' recebidos. Pegamos os 15 primeiros dessa lista.
source("scripts_tempos_viagem/pairOD_make.R")
#' INPUT
#'    - data/intermediate/matrix_grouped/<ALL FILES>
#'    - helper_geo
#' OUTPUT
#'    - "data/intermediate/osrm/list_routes.RDS"
#'    - "data/intermediate/osrm/list_pairs_od.RDS"

# ############################################################################ #
####          CLEAN PAIR OD BASED ON LICENSE PLATES CONNECTIONS             ####
# ############################################################################ #
#' >> Pega os bancos de dados gerados no código anterior e realiza limpezas:
#'    1. Exclui radares sem pares.
#'      - No banco de dados existem radares móveis (começam com "1") que não
#'      costumam ter observações no universo de registros.
source("scripts_tempos_viagem/pairOD_clean.R")
#' INPUT
#'    - "data/intermediate/osrm/list_routes.RDS"
#'    - "data/intermediate/osrm/list_pairs_od.RDS"
#' OUTPUT
#'    - df_routes <- "data/intermediate/osrm/routes_od.gpkg"
#'    - df_pairs <- "data/intermediate/osrm/pairs_od.parquet"

# ############################################################################ #
####          JOIN ROUTES WITH OFICIAL STREET CLASSIFICATION                ####
# ############################################################################ #
#' >> As rotas criadas são com base no pacote OSRM. Para poder de fato 
#' classificar cada uma das rotas entre tratada/controle eu preciso identificar
#' o viário correspondente à rota. Para isso vou usar a classificação viária 
#' oficial da CET.
#' >> Os radares estão em vias (excessão é via "R   ANTONIO JOSE ANACLETO")
#'    - "ARTERIAL", "COLETORA", "RODOVIA", "VTR"
#' >> Gero um buffer de 200m ao redor da rota, e puxo todas as vias que caem 
#' dentro do buffer.
#' >>>>> Dentro do código tem um "case study": Pq Povo >> Cebolão
source("scripts_tempos_viagem/pairOD_streets.R")
#' INPUT
#'    - "data/input/SIRGAS_SHP_classeviariacet/SIRGAS_SHP_classeviariacet.shp"
#'    - "data/input/vias_vel_reduz/ViasVelocidadeReduzida_Bloomberg_SIRGAS200023S.shp"
#'    - "data/intermediate/osrm/pairs_od.parquet"     (apenas ano 2015 mês <= 7)
#'    - "data/intermediate/osrm/routes_od.gpkg"
#' OUTPUT
#'    - "data/intermediate/osrm/routes_od_streets_2015.gpkg"

# ############################################################################ #
####              CLASSIFY PAIR OD IN TREATED OR CONTROL                    ####
# ############################################################################ #
#' >> Pega os bancos de dados gerados no código anterior e classifica entre 
#' tratado e controle com base no shape de vias tratadas da Bloomberg
#' >> O período antes de qualquer alteração nas velocidades vai de 2015-01-01 a 
#' 2015-07-19, então:
#'       1. Filtro apenas para rotas que possuem o menos 23 observações no período
#' >> Diversas rotas são muito pequenas, e esses casos normalmente são erros. 
#'       2. Filtro para rotas de 1 km de distância ou mais
#' >> Testo duas maneiras de classificar as rotas entre tratada/controle:
#'    A) Rotas que possuem apenas 1 via, e uso a classificação da via
#'    B) Rotas que possuem status homogêneo, ou seja, mesmo que a rota use duas
#'    vias ou mais, se:
#'      - todos segmentos tratados -> rota tratada
#'      - todos segmentos controle -> rota controle
#'      - alguns segmentos tratados e outros controle -> rota descartada
source("scripts_tempos_viagem/pairOD_classify.R")
#' INPUT
#'    - data/intermediate/matrix_grouped/<ALL FILES>
#'    - "data/intermediate/osrm/pairs_od.parquet"
#'    - "data/intermediate/osrm/routes_od.gpkg"
#'    - "data/input/vias_vel_reduz/ViasVelocidadeReduzida_Bloomberg_SIRGAS200023S.shp"
#'    - "data/intermediate/osrm/routes_od_streets_2015.gpkg"                    
#' OUTPUT
#'    - "data/intermediate/osrm/pairs_od_sample.parquet"

# ############################################################################ #
####              CLASSIFY PAIR OD IN TREATED OR CONTROL                    ####
# ############################################################################ #
#' >> Pega as rotas definindas anteriormente e agrupa os dados identificados. 
#' Uma variável chave é a razão entre a duração esperada (osrm) e o tempo de 
#' viagem de fato observado nos dados. O tempo de viagem observado pode ser 
#' maior que o tempo de viagem do osrm porque:
#'    - há congestionamento na via
#'    - não se trata de uma viagem concatenada.
#' Para excluir os possíveis casos de viagens não concatenadas, eu:
#'    - FILTROS:
#'        - dur_obs <= max_trip_duration*60
#'        - dur_obs/duration <= quantile(df$dur_ratio, probs = 0.90, na.rm = TRUE)
#' No final os dados terão a seguinte estrutura:
#'    - GRUPO: from_id, to_id, dep_hour
#'    - VARIÁVEIS: n_vehicles, dur_obs_mean, dur_obs_sd
source("scripts_tempos_viagem/make_matrix_grouped_osrm.R")
#' INPUT
#'    - data/intermediate/matrix
#'    - data/intermediate/osrm/routes_od_streets_2015.gpkg
#'    - FUNS >> matrix_group_trips_hour()
#' OUTPUT
#'    - data/intermediate/matrix_grouped_osrm/<DIA>.parquet

# ############################################################################ #
####                              DESCRIPTIVES                              ####
# ############################################################################ #
source("scripts_tempos_viagem/rotas_por_tratamento.R")
#' INPUT
#'    - "data/output/df_dep_hour_hour_", vec_dep_hours[h], ".parquet"
#'    - "data/intermediate/osrm/routes_od_streets_2015.gpkg"
#'    - "data/intermediate/osrm/pairs_od_sample.parquet"
#' OUTPUT
#'    - "figures/map_vias_tratadas_controle_grupo", vec_meses[i],".png"
#'    - "figures/map_vias_tratadas_controle_grupoALL.png"
#'    - "figures/map_vias_tratadas_controle_grupo_07others.png"

source("scripts_tempos_viagem/make_plots.R")
#' INPUT
#'    - "data/output/df_dep_hour_hour_", vec_dep_hours[h], ".parquet"
#' OUTPUT
#'    - "figures/rotas_por_dia_", sprintf("%02d", vec_dep_hours[i]), "_sem_controle.png"
#'    - "figures/rotas_por_dia_", sprintf("%02d", vec_dep_hours[i]), ".png"
#'    - "figures/rotas_por_mes_", sprintf("%02d", vec_dep_hours[i]), "_sem_controle.png"
#'    - "figures/rotas_por_mes_", sprintf("%02d", vec_dep_hours[i]), ".png"
#'    - "figures/tempos_de_viagem_", sprintf("%02d", vec_dep_hours[i]), ".png"
#'    - "figures/velocidade_viagem_", sprintf("%02d", vec_dep_hours[i]), ".png"
#'    - "figures/distancia_viagem_", sprintf("%02d", vec_dep_hours[i]), ".png"
#'    - "figures/n_veiculos_mes_", sprintf("%02d", vec_dep_hours[i]), ".png"
#'    - "figures/n_veiculos_mes_media", sprintf("%02d", vec_dep_hours[i]), ".png"
#'    - "figures/radares_por_mes_all_sem_controle.png"
#'    - "figures/radares_por_mes_all.png"
#'    - "figures/rotas_por_mes_all_sem_controle.png"
#'    - "figures/rotas_por_mes_all.png"
#'    - "figures/n_veiculos_mes_all.png"
#'    - "figures/n_veiculos_mes_mean_all.png"
#'    - "figures/distancia_viagem_all.png"

source("scripts_tempos_viagem/map_rotas_tratadas_controle.R")
#' INPUT
#'    - "data/intermediate/osrm/routes_od_streets_2015.gpkg"
#'    - "data/intermediate/osrm/pairs_od_sample.parquet"
#' OUTPUT
#'    - "figures/map_vias_tratadas_controle.png"

# ############################################################################ #
####                      CREATE DATA FOR ANALYSIS                          ####
# ############################################################################ #
#' Esse código cria os dados no formato necessário para o DiD de rotas.
#' 1) definir intervalo para análise: "2015-01-01", "%Y-%m-%d" até "2016-12-31"
#' 2) abrir rotas de interesse
#' 3) abrir vias com velocidades alteradas
#' 4) unir bancos e verificar quantas rotas há de cada tipo
#' 5) agrupar rotas por hora
source("scripts_tempos_viagens/did_make_data.R")
#' INPUT
#'    - "data/intermediate/osrm/pairs_od_sample.parquet"
#'    - "data/input/vias_vel_reduz/ViasVelocidadeReduzida_Bloomberg_SIRGAS200023S.shp"
#'    - "/Users/tainasouzapacheco/Downloads/matrix_grouped_osrm/", vec_days, ".parquet"
#' OUTPUT
#'    - write_parquet(.x, paste0("data/output/df_dep_hour_", .y, ".parquet")

# ############################################################################ #
####                       ANALYSIS - ALL GROUPS                            ####
# ############################################################################ #
source("scripts_tempos_viagens/did_analise.R")
#' INPUT
#'    - "data/output/df_dep_hour_hour_", vec_dep_hours[h], ".parquet"
#' OUTPUT
#'    - paste0("results/att_hour_", sprintf("%02d", vec_dep_hours[i]), ".rds")
#'    - "figures/ES_per_group_hour_", sprintf("%02d", vec_dep_hours[i]), "_", control_type[j], ".png"
#'    - "figures/siggroups_hour_", sprintf("%02d", vec_dep_hours[i]), "_", control_type[j], ".png"
#'    - "figures/siggroups_hour_", sprintf("%02d", vec_dep_hours[i]), "_", control_type[j], "_1col.png"
#'    - "figures/eventstudy_hour_", sprintf("%02d", vec_dep_hours[i]), "_", control_type[j], ".png"
#'    - "figures/did_att_all_hours.png"
#'    - "figures/did_att_all_hours_nevertreated.png"
#'    - "figures/did_att_all_hours_3groups.png"
#'    - "results/att_summary_table.csv"
#'    - "results/aggte_combined_summary.csv"

source("scripts_tempos_viagens/did_analise_log.R")
#' INPUT
#'    - "data/output/df_dep_hour_hour_", vec_dep_hours[h], ".parquet"
#' OUTPUT
#'    - ("results/att_hour_log", sprintf("%02d", vec_dep_hours[i]), ".rds")
#'    - mesmas figuras de antes, mas na pasta "figures_log"

# ############################################################################ #
####                    ANALYSIS - EXCLUDING GROUP 7                        ####
# ############################################################################ #
source("scripts_tempos_viagens/did_analise_no07.R")
source("scripts_tempos_viagens/did_analise_log_no07.R")
source("scripts_tempos_viagens/did_analise_only07.R")
source("scripts_tempos_viagens/did_analise_log_only07.R")
#' INPUT
#'    - "data/output/df_dep_hour_hour_", vec_dep_hours[h], ".parquet"
#' OUTPUT
#'    - results/
#'        - aggte_only07_combined_summary
#'        - aggteno07__combined_summary
#'        - att_no07_summary_table
#'        - att_only07_summary_table
#'    - figures_no07
#'    - figures_log_no07
#'    - figures_only07
#'    - figures_log_only07


# ############################################################################ #
####         MAKE DATA - VOLUMES AND SPEEDS AT THE SPEED CAMERA             ####
# ############################################################################ #
source("scripts_tempos_viagens/make_df_radares.R")
#' INPUT
#'    - "data/input/vias_vel_reduz/ViasVelocidadeReduzida_Bloomberg_SIRGAS200023S.shp"
#'    - helper_geo
#'    - "/Users/tainasouzapacheco/Library/CloudStorage/Dropbox/Academico/UAB/tese/ch_overpass/data/intermediate/parquet/"
#'        - pattern = "\ \ d{8}_05\\.parquet$"
#'    - 
#' OUTPUT
#'    - "figures/radares_data_vigor.png"
#'    - "data/intermediate/radares_5min_vol.parquet"
#'    - "data/intermediate/radares_5min_vol_week_averages.parquet"
#'    - "data/intermediate/radares_5min_vol_day_averages.parquet"

source("scripts_tempos_viagens/n_radares_por_mes.R")
#' INPUT
#'    - "data/intermediate/radares_5min_vol_week_averages.parquet"
#' OUTPUT
#'    - table(df_result_week$mes_vigor)

# ############################################################################ #
####        DiD ANALYSIS - VOLUMES AND SPEEDS AT THE SPEED CAMERA           ####
# ############################################################################ #
source("scripts_tempos_viagens/did_analise_radares_vol.R")
#' INPUT
#'    - "data/intermediate/radares_5min_vol_week_averages.parquet"
#' OUTPUT
#'    - "results_radares_vol/att_hour_vol_dia.rds"
#'    - "figures_radares_vol/ES_per_group_hour_vol_dia_", control_type[j], ".png"
#'    - "figures_radares_vol/siggroups_hour_vol_dia_", control_type[j], ".png"
#'    - "figures_radares_vol/siggroups_hour_vol_dia_", control_type[j], "_1col.png"
#'    - "figures_radares_vol/eventstudy_hour_vol_dia_", control_type[j], ".png"
#'    - "figures_radares_vol/did_att_all_vol_dia.png"

source("scripts_tempos_viagens/did_analise_radares_vol_dia.R")
#' INPUT
#'    - "data/intermediate/radares_5min_vol_day_averages.parquet"
#' OUTPUT
#'    - o mesmo de antes, mas nas pastas:
#'        - results_radares_vol_dia
#'        - figures_radares_vol_dia

source("scripts_tempos_viagens/did_analise_radares_vol_hora.R")
#' INPUT
#'    - "data/intermediate/radares_5min_vol_week_averages.parquet"
#'    - "/Users/tainasouzapacheco/Library/CloudStorage/Dropbox/Academico/UAB/tese/ch_overpass/data/intermediate/parquet/"
#'        - pattern = "\\ d{8}_60\\.parquet$"
#' OUTPUT    
#'    - "data/intermediate/radares_hour/" >> "radares_hora_{h}.parquet"
#'    - mesmo de antes, mas na pasta
#'        - results_vol_hour
#'        - figures_vol_hour
#'    - figures_vol_hour/did_att_all_hours_nevertreated.png
#'    - figures_vol_hour/did_att_all_hours_3groups.png
#'    - "results_vol_hour/att_summary_table.csv"
#'    - "results_vol_hour/aggte_combined_summary.csv"

source("scripts_tempos_viagens/did_analise_radares_vel_hora.R")
#' INPUT
#'    - "data/intermediate/radares_5min_vol_week_averages.parquet"
#'    - "data/intermediate/radares_hour/" >> "radares_hora_{h}.parquet"
#' OUTPUT
#'    - res_folder <- "results_vel_hour"
#'    - fig_folder <- "figures_vel_hour"

source("scripts_tempos_viagens/did_analise_radares_vel_hora_no07.R")
#' INPUT
#'    - "data/intermediate/radares_fhp15min/" >> "radares_hora_{.x}.parquet"
#' OUTPUT
#'    - res_folder <- "results_vel_hour_no07"
#'    - fig_folder <- "figures_vel_hour_no07"

source("scripts_tempos_viagens/did_analise_radares_vel_hora_only07.R")
#' OUTPUT
#'    - res_folder <- "results_vel_hour_only07"
#'    - fig_folder <- "figures_vel_hour_only07"

source("scripts_tempos_viagens/did_analise_radares_vel_hora_log.R")
#' OUTPUT
#'    - res_folder <- "results_vel_hour_log"
#'    - fig_folder <- "figures_vel_hour_log"

source("scripts_tempos_viagens/did_analise_radares_vel_hora_log_no07.R")
#' OUTPUT
#'    - res_folder <- "results_vel_hour_log_no07"
#'    - fig_folder <- "figures_vel_hour_log_no07"

source("scripts_tempos_viagens/did_analise_radares_vel_hora_log_only07.R")
#' OUTPUT
#'    - res_folder <- "results_vel_hour_log_only07"
#'    - fig_folder <- "figures_vel_hour_log_only07"


# ############################################################################ #
####          MAKE DATA - HYPERCONGESTION AT THE SPEED CAMERA               ####
# ############################################################################ #
#' definir a velocidade máxima em cada radar em cada dia
#'    - para o grupo de controle é a velocidade do dicionário dos radares
#'    - para o grupo tratado, tenho que ajustar
#'    
#' pegar o banco de dados da previsão da velocidade de hypercongestion
#' 
#' rodar separado grupo de controle e grupo de tratamento
#' CONTROLE
#'    - abrir banco de 5 minutos
#'    - left_join com banco de hypercongestion (by = id)
#'    - classificar em hypercongestion com base na velocidade média do 5 minutos
#'    - agrupar por hora: n = n(), n_hypercongestion, fazer a probabilidade para a hora
#'    - agrupar por dia, n = n(), n_hypercongestion, fazer a probabilidade para o dia
#' TRATAMENTO
#'    - 

source("scripts_tempos_viagens/make_hypercongestion.R")
source("scripts_tempos_viagens/hyperc_viz.R")
source("scripts_tempos_viagens/make_hypercongestion_data.R")
#'    - "data/intermediate/radares_hypc_day.parquet"
#'    - "data/intermediate/radares_hypc_hour.parquet"

source("scripts_tempos_viagens/did_analise_hypc_hour.R")
source("scripts_tempos_viagens/did_analise_hypc_hour_no07.R")
source("scripts_tempos_viagens/did_analise_hypc_hour_only07.R")

source("scripts_tempos_viagens/hyperc_viz_plots.R")

# ############################################################################ #
####                      HOMOGENEIZAÇÃO DE TRÁFIGO                         ####
# ############################################################################ #
#' Três estratégias:
#'    1) FPH =Vhp/(4 * V15) (volume total da hora dividido por 4x15min de maior volume)
#'    2) FPH =Vhp/(12 * V5) (volume total da hora dividido por 12x5min de maior volume)
#'    3) std do volume por hora em cada semana

# source("scripts_tempos_viagens/make_fhp_df.R")
source("scripts_tempos_viagens/did_analise_fhp_15min.R")
source("scripts_tempos_viagens/did_analise_fhp_15min_no07.R")
source("scripts_tempos_viagens/did_analise_fhp_15min_only07.R")
source("scripts_tempos_viagens/did_analise_fhp_5min.R")
source("scripts_tempos_viagens/did_analise_fhp_5min_no07.R")
source("scripts_tempos_viagens/did_analise_fhp_5min_only07.R")

# ############################################################################ #
####                                 OLD                                    ####
# ############################################################################ #
#' >> Olha para os deslocamentos entre 2015-01-01 e 2016-12-31, filtra para vias
#' que tiveram a velocidade alterada em 2015 ou depois (ano_vigor > 2014)
#' >> Código está pouco estruturado, ainda são testes de possíveis análises.
#' INPUT
#'    - "data/intermediate/osrm/pairs_od_sample.parquet"
#'    - "data/input/vias_vel_reduz/ViasVelocidadeReduzida_Bloomberg_SIRGAS200023S.shp"
#'    - paste0("data/intermediate/matrix_grouped_osrm/", vec_days, ".parquet") 
#' OUTPUT
#'    - "figures/wald_test_all_ep.png"              (todas as rotas, evening peak)
#'    - "figures/tot_rotas_dia_all_ep.png" (número de rotas por dia, evening peak) 
#'    - figures/tot_rotas_dia_all_all.png  (número de rotas com ao menos 1 observação no dia) 
# source("scripts_tempos_viagem/analise_preliminar.R")
#' IDEIAS:
#'    - Efeito composição: ao longo do tempo temos cada vez mais rotas tanto no
#'    grupo de tratamento quanto no grupo de controle. Isso acontece porque 2015
#'    foi um ano de expansão dos radares na cidade. 
#'         a. trabalhar com amostragens aleatórias criando um grupo de controle
#'         comparável ao grupo de rotas no grupo de tratamento (matching)
#'         b. Como temos um grupo de controle maior, então podemos fazer o 
#'         matching N vezes, e ver como o resultado muda. Vai haver variabilidade,
#'         mas a distribuição deveria estar centrada no valor verdadeiro. 
#'    - Na verdade esse efeito composição é ainda mais complexo, porque eu estou 
#'    no mesmo valor de "running_variable" observações que estão em julho com 
#'    outras que estão em outubro. Assim, vai sempre ter mais observações perto 
#'    zero porque foi a partir do mês 07 que as vias passaram a ter redução de
#'    velocidade. 
#'        - isso é problemático pela sazonalidade nos tempos de deslocamento. 
#'        Talvez incluir um efeito fixo de mês possa ajudar a limpar isso. 
#'        - ou fazer uma equação que estime os tempos de viagem, e ficar só com
#'        os resíduos (que nem o paper da expansão do metrô na China)
#'
#' VARIÁVEIS PARA FAZER O MATCHING:
#'    - classificação viária (from_id, to_id)
#'    - velocidade máxima (from_id, to_id)
#'    - número de lanes (from_id, to_id)
#'    - zona da cidade (8 zonas)
#'    - maior volume no pico da manhã ou da tarde         


