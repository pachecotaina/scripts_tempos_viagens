rm(list = ls())
gc()

options(scipen=999)

# set JAVA parameters (for r5r)
Sys.setenv("JAVA_HOME" = "/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home")
Sys.setenv("PATH" = paste(Sys.getenv("PATH"), "/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home/bin", sep = ":"))
options(java.parameters = '-Xmx20G')

# Carregando os pacotes e instalando algum se necessário ----
load.lib <- c("tidyverse", "sf", "xlsx", "readxl", "foreign", "stringr",
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
              "arrow")
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

source("scripts_tempos_viagens/funs/helper_feriados.R")


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
#'       1. Filtro apenas para rotas que possuem o menos 5 observações no período
#' >> Diversas rotas são muito pequenas, e esses casos normalmente são erros. 
#'       2. Filtro para rotas de 100m de distância ou mais
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
####                      CREATE DATA FOR ANALYSIS                          ####
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
source("scripts_tempos_viagem/analise_preliminar.R")
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


