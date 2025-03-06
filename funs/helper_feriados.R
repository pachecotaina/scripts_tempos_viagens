#'https://www.capital.sp.gov.br/w/noticia/prefeitura-divulga-calendario-de-feriados-pontos-facultativos-e-suspensao-de-expediente-para-2024
#' Serão pontos facultativos os dias de Carnaval (12 e 13 de fevereiro), 
#' a Quarta-Feira de Cinzas até as 12h (14 de fevereiro), 
#' Dia do Servidor Público (28 de outubro), véspera de Natal (24 de dezembro) e 
#' véspera de Ano Novo (31 de dezembro). 
# Estão mantidos os feriados Nacionais, Estaduais e Municipais abaixo:
# 1 de janeiro - Confraternização Universal
# 25 de janeiro - Aniversário da Cidade
# 29 de março - Paixão de Cristo
# 21 de abril - Tiradentes
# 1 de maio - Dia Mundial do Trabalho
# 30 de maio - Corpus Christi
# 9 de julho - Data Magna do Estado de São Paulo
# 7 de setembro - Independência do Brasil
# 12 de outubro - Nossa Senhora Aparecida (Padroeira do Brasil)
# 2 de novembro - Finados
# 15 de novembro - Proclamação da República
# 20 de novembro - Dia Nacional de Zumbi e da Consciência Negra
# 25 de dezembro - Natal

vec_years <- c(2016:2020)
bank_holiday <- list()
for (i in seq_along(vec_years)) {
  bank_holiday[[i]] <- tibble(
    "date" = paste0(
      vec_years[i],
      c("/01/01", "/01/25", "/03/29", "/04/21", "/05/01", 
        "/05/30", "/06/09", "/09/07", "/10/12", "/10/15", 
        "/10/28", "/11/02","/11/15", "/11/20", "/12/24", 
        "/12/25", "/12/31")))
}

remove(vec_years)

bank_holiday <- bind_rows(bank_holiday) %>% 
  filter(date %notin% c("2018/11/20", "2018/11/15"))

# https://www.feriados.com.br/2016 <2017/2018/2019/2020>
# carnaval
bank_holiday <- bank_holiday %>% 
  rbind(tibble(
    "date" = c("2016/02/08", "2016/02/09", "2016/02/10",
               "2017/02/27", "2017/02/28", "2017/03/01",
               "2018/02/12", "2018/02/13", "2018/02/14",
               "2019/03/04", "2019/03/05", "2019/03/06",
               "2020/02/24", "2020/02/25", "2020/02/26")))

# corpus christi
bank_holiday <- bank_holiday %>% 
  rbind(tibble(
    "date" = c("2016/05/26",
               "2017/06/15",
               "2018/05/31",
               "2019/06/20",
               "2020/06/11")))

# emendas
bank_holiday <- bank_holiday %>% 
  rbind(tibble(
    "date" = c("2016/02/05", # sexta-feira de Carnaval
               "2016/03/25", # sexta-feira santa (antes da Páscoa)
               "2016/04/22", # sexta-feira após Tiradentes
               "2016/05/27", # sexta-feira após Corpus Christi
               # "2016/09/05", "2016/09/06", "2016/09/08", "2016/09/09", # semana independencia
               # "2016/10/10", "2016/10/11", "2016/10/13", "2016/10/14", # semana do saco cheio
               "2016/11/14", # segunda-feira antes da Proclamação da República
               
               "2017/02/24", # sexta-feira de Carnaval
               "2017/04/14", # sexta-feira santa (antes da Páscoa)
               "2017/06/16", # sexta-feira após Corpus Christi
               # "2017/09/04", "2017/09/05", "2017/09/06", "2017/09/08", # semana independencia
               "2017/09/08", # sexta-feira após 7 de setembro
               # "2017/10/09", "2017/10/10", "2017/10/11", "2017/10/13", # semana do saco cheio
               "2017/10/13", # sexta-feira após 12/10
               "2017/11/03",  # sexta-feira após Finados
               
               "2018/02/09", # sexta-feira de Carnaval
               "2018/03/20", # sexta-feira santa (antes da Páscoa)
               "2018/04/31", # segunda-feira antes do dia do Trabalho
               "2018/06/01", # sexta-feira após Corpus Christi
               # "2018/09/03", "2018/09/04", "2018/09/05", "2018/09/06", # semana independencia
               # "2018/10/08", "2018/10/09", "2018/10/10", "2018/10/11", # semana do saco cheio
               #"2018/11/16", # sexta-feira após Proclamação da República
               #"2018/11/19", # segunda-feira antes da Consciência Negra
               
               "2019/03/01", # sexta-feira de Carnaval
               "2019/04/19", # sexta-feira santa (antes da Páscoa)
               "2019/06/21", # sexta-feira após Corpus Christi
               # "2019/09/02", "2019/09/03", "2019/09/04", "2019/09/05", "2019/09/06", # semana independencia
               # "2019/10/07", "2019/10/08", "2019/10/09", "2019/10/10", "2019/10/11", # semana do saco cheio
               
               "2020/02/21", # sexta-feira de Carnaval
               "2020/04/10", # sexta-feira santa (antes da Páscoa)
               "2020/04/20", # segunda-feira antes de Tiradentes
               "2020/06/12" # sexta-feira após Corpus Christi
               # "2020/09/08", "2020/09/09", "2020/09/10", "2020/09/11", # semana independencia
               # "2020/10/13", "2020/10/14", "2020/10/15", "2020/10/16" # semana do saco cheio
    )))

bank_holiday <- bank_holiday %>% 
  mutate(date = as.Date(date))

write_parquet(bank_holiday, "data/intermediate/bank_holiday.parquet")
