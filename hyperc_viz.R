df_results <- read_parquet(
  "data/output/hypercongestion_by_id.parquet") %>% 
  mutate(
    period = case_when(
      period == "before" ~ "Tratamento - Antes da mudança",
      period == "after" ~ "Tratamento - Após a mudança",
      period == "all" ~ "Controle - todo período"),
    period = factor(
      period,
      levels = c(
        "Tratamento - Antes da mudança",
        "Tratamento - Após a mudança",
        "Controle - todo período"))) %>% 
  filter(problems == "ok")

length(unique(df_results$id))

df_label <- df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  mutate(
    speed_dif = vel_carro_moto - max_speed,
    max_speed_pct = max_speed/vel_carro_moto) %>% 
  group_by(period) %>% 
  summarise(max_speed_pct_mean = mean(max_speed_pct, na.rm = TRUE),
            xposition = 1)

df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  mutate(speed_dif = vel_carro_moto - max_speed,
         max_speed_pct = max_speed/vel_carro_moto,
         max_speed_pct_mean = mean(max_speed_pct, na.rm = TRUE)) %>% 
  arrange(max_speed_pct) %>% 
  mutate(order = factor(row_number())) %>% 
  ggplot(aes(x = order, y = max_speed_pct)) +
  geom_point() +
  facet_grid(.~period) +
  geom_hline(data = df_label, aes(yintercept = max_speed_pct_mean), color = "red") +
  geom_text(data = df_label, 
            aes(y = max_speed_pct_mean+0.03, 
                x = xposition+300, 
                label = paste0(round(max_speed_pct_mean*100, 2), "%")), 
            color = "red") +
  scale_y_continuous(labels = scales::percent_format())+
  labs(x = "",
       y = "Velocidade de supersaturação\nem relação à velocidade máxima") +
  my_theme +
  theme(axis.ticks.x = element_blank(),
        axis.text.x = element_blank())


df_label_mean <- df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  mutate(
    speed_dif = vel_carro_moto - max_speed,
    max_speed_pct = max_speed/vel_carro_moto) %>% 
  group_by(period) %>% 
  summarise(max_speed_mean = mean(max_speed, na.rm = TRUE),
            xposition = 1)

df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  arrange(max_speed) %>% 
  mutate(order = factor(row_number())) %>% 
  ggplot(aes(x = order, y = max_speed)) +
  geom_point() +
  facet_grid(.~period) +
  geom_hline(data = df_label_mean, aes(yintercept = max_speed_mean), color = "red") +
  geom_text(data = df_label_mean, 
            aes(y = max_speed_mean+3, 
                x = xposition+300, 
                label = paste0(round(max_speed_mean, 2), "km/h")), 
            color = "red") +
  scale_y_continuous(labels = scales::number_format())+
  labs(x = "",
       y = "Velocidade de supersaturação") +
  my_theme +
  theme(axis.ticks.x = element_blank(),
        axis.text.x = element_blank())



df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  arrange(max_speed) %>% 
  mutate(order = factor(row_number())) %>% 
  pivot_longer(
    -c(id, period, max_flow, type, problems, order),
    names_to = "vel_type", values_to = "values")   %>% 
  ggplot(aes(x = order, y = values, color = vel_type)) +
  geom_point() +
  facet_grid(.~period)



#### apenas vias de 50km/h --------
df_label_mean50 <- df_results %>% 
  filter(type == "hypercongestion" & max_speed > 5 & problems == "ok") %>% 
  filter(vel_carro_moto == 50) %>% 
  group_by(period) %>% 
  summarise(
    max_speed_mean = mean(max_speed, na.rm = TRUE),
    max_speed_sd = sd(max_speed, na.rm = TRUE),
    xposition = 1) %>% 
  mutate(
    label = paste0(round(max_speed_mean, 2), "km/h [sd. ", round(max_speed_sd, 2), "]")
  )

df_results %>% 
  filter(type == "hypercongestion" & max_speed > 5 & problems == "ok") %>% 
  filter(vel_carro_moto == 50) %>% 
  arrange(max_speed) %>% 
  mutate(order = factor(row_number())) %>% 
  ggplot(aes(x = order, y = max_speed)) +
  geom_point() +
  facet_grid(.~period) +
  geom_hline(data = df_label_mean50, aes(yintercept = max_speed_mean), color = "red") +
  geom_text(
    data = df_label_mean50, 
    aes(y = max_speed_mean+2, 
        x = xposition+90, 
        label = label), 
        # label = paste0(round(max_speed_mean, 2), "km/h")), 
    color = "red",
    size = 3) +
  scale_y_continuous(labels = scales::number_format())+
  labs(x = "",
       y = "Velocidade de supersaturação") +
  my_theme +
  theme(axis.ticks.x = element_blank(),
        axis.text.x = element_blank())

ggsave(
  paste0("figures_hypc_viz/velocidade_hypc_50kmh.png"), 
  plot = last_plot(), 
  width = 10, 
  height = 4)

#### apenas vias de 40km/h --------
df_label_mean40 <- df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  filter(vel_carro_moto == 40) %>% 
  mutate(
    speed_dif = vel_carro_moto - max_speed,
    max_speed_pct = max_speed/vel_carro_moto) %>% 
  group_by(period) %>% 
  summarise(max_speed_mean = mean(max_speed, na.rm = TRUE),
            xposition = 1)

df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  filter(vel_carro_moto == 40) %>% 
  arrange(max_speed) %>% 
  mutate(order = factor(row_number())) %>% 
  ggplot(aes(x = order, y = max_speed)) +
  geom_point() +
  facet_grid(.~period) +
  geom_hline(data = df_label_mean40, aes(yintercept = max_speed_mean), color = "red") +
  geom_text(
    data = df_label_mean40, 
    aes(y = max_speed_mean+2, 
        x = xposition+100, 
        label = paste0(round(max_speed_mean, 2), "km/h")), 
    color = "red") +
  scale_y_continuous(labels = scales::number_format())+
  labs(x = "",
       y = "Velocidade de supersaturação") +
  my_theme +
  theme(axis.ticks.x = element_blank(),
        axis.text.x = element_blank())

#### apenas vias de 60km/h --------
df_label_mean60 <- df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  filter(vel_carro_moto == 60) %>% 
  mutate(
    speed_dif = vel_carro_moto - max_speed,
    max_speed_pct = max_speed/vel_carro_moto) %>% 
  group_by(period) %>% 
  summarise(max_speed_mean = mean(max_speed, na.rm = TRUE),
            xposition = 1)

df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  filter(vel_carro_moto == 60) %>% 
  arrange(max_speed) %>% 
  mutate(order = factor(row_number())) %>% 
  ggplot(aes(x = order, y = max_speed)) +
  geom_point() +
  facet_grid(.~period) +
  geom_hline(data = df_label_mean60, aes(yintercept = max_speed_mean), color = "red") +
  geom_text(
    data = df_label_mean60, 
    aes(y = max_speed_mean+2, 
        x = xposition+10, 
        label = paste0(round(max_speed_mean, 2), "km/h")), 
    color = "red") +
  scale_y_continuous(labels = scales::number_format())+
  labs(x = "",
       y = "Velocidade de supersaturação") +
  my_theme +
  theme(axis.ticks.x = element_blank(),
        axis.text.x = element_blank())

#### apenas vias de 40km/h --------
df_label_mean70 <- df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  filter(vel_carro_moto == 70) %>% 
  mutate(
    speed_dif = vel_carro_moto - max_speed,
    max_speed_pct = max_speed/vel_carro_moto) %>% 
  group_by(period) %>% 
  summarise(max_speed_mean = mean(max_speed, na.rm = TRUE),
            xposition = 1)

df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  filter(vel_carro_moto == 70) %>% 
  arrange(max_speed) %>% 
  mutate(order = factor(row_number())) %>% 
  ggplot(aes(x = order, y = max_speed)) +
  geom_point() +
  facet_grid(.~period) +
  geom_hline(data = df_label_mean70, aes(yintercept = max_speed_mean), color = "red") +
  geom_text(
    data = df_label_mean70, 
    aes(y = max_speed_mean+2, 
        x = xposition+1, 
        label = paste0(round(max_speed_mean, 2), "km/h")), 
    color = "red") +
  scale_y_continuous(labels = scales::number_format())+
  labs(x = "",
       y = "Velocidade de supersaturação") +
  my_theme +
  theme(axis.ticks.x = element_blank(),
        axis.text.x = element_blank())

#### apenas vias de mais de 70km/h --------
df_label_mean70plus <- df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  filter(vel_carro_moto > 70) %>% 
  mutate(
    speed_dif = vel_carro_moto - max_speed,
    max_speed_pct = max_speed/vel_carro_moto) %>% 
  group_by(period) %>% 
  summarise(max_speed_mean = mean(max_speed, na.rm = TRUE),
            xposition = 1)

df_results %>% 
  filter(type == "hypercongestion" & max_speed > 0 & problems == "ok") %>% 
  filter(vel_carro_moto > 70) %>% 
  arrange(max_speed) %>% 
  mutate(order = factor(row_number())) %>% 
  ggplot(aes(x = order, y = max_speed)) +
  geom_point() +
  facet_grid(.~period) +
  geom_hline(data = df_label_mean70plus, aes(yintercept = max_speed_mean), color = "red") +
  geom_text(
    data = df_label_mean70plus, 
    aes(y = max_speed_mean+2, 
        x = xposition+10, 
        label = paste0(round(max_speed_mean, 2), "km/h")), 
    color = "red") +
  scale_y_continuous(labels = scales::number_format())+
  labs(x = "",
       y = "Velocidade de supersaturação") +
  my_theme +
  theme(axis.ticks.x = element_blank(),
        axis.text.x = element_blank())
