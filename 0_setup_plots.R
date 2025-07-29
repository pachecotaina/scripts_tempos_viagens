# ############################################################################ #
####                         TEMPLATE: HISTOGRAM                            ####
# ############################################################################ #
# Define reusable theme settings
my_theme <- theme_classic(base_size = 14) +  
  theme(
    plot.title = element_text(hjust = 0.5, face = "bold", size = 16),
    plot.subtitle = element_text(hjust = 0.5, face = "bold", size = 14),
    axis.title = element_text(face = "bold"),
    axis.text = element_text(size = 12),
    panel.grid.major = element_line(color = "gray90", linewidth = 0.3),
    panel.grid.minor = element_blank(),
    plot.caption = element_text(hjust = 0),  # Left-align caption
    plot.caption.position = "plot",
    legend.position = "top",
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.3)#,
    # legend.box.margin = margin(t = -10, r = 0, b = 0, l = 0)
  )

# Define reusable y-axis formatting
my_scale_y <- scale_y_continuous(labels = scales::number_format())

# zoning16 %>% 
#   ggplot(aes(x = BAR_max_diff)) +
#   geom_histogram(binwidth = 0.5, fill = "darkgrey", color = "black") +
# labs(
#   title = "",
#   x = "Max. BAR change (2016 relative to 2004)",
#   y = "Count (number of blocks)"
#   caption = "Source: Your Data"
# ) +
#   scale_y_continuous(labels = scales::number_format()) +
#   theme_classic(base_size = 14) +  # Clean theme
# theme(
#   plot.title = element_text(hjust = 0.5, face = "bold", size = 16),
#   axis.title = element_text(face = "bold"),
#   axis.text = element_text(size = 12),
#   panel.grid.major = element_line(color = "gray90", linewidth = 0.3),
#   panel.grid.minor = element_blank(),
#   plot.caption = element_text(hjust = 0),  # Left-align caption
#   plot.caption.position = "plot"  # Place caption inside the plot area
# )
#
# ggsave(
#   paste0("figures_PDE/hist_BAR_max_diff.jpeg"),
#   width = 12*1.66,
#   height = 12,
#   units = "cm")

# ############################################################################ #
####                          TEMPLATE: SP MAP                              ####
# ############################################################################ #
# zoning16 %>% filter(!is.na(diff_map)) %>% 
#   ggplot() +
#   geom_sf(aes(fill = diff_map), color = NA) +
#   geom_sf(data = shp_subs, color = "black", fill = NA, size = 3) +
#   scale_fill_manual(
#     values = c(
#       "-3.9 to -2.0" = "#54278f", 
#       "-1.0 to -0.0" = "#cbc9e2", 
#       "no change" = "darkgrey",
#       "0.0 to 1.9" = "#bae4b3",
#       "2.0 to 3.9" = "#006d2c")) +
my_theme_map <- theme_minimal() +
  theme(
    axis.text.x = element_blank(),
    axis.text.y = element_blank(),
    axis.ticks = element_blank(),
    legend.spacing.y = unit(-2, "pt"),
    legend.position = c(0.75, 0.3),  # Moves legend inside (x = right, y = bottom)
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.3),  # Adds a white box
    plot.caption = element_text(hjust = 0),  # Left-align caption
    plot.caption.position = "plot"  # Place caption inside the plot area
  )

my_north_arrow <- 
  annotation_north_arrow(
    location = "topright",  # Position on the map
    which_north = "grid",   # True north
    style = north_arrow_minimal(
      line_width = 0.5,  # Reduce the line thickness
      text_size = 6      # Reduce text size
    )
  ) 

my_scale <- 
  annotation_scale(
    location = "br",  # Position on the map
    width_hint = 0.3  # Width of the scale bar
  ) 

# 
# ggsave(
#   paste0("figures_PDE/map_BAR_max_diff.jpeg"),
#   width = 15,
#   height = 15*1.25,
#   units = "cm")