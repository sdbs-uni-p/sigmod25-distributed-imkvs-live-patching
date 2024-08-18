#!/usr/bin/env -S Rscript --no-save --no-restore

source("lib.R")
source("util.R")

args = commandArgs(trailingOnly=TRUE)
# OUTPUT.DIR <<- "output"
OUTPUT.DIR <<- tail(args, n=1)

# csv <- "input/realworldpatches.csv"
csv <- args[1]
data <- read.csv(csv, sep=',')
# data.plot <- data %>% filter(port == 7000) # Use only first node
data.plot <- data
data.plot$patch_size_kib <- data.plot$patch_size_byte / 1024

print(data.plot)
print(data.plot %>% summarize(patch_time_ms, patch_size_kib))

plot <- ggplot(data=data.plot) +
  xlab("Patch File Size [KiB]") +
  ylab("") +
  scale_x_log10() +
  scale_y_log10() +
  expand_limits(y=c(0.06, 70))
  

plot <- plot +
  geom_point(
             aes(x = patch_size_kib,
                 y = patch_time_ms),
             size = 0.8,
             shape = 4,
             alpha = 0.5,
             color = '#006400',
             
             show.legend = F)
plot <- plot +
  geom_label_repel(
    data = data.plot %>%
      mutate(max_patch_time_ms = max(patch_time_ms)) %>%
      filter(patch_time_ms == max_patch_time_ms),
    aes(
      x = patch_size_kib,
      y = patch_time_ms,
      label = round(patch_time_ms, 2),
    ),
    label.padding = unit(0.25, "mm"),
    alpha = 0.75,
    fill = "purple",
    color = "white",
    max.overlaps = Inf,
    
    #Do not show the border because the border is white for a white color
    label.size = NA,
    # Draw every segment
    min.segment.length = 0,
    segment.size = 0.3,
    box.padding = 0.5,
    nudge_x = 0.04,
    nudge_y = -2,
    segment.color = "red",
    segment.alpha = 0.7,
    size = 2.5,
    max.time = 10,
    max.iter = 1000000,
    stat = "unique"
  )

plot <- plot +
  geom_label_repel(
    data = data.plot %>%
      mutate(min_patch_time_ms = min(patch_time_ms)) %>%
      filter(patch_time_ms == min_patch_time_ms),
    aes(
      x = patch_size_kib,
      y = patch_time_ms,
      label = round(patch_time_ms, 2),
    ),
    label.padding = unit(0.25, "mm"),
    alpha = 0.75,
    fill = "purple",
    color = "white",
    max.overlaps = Inf,
    
    #Do not show the border because the border is white for a white color
    label.size = NA,
    # Draw every segment
    min.segment.length = 0,
    segment.size = 0.3,
    box.padding = 0.5,
    nudge_x = -0.1,
    nudge_y = 1,
    segment.color = "red",
    segment.alpha = 0.7,
    size = 2.5,
    max.time = 10,
    max.iter = 1000000,
    stat = "unique"
  )

plot <- plot + plot.theme.paper()

# width=15, height = 18, use.grid=FALSE
ggplot.save(plot_grid(plot + 
              theme(legend.position = c(0.46,1.83),
                    plot.margin = margin(2, 0.5, 0.5, 0.5, unit="mm"),
                    legend.direction ="horizontal",
                    legend.text = element_text(size = FONT.SIZE - 1, margin=margin(l=-5)),
                    legend.key.size = unit(1, "mm"),
                    legend.margin = margin(0,0,0,0),
                    axis.title.y = element_text(margin=margin(r=10)),
                    legend.title = element_blank(),
                    legend.background = element_blank()) +
              guides(fill = guide_legend(ncol = 4))) +
              draw_label("Patch Application\nDuration [ms]", x=0.022, y=0.5, vjust=0.5, angle= 90,
                         fontfamily = "paper", fontface = "bold", size = FONT.SIZE)
            , "Patch-Duration-RealWorld", width=10, height=2.5, use.grid=FALSE)

