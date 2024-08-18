#!/usr/bin/env -S Rscript --no-save --no-restore

source("lib.R")
source("util.R")

args = commandArgs(trailingOnly=TRUE)
# OUTPUT.DIR <<- "output"
OUTPUT.DIR <<- tail(args, n=1)

# database.patch.idle <- "input/synchronization-time-patch-idle.duckdb"
database.patch.idle <- args[1]

#########################################
###### DATA PREPARATION #################
#########################################
con.patch.idle <- create.con(database.patch.idle)

fetch.synchronization.patch <- function(con) {
  data <- dbGetQuery(con,
                     "
SELECT apply_time_s - new_time_s AS synchronization_duration_s, version, run.*
FROM
  (SELECT MAX(time_s) apply_time_s, run_id, version FROM wf_r_patch_applied GROUP BY ALL) patch_applied
  JOIN
  -- Patch is initially applied to one node, which distributes the patch to all other nodes. We use time of initial node.
  (SELECT MIN(time_s) new_time_s, run_id, version FROM wf_r_new_patch GROUP BY ALL) new_patch
  USING (run_id, version)
  JOIN run USING(run_id)
                     ")
  factor.experiment(data, con)
}
data.patch.idle <- fetch.synchronization.patch(con.patch.idle)

set.masters.replicas <- function(df) {
  df.masters <- df %>% filter(o_replicas_per_master == 1)
  df.masters$facet <- "<X> P. - 1 R."
  df.masters$o_masters_replicas_id_text <- paste(df.masters$o_masters, "P.")
  
  df.replicas <- df %>% filter(o_masters == 15)
  df.replicas$facet <- "15 P. - <X> R."
  df.replicas$o_masters_replicas_id_text <- paste(df.replicas$o_replicas_per_master, "R.")
  
  o_masters_replicas_id_text_factor_order <- c(
    paste(sort(unique(df.masters$o_masters)), "P."),
    paste(sort(unique(df.replicas$o_replicas_per_master)), "R.")
  )
  data <- rbind(df.masters, df.replicas)
  data$o_masters_replicas_id_text <- factor(data$o_masters_replicas_id_text, levels=o_masters_replicas_id_text_factor_order)
  return(data)
}
# For idle, we use the max memory as X axis
data.patch.idle$x <- factor(data.patch.idle$data_max_memory_usage_gb)
data.patch.idle$row <- "Idle"

data.patch.idle <- set.masters.replicas(data.patch.idle)
data <- data.patch.idle
#########################################
############## PLOTTING #################
#########################################
minmax <- function(x) {
  subset(x, x == max(x) | x == min(x))
}
ylim_expansion <- list(0, 0)
ylimpullidle <- mapply("-", range(data$synchronization_duration_s[data$patch_method == "Pull" & data$row == "Idle"]), ylim_expansion)

ylimpushidle <- mapply("-", range(data$synchronization_duration_s[data$patch_method == "Push" & data$row == "Idle"]), ylim_expansion)

plot <- ggplot() +
  ylab("Update Lag [s]") +
  xlab("Memory State [GiB]") +
  facet_nested(patch_method ~  facet, 
               scale="free_y",
               independent="y") +
  facetted_pos_scales(
    y = list(
      patch_method == "Pull" & facet != "15 P. - <X> R." ~ scale_y_continuous(limits = ylimpullidle),
      patch_method == "Pull" & facet == "15 P. - <X> R." ~ scale_y_continuous(limits = ylimpullidle, guide="none"),
      
      patch_method == "Push" & facet != "15 P. - <X> R." ~ scale_y_continuous(limits = ylimpushidle, expand = c(0, 0.02), breaks=c(0.1, 0.15, 0.2), labels=c("0.1", "", "0.2")),
      patch_method == "Push" & facet == "15 P. - <X> R." ~ scale_y_continuous(limits = ylimpushidle, expand = c(0, 0.02), guide="none")
    )
  )


plot <- plot +
  geom_boxplot(
    data = data,
    aes(x = x,
        y = synchronization_duration_s,
        fill=o_masters_replicas_id_text),
    position = position_dodge(width = 0.9),
    linewidth=0.2,
    fatten=0.6,
    outlier.shape = NA
  ) + stat_summary(
    fun.y = minmax, 
    data=data, 
    mapping=aes(x = x,
                y = synchronization_duration_s,
                color=o_masters_replicas_id_text),
    position = position_dodge(width = 0.9),
    shape=4,
    size=0.5,
    geom="point",
    show.legend=F) +
  scale_fill_viridis(discrete=TRUE, option='turbo') +
  scale_color_viridis(discrete=TRUE, option='turbo')
plot <- plot + plot.theme.paper()

# width=15, height = 18, use.grid=FALSE
ggplot.save(plot +
              theme(legend.position = c(0.5,1.3),
                    plot.margin = margin(5, 1.5, 2, 0.5, unit="mm"),
                    #axis.title.x = element_text(margin = margin(t=-2), vjust=-0.5),
                    #axis.title.y = element_text(margin = margin(r=1)),
                    legend.direction ="horizontal",
                    legend.text = element_text(size = FONT.SIZE - 1, margin=margin(l=-3)),
                    legend.key.size = unit(1, "mm"),
                    legend.margin = margin(0,0,0,0),
                    legend.title = element_blank(),
                    legend.background = element_blank(),
                    # Horizontal spacing between facets
                    panel.spacing.x = unit(0.6, "mm"),
                    # Vertical spacing between facets
                    panel.spacing.y = unit(0.6, "mm"),
                    # Reduce space between the legend rows
                    legend.spacing.y = unit(0.3, "mm")) +
            
              guides(fill = guide_legend(byrow = T, ncol=5))
            , "Synchronization-Time-Patch-Boxplot-Idle", width=10, height=3.3, use.grid=F)


