#!/usr/bin/env -S Rscript --no-save --no-restore

source("lib.R")
source("util.R")

args = commandArgs(trailingOnly=TRUE)
# OUTPUT.DIR <<- "output"
OUTPUT.DIR <<- tail(args, n=1)

# database.baseline <- "input/network-reference.duckdb"
database.baseline <- args[1]
# database.patch <- "input/network-patch.duckdb"
database.patch <- args[2]
# database.failover <- "input/network-failover.duckdb"
database.failover <- args[3]
# database.failover.snapshot <- "input/network-failover-snapshot.duckdb"
database.failover.snapshot <- args[4]

#########################################
###### DATA PREPARATION #################
#########################################
con.baseline <- create.con(database.baseline)
con.patch <- create.con(database.patch)
con.failover <- create.con(database.failover)
con.failover.snapshot <- create.con(database.failover.snapshot)

fetch.network <- function(con, input_commit=NA) {
  commit <- "1=1"
  if (!is.na(input_commit)) {
    commit <- paste("run_commit = '", input_commit, "'", sep="")
  }
  
  print(con)
  data <- dbGetQuery(
    con,
    sqlInterpolate(
      con,
      "
WITH all_network AS (
  SELECT SUM(bytes) / 1024.**2 AS total_mibi_bytes, 
         SUM(bytes) / 1024. AS total_kibi_bytes, 
         run_id
  FROM redis_all_network
  WHERE time_s >= 10 -- 10s warmup phase
  GROUP BY ALL
), cluster_network AS (
  SELECT SUM(bytes) / 1024.**2 AS total_mibi_bytes, 
         SUM(bytes) / 1024. AS total_kibi_bytes, 
         run_id
  FROM redis_cluster_network
  WHERE time_s >= 10 -- 10s warmup phase
  GROUP BY ALL
)
SELECT all_net.total_mibi_bytes AS all_total_mibi_bytes,
       cluster_net.total_mibi_bytes AS cluster_total_mibi_bytes,
       all_net.total_kibi_bytes / (benchmark_framework_end_time_s - 10.) AS all_total_kibi_bytes_per_s,
       cluster_net.total_kibi_bytes / (benchmark_framework_end_time_s - 10.) AS cluster_total_kibi_bytes_per_s,
       run.*
FROM all_network AS all_net
  JOIN cluster_network AS cluster_net USING(run_id)
  JOIN run USING(run_id)
WHERE ?commit;"
      ,
      commit=SQL(commit)
    ))
  # Start at 0 time as we have a warmup phase
  factor.experiment(data, con)
}

data.baseline <- fetch.network(con.baseline)
data.patch <- fetch.network(con.patch)
data.failover <- fetch.network(con.failover)
data.failover.snapshot <- fetch.network(con.failover.snapshot)

data.baseline$group <- "low-network"
data.baseline.base <- data.baseline %>% filter(run_commit == 'network')
data.baseline.base$facet <- "Baseline"
data.baseline.base$name <- "Baseline"
data.baseline.base$name2 <- "Baseline"
data.baseline.patch <- data.baseline %>% filter(run_commit == 'livepatch-network')
data.baseline.patch$facet <- "Live Patch"
data.baseline.patch$name <- "No Patch"
data.baseline.patch$name2 <- "Baseline (Live Patch)"

data.patch$group <- "low-network"
data.patch$facet <- "Live Patch"
data.patch$name <- paste("LP ", data.patch$patch_method,  sep="")
data.patch$name2 <- paste("LP ", data.patch$patch_method,  sep="")

data.failover$facet <- "CP Full"
data.failover$name <- "CP Full"
data.failover$name2 <- "CP Full"
data.failover$group <- "high-network"

data.failover.snapshot$facet <- "CP Part"
data.failover.snapshot$name <- "CP Part"
data.failover.snapshot$name2 <- "CP Part"
data.failover.snapshot$group <- "low-network"

data <- rbind(data.baseline.base,data.baseline.patch, data.patch, data.failover, data.failover.snapshot)
data$name <- factor(data$name, levels = c("Baseline", "No Patch", "LP Pull", "LP Push", "CP Part", "CP Full"))
data$name2 <- factor(data$name2, levels = c("Baseline", "No Patch", "LP Pull", "LP Push", "CP Part", "CP Full"))
data$data_max_memory_usage_gb <- factor(data$data_max_memory_usage_gb)
data$facet <- factor(data$facet, levels=c("Baseline", "Live Patch", "CP Part", "CP Full"))

data <- data %>% pivot_longer(c(all_total_kibi_bytes_per_s),
                      names_to = "network",
                      values_to = "value")
data$network <- factor(data$network)
levels(data$network) <- list("All"="all_total_kibi_bytes_per_s")

#########################################
############## PLOTTING #################
#########################################
data[data$facet == "CP Full" , ]$value <- data[data$facet == "CP Full", ]$value / 1024.

ylim_expansion_low <- list(5, -100)
ylim_expansion_high <- list(0, 0)
ylimlow <- mapply("-", range(data$value[data$network == "All" & data$facet != 'CP Full']), ylim_expansion_low)
#ylimlowfailover <- mapply("-", range(data$value[data$group == "low-network" & data$network == "Client" & data$facet == 'Failover']), ylim_expansion)
ylimhigh <- mapply("-", range(data$value[data$network == "All" & data$facet == 'CP Full']), ylim_expansion_high)


plot <- ggplot(data=data) +
  xlab("Memory State Size [GiB]") +
  facet_nested( ~ name,
               scales = "free",
               space= "free_x",
               independent = "y") +
  facetted_pos_scales(
    y = list(
      name == "Baseline" ~ scale_y_log10(limits = ylimlow),
      name == "No Patch" ~ scale_y_log10(limits = ylimlow, guide = "none"),
      name == "LP Pull" ~ scale_y_log10(limits = ylimlow, guide = "none"),
      name == "LP Push" ~ scale_y_log10(limits = ylimlow, guide = "none"),
      name == "CP Part" ~ scale_y_log10(limits = ylimlow, guide = "none"),
      name == "CP Full" ~ scale_y_sqrt(limits = ylimhigh)
    )
  )

plot <- plot +
  geom_point(
    aes(
      x = data_max_memory_usage_gb,
      y = value,
      color = o_masters_replicas_id_text,
      group = o_masters_replicas_id_text,
    ),
    size = 1,
    shape = 0
  ) +
  geom_line(
    aes(
      x = data_max_memory_usage_gb,
      y = value,
      color = o_masters_replicas_id_text,
      group = o_masters_replicas_id_text,
    ),
    linewidth = 0.3
  ) +
  scale_fill_viridis(discrete = TRUE, option='turbo')

plot <- plot + plot.theme.paper()

# width=15, height = 18, use.grid=FALSE
ggplot.save(plot_grid(plot +
              theme(legend.position = c(0.5,1.55),
                    plot.margin = margin(4.5, 0.5, 0.5, 5, unit="mm"),
                    #axis.title.y = element_text(margin = margin(r=-2)),
                    legend.direction ="horizontal",
                    legend.text = element_text(size = FONT.SIZE - 1, margin=margin(l=-3, r=5)),
                    legend.key.size = unit(1, "mm"),
                    legend.margin = margin(0,0,0,0),
                    legend.title = element_blank(),
                    legend.background = element_blank(),
                    axis.title.y = element_blank(),
                    axis.text.x = element_text(angle = -35, hjust = 0.5, vjust=-0.2),
                    # Decrease width of 4. column
                    panel.spacing.x = unit(c(0.2, 0.2, 0.2,0.2, 3.3), "mm"),
                    legend.spacing.y = unit(0.0, "mm"),
                    axis.title.x = element_text(margin = margin(t=-1))
                    ) +
              guides(color = guide_legend(nrow = 2))) +
              draw_label("Avg. Network\nTraffic [KiB/s]", x=0.028, y=0.45, vjust=0.5, angle= 90,
                         fontfamily = "paper", fontface = "bold", size = FONT.SIZE) +
              draw_label("[MiB/s]", x=0.794, y=0.45, vjust=0.5, angle= 90,
                         fontfamily = "paper", fontface = "bold", size = FONT.SIZE) 
            , "Network-Line", width=7.5, height=2.3, use.grid=FALSE)

