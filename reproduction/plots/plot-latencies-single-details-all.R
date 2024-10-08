#!/usr/bin/env -S Rscript --no-save --no-restore

source("lib.R")
source("util.R")

args = commandArgs(trailingOnly=TRUE)
# OUTPUT.DIR <<- "output"
OUTPUT.DIR <<- tail(args, n=1)

# database.baseline <- "input/reference.duckdb"
database.baseline <- args[1]
# database.patch <- "input/latencies-patch.duckdb"
database.patch <- args[2]
# database.failover <- "input/latencies-failover.duckdb"
database.failover <- args[3]
# database.failover.snapshot <- "input/latencies-failover-snapshot.duckdb"
database.failover.snapshot <- args[4]

#########################################
###### DATA PREPARATION #################
#########################################
con.baseline <- create.con(database.baseline)
con.patch <- create.con(database.patch)
con.failover <- create.con(database.failover)
con.failover.snapshot <- create.con(database.failover.snapshot)



fetch.latencies <- function(con,
                            extreme_upper_quantile = 0.99995,
                            sample_standard_rate = 0.001,
                            input_commit = NA) {
  commit <- "1=1"
  if (!is.na(input_commit)) {
    commit <- paste("run_commit = '", input_commit, "'", sep="")
  }
  
  print(con)
  query <- sqlInterpolate(
    con,
    "
  WITH quantiles AS (
    SELECT run_id,
      QUANTILE_CONT(latency_ms, ?extreme_upper_quantile) as extreme_quantile
    FROM latencies
    GROUP BY ALL
  ), extreme_latencies AS (
    SELECT run_id,
      latency_ms,
      time_s,
      port
    FROM latencies JOIN quantiles USING(run_id)
    WHERE latencies.latency_ms >= quantiles.extreme_quantile
  ), standard_sample_latencies AS (
    SELECT run_id,
      latency_ms,
      time_s,
      port
    FROM latencies JOIN quantiles USING(run_id)
    WHERE latencies.latency_ms < quantiles.extreme_quantile
    USING SAMPLE ?sample_standard_rate PERCENT (bernoulli)
  )
  SELECT 'Tail Latency' AS latency_type,
    run.*,
    latency_ms,
    time_s,
    port,
    node_name
  FROM extreme_latencies 
    JOIN run USING(run_id)
    JOIN master_replica_group_names USING(run_id)
  WHERE list_contains(ports, port)
    AND time_s >= 10 -- warmup
    AND ?commit
  UNION ALL
  SELECT 'Standard Latency' AS latency_type,
    run.*,
    latency_ms,
    time_s,
    port,
    node_name
  FROM standard_sample_latencies 
    JOIN run USING(run_id)
    JOIN master_replica_group_names USING(run_id)
  WHERE list_contains(ports, port)
    AND time_s >= 10 -- warmup
    AND ?commit
  ",
    extreme_upper_quantile = extreme_upper_quantile,
    sample_standard_rate = sample_standard_rate,
    commit = SQL(commit)
  )
  data <- dbGetQuery(con, query)
  factor.experiment(data, con)
}

# Close cons to relase resources
data.baseline <- fetch.latencies(con.baseline, input_commit="7.0.11")
close.con(con.baseline)
data.patch <- fetch.latencies(con.patch)
close.con(con.patch)
data.failover <- fetch.latencies(con.failover)
close.con(con.failover)
data.failover.snapshot <- fetch.latencies(con.failover.snapshot)
close.con(con.failover.snapshot)

con.baseline <- create.con(database.baseline)
con.patch <- create.con(database.patch)
con.failover <- create.con(database.failover)
con.failover.snapshot <- create.con(database.failover.snapshot)

data.baseline$name <- paste("Baseline (", data.baseline$run_commit, ")", sep="")
data.patch$name <- paste("LP ", data.patch$patch_method, sep="")
data.failover$name <- "CP Full"
data.failover.snapshot$name <- "CP Part"

data.baseline$facet <- "Baseline"
data.patch$facet <- paste("LP ", data.patch$patch_method, sep="")
data.failover$facet <- "CP Full"
data.failover.snapshot$facet <- "CP Part"

data <- rbind(data.baseline, data.patch, data.failover, data.failover.snapshot)

data <- factor.patch.strategy.names(data, "name")
data <- factor.patch.strategy.names(data, "facet")

data$latency_type <- factor(data$latency_type, levels=c("Tail Latency", "Standard Latency"))
#############################################################################################
# Patch Data:
fetch.patch.time <- function(con) {
  # This produces too much data (one data point for each node.)
  # I only want to have one data for each cluster. So we use the patch_received time.
  # SELECT time_s, run.*
  # FROM wf_l_e2e_patched JOIN run USING(run_id);
  data <- dbGetQuery(con,
                     "
                     SELECT time_s, node_name, run.* 
                     FROM wf_r_new_patch 
                       JOIN run USING(run_id) 
                       JOIN master_replica_group_names USING(run_id)
                     WHERE list_contains(ports, port)
                     AND from_client = 1;
                     ")
  factor.experiment(data, con)
}

action.data.patch <- fetch.patch.time(con.patch)
action.data.patch$facet <- paste("LP ", action.data.patch$patch_method, sep="")
action.data.patch$name <- paste("LP ", action.data.patch$patch_method, sep="")
action.data.patch <- factor.patch.strategy.names(action.data.patch, "name")
action.data.patch <- factor.patch.strategy.names(action.data.patch, "facet")

#############################################################################################
# Failover  Data:
action.data.failover.restart <- fetch.failover.restart.data(con.failover)
action.data.failover.restart$facet <- "CP Full"
action.data.failover.restart$name <- "CP Full"

action.data.failover.failover <- fetch.failover.failover.data(con.failover)
action.data.failover.failover$facet <- "CP Full"
action.data.failover.failover$name <- "CP Full"

action.data.failover.snapshot.restart <- fetch.failover.restart.data(con.failover.snapshot)
action.data.failover.snapshot.restart$facet <- "CP Part"
action.data.failover.snapshot.restart$name <- "CP Part"

action.data.failover.snapshot.failover <- fetch.failover.failover.data(con.failover.snapshot)
action.data.failover.snapshot.failover$facet <- "CP Part"
action.data.failover.snapshot.failover$name <- "CP Part"

action.data.failover.restart <- rbind(action.data.failover.restart, action.data.failover.snapshot.restart)
action.data.failover.restart <- factor.patch.strategy.names(action.data.failover.restart, "name")
action.data.failover.restart <- factor.patch.strategy.names(action.data.failover.restart, "facet")


action.data.failover.failover <- rbind(action.data.failover.failover, action.data.failover.snapshot.failover)
action.data.failover.failover <- factor.patch.strategy.names(action.data.failover.failover, "name")
action.data.failover.failover <- factor.patch.strategy.names(action.data.failover.failover, "facet")


fetch.bgsave <- function(con) {
  data <- dbGetQuery(con, 
                     "
  SELECT time_s, node_name, run.*
  FROM redis_bgsaves JOIN run USING(run_id)
  JOIN master_replica_group_names USING(run_id)
  WHERE list_contains(ports, port)
  AND time_s >= 10; -- 10s warmup
  ")
  factor.experiment(data, con)
}
data.bgsave <- fetch.bgsave(con.failover)
data.bgsave$facet <- "CP Full"
data.bgsave$name <- "CP Full"


data.bgsave.snapshot <- fetch.bgsave(con.failover.snapshot)
data.bgsave.snapshot$facet <- "CP Part"
data.bgsave.snapshot$name <- "CP Part"

data.bgsave <- rbind(data.bgsave, data.bgsave.snapshot)

data.bgsave <- factor.patch.strategy.names(data.bgsave, "name")
data.bgsave <- factor.patch.strategy.names(data.bgsave, "facet")

#################################################
# Filter
do.filter <- function(df, do.print=FALSE, all.groups = TRUE) {
  df %>% 
    filter(data_max_memory_usage_gb == 30)
}

data <- do.filter(data, all.groups = FALSE)
action.data.patch <- do.filter(action.data.patch, all.groups = FALSE)
action.data.failover.restart <- do.filter(action.data.failover.restart, all.groups = FALSE)
action.data.failover.failover <- do.filter(action.data.failover.failover, all.groups = FALSE)
data.bgsave <- do.filter(data.bgsave, all.groups = FALSE)
#########################################
############## PLOTTING #################
#########################################

plot <- ggplot() +
  ylab("Request Latency [ms]") +
  xlab("Elapsed Time [s]") +
  facet_nested(o_masters_replicas_id_text + node_name ~ benchmark_output_name + facet,
               scales="free_y",
               independent = "y") +
  coord_cartesian(xlim=c(0, 140)) +
  scale_x_continuous(breaks=c(0, 50, 120), labels=c(0, 50, 120))

  
plot <- add.plot.failover.restart.rectangles(plot, do.filter(action.data.failover.restart))
plot <- plot +
  geom_point(data=data,
    aes(
      x = time_s-10,
      y = latency_ms,
      color = latency_type,
      size = latency_type,
    )
  ) +
  scale_size_manual(values=c(0.5, 0.1)) + 
  scale_color_manual(values=c("black", "orange"))

plot <- plot + 
       geom_label_repel(
           data = data %>%
             group_by(run_id, node_name) %>%
             mutate(max_latency_ms = max(latency_ms)) %>%
             ungroup() %>%
             mutate(label = ifelse(latency_ms == max_latency_ms, round(max_latency_ms, 0), "")) %>%
             group_by(run_id, node_name) %>%
             mutate(label = ifelse(row_number() == which.max(latency_ms), label, "")) %>%
             ungroup(),
           aes(
             x = time_s-10,
             y = max_latency_ms,
             label = label
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
           segment.color = "red",
           segment.alpha = 0.7,
           size = 2.5,
           max.time = 10,
           max.iter = 1000000,
           stat = "unique"
         )

plot <- plot +
  geom_vline(data=action.data.patch,
             aes(xintercept=time_s-10),
             color='blue',
             linetype = "dashed",
             size=0.1,
             alpha=0.7,
             show.legend = F) +
  geom_vline(data=action.data.failover.failover,
             aes(xintercept=failover_start_time_s-10),
             color='black',
             linetype = "dashed",
             size=0.1,
             alpha=0.7,
             show.legend = F)

plot <- plot +
  geom_point(data=data.bgsave,
             aes(x = time_s-10,
                 y = 0,
                 group=name),
             size = 1.5,
             shape = 17,
             alpha=0.7,
             color='black',
             show.legend = F)

plot <- plot + plot.theme.paper()

# width=15, height = 18, use.grid=FALSE
ggplot.save(plot +
              theme(legend.position = "top",
                    plot.margin = margin(4, 0.5, 0.5, 0.5, unit="mm"),
                    #axis.title.y = element_text(margin = margin(), hjust=0.6),
                    legend.direction ="horizontal",
                    legend.text = element_text(size = FONT.SIZE - 1, margin=margin(l=-3)),
                    legend.key.size = unit(1, "mm"),
                    legend.margin = margin(0,0,0,0),
                    legend.title = element_blank(),
                    legend.background = element_blank(),
                    # Horizontal spacing between facets
                    panel.spacing.x = unit(0.6, "mm"),
                    panel.spacing.y = unit(1.3, "mm")) +
              guides(color = guide_legend(nrow = 1))
            , "Latencies-Single-Details-All", width=3, height=1.5, use.grid=TRUE)
  
