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

fetch.network <- function(con, time.division = 1, input_commit=NA) {
  # Disable commit
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
WITH all_ranges AS (
  -- 10s warmup phase
  SELECT UNNEST(GENERATE_SERIES((10.0 / ?gap)::INT, FLOOR((benchmark_framework_end_time_s) / ?gap)::INT)) AS slot,
      run_id
  FROM run
), network_range AS (
    SELECT FLOOR(time_s / ?gap)::INT AS slot,
        bytes,
        run_id
    FROM redis_all_network
), network_bytes_per_range AS (
    SELECT SUM(bytes) AS total_bytes,
           slot,
           run_id
    FROM network_range
    GROUP BY ALL
), network_bytes_per_range_zero AS (
    SELECT COALESCE(total_bytes, 0) AS total_bytes,
           slot,
           run_id
    FROM network_bytes_per_range 
        RIGHT JOIN all_ranges USING(run_id, slot)
)
SELECT total_bytes, 
       slot * ?gap AS start_time_s, 
       run.*
FROM network_bytes_per_range_zero
    JOIN run USING(run_id)
  WHERE ?commit;"
    ,
    gap = time.division,
    commit=SQL(commit)
    ))
# Start at 0 time as we have a warmup phase
#data$start_time_s <- data$start_time_s - min(data$start_time_s)
factor.experiment(data, con)
}

data.baseline <- fetch.network(con.baseline)
data.baseline$facet <- ""
data.baseline$name <- ""
data.baseline[data.baseline$run_commit == "network", ]$facet <- "Baseline"
data.baseline[data.baseline$run_commit == "network", ]$name <- "Baseline"
data.baseline[data.baseline$run_commit == "livepatch-network", ]$facet <- "Live Patch"
data.baseline[data.baseline$run_commit == "livepatch-network", ]$name <- "No Patch"

data.patch <- fetch.network(con.patch)
data.patch$facet <- "Live Patch"
data.patch$name <- paste("LP ", data.patch$patch_method, sep="")


data.failover <- fetch.network(con.failover)
data.failover$facet <- "CP Full"
data.failover$name <- "CP Full"

data.failover.snapshot <- fetch.network(con.failover.snapshot)
data.failover.snapshot$facet <- "CP Part"
data.failover.snapshot$name <- "CP Part"

data <- rbind(data.baseline, data.patch, data.failover, data.failover.snapshot)
#############################################################################################
# Patch Data:
fetch.patch.time <- function(con) {
  # This produces too much data (one data point for each node.)
  # I only want to have one data for each cluster. So we use the patch_received time.
  # SELECT time_s, run.*
  # FROM wf_l_e2e_patched JOIN run USING(run_id);
  data <- dbGetQuery(con,
                     "
                     SELECT time_s, run.* 
                     FROM wf_r_new_patch JOIN run USING(run_id) 
                     WHERE from_client = 1;
                     ")
  factor.experiment(data, con)
}

action.data.patch <- fetch.patch.time(con.patch)
action.data.patch$facet <- "Live Patch"
action.data.patch$name <- paste("LP ", action.data.patch$patch_method, sep="")

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
  SELECT time_s, run.*
  FROM redis_bgsaves JOIN run USING(run_id)
  WHERE time_s >= 10; -- 10s warmup
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

data.filter <- function(df) {
  df <- data.frame(df)
  df$data_max_memory_usage_gb <- factor(df$data_max_memory_usage_gb)
  df$facet <- factor(df$facet, levels=c("Baseline", "Live Patch", "CP Part", "CP Full"))
  df$name <- factor(df$name, levels=c("Baseline", "No Patch", "LP Pull", "LP Push", "CP Part", "CP Full"))
  return(df)
}



#########################################
############## PLOTTING #################
#########################################
# KiB
data$total_bytes <- data$total_bytes / 1024.


plot <- ggplot(data=data.filter(data)) +
  ylab("Network Traffic [KiB]") +
  xlab("Elapsed Time [s]") +
  scale_x_continuous(guide=guide_axis(check.overlap = TRUE))+
  facet_nested(o_masters_replicas_id_text + data_max_memory_usage_gb ~ name,
               scales="free",
               independent = T)

plot <- plot +
  geom_line(
    aes(x = start_time_s-10,
        y = total_bytes,
        
    ),
    color = "#006400",
    alpha=0.7,
    linewidth=0.3
  )
plot <- add.plot.failover.restart.rectangles(plot, data.filter(action.data.failover.restart))
plot <- plot +
  geom_vline(data=data.filter(action.data.patch),
             aes(xintercept=time_s-10,
                 group=name),
             color="black",
             linetype = "dashed",
             size=0.1,
             alpha=0.7,
             show.legend = F) +
  geom_vline(data=data.filter(action.data.failover.failover),
             aes(xintercept=failover_start_time_s-10,
                 group=name),
             linetype = "dashed",
             color="black",
             size=0.1,
             alpha=0.7,
             show.legend = F) 


plot <- plot +
  geom_point(data=data.filter(data.bgsave),
             aes(x = time_s-10,
                 y = -Inf,
                 group=name),
             size = 1.5,
             shape = 17,
             alpha=0.7,
             color='black',
             show.legend = F)

plot <- plot + plot.theme.paper()

ggplot.save(plot +
              theme(legend.position = c(0.5,1.28),
                    plot.margin = margin(0.5, 0.5, 0.5, 0.5, unit="mm"),
                    #axis.title.y = element_text(margin = margin(r=-2)),
                    legend.direction ="horizontal",
                    legend.text = element_text(size = FONT.SIZE - 1, margin=margin(l=-3)),
                    axis.title.y = element_text(margin=margin(r=15)),
                    #axis.text.x = element_text(angle = 30, hjust = 1),
                    legend.key.size = unit(1, "mm"),
                    legend.margin = margin(0,0,0,0),
                    legend.title = element_blank(),
                    legend.background = element_blank(),
                    # Horizontal spacing between facets
                    panel.spacing.x = unit(c(1, 1, 1, 1, 5), "mm"),
                    panel.spacing.y = unit(1, "mm")) +
              guides(color = guide_legend(nrow = 1))
            , "Network-Time-All", width=4, height=1.5, use.grid=TRUE)

