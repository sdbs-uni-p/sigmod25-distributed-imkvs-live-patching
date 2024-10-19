#!/usr/bin/env -S Rscript --no-save --no-restore

source("lib.R")
source("util.R")

args = commandArgs(trailingOnly=TRUE)
# OUTPUT.DIR <<- "output"
OUTPUT.DIR <<- tail(args, n=1)


# database.patch <- "input/synchronization-time-teaser-patch-idle.duckdb"
database.patch <- args[1]
# database.failover <- "input/synchronization-time-teaser-failover-idle.duckdb"
database.failover <- args[2]
# database.failover.snapshot <- "input/synchronization-time-teaser-failover-snapshot-idle.duckdb"
database.failover.snapshot <- args[3]
#########################################
###### DATA PREPARATION #################
#########################################
con.patch <- create.con(database.patch)
con.failover <- create.con(database.failover)
con.failover.snapshot <- create.con(database.failover.snapshot)

fetch.synchronization.patch <- function(con) {
  data <- dbGetQuery(con,
                     "
SELECT apply_time_s - new_time_s AS synchronization_duration_s, run.*
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
fetch.synchronization.failover <- function(con) {
  data <- dbGetQuery(con,
                     "
SELECT SUM(duration) AS synchronization_duration_s, run.*
FROM (
WITH ShutdownStart AS (
  SELECT *
  FROM failover
  WHERE action = 'shutdown start'
), StartupStart AS (
  SELECT *
  FROM failover
  WHERE action = 'start start'
), FirstSyncEnd AS (
  SELECT * 
  FROM failover
  WHERE action = 'node sync end'
), FullInSyncEnd AS (
  SELECT * 
  FROM failover
  WHERE action = 'node catchup end'
)
SELECT SUM(FullInSyncEnd.action_time_s - ShutdownStart.action_time_s) AS duration, 
       'Restart' as action, 
       run.* 
FROM ShutdownStart 
    JOIN FullInSyncEnd USING(run_id, log_time, port)
    JOIN run USING(run_id)
    JOIN master_replica_group_names USING(run_id)
  WHERE list_contains(ports, port)
  AND (redis_cluster_config LIKE '%15-1.yaml' OR redis_cluster_config NOT LIKE '15-%.yaml')
  GROUP BY ALL
UNION ALL
SELECT SUM(duration) / 1000 AS duration, 'Failover' AS action, run.*
FROM (
  -- SELECT DISTINCT duration_ms / 1000 AS duration, port, run_id FROM failover WHERE name = 'failover'
  SELECT duration_ms AS duration, port, run_id FROM redis_failover
) JOIN run USING(run_id)
WHERE (redis_cluster_config LIKE '%15-1.yaml' OR redis_cluster_config NOT LIKE '15-%.yaml')
GROUP BY ALL
) JOIN run USING (run_id)
GROUP BY ALL;
                     ")
  factor.experiment(data, con)
}


data.patch <- fetch.synchronization.patch(con.patch)
data.patch$name <- paste("LP", data.patch$patch_method)

data.failover <- fetch.synchronization.failover(con.failover)
data.failover$name <- "CP Full"

data.failover.snapshot <- fetch.synchronization.failover(con.failover.snapshot)
data.failover.snapshot$name <- "CP Part"


data <- rbind(data.patch, data.failover, data.failover.snapshot)
data <- factor.patch.strategy.names(data, "name")
print(data)
#########################################
############## PLOTTING #################
#########################################
data$synchronization_duration_s <- data$synchronization_duration_s / 60
plot <- ggplot() +
  ylab("Update  \nLag [min]") +
  xlab("Memory State Size [GiB]") +
  coord_cartesian(ylim=c(-1, max(data$synchronization_duration_s) + 1)) +
  scale_y_continuous(breaks = c(0, 5, 10, 15, 20), labels=c("0", "", "10", "", "20"))

plot <- plot +
  geom_point(data = data,
             aes(
               x=data_max_memory_usage_gb,
               y=synchronization_duration_s,
               color=name,
               group=name,
             ),
             shape = 0,
             size=1,
  ) + 
  geom_line(
    data = data,
    aes(
      x=data_max_memory_usage_gb,
      y=synchronization_duration_s,
      color=name,
      group=name,
    ),
    linewidth=0.3,
  ) +
  scale_color_manual(values=c("#3d74fe", "#800080", "#f56a19", "#b21a01"))
plot <- plot + plot.theme.paper()

# width=15, height = 18, use.grid=FALSE
ggplot.save(plot +
              theme(legend.position = c(0.15,0.68),
                    plot.margin = margin(0.5, 0.5, 0.5, 0.5, unit="mm"),
                    #axis.title.x = element_text(margin = margin(t=-1)),
                    axis.title.y = element_text(margin = margin(r=3), hjust =1),
                    legend.direction ="horizontal",
                    legend.text = element_text(size = FONT.SIZE - 1, margin=margin(l=-3)),
                    legend.key.size = unit(1, "mm"),
                    legend.margin = margin(0,0,0,0),
                    legend.title = element_blank(),
                    legend.background = element_blank(),
                    # Horizontal spacing between facets
                    panel.spacing.x = unit(0.5, "mm"),
                    # Reduce space between the legend rows
                    legend.spacing.y = unit(0.2, "mm")
              ) +
              guides(color = guide_legend(byrow = T, ncol=2))
            , "Synchronization-Time-Teaser", width=7.5, height=1.4, use.grid=F)

