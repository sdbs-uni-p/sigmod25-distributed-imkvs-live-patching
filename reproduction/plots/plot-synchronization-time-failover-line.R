#!/usr/bin/env -S Rscript --no-save --no-restore

source("lib.R")
source("util.R")

args = commandArgs(trailingOnly=TRUE)
# OUTPUT.DIR <<- "output"
OUTPUT.DIR <<- tail(args, n=1)

# database.failover <- "input/synchronization-time-failover-idle.duckdb"
database.failover <- args[1]
# database.failover.snapshot <- "input/synchronization-time-failover-snapshot-idle.duckdb"
database.failover.snapshot <- args[2]
#########################################
###### DATA PREPARATION #################
#########################################
con.failover <- create.con(database.failover)
con.failover.snapshot <- create.con(database.failover.snapshot)

fetch.total.synchronization.failover <- function(con) {
  data <- dbGetQuery(con,
                     "
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
SELECT SUM(FullInSyncEnd.action_time_s - ShutdownStart.action_time_s) AS total_duration, 
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
SELECT SUM(duration) / 1000 AS total_duration, 'Failover' AS action, run.*
FROM (
  -- SELECT DISTINCT duration_ms / 1000 AS duration, port, run_id FROM failover WHERE name = 'failover'
  SELECT duration_ms AS duration, port, run_id FROM redis_failover
) JOIN run USING(run_id)
WHERE (redis_cluster_config LIKE '%15-1.yaml' OR redis_cluster_config NOT LIKE '15-%.yaml')
GROUP BY ALL;
                     ")
  factor.experiment(data, con)
}
data.failover <- fetch.total.synchronization.failover(con.failover)
data.failover$name <- "CP Full"

data.failover.snapshot <- fetch.total.synchronization.failover(con.failover.snapshot)
data.failover.snapshot$name <- "CP Part"

data.failover <- rbind(data.failover, data.failover.snapshot)

data.failover.masters <-data.failover %>% filter(o_replicas_per_master == 1)
data.failover.masters$facet <- "<X> P. - 1 R."
data.failover.masters$o_masters_replicas_id_text <- paste(data.failover.masters$o_masters, "P.")

data.failover.replicas <-data.failover %>% filter(o_masters == 15)
data.failover.replicas$facet <- "15 P. - <X> R."
data.failover.replicas$o_masters_replicas_id_text <- paste(data.failover.replicas$o_replicas_per_master, "R.")

o_masters_replicas_id_text_factor_order <- c(
  paste(sort(unique(data.failover.masters$o_masters)), "P."),
  paste(sort(unique(data.failover.replicas$o_replicas_per_master)), "R.")
)
  

data <- rbind(data.failover.replicas, data.failover.masters)
data <- factor.patch.strategy.names(data, "name")

data$data_max_memory_usage_gb <- factor(data$data_max_memory_usage_gb)
data$o_masters_replicas_id_text <- factor(data$o_masters_replicas_id_text, levels=o_masters_replicas_id_text_factor_order)
data$action <- factor(data$action, levels=c("Restart", "Failover"))
print(data %>% arrange(o_masters_replicas_id, data_max_memory_usage_gb, action, name) %>% summarise(total_duration, data_max_memory_usage_gb, action, name, o_masters_replicas_id))
#########################################
############## PLOTTING #################
#########################################
ylim_expansion <- list(0, 0)
ylimfailover <- mapply("-", range(data$total_duration[data$action == "Failover"]), ylim_expansion)
ylimrestart <- mapply("-", range(data$total_duration[data$action == "Restart"]), ylim_expansion)


plot <- ggplot() +
  ylab("Update Lag [s]") +
  xlab("Memory State [GiB]") +
  facet_nested(action ~ facet + name,
             scale="free_y",
             independent = "y") +
  facetted_pos_scales(
        y = list(
          action == "Failover" & facet == "<X> P. - 1 R." & name == "CP Part" ~ scale_y_continuous(limits = ylimfailover),
          action == "Failover" & facet == "<X> P. - 1 R." & name == "CP Full" ~ scale_y_continuous(limits = ylimfailover, guide = "none"),
          action == "Failover" & facet == "15 P. - <X> R." & name == "CP Part" ~ scale_y_continuous(limits = ylimfailover, guide = "none"),
          action == "Failover" & facet == "15 P. - <X> R." & name == "CP Full" ~ scale_y_continuous(limits = ylimfailover, guide = "none"),
          
          action == "Restart" & facet == "<X> P. - 1 R." & name == "CP Part" ~ scale_y_continuous(limits = ylimrestart),
          action == "Restart" & facet == "<X> P. - 1 R." & name == "CP Full" ~ scale_y_continuous(limits = ylimrestart, guide = "none"),
          action == "Restart" & facet == "15 P. - <X> R." & name == "CP Part" ~ scale_y_continuous(limits = ylimrestart, guide = "none"),
          action == "Restart" & facet == "15 P. - <X> R." & name == "CP Full" ~ scale_y_continuous(limits = ylimrestart, guide = "none")
         
        )
      )

plot <- plot +
  geom_point(data = data,
             aes(
               x=data_max_memory_usage_gb,
               y=total_duration,
               color=o_masters_replicas_id_text,
               group=o_masters_replicas_id_text,
             ),
             shape = 0,
             size=1,
             ) + 
  geom_line(
    data = data,
    aes(
      x=data_max_memory_usage_gb,
      y=total_duration,
      color=o_masters_replicas_id_text,
      group=o_masters_replicas_id_text,
    ),
    linewidth=0.3,
  ) +
  scale_color_viridis(discrete=TRUE, option='turbo')
plot <- plot + plot.theme.paper()

# width=15, height = 18, use.grid=FALSE
ggplot.save(plot +
              theme(legend.position = c(0.5,1.4),
                    plot.margin = margin(4, 0.5, 0.5, 0.5, unit="mm"),
                    #axis.title.x = element_text(margin = margin(t=-1)),
                    #axis.title.y = element_text(margin = margin(r=3)),
                    legend.direction ="horizontal",
                    legend.text = element_text(size = FONT.SIZE - 1, margin=margin(l=-3)),
                    legend.key.size = unit(1, "mm"),
                    legend.margin = margin(0,0,0,0),
                    legend.title = element_blank(),
                    legend.background = element_blank(),
                    # Horizontal spacing between facets
                    panel.spacing.x = unit(c(0.5, 1.5, 0.5), "mm"),
                    panel.spacing.y = unit(1.3, "mm"),
                    # Reduce space between the legend rows
                    legend.spacing.y = unit(0.2, "mm")
                    ) +
              scale_x_discrete(expand = c(0.1, 0.1)) +
              guides(color = guide_legend(byrow = T, ncol=5))
            , "Synchronization-Time-Failover-Time", width=10, height=3.65, use.grid=F)

