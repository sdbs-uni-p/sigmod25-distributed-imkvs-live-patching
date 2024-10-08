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

fetch.rps.time <- function(con, time.division = 0.1, input_commit=NA) {
  commit <- "1=1" # do not limit based on run_commit; just a noop
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
  -- 10 s warmup
  SELECT UNNEST(GENERATE_SERIES((10 / ?gap)::INT, FLOOR((total_duration_s - 5) / ?gap)::INT)) AS slot,
      port,
      node_name,
      run_id
  FROM latencies_info 
      JOIN (SELECT DISTINCT port, run_id FROM latencies) USING(run_id)
      JOIN master_replica_group_names USING(run_id)
  WHERE list_contains(ports, port)
), latency_range AS (
    SELECT FLOOR(time_s / ?gap)::INT AS slot,
        run_id,
        port
    FROM latencies
), latency_count_per_range AS (
    SELECT COUNT(*) AS total_latencies,
           slot,
           port,
           run_id
    FROM latency_range
    GROUP BY ALL
), latency_count_per_range_zero AS (
    SELECT SUM(COALESCE(total_latencies, 0)) AS total_latencies,
           slot,
           node_name,
           run_id
    FROM latency_count_per_range 
        RIGHT JOIN all_ranges USING(run_id, port, slot)
    -- Now we change from 'port' to 'node_name', so we have to sum the latencies
    GROUP BY ALL
)
SELECT total_latencies, 
       slot * ?gap AS start, 
       node_name,
       run.*
FROM latency_count_per_range_zero
    JOIN run USING(run_id)
  WHERE ?commit"
    ,
    gap = time.division,
    commit=SQL(commit)
    ))
# See below if you want to use ports!
factor.experiment(data, con)
}

data.baseline <- fetch.rps.time(con.baseline, input_commit="7.0.11")
data.patch <- fetch.rps.time(con.patch)
data.failover <- fetch.rps.time(con.failover)
data.failover.snapshot <- fetch.rps.time(con.failover.snapshot)

data.baseline$name <- "Baseline"
data.patch$name <- paste("LP ", data.patch$patch_method, sep="")
data.failover$name <- "CP Full"
data.failover.snapshot$name <- "CP Part"

data.failover.with.baseline <- rbind(data.failover, data.baseline)
data.failover.with.baseline$facet <- "CP Full"

data.failover.snapshot.with.baseline <- rbind(data.failover.snapshot, data.baseline)
data.failover.snapshot.with.baseline$facet <- "CP Part"

data.patch$facet <- paste("LP ", data.patch$patch_method, sep="")
data.baseline.patch.pull <- data.frame(data.baseline)
data.baseline.patch.pull$facet <- "LP Pull"
data.baseline.patch.push <- data.frame(data.baseline)
data.baseline.patch.push$facet <- "LP Push"

data <- rbind(data.failover.snapshot.with.baseline, data.failover.with.baseline, data.patch, data.baseline.patch.push, data.baseline.patch.pull)

data <- factor.patch.strategy.names(data, "name")
data <- factor.patch.strategy.names(data, "facet")

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
action.data.patch$name <- paste("Live Patch (", action.data.patch$patch_method, ")", sep="")
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

#########################################################
# Filter data
do.filter <- function(df, do.print=FALSE, all.groups = TRUE) {
  df <- df %>% 
    filter(o_masters_replicas_id == '3-1' | o_masters_replicas_id == '5-2') %>%
    filter(data_max_memory_usage_gb == 30) %>%
    filter(benchmark_output_name == 'set')

  if (!all.groups) {
    df <- df %>%
      filter(node_name %in% c("Gr. 1", "Gr. 2"))
  }
  if (do.print) {
    print(df)
  }
  df
}
data <- do.filter(data, all.groups=FALSE)
action.data.patch <- do.filter(action.data.patch, all.groups=FALSE)

action.data.failover.failover <- do.filter(action.data.failover.failover, all.groups=FALSE)
action.data.failover.restart <- do.filter(action.data.failover.restart, all.groups=FALSE)

data.bgsave <- do.filter(data.bgsave)
#########################################
############## PLOTTING #################
#########################################
ylim_expansion <- list(0, 0)
ylimget131 <- mapply("-", range(data$total_latencies[data$o_masters_replicas_id_text == "3 P. - 1 R." & data$benchmark_output_name == "get" & data$node_name == "Gr. 1"]) / 1000, ylim_expansion)
ylimget231 <- mapply("-", range(data$total_latencies[data$o_masters_replicas_id_text == "3 P. - 1 R." & data$benchmark_output_name == "get" & data$node_name == "Gr. 2"]) / 1000, ylim_expansion)

ylimset131 <- mapply("-", range(data$total_latencies[data$o_masters_replicas_id_text == "3 P. - 1 R." & data$benchmark_output_name == "set" & data$node_name == "Gr. 1"]) / 1000, ylim_expansion)
ylimset231 <- mapply("-", range(data$total_latencies[data$o_masters_replicas_id_text == "3 P. - 1 R." & data$benchmark_output_name == "set" & data$node_name == "Gr. 2"]) / 1000, ylim_expansion)

ylimget1153 <- mapply("-", range(data$total_latencies[data$o_masters_replicas_id_text == "5 P. - 2 R." & data$benchmark_output_name == "get" & data$node_name == "Gr. 1"]) / 1000, ylim_expansion)
ylimget2153 <- mapply("-", range(data$total_latencies[data$o_masters_replicas_id_text == "5 P. - 2 R." & data$benchmark_output_name == "get" & data$node_name == "Gr. 2"]) / 1000, ylim_expansion)

ylimset1153 <- mapply("-", range(data$total_latencies[data$o_masters_replicas_id_text == "5 P. - 2 R." & data$benchmark_output_name == "set" & data$node_name == "Gr. 1"]) / 1000, ylim_expansion)
ylimset2153 <- mapply("-", range(data$total_latencies[data$o_masters_replicas_id_text == "5 P. - 2 R." & data$benchmark_output_name == "set" & data$node_name == "Gr. 2"]) / 1000, ylim_expansion)




plot <- ggplot(data=data) +
  ylab("kRequests (per 100ms)") +
  xlab("Elapsed Time [s]") +
  facet_nested(o_masters_replicas_id_text + node_name ~ facet,
               scales="free_y",
               independent = "y") +
  coord_cartesian(xlim=c(0, 150)) +
  scale_x_continuous(breaks=c(0, 50, 120), labels=c(0, 50, 120)) + 
  facetted_pos_scales(
    y = list(
      facet == "CP Part" & node_name == "Gr. 1" & o_masters_replicas_id_text == "3 P. - 1 R." ~ scale_y_continuous(limits = ylimset131, breaks =c(0, 2.5, 5, 7.5), labels=c(" 0", "", " 5", "")),
      facet == "CP Full" & node_name == "Gr. 1" & o_masters_replicas_id_text == "3 P. - 1 R." ~ scale_y_continuous(limits = ylimset131, guide="none"),
      facet == "LP Push" & node_name == "Gr. 1" & o_masters_replicas_id_text == "3 P. - 1 R." ~ scale_y_continuous(limits = ylimset131, guide="none"),
      facet == "LP Pull" & node_name == "Gr. 1" & o_masters_replicas_id_text == "3 P. - 1 R." ~ scale_y_continuous(limits = ylimset131, guide="none"),
      
      facet == "CP Part" & node_name == "Gr. 2" & o_masters_replicas_id_text == "3 P. - 1 R."  ~ scale_y_continuous(limits = ylimset231, breaks =c(6, 7, 8), labels=c(" 6", "", " 8")),
      facet == "CP Full" & node_name == "Gr. 2" & o_masters_replicas_id_text == "3 P. - 1 R."  ~ scale_y_continuous(limits = ylimset231, guide="none"),
      facet == "LP Push" & node_name == "Gr. 2" & o_masters_replicas_id_text == "3 P. - 1 R."  ~ scale_y_continuous(limits = ylimset231, guide="none"),
      facet == "LP Pull" & node_name == "Gr. 2" & o_masters_replicas_id_text == "3 P. - 1 R."  ~ scale_y_continuous(limits = ylimset231, guide="none"),
      
      facet == "CP Part" & node_name == "Gr. 1" & o_masters_replicas_id_text == "5 P. - 2 R." ~ scale_y_continuous(limits = ylimset1153, guide = guide_axis(check.overlap = TRUE)),
      facet == "CP Full" & node_name == "Gr. 1" & o_masters_replicas_id_text == "5 P. - 2 R." ~ scale_y_continuous(limits = ylimset1153, guide="none"),
      facet == "LP Push" & node_name == "Gr. 1" & o_masters_replicas_id_text == "5 P. - 2 R." ~ scale_y_continuous(limits = ylimset1153, guide="none"),
      facet == "LP Pull" & node_name == "Gr. 1" & o_masters_replicas_id_text == "5 P. - 2 R." ~ scale_y_continuous(limits = ylimset1153, guide="none"),
      
      facet == "CP Part" & node_name == "Gr. 2" & o_masters_replicas_id_text == "5 P. - 2 R."  ~ scale_y_continuous(limits = ylimset2153, breaks =c(5, 6, 7), labels=c(" 5", "", " 7")),
      facet == "CP Full" & node_name == "Gr. 2" & o_masters_replicas_id_text == "5 P. - 2 R."  ~ scale_y_continuous(limits = ylimset2153, guide="none"),
      facet == "LP Push" & node_name == "Gr. 2" & o_masters_replicas_id_text == "5 P. - 2 R."  ~ scale_y_continuous(limits = ylimset2153, guide="none"),
      facet == "LP Pull" & node_name == "Gr. 2" & o_masters_replicas_id_text == "5 P. - 2 R."  ~ scale_y_continuous(limits = ylimset2153, guide="none")
    )
  )

plot <- add.plot.failover.restart.rectangles(plot, do.filter(action.data.failover.restart))

plot <- plot +
  geom_line(
    aes(x = start-10,
        y = total_latencies / 1000,
        color = name,
        group=name
    ),
    alpha=0.7,
    linewidth=0.3,
  ) +
  scale_color_manual(values=c("black", "#3d74fe", "#800080", "#f56a19", "#b21a01"))


plot <- plot +
  geom_vline(data=action.data.patch,
             aes(xintercept=time_s-10,
                 group=name),
             color="black",
             linetype = "dashed",
             size=0.1,
             alpha=0.7,
             show.legend = F) +
  geom_vline(data=action.data.failover.failover,
             aes(xintercept=failover_start_time_s -10,
                 group=name),
             color="black",
             linetype = "dashed",
             size=0.1,
             alpha=0.7,
             show.legend = F)




plot <- plot +
  geom_point(data=data.bgsave,
             aes(x = time_s - 10,
                 y = -Inf,
                 group=name),
             size = 1.5,
             shape = 17,
             alpha=0.7,
             color='black',
             show.legend = F)

plot <- plot + plot.theme.paper()

ggplot.save(plot +
            theme(legend.position = c(0.45,1.135),
                  plot.margin = margin(4, 0.5, 0.5, 0.5, unit="mm"),
                  #axis.title.y = element_text(margin = margin(r=-5)),
                  legend.direction ="horizontal",
                  legend.text = element_text(size = FONT.SIZE - 1, margin=margin(l=-3)),
                  legend.key.size = unit(1, "mm"),
                  legend.margin = margin(0,0,0,0),
                  legend.title = element_blank(),
                  legend.background = element_blank(),
                  # Horizontal spacing between facets
                  panel.spacing.x = unit(0.6, "mm")) +
              guides(color = guide_legend(nrow = 1))
            , "RPS-Time-Single-Details", width=10, height=4.3, use.grid=FALSE)

