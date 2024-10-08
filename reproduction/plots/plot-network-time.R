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
  df <- df %>% 
    filter(data_max_memory_usage_gb %in% c(200)) %>%
    filter(o_masters_replicas_id_text %in% c("3 P. - 1 R.", "15 P. - 1 R."))
  df$o_masters_replicas_id_text <- paste(df$o_masters, " P.", sep="")
  df$facet <- factor(df$facet, levels=c("Baseline", "Live Patch", "CP Part", "CP Full"))
  df$name <- factor(df$name, levels=c("Baseline", "No Patch", "LP Pull", "LP Push", "CP Part", "CP Full"))
  df$o_masters_replicas_id_text <- factor(df$o_masters_replicas_id_text, levels=c("3 P.", "15 P."))
  return(df)
}



#########################################
############## PLOTTING #################
#########################################
# KiB
data$total_bytes <- data$total_bytes / 1024.
# MiB
data[data$facet == "CP Full", ]$total_bytes <- data[data$facet == "CP Full", ]$total_bytes / 1024.

ylim_expansion <- list(0, 0)

ylimlow3 <- mapply("-", range(data$total_bytes[!(data$facet %in% c("CP Full", "CP Part")) & data$o_masters_replicas_id_text == "3 P. - 1 R."]), ylim_expansion)
ylimmid3 <- mapply("-", range(data$total_bytes[data$facet == "CP Part" & data$o_masters_replicas_id_text == "3 P. - 1 R."]), ylim_expansion)
ylimhigh3 <- mapply("-", range(data$total_bytes[data$facet == "CP Full" & data$o_masters_replicas_id_text == "3 P. - 1 R."]), ylim_expansion)

ylimlow15 <- mapply("-", range(data$total_bytes[!(data$facet %in% c("CP Full", "CP Part")) & data$o_masters_replicas_id_text == "15 P. - 1 R."]), ylim_expansion)
ylimmid15 <- mapply("-", range(data$total_bytes[data$facet == "CP Part" & data$o_masters_replicas_id_text == "15 P. - 1 R."]), ylim_expansion)
ylimhigh15 <- mapply("-", range(data$total_bytes[data$facet == "CP Full" & data$o_masters_replicas_id_text == "15 P. - 1 R."]), ylim_expansion)


plot <- ggplot(data=data.filter(data)) +
  ylab("Network Traffic") +
  xlab("Elapsed Time [s]") +
  scale_x_continuous(guide=guide_axis(check.overlap = TRUE))+
  facet_nested(o_masters_replicas_id_text ~ name,
               scales="free",
               independent = T) +
facetted_pos_scales(
  y = list(
    o_masters_replicas_id_text == "3 P." & name == "Baseline" ~ scale_y_continuous(limits = ylimlow3),
    o_masters_replicas_id_text == "3 P." & name == "No Patch" ~ scale_y_continuous(limits = ylimlow3, guide="none"),
    o_masters_replicas_id_text == "3 P." & name == "LP Pull" ~ scale_y_continuous(limits = ylimlow3, guide="none"),
    o_masters_replicas_id_text == "3 P." & name == "LP Push" ~ scale_y_continuous(limits = ylimlow3, guide="none"),
    o_masters_replicas_id_text == "3 P." & name == "CP Part" ~ scale_y_continuous(limits = ylimmid3),
    o_masters_replicas_id_text == "3 P." & name == "CP Full" ~ scale_y_continuous(limits = ylimhigh3),
    
    o_masters_replicas_id_text == "15 P." & name == "Baseline" ~ scale_y_continuous(limits = ylimlow15),
    o_masters_replicas_id_text == "15 P." & name == "No Patch" ~ scale_y_continuous(limits = ylimlow15, guide="none"),
    o_masters_replicas_id_text == "15 P." & name == "LP Pull" ~ scale_y_continuous(limits = ylimlow15, guide="none"),
    o_masters_replicas_id_text == "15 P." & name == "LP Push" ~ scale_y_continuous(limits = ylimlow15, guide="none"),
    o_masters_replicas_id_text == "15 P." & name == "CP Part" ~ scale_y_continuous(limits = ylimmid15, breaks=c(200, 400, 600, 800, 1000), labels=c("", "400", "", "800", "")),
    o_masters_replicas_id_text == "15 P." & name == "CP Full" ~ scale_y_continuous(limits = ylimhigh15, breaks=c(0, 50, 100, 150, 200), labels=c("0", "", "100", "", "200"))
  ),
  x = list(
    o_masters_replicas_id_text == "3 P." & name == "Baseline" ~ scale_x_continuous(breaks = c(0, 500, 1000, 1500), labels=c("0", "", "1000", "")),
    o_masters_replicas_id_text == "3 P." & name == "No Patch" ~ scale_x_continuous(breaks = c(0, 100, 200, 300, 400, 500), labels=c("0", "", "200", "", "400", "")),
    o_masters_replicas_id_text == "3 P." & name == "LP Pull" ~ scale_x_continuous(breaks = c(0, 100, 200, 300, 400, 500), labels=c("0", "", "200", "", "400", "")),
    o_masters_replicas_id_text == "3 P." & name == "LP Push" ~ scale_x_continuous(breaks = c(0, 100, 200, 300, 400, 500), labels=c("0", "", "200", "", "400", "")),
    o_masters_replicas_id_text == "3 P." & name == "CP Part" ~ scale_x_continuous(),
    o_masters_replicas_id_text == "3 P." & name == "CP Full" ~ scale_x_continuous(),
    
    o_masters_replicas_id_text == "15 P." & name == "Baseline" ~ scale_x_continuous(breaks = c(0, 500, 1000, 1500), labels=c("0", "", "1000", "")),
    o_masters_replicas_id_text == "15 P." & name == "No Patch" ~ scale_x_continuous(breaks = c(0, 100, 200, 300, 400, 500), labels=c("0", "", "200", "", "400", "")),
    o_masters_replicas_id_text == "15 P." & name == "LP Pull" ~ scale_x_continuous(breaks = c(0, 100, 200, 300, 400, 500), labels=c("0", "", "200", "", "400", "")),
    o_masters_replicas_id_text == "15 P." & name == "LP Push" ~ scale_x_continuous(breaks = c(0, 100, 200, 300, 400, 500), labels=c("0", "", "200", "", "400", "")),
    o_masters_replicas_id_text == "15 P." & name == "CP Part" ~ scale_x_continuous(breaks = c(0, 500, 1000, 1500, 2000), labels=c("0", "500", "1000", "1500", "2000")),
    o_masters_replicas_id_text == "15 P." & name == "CP Full" ~ scale_x_continuous(breaks = c(0, 500, 1000, 1500, 2000), labels=c("0", "500", "1000", "1500", "2000"))
  )
)


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

# width=15, height = 18, use.grid=FALSE
ggplot.save(plot_grid(plot +
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
              force_panelsizes(cols = unit(c(1.3, 1.3, 1.3, 1.3, 3.1, 4), "cm")) +
              guides(color = guide_legend(nrow = 1))) +
              draw_label("[KiB/s]", x=0.04, y=0.36, vjust=0.5, angle= 90,
                         fontfamily = "paper", fontface = "bold", size = FONT.SIZE) +
              draw_label("[KiB/s]", x=0.04, y=0.78, vjust=0.5, angle= 90,
                         fontfamily = "paper", fontface = "bold", size = FONT.SIZE) +
              draw_label("[MiB/s]", x=0.676, y=0.36, vjust=0.5, angle= 90,
                         fontfamily = "paper", fontface = "bold", size = FONT.SIZE) +
              draw_label("[MiB/s]", x=0.676, y=0.78, vjust=0.5, angle= 90,
                         fontfamily = "paper", fontface = "bold", size = FONT.SIZE)
            , "Network-Time", width=16, height=2.7, use.grid=FALSE)

