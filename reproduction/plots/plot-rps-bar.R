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

fetch.rps.total <- function(con) {
  print(con)
  data <- dbGetQuery(con,
                     "
SELECT total_count / (total_duration_s::FLOAT - 10) AS rps, run.*
FROM (
  SELECT COUNT(*) AS total_count, run_id
  FROM latencies
  WHERE time_s >= 10
  GROUP BY ALL
) LEFT JOIN latencies_info USING(run_id)
  LEFT JOIN run USING(run_id);
                     "
  )
  factor.experiment(data, con)
}

data.baseline <- fetch.rps.total(con.baseline) %>% filter(run_commit == "7.0.11")
data.patch <- fetch.rps.total(con.patch)
data.failover <- fetch.rps.total(con.failover)
data.failover.snapshot <- fetch.rps.total(con.failover.snapshot)

data.baseline$name <- "Baseline"# paste("Baseline (", data.baseline$run_commit, ")", sep="")
#data.baseline[data.baseline$name == "Baseline (7.0.11)", ]$name <- "Baseline"
#data.baseline[data.baseline$name == "Baseline (livepatch)", ]$name <- "Baseline (LP)"
data.patch$name <- paste("LP ", data.patch$patch_method, sep="")
data.failover$name <- "CP Full"
data.failover.snapshot$name <- "CP Part"

data <- rbind(data.patch, data.failover, data.failover.snapshot, data.baseline)

data <- factor.patch.strategy.names(data, "name")
print(data %>% filter(data_max_memory_usage_gb == 30 & o_masters_replicas_id == '3-1' & benchmark_output_name == 'set') %>% summarise(rps))
#########################################
############## PLOTTING #################
#########################################
plot <- ggplot(data=data %>% filter(data_max_memory_usage_gb == 30)) +
  ylab("kRequests\nper Second") +
  xlab("Benchmark") +
  facet_nested(~o_masters_replicas_id_text)

plot <- plot +
  geom_bar(
    aes(
      x = benchmark_output_name,
      y = rps / 1000,
      fill = name,
    ),
    stat="identity",
    position = position_dodge2(width = 0.9, preserve = "single")
  ) +
  scale_fill_manual(values=c("black", "#3d74fe", "#800080", "#f56a19", "#b21a01"))

plot <- plot + plot.theme.paper()

# width=15, height = 18, use.grid=FALSE
ggplot.save(plot + 
              theme(legend.position = c(0.46,1.83),
                    plot.margin = margin(2, 0.5, 0.5, 0.5, unit="mm"),
                    legend.direction ="horizontal",
                    legend.text = element_text(size = FONT.SIZE - 1, margin=margin(l=-5)),
                    legend.key.size = unit(1, "mm"),
                    legend.margin = margin(0,0,0,0),
                    legend.title = element_blank(),
                    legend.background = element_blank(),
                    axis.text.x = element_text(angle = 20, hjust = 1)) +
              guides(fill = guide_legend(ncol = 5))
  , "RPS-Bar", width=7.5, height=2.4, use.grid=FALSE)

