#########################################
###### INPUT ARGUMENTS ##################
#########################################
read.input.args <- function() {
  args = commandArgs(trailingOnly=TRUE)
  if (length(args) != 2) {
    stop("Please specify the DuckDB file to load and the output directory.")
  }
  DUCKDB.FILE <<- args[1]
  OUTPUT.DIR <<- args[2]
}

create.con <- function(file) {
  con <- dbConnect(duckdb::duckdb(), file, read_only=TRUE)
  dbExecute(con,
             "
CREATE TEMPORARY VIEW initial_cluster_status AS (
  SELECT cluster_status.*
  FROM cluster_status 
    JOIN (SELECT MIN(time) AS min_time, run_id from cluster_status GROUP BY run_id) USING(run_id)
  WHERE time = min_time
);")
  dbExecute(con,
            "
CREATE TEMPORARY VIEW node_groups AS (
  SELECT master_port, 
    LIST_SORT(LIST_APPEND(replica_ports, master_port)) AS ports,
    run_id
  FROM (
    SELECT masters.port AS master_port, LIST(replicas.port) replica_ports, run_id
    FROM initial_cluster_status AS masters JOIN initial_cluster_status AS replicas USING(run_id)
    WHERE masters.port = replicas.master_port
    GROUP BY ALL
  )
);")
  dbExecute(con,
            "
CREATE TEMPORARY VIEW master_replica_group_names AS (
SELECT
  ports,
  run_id, 
  'Gr. ' || row_number() OVER(PARTITION BY run_id ORDER BY master_port) AS node_name
FROM node_groups
)
            ")
  
  return(con)
}

create.cons <- function(files) {
  lapply(files, create.con)
}

close.cons <- function(cons) {
  for (con in cons) {
    close.con(con)
  }
}
close.con <- function(con) {
  dbDisconnect(con, shutdown=TRUE)
}

#########################################
###### CLUSTER STATUS ###################
#########################################
get.initial.status.data <- function(con, name, role) {
  dbGetQuery(con, sqlInterpolate(con,
  "SELECT run_id, COUNT(*) AS ?name
    FROM cluster_status JOIN (SELECT MIN(time) AS min_time, run_id from cluster_status GROUP BY run_id) USING(run_id)
    WHERE role = ?role
    AND time = min_time
    GROUP BY run_id;"
  ,
  name=name,
  role=role))
}

get.number.masters <- function(con) {
  get.initial.status.data(con, "o_masters", 'master')
}

get.number.replicas <- function(con) {
  get.initial.status.data(con, "o_replicas", 'slave')
}


#########################################
###### DATA PREPARATION #################
#########################################

PATCH.METHOD.FACTOR <- list(
  "Pull" = "lazy",
  "LS" = "lazy_sync",
  "Push" = "eager",
  "ES" = "eager_sync",
  "-" = "no_patch"
)

match.node.groups <- function(con, df, match_column_name="port") {
  df.groups <- dbGetQuery(
    con, "SELECT master_port, ports, run_id FROM node_groups;")
  
  # Add all port groups for each run_id and filter the results again to contain only rows 
  # for which the port belongs to the port group.
  df <- merge(x=df, y=df.groups, by="run_id", all.x=TRUE,  allow.cartesian=TRUE)
  df <- filter(df, mapply(function(port, ports) port %in% ports, df$port, df$ports))
  df$ports <- factor(sapply(df$ports, paste, collapse='-'))
  df$port <- factor(df$port)
  df
}

factor.experiment <- function(df, con=NULL) {
  if (!is.null(con)) {
    master_replicas <- merge(x=get.number.masters(con), y=get.number.replicas(con), by="run_id")
    # Sort
    master_replicas <- master_replicas[order(master_replicas$o_masters, master_replicas$o_replicas),]
    master_replicas$o_replicas_per_master <- master_replicas$o_replicas / master_replicas$o_masters
    master_replicas$o_masters_replicas_id <- paste(master_replicas$o_masters, master_replicas$o_replicas_per_master, sep="-")
    master_replicas$o_masters_replicas_id_text <- paste(master_replicas$o_masters, " P. - ", master_replicas$o_replicas_per_master, " R.", sep="")
    df <- merge(x=df, y=master_replicas, by="run_id", all.x=TRUE)
    
    
    df$o_masters_replicas_id <- factor(df$o_masters_replicas_id, levels=unique(master_replicas$o_masters_replicas_id))
    df$o_masters_replicas_id_text <- factor(df$o_masters_replicas_id_text, levels=unique(master_replicas$o_masters_replicas_id_text))
    df$o_masters <- factor(df$o_masters, levels=unique(master_replicas$o_masters))
    df$o_replicas <- factor(df$o_replicas, levels=unique(master_replicas$o_replicas))
    df$o_replicas_per_master <- factor(df$o_replicas_per_master, levels=unique(master_replicas$o_replicas_per_master))
  }
  
  df$failover_failover_after_s <- factor(df$failover_failover_after_s)
  df$benchmark_name <- factor(df$benchmark_name)
  df$benchmark_output <- factor(df$benchmark_output)
  df$patch_distribution <- factor(df$patch_distribution)
  df$patch_method <- factor(df$patch_method)
  levels(df$patch_method) <- PATCH.METHOD.FACTOR
  df$data_max_memory_usage_gb_name <- paste(df$data_max_memory_usage_gb, " GiB", sep="")
  df$data_max_memory_usage_gb_name <- factor(df$data_max_memory_usage_gb_name)
  
  names(df) <- tolower(names(df))
  df %>% filter(benchmark_output_name != 'incr')
  #df  
}

factor.patch.strategy.names <- function(df, var) {
  df[[var]] <- droplevels(factor(df[[var]], levels=c("Baseline",
                                            "Baseline (LP)",
                                            "CP Part",
                                            "CP Full",
                                            "LP Pull",
                                            "LP Push")))
  arrange(df, var)
}

factor.patch.strategy.names.all <- function(df, var) {
  df[[var]] <- factor(df[[var]], levels=c("Baseline",
                                          "Baseline (LP)",
                                          "Conv. P.",
                                          "LP Pull",
                                          "LP Push"))
  arrange(df, var)
}


#########################################
###### DATA PREPARATION #################
#########################################
ggplot.save <- function(plot, file_name, width=3, height=3, use.grid=TRUE, 
                         png=FALSE,
                         pdf=TRUE,
                         svg=TRUE,
                        dpi=300) {
  if (use.grid) {
    width <- ggplot.facet.columns(plot) * width
    height <- ggplot.facet.rows(plot) * height
  }
  if (png) {
    ggsave(
      str_c(file_name, ".png"),
      dpi = dpi,
      plot,
      path = OUTPUT.DIR,
      limitsize = FALSE,
      width = width,
      height = height,
      units = "cm"
    )
  }
  if (pdf) {
   ggsave(
      str_c(file_name, ".pdf"),
      dpi = dpi,
      plot,
      path = OUTPUT.DIR,
      limitsize = FALSE,
      width = width,
      height = height,
      units = "cm"
    )
  }
  if (svg) {
     ggsave(
       str_c(file_name, ".svg"),
       dpi = dpi,
       plot,
       path = OUTPUT.DIR,
       limitsize = FALSE,
       width = width,
       height = height,
       units = "cm"
     )
  }
}

ggplot.facet.columns <- function(plot) {
  length(unique(ggplot_build(plot)$layout$layout$COL))
}

ggplot.facet.rows <- function(plot) {
  length(unique(ggplot_build(plot)$layout$layout$ROW))
}


factor.levels.index.commit <- function(df, commit_length = 12) {
  if (!is.factor(df$experiment_commit)) {
    df$experiment_commit <- factor(df$experiment_commit)
  }
  
  indexed_experiment_commit_levels <- c()
  index <- 1
  for (commit in levels(df$experiment_commit)) {
    if (startsWith(commit, "wfpatch.patch-")) {
      #commit <- sub("wfpatch.patch-", "W-", commit)
      commit <- sub("wfpatch.patch-", "", commit)
      if (commit_length < nchar(commit)) {
        commit <- strtrim(commit, commit_length)
      }
    }
    #indexed_commit <- str_c(index, ": ", commit)
    indexed_commit <- str_c(commit)
    indexed_experiment_commit_levels <-
      append(indexed_experiment_commit_levels, indexed_commit)
    index <- index + 1
  }
  # Order is preserved when creating the list (itartion is done over levels)
  levels(df$experiment_commit) <- indexed_experiment_commit_levels
  df
}

FONT.SIZE <- 6

plot.theme.paper <- function() {
  theme_bw() +
    theme(
      # All
      text=element_text(family="paper"),
      # Style facet grid
      strip.text = element_text(face = "bold")
     ) +
    theme(      
      # Text of ticks
      axis.text = element_text(size=FONT.SIZE),
      # Text of axis
      axis.title = element_text(size = FONT.SIZE, face="bold"),
      # Facet
      strip.text = element_text(size = FONT.SIZE, margin = margin(0.5,0.5,0.5,0.5, "mm")),
      # Title of legend
      legend.title = element_text(size = FONT.SIZE), 
      # Element/item of legend
      legend.text = element_text(size = FONT.SIZE),
    )
}

random <- function() {
  theme(legend.position="top",
        legend.direction = "horizontal", 
        legend.justification="center",
        legend.box.just = "bottom",
        
        legend.margin=margin(0,0,0,0),
        #legend.spacing = unit(0, "pt"),
        legend.spacing.x = unit(0.3, 'line'),
        legend.box.spacing = unit(3, "pt"),
        #legend.box.margin = margin(0, 0, 0, 0),
        #legend.margin = margin(0)),
        legend.key.size = unit(0.5,"line"),
        #legend.key.width = unit(0.2, "line"),
  )
}




prepare.patch.latencies <- function(df) {
  df$p_patch_only_latency_us <-
    df$p_patch_latency_us - ifelse(
      is.na(df$p_global_quiescence_latency_us),
      (
        df$p_local_as_switch_latency_us + df$p_local_as_new_latency_us
      ),
      df$p_global_quiescence_latency_us
    )
  
  df
}

invert.hex.color.advanced <-
  function(hex_color, black_white = FALSE) {
    leading_hash <- FALSE
    if (substr(hex_color, 1, 1) == "#") {
      # Remove leading '#'
      hex_color <- sub('.', '', hex_color)
      leading_hash <- TRUE
    }
    
    alpha <- ""
    if (nchar(hex_color) == 8) {
      # Hex contains alpha. E.g. XXXXXX<ALPHA><ALPHA> (last two characters)
      # Preserve alpha, do not invert it.
      alpha <-
        substr(hex_color, nchar(hex_color) - 1, nchar(hex_color))
      hex_color <- substr(hex_color, 0, nchar(hex_color) - 2)
    }
    
    if (black_white) {
      # Black white only
      # https://stackoverflow.com/questions/3942878/how-to-decide-font-color-in-white-or-black-depending-on-background-color/3943023#3943023
      r <- as.integer(as.hexmode(substr(hex_color, 1, 2)))
      g <- as.integer(as.hexmode(substr(hex_color, 3, 4)))
      b <- as.integer(as.hexmode(substr(hex_color, 5, 6)))
      
      c <- function(value) {
        value <- value / 255.0
        if (value <= 0.03928) {
          value / 12.92
        } else {
          ((value + 0.055) / 1.055) ^ 2.4
        }
      }
      
      c_values <- unlist(lapply(list(r, g, b), c))
      L <-
        0.2126 * c_values[[1]] + 0.7152 * c_values[[2]] + 0.0722 * c_values[[3]]
      
      if (L > sqrt(1.05 * 0.05) - 0.05) {
        hex_inverted <- "000000"
      } else {
        hex_inverted <- "FFFFFF"
      }
    } else {
      # Just invert color
      hex_inverted <-
        as.hexmode(bitwXor(as.hexmode(hex_color), as.hexmode(0xFFFFFF)))
      hex_inverted <- toupper(as.character(hex_inverted))
    }
    
    if (nchar(hex_inverted) < 6) {
      # Add leading zeros again
      # E.g. hex_color = "FFFF00" will result in "FF"
      # library(stringr)
      hex_inverted <- str_pad(hex_inverted, 6, pad = "0")
    }
    
    if (leading_hash) {
      paste0("#", hex_inverted, alpha)
    } else {
      paste0(hex_inverted, alpha)
    }
  }


fetch.failover.restart.data <- function(con) {
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
SELECT ShutdownStart.action_time_s AS shutdown_start_time_s,
       StartupStart.action_time_s AS start_start_time_s,
       FirstSyncEnd.action_time_s AS first_sync_end_time_s,
       FullInSyncEnd.action_time_s AS full_in_sync_end_time_s,
       port,
       node_name,
       run.*
FROM ShutdownStart 
    JOIN StartupStart  USING(run_id, log_time, port)
    JOIN FirstSyncEnd  USING(run_id, log_time, port)
    JOIN FullInSyncEnd USING(run_id, log_time, port)
    JOIN run USING(run_id)
    JOIN master_replica_group_names USING(run_id)
  WHERE list_contains(ports, port);
             ")
  factor.experiment(data, con)
}

fetch.failover.failover.data <- function(con) {
  data <- dbGetQuery(con,
             "
SELECT action_time_s AS failover_start_time_s,
       port,
       node_name,
       run.*
FROM failover
    JOIN run USING(run_id)
    JOIN master_replica_group_names USING(run_id)
  WHERE list_contains(ports, port)
  AND action LIKE 'failover% start';
             ")
  factor.experiment(data, con)
}

add.plot.failover.restart.rectangles <- function(plot, df, ymin=-Inf) {
  plot + 
    # Shutdown Start - Node is up again
    geom_rect(data=df,
              aes(xmin=shutdown_start_time_s-10,
                  xmax=start_start_time_s-10,
                  ymin=ymin,
                  ymax=Inf,
                  group=name),
              fill="#4d004b",
              alpha=0.25,
              color=NA,
              show.legend = F) +
    # Node is up again - First Sync. Finished (Partial or Full Resync.)
    geom_rect(data=df,
              aes(xmin=start_start_time_s-10,
                  xmax=first_sync_end_time_s-10,
                  ymin=ymin,
                  ymax=Inf,
                  group=name),
              fill="#084081",
              alpha=0.25,
              color=NA,
              show.legend = F) +
    # First Sync. Finished (Partial or Full Resync.) - Node Catched Up with Sync.
    geom_rect(data=df,
              aes(xmin=first_sync_end_time_s-10,
                  xmax=full_in_sync_end_time_s-10,
                  ymin=ymin,
                  ymax=Inf,
                  group=name),
              fill="#7f0000",
              alpha=0.25,
              color=NA,
              show.legend = F)
}
