data.action <- function(con) {
  data <- dbGetQuery(con,
                     "SELECT * FROM failover
        LEFT JOIN run USING(run_id);")
}

data.restart.time <- function(con) {
  data <- dbGetQuery(
    con,
    "
SELECT DISTINCT
  duration_ms / 1000. AS restart_duration_s,
  start_time_s AS restart_start_time_s,
  ports,
  port,
  role,
  run.*
FROM failover
  LEFT JOIN run USING(run_id)
  LEFT JOIN initial_cluster_status USING(run_id, port)
  LEFT JOIN node_groups USING(run_id)
WHERE LIST_CONTAINS(node_groups.ports, port)
AND name = 'restart';"
  )
  data$ports <- sapply(data$ports, paste, collapse = '-')
  factor.experiment(data, con)
}

data.failover.time <- function(con) {
  data <- dbGetQuery(
    con,
    "SELECT action_time_s,
         action,
         ports,
         run.*
    FROM failover
        LEFT JOIN run USING(run_id)
        LEFT JOIN node_groups USING(run_id)
    WHERE LIST_CONTAINS(node_groups.ports, port)
    AND action LIKE 'failover %start';"
  )
  data$action <- factor(data$action)
  data$ports <- sapply(data$ports, paste, collapse = '-')
  factor.experiment(data, con)
}

data.failover.full.time <- function(con) {
  data <- dbGetQuery(
    con,
    "SELECT DISTINCT
         start_time_s,
         duration_ms / 1000. AS duration_s,
         ports,
         run.*
    FROM failover
        LEFT JOIN run USING(run_id)
        LEFT JOIN node_groups USING(run_id)
    WHERE LIST_CONTAINS(node_groups.ports, port)
    AND name = 'failover';"
  )
  data$ports <- sapply(data$ports, paste, collapse = '-')
  factor.experiment(data, con)
}


data.network.summary <- function(con) {
  print(con)
  data <- dbGetQuery(
    con,
    "
    SELECT SUM(bytes) AS total_bytes, run.*
    FROM redis_network
      LEFT JOIN run USING(run_id)
    GROUP BY ALL;"
    )
factor.experiment(data, con)
}


data.network <- function(con, time.division = 0.5) {
  print(con)
  data <- dbGetQuery(
    con,
    sqlInterpolate(
      con,
      "
      WITH ranges AS (
    SELECT UNNEST(GENERATE_SERIES(0, CEIL(duration.total_duration_s / ?gap)::INT)) * ?gap AS start,
           duration.run_id AS run_id
    FROM (
        -- end_time_s for failover; a failover may take some time to restart..
        -- total_duration_s as fallback for all other scenarios
        -- +30 Seconds just to see also last seconds
        SELECT run_id, COALESCE(MAX(end_time_s), MAX(total_duration_s)) + 30 AS total_duration_s
        FROM latencies_info 
          LEFT JOIN failover USING(run_id)
        GROUP BY ALL
    ) duration
), total_bytes AS (
    SELECT SUM(bytes) AS total_bytes, port, run_id, start
    FROM redis_network
      FULL JOIN ranges USING(run_id)
    WHERE time_s >= start
    AND time_s < start + ?gap
    GROUP BY ALL
)
SELECT *
FROM total_bytes
  LEFT JOIN run USING(run_id);
      "
    ,
    gap = time.division
    ))
data$port <- factor(data$port)
factor.experiment(data, con)
}



fetch.rps.time <- function(con, time.division = 0.5) {
  print(con)
  data <- dbGetQuery(
    con,
    sqlInterpolate(
      con,
      "
WITH ranges AS (
  SELECT UNNEST(GENERATE_SERIES(0, FLOOR((total_duration_s - 1) / ?gap)::INT)) * ?gap AS start,
      run_id
  FROM latencies_info
), total_latencies AS (
    SELECT COUNT(*) AS latencies,
        run_id,
        ranges.start AS start
    FROM latencies
        FULL JOIN ranges USING(run_id)
    WHERE latencies.time_s >= start
        AND latencies.time_s < start + ?gap
    GROUP BY ALL
), total_latencies_zero AS (
    SELECT latencies, run_id, start
    FROM total_latencies
        RIGHT JOIN ranges USING(start, run_id)
    GROUP BY ALL
)
SELECT COALESCE(latencies, 0) AS total_latencies, start,
       run.*
FROM total_latencies_zero
  LEFT JOIN run USING(run_id)"
    ,
    gap = time.division
    ))
# See below if you want to use ports!
factor.experiment(data, con)
}


data.rps.time <- function(con, time.division = 0.5) {
  print(con)
  data <- dbGetQuery(
    con,
    sqlInterpolate(
      con,
      "
WITH ranges AS (
    SELECT UNNEST(GENERATE_SERIES(0, CEIL(duration.total_duration_s / ?gap)::INT)) * ?gap AS start,
           duration.run_id AS run_id
    FROM (
        -- end_time_s for failover; a failover may take some time to restart..
        -- total_duration_s as fallback for all other scenarios
        -- +30 Seconds just to see also last 30 seconds
        SELECT run_id, COALESCE(MAX(end_time_s), MAX(total_duration_s)) + 30 AS total_duration_s
        FROM latencies_info 
          LEFT JOIN failover USING(run_id)
        GROUP BY ALL
    ) duration
), total_latencies AS (
    SELECT COUNT(*) AS latencies,
        run_id,
        ranges.start AS start,
        port
    FROM latencies
        FULL JOIN ranges USING(run_id)
    WHERE latencies.time_s >= start
        AND latencies.time_s < start + ?gap
    GROUP BY ALL
), total_latencies_zero AS (
    SELECT latencies, run_id, start, port
    FROM total_latencies
    RIGHT JOIN ranges USING(start, run_id)
    GROUP BY ALL
)
SELECT SUM(COALESCE(latencies, 0)) AS total_latencies, start, NULL AS ports,
       run.*
FROM total_latencies_zero
  LEFT JOIN run USING(run_id)
GROUP BY ALL
UNION ALL
SELECT SUM(COALESCE(latencies, 0)) AS total_latencies, start, ports,
       run.*
FROM total_latencies_zero
  LEFT JOIN run USING(run_id)
  LEFT JOIN node_groups USING(run_id)
  WHERE LIST_CONTAINS(node_groups.ports, total_latencies_zero.port)
--  AND master_port = 7000
GROUP BY ALL;"
    ,
    gap = time.division
    ))

data$ports <- sapply(data$ports, paste, collapse='-')
data$port_group <- ""

data$port_group[data$ports != ""] <- "Master/Replica"
data$port_group[data$ports == ""] <- "All"

data$port_group <- factor(data$port_group)
data$ports <- factor(data$ports)
#data$port <- factor(data$port)

factor.experiment(data, con)
}

data.patch.time <- function(con) {
  data <- dbGetQuery(
    con,
    "
SELECT end_s - start_s AS duration_s,
  run.*
FROM (SELECT run_id, MIN(time_s) AS start_s FROM wf_r_new_patch GROUP BY ALL)
  JOIN (SELECT run_id, MAX(time_s) AS end_s FROM wf_r_patch_applied GROUP BY ALL) USING(run_id)
  LEFT JOIN run USING(run_id);

    "
  )
  factor.experiment(data, con)
}

data.patch.flow <- function(con) {
  data <- dbGetQuery(
    con,
    "
SELECT *
FROM (
  SELECT port, 'New Patch' AS name, time_s, version, run_id
  FROM wf_r_new_patch
  UNION ALL
  SELECT port, 'Patch Applied' AS name, time_s, version, run_id
  FROM wf_r_patch_applied
  UNION ALL
  SELECT port, 'Patch Received' AS name, time_s, version, run_id
  FROM wf_r_patch_received
) LEFT JOIN run USING(run_id)
  JOIN (SELECT * FROM initial_cluster_status WHERE role = 'master') USING(run_id, port);
    "
  )
  data$version <- factor(data$version)
  data$port <- factor(data$port)
  data$name <- factor(data$name)
  factor.experiment(data, con)
}
