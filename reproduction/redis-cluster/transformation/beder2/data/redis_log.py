from datetime import datetime

import pandas as pd


def _extract_time(log_string: str) -> float:
    # 78674:M 24 Jan 2024 13:09:08.597 * Starting BGSAVE for SYNC with target: replicas sockets
    date_time_str = log_string.split()[1:5]
    date_time_str = " ".join(date_time_str)

    # Parse the date and time string into a datetime object
    log_datetime = datetime.strptime(date_time_str, "%d %b %Y %H:%M:%S.%f")

    # Convert the datetime object to a Unix timestamp (float)
    return log_datetime.timestamp()


def read_redis_restart(file: str) -> pd.DataFrame:
    with open(file) as f:
        lines = f.readlines()
    restart_start = [
        _extract_time(line) for line in lines if "User requested shutdown..." in line
    ]
    restart_end = [
        _extract_time(line)
        for line in lines
        if "MASTER <-> REPLICA sync: Finished with success" in line
        or "Successful partial resynchronization with master" in line
    ]

    # 1. Situation: Master Node
    # 2. Situation: Replica Node

    if len(restart_start) != 2:
        # Node was not restarted. Skip it!
        return None
    # Now, we want to have the first synchronization AFTER the first restart.
    restart_start_time = restart_start[0]
    # restart_end is already sorted! so check which time is greater than our start time and select the first entry!
    restart_end_time = [r for r in restart_end if r >= restart_start_time][0]

    return pd.DataFrame(
        {
            "start_time": [restart_start_time],
            "end_time": [restart_end_time],
            "duration_s": [restart_end_time - restart_start_time],
        }
    )


def read_redis_failover(file: str) -> pd.DataFrame:
    with open(file) as f:
        lines = f.readlines()
    failover_start = [
        line for line in lines if "Manual failover user request accepted" in line
    ]
    failover_end = [
        line for line in lines if "Failover election won: I'm the new master." in line
    ]

    if len(failover_end) > 1:
        raise Exception(
            f"Parsing failover information from {file} failed!\n{failover_start}\n{failover_end}"
        )
    if len(failover_start) == 0:
        return None
    failover_start = failover_start[
        0
    ]  # It may be the case that a failover timed out. We use the first failover request!
    failover_end = failover_end[0]  # There should only be one entry..
    start_time = _extract_time(failover_start)
    end_time = _extract_time(failover_end)
    return pd.DataFrame(
        {
            "start_time": [start_time],
            "end_time": [end_time],
            "duration_ms": [end_time * 1000 - start_time * 1000],
        }
    )


def read_redis_log(
    file: str,
) -> pd.DataFrame:
    with open(file) as f:
        lines = f.readlines()
        # Partial sync (snapshot is created on shutdown)
        # 23961:S 19 Mar 2024 08:13:33.819 * Saving the final RDB snapshot before exiting.

        # Full sync (snapshot is created for sync.)
        # 78674:M 24 Jan 2024 13:09:08.597 * Starting BGSAVE for SYNC with target: replicas sockets
        bgsaves = [
            line
            for line in lines
            if (
                "Starting BGSAVE for SYNC with target" in line
                or "Saving the final RDB snapshot before exiting" in line
            )
        ]
    return pd.DataFrame({"time": [_extract_time(line) for line in bgsaves]})
