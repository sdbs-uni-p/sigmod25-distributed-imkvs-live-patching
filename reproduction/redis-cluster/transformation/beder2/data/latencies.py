import time

import dask.dataframe as dd
import pandas as pd
import yaml

HEADER_CONVERSIONS = {"start_time": "start_time", "end_time": "end_time"}


# def _latencies_file_content(file: str) -> List[str]:
#    with open(file) as f:
#        return [line for line in f.readlines() if not line.startswith("WARNING:")]


def read_info(file: str) -> pd.DataFrame:
    with open(file) as f:
        lines = [f.readline(), f.readline()]

    df = pd.DataFrame(
        {
            keys[0]: [yaml.safe_load(lines[idx])[keys[1]]]
            for idx, keys in enumerate(HEADER_CONVERSIONS.items())
        }
    )
    df["total_duration_s"] = df["end_time"] - df["start_time"]
    return df


def read_latencies(file: str) -> pd.DataFrame:
    # thread_id,latency_ms,time,port
    start = time.time()
    df = dd.read_csv(
        file,
        skiprows=len(HEADER_CONVERSIONS),  # Skip first two rows
        header=0,
        names=[
            "thread_id",
            "latency_ms",
            "time",
            "port",
            "client_id",
            "key",
        ],
    ).compute()

    df["time_s"] = None
    if len(df[df["latency_ms"] < 0]) > 0:
        print(df)
        print("DataFrame contains negative latency. Maybe a overflow happened?")
        exit(1)

    return df
