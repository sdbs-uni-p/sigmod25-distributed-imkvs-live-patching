import pandas as pd


def read_redis_network(file: str) -> pd.DataFrame:
    return pd.read_csv(file, header=None, names=["bytes", "time"])
