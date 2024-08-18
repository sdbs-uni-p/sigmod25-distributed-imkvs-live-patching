import os
import shutil
import subprocess
import tempfile
import time

import dask.dataframe as dd
import pandas as pd
import redis
import yaml
from redis.exceptions import BusyLoadingError


def read_redis_rdb_keys(
    rdb_file: str, redis_server_bin: str, redis_cli_bin: str
) -> pd.DataFrame:
    with tempfile.TemporaryDirectory() as tmpdir:
        rdb_file = shutil.copy(
            rdb_file, os.path.join(tmpdir, os.path.basename(rdb_file))
        )
        redis_server_bin = shutil.copy(
            redis_server_bin, os.path.join(tmpdir, os.path.basename(redis_server_bin))
        )

        socket_file = tempfile.NamedTemporaryFile(dir=tmpdir, delete=False)
        redis_proc = subprocess.Popen(
            [redis_server_bin, "--port", "0", "--unixsocket", socket_file.name],
            cwd=tmpdir,
        )
        time.sleep(1)  # Wait for redis to start
        con = redis.Redis(unix_socket_path=socket_file.name, decode_responses=True)
        # Wait until redis is prepared and ready
        while True:
            try:
                con.ping()
                break
            except BusyLoadingError:
                continue

        keys_file = tempfile.NamedTemporaryFile(dir=tmpdir, delete=False)
        with open(keys_file.name, "w") as f:
            subprocess.run(
                [redis_cli_bin, "-s", socket_file.name, "KEYS", "*"], stdout=f
            )

        try:
            df = dd.read_csv(
                keys_file.name, header=None, names=["key"], dtype={"key": str}
            )
            # df.columns = ["key"]
            # df = df.astype({"key": "str"})
            df = df[~df["key"].str.startswith("load-")]
            df = df.compute()
        except pd.errors.EmptyDataError:
            df = pd.DataFrame(columns=["key"])
        # df = df.astype({"key": "int"})

        # while True:
        #    try:
        #        # Do not load "load-XYZ" keys..

        #        df = pd.DataFrame({"key": con.keys()})
        #        df = df[~df["key"].str.startswith("load-")]
        #        df = df.astype({"key": "int"})
        #        break
        #    except BusyLoadingError:
        #        continue

        con.shutdown(force=True, now=True, nosave=True)
        time.sleep(1)

        try:
            os.kill(redis_proc.pid, 9)
            time.sleep(1)
        except Exception:
            pass
        # redis_proc.wait()

    return df
