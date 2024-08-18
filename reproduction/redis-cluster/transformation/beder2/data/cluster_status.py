from functools import reduce

import pandas as pd
import yaml


def read_cluster_status_data(file: str) -> pd.DataFrame:
    with open(file) as f:
        status = yaml.safe_load(f)
    return reduce(
        lambda x, y: pd.concat([x, y]),
        [
            pd.DataFrame(
                {
                    "time": [time],
                    "port": [port],
                    "master_port": [values["master_port"]],
                    "role": [values["role"]],
                }
            )
            for time, port_group in status.items()
            for port, values in port_group.items()
        ],
        pd.DataFrame(),
    )
