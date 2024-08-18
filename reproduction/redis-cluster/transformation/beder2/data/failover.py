import pandas as pd
import yaml


def read_failover_data(file: str, start_time: float) -> pd.DataFrame:
    with open(file) as f:
        config = yaml.safe_load(f)

    res = []
    # In this function, we highly rely on the format of the yaml!
    for log_time in config.keys():
        # We use this dict for insertion
        data = config[log_time]
        data["log_time"] = log_time
        data["log_time_s"] = log_time - start_time
        data["start_time_s"] = data["start_time"] - start_time
        data["end_time_s"] = data["end_time"] - start_time

        # Get actions and remove it because we flatten it.
        actions = data["actions"]
        del data["actions"]
        for action in actions:
            action["action_time_s"] = action["action_time"] - start_time
        res += [{**data, **action} for action in actions]

    return pd.DataFrame(res)
