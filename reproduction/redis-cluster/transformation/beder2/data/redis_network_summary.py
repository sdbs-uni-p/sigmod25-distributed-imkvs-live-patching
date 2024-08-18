import pandas as pd
import yaml


def read_redis_network(file: str) -> pd.DataFrame:
    with open(file) as f:
        lines = f.readlines()
    configs = [yaml.safe_load(line)["ClusterPacketsProcessed"] for line in lines]
    return pd.DataFrame(
        {
            "cluster_packets_processed": configs,
        }
    )

    # with open(file) as f:
    #    lines = f.readlines()
    #    headers = [idx for idx, line in enumerate(lines) if line.startswith("Bytes")]
    #    headers = [*headers, len(lines)]

    #    groups = zip(headers, headers[1:])
    #    all_dfs = []
    #    for idx, group in enumerate(groups):
    #        start, end = group
    #        df = pd.read_csv(StringIO("\n".join(lines[start:end])))
    #        df = df.rename(
    #            columns={"Bytes": "total_bytes", "Messages": "total_messages"}
    #        )
    #        df["entry"] = idx
    #        all_dfs.append(df)
    # r = pd.concat(all_dfs)
    # return r
