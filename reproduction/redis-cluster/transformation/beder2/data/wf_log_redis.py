import re
from functools import reduce
from typing import Any, Callable, Dict, List

import pandas as pd


def _read_data(
    file: str,
    line_prefixes: List[str],
    attribute_conversions: List[Callable[[str], Dict[str, Any]]],
    ignore_missmatch: bool = True,
    attrs_must_match_conversions: bool = True,
) -> pd.DataFrame:
    redis_pattern = re.compile("\[.*?\]")
    with open(file) as f:
        lines: List[List[str]] = []
        for line in [l.strip() for l in f.readlines() if l.startswith("[REDIS]")]:
            # The list looks like this:
            # ['[REDIS]', '[New Patch Registered]', '[1692389869508.875000]', '[5b1a20d833e88703832b6a0bf7ec99d5dae17bbb]', '[5]', '[5]']
            # We strip the brackets.
            # And we remove the 'REDIS' from the beginning
            infos = [i[1:-1] for i in redis_pattern.findall(line)][1:]
            if infos[0] in line_prefixes:
                # And now strip the action name (e.g. New Patch Registered)
                lines.append(infos[1:])

    def extract_infos(infos: List[str]) -> pd.DataFrame:
        if attrs_must_match_conversions and len(infos) != len(attribute_conversions):
            if not ignore_missmatch:
                raise ValueError(
                    "Invalid length of attribute conversions. It must be less or equal the amount of attrs"
                    f" available. Attributes: {len(attrs)}. Conversions: {len(attribute_conversions)}."
                    f"{info_line}"
                )
            # Return empty data frame
            return pd.DataFrame()
        df_data = {
            key: [value]
            for idx, fn in enumerate(attribute_conversions)
            for key, value in fn(infos[idx]).items()
        }
        return pd.DataFrame(df_data)

    return reduce(
        lambda x, y: pd.concat([x, y]),
        [extract_infos(line) for line in lines],
        pd.DataFrame(),
    )


def read_new_patch(file: str) -> pd.DataFrame:
    # [REDIS] [New Patch Registered] [1692389869508.875000] [5b1a20d833e88703832b6a0bf7ec99d5dae17bbb] [5] [5]
    conversions = [
        lambda x: {"time": float(x)},
        lambda x: {"name": x},
        lambda x: {"version": int(x)},
        lambda x: {"method": int(x)},
        lambda x: {"from_client": True if int(x) == 1 else False},
    ]
    return _read_data(file, ["New Patch Registered"], conversions)


def read_patch_applied(file: str) -> pd.DataFrame:
    # [REDIS] [Patch Applied] [1692389870056.492920] [5b1a20d833e88703832b6a0bf7ec99d5dae17bbb] [11] [5]
    conversions = [
        lambda x: {"time": float(x)},
        lambda x: {"name": x},
        lambda x: {"version": int(x)},
        lambda x: {"method": int(x)},
    ]
    return _read_data(file, ["Patch Applied"], conversions)


def read_patch_signaled(file: str) -> pd.DataFrame:
    # [REDIS] [Patch Signaled] [1701942945820.407715] [9c9a07a85d3400869ab93eeb10c75524d804a987] [0] [9]
    conversions = [
        lambda x: {"time": float(x)},
        lambda x: {"name": x},
        lambda x: {"version": int(x)},
        lambda x: {"method": int(x)},
    ]
    return _read_data(file, ["Patch Signaled"], conversions)


def read_patch_received(file: str) -> pd.DataFrame:
    # [REDIS] [Received Patch] [%lf] [%.40s] [%.40s] [%d] [%d]
    conversions = [
        lambda x: {"time": float(x)},
        lambda x: {"name": x},
        lambda x: {"sender": x},
        lambda x: {"version": int(x)},
        lambda x: {"method": int(x)},
    ]
    return _read_data(file, ["Patch Received"], conversions)


def read_patch_sent(file: str) -> pd.DataFrame:
    # [REDIS] [Received Patch] [%lf] [%.40s] [%.40s] [%d] [%d]
    conversions = [
        lambda x: {"time": float(x)},
        lambda x: {"name": x},
        lambda x: {"receiver": x},
        lambda x: {"version": int(x)},
    ]
    return _read_data(file, ["Patch Sent"], conversions)


def read_patch_request(file: str) -> pd.DataFrame:
    # [REDIS] [Received Patch] [%lf] [%.40s] [%.40s] [%d] [%d]
    conversions = [
        lambda x: {"time": float(x)},
        lambda x: {"name": x},
        lambda x: {"receiver": x},
        lambda x: {"version": int(x)},
    ]
    return _read_data(file, ["Patch Request"], conversions)
