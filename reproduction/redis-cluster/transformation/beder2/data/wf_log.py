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
    with open(file) as f:
        lines = []
        for line in f.readlines():
            for prefix in line_prefixes:
                if line.startswith(prefix):
                    lines.append(line.strip())

    def extract_infos(info_line: str) -> pd.DataFrame:
        info_line = info_line.removeprefix("- [").removesuffix("]")
        attrs = [attr.strip() for attr in info_line.split(",")]
        if attrs_must_match_conversions and len(attrs) != len(attribute_conversions):
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
            for key, value in fn(attrs[idx]).items()
        }
        return pd.DataFrame(df_data)

    return reduce(
        lambda x, y: pd.concat([x, y]),
        [extract_infos(line) for line in lines],
        pd.DataFrame(),
    )


def read_pte_data(file: str) -> pd.DataFrame:
    conversions = [
        lambda x: {"name": x},
        lambda x: {"size_kB": int(x)},
    ]
    return _read_data(file, ["- [VmPTE-"], conversions)


def read_vma_data(file: str) -> pd.DataFrame:
    # - [vma-count-startup, 0, rw-p, FILE-BACKED, 25, 864, 0, 0, 68, 524]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"wfpatch_generation": int(x)},
        lambda x: {"perm": x},
        lambda x: {"type": x},
        lambda x: {"count": int(x)},
        lambda x: {"size": int(x)},
        lambda x: {"shared_clean": int(x)},
        lambda x: {"shared_dirty": int(x)},
        lambda x: {"private_clean": int(x)},
        lambda x: {"private_dirty": int(x)},
    ]
    return _read_data(file, ["- [vma-count-"], conversions)


def read_e2e_patched_data(file: str) -> pd.DataFrame:
    conversions = [
        lambda x: {"name": x},
        lambda x: {"time": float(x)},
        lambda x: {"duration_ms": float(x)},
        lambda x: {"patch_method": int(x)},
        lambda x: {"thread_group_name": x[1:-1]},
    ]
    return _read_data(file, ["- [e2e-patch"], conversions)


def read_patched_data(file: str) -> pd.DataFrame:
    # - [patched, 13059.2741, "/home/michael/hotpatch/development/mariadb-patch-benchmark/build-output/wfpatch.patch-06fae75859821fe36f68eb2d77f007f014143282/patch--table_cache.cc.o", "(null)"]  # noqa: E501
    conversions = [
        lambda x: {"name": x},
        lambda x: {"duration_ms": float(x)},
    ]
    return _read_data(
        file, ["- [patched"], conversions, attrs_must_match_conversions=False
    )


def read_as_new_data(file: str) -> pd.DataFrame:
    # - [address-space-new, 2.0505, "(null)"]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"duration_ms": float(x)},
        lambda x: {"thread_group_name": x[1:-1]},
    ]
    return _read_data(file, ["- [address-space-new"], conversions)


def read_as_switch_data(file: str) -> pd.DataFrame:
    # - [address-space-switch, 0.0148]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"duration_ms": float(x)},
    ]
    return _read_data(file, ["- [address-space-switch"], conversions)


def read_as_delete_data(file: str) -> pd.DataFrame:
    # - [address-space-delete, 0.0004, "(null)"]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"duration_ms": float(x)},
        lambda x: {"thread_group_name": x[1:-1]},
    ]
    return _read_data(file, ["- [address-space-delete"], conversions)


def read_reach_quiescence_data(file: str) -> pd.DataFrame:
    # - [reach-quiescence-point, 0.0050, "connection_handler", "(null)", 2006706]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"duration_ms": float(x)},
        lambda x: {"thread_name": x[1:-1]},  # remove leading and trailing "
        lambda x: {"thread_group_name": x[1:-1]},
        lambda x: {"thread_id": int(x)},
    ]
    return _read_data(file, ["- [reach-quiescence-point"], conversions)


def read_quiescence_data(file: str) -> pd.DataFrame:
    # - [quiescence, 0.2762, "(null)"]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"duration_ms": float(x)},
        lambda x: {"thread_group_name": x[1:-1]},
    ]
    return _read_data(file, ["- [quiescence"], conversions)


def read_migrated_data(file: str) -> pd.DataFrame:
    # - [migrated, 0.2556, 1, "connection_handler", "(null)", 2006709]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"duration_ms": float(x)},
        lambda x: {"current_address_space_version": int(x)},
        lambda x: {"thread_name": x[1:-1]},  # remove leading and trailing "
        lambda x: {"thread_group_name": x[1:-1]},
        lambda x: {"thread_id": int(x)},
    ]
    return _read_data(file, ["- [migrated"], conversions)


def read_apply_data(file: str) -> pd.DataFrame:
    # - [apply, 1665070492.768831972, local, 11, "(null)"]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"time": float(x)},
        lambda x: {"quiescence": x},
        lambda x: {"amount_threads": int(x)},
        lambda x: {"thread_group_name": x[1:-1]},  # remove leading and trailing "
    ]
    return _read_data(file, ["- [apply"], conversions)


def read_finished_data(file: str) -> pd.DataFrame:
    # - [finished, 13125.5862, "(null)"]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"duration_ms": float(x)},
        lambda x: {"thread_group_name": x[1:-1]},  # remove leading and trailing "
    ]
    return _read_data(file, ["- [finished"], conversions)


def read_birth_data(file: str) -> pd.DataFrame:
    # - [birth, 1665070477709.47, "connection_handler", "(null)", 2006698]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"time": float(x)},
        lambda x: {"thread_name": x[1:-1]},
        lambda x: {"thread_group_name": x[1:-1]},
        lambda x: {"thread_id": int(x)},
    ]
    return _read_data(file, ["- [birth"], conversions)


def read_death_data(file: str) -> pd.DataFrame:
    # - [death, 58245.94, "connection_handler", "(null)", 2006705]
    conversions = [
        lambda x: {"name": x},
        lambda x: {"duration_ms": float(x)},
        lambda x: {"thread_name": x[1:-1]},
        lambda x: {"thread_group_name": x[1:-1]},
        lambda x: {"thread_id": int(x)},
    ]
    return _read_data(file, ["- [death"], conversions)
