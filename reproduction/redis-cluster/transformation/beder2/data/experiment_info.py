from functools import reduce
from typing import Any, List

import pandas as pd
import yaml


def read_run_info_data(file: str) -> pd.DataFrame:
    matchings = [
        ("run_commit", ["commit"]),
        ("run_success", ["success"]),
        ("run_benchmark_failure", ["benchmark_failure"]),
        ("run_benchmark_timeout", ["benchmark_timeout"]),
        ("run_db_failure", ["db_failure"]),
        ("run_name", ["name"]),
        ("patch_generation_patches", ["patch_generation", "patches"]),
        ("patch_generation_patch_paths", ["patch_generation", "patch_paths"]),
        ("data_requests", ["data", "requests"]),
        ("data_keyspace", ["data", "keyspace"]),
        ("data_data_size", ["data", "data_size"]),
        ("data_max_memory_usage_gb", ["data", "max_memory_usage_gb"]),
        ("benchmark_taskset_start", ["benchmark", "taskset_start"]),
        ("benchmark_taskset_end", ["benchmark", "taskset_end"]),
        ("benchmark_taskset_step", ["benchmark", "taskset_step"]),
        ("benchmark_name", ["benchmark", "name"]),
        ("benchmark_output_name", ["benchmark", "output_name"]),
        ("benchmark_requests", ["benchmark", "requests"]),
        ("benchmark_time_s", ["benchmark", "time_s"]),
        ("benchmark_idle_before", ["benchmark", "idle_before"]),
        ("benchmark_keyspace", ["benchmark", "keyspace"]),
        ("benchmark_data_size", ["benchmark", "data_size"]),
        ("benchmark_clients", ["benchmark", "clients"]),
        ("benchmark_threads", ["benchmark", "threads"]),
        ("benchmark_incr_key_only", ["benchmark", "incr_key_only"]),
        ("benchmark_dump_db_at_stop", ["benchmark", "dump_db_at_stop"]),
        ("benchmark_max_memory_usage_gb", ["benchmark", "max_memory_usage_gb"]),
        ("benchmark_framework_start_time", ["benchmark", "framework_start_time"]),
        ("benchmark_framework_end_time", ["benchmark", "framework_end_time"]),
        ("failover_failover_after_s", ["failover", "failover_after_s"]),
        ("failover_skip_failover", ["failover", "skip_failover"]),
        ("failover_skip_restart_master", ["failover", "skip_restart_master"]),
        ("failover_skip_restart_replica", ["failover", "skip_restart_replica"]),
        ("redis_cluster_config", ["redis_cluster", "config"]),
        ("redis_cluster_status_every_s", ["redis_cluster", "status_every_s"]),
        ("patch_method", ["patch", "method"]),
        ("patch_distribution", ["patch", "distribution"]),
        ("patch_reverse_version", ["patch", "reverse_version"]),
        ("patch_apply_patch_after_s", ["patch", "apply_patch_after_s"]),
        ("patch_measure_vma", ["patch", "measure_vma"]),
        ("patch_measure_pte", ["patch", "measure_pte"]),
        ("patch_patch_only_active", ["patch", "patch_only_active"]),
        ("patch_apply_all_patches_at_once", ["patch", "apply_all_patches_at_once"]),
        ("patch_single_as_for_each_thread", ["patch", "single_as_for_each_thread"]),
        ("patch_skip_patch_file", ["patch", "skip_patch_file"]),
    ]
    with open(file) as f:
        run_info = yaml.safe_load(f)

    def get_attribute(path: List[str]) -> Any:
        return reduce(
            lambda dic, key: dic[key] if (dic and key in dic) else None, path, run_info
        )

    # This function is used for eg. jvm_arguments, or database flags as we do not store lists in DuckDB
    # DuckDB is capable of lists, but probably the R driver has problems with it.
    def list_to_str(value: Any, separator: str = ";") -> str:
        if isinstance(value, list):
            return separator.join(str(v) for v in value)
        return value

    df_data = {
        db_column: [list_to_str(get_attribute(info_path))]
        for db_column, info_path in matchings
    }
    return pd.DataFrame(df_data)
