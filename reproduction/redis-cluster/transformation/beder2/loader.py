import hashlib
import multiprocessing
import os
import random
import re
import sys
from argparse import ArgumentParser, Namespace
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from functools import reduce
from multiprocessing.managers import BaseManager
from typing import Callable, List, Optional

import pandas as pd

import data.cluster_status as cluster_status
import data.db_keys as db_keys
import data.experiment_info as experiment_info
import data.failover as failover
import data.latencies as latencies
import data.patch_elf as patch_elf
import data.redis_log as redis_log
import data.redis_network as redis_network
import data.redis_network_summary as redis_network_summary
import data.wf_log as wf_log
import data.wf_log_redis as wf_log_redis
from duckdb_storage import (
    DuckDBStorage,
    DuckDBStorageThread,
    Storage,
    StorageProcessCollector,
)
from future_collector import FutureCollector

USE_RANDOM_RUN_ID = False
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def get_run_dirs(experiment_dir: str, success_only: bool) -> List[str]:
    run_dirs = [
        os.path.join(experiment_dir, file)
        for file in os.listdir(experiment_dir)
        if os.path.isdir(os.path.join(experiment_dir, file))
    ]
    if success_only:
        run_dirs = [
            run_dir
            for run_dir in run_dirs
            if os.path.isfile(os.path.join(run_dir, "SUCCESS"))
        ]
    return run_dirs


def load_run(
    storage: Storage,
    run_dir: str,
    run_objects_name: str,
    wf_log_name: str,
    experiment_dir: str,
    workers: int,
) -> None:
    print(run_dir)
    run_id, start_time = load_run_info_data(
        storage, os.path.join(run_dir, run_objects_name)
    )
    latency_files = [
        os.path.join(run_dir, f)
        for f in os.listdir(run_dir)
        if re.match(r"latencies(\.[0-9]+)?\.csv", f)
    ]
    if len(latency_files) > 0:
        # All benchmarks are started at the same time. Just use first file for the info.
        load_latencies_info(storage, latency_files[0], run_id)

    # pool = ThreadPoolExecutor(max_workers=6)
    pool = FutureCollector(ProcessPoolExecutor(max_workers=workers))
    # pool = FutureCollector(ProcessPoolExecutor(max_workers=3))
    for latency_file in latency_files:
        pool.submit(
            load_latencies,
            storage,
            latency_file,
            run_id,
            start_time,
        )
    pool.submit(
        load_failover,
        storage,
        os.path.join(run_dir, "failover.yaml"),
        run_id,
        start_time,
    )
    pool.submit(
        load_cluster_status,
        storage,
        os.path.join(run_dir, "cluster", "cluster-status.yaml"),
        run_id,
        start_time,
    )
    for node_dir, port in [
        (node_dir, int(port))
        for node_dir, port in [
            (os.path.join(run_dir, "cluster", node_dir), node_dir)
            for node_dir in os.listdir(os.path.join(run_dir, "cluster"))
            if node_dir != "patches"
        ]
        if os.path.isdir(node_dir)
    ]:
        pool.submit(
            load_redis_network_summary,
            storage,
            os.path.join(node_dir, "network-summary.yaml"),
            run_id,
            port,
            start_time,
        )
        pool.submit(
            load_redis_network,
            storage,
            "redis_all_network",
            os.path.join(node_dir, "network-all.csv"),
            run_id,
            port,
            start_time,
        )
        pool.submit(
            load_redis_network,
            storage,
            "redis_cluster_network",
            os.path.join(node_dir, "network-cluster.csv"),
            run_id,
            port,
            start_time,
        )
        pool.submit(
            load_redis_network,
            storage,
            "redis_io_write",
            os.path.join(node_dir, "io-write-all.csv"),
            run_id,
            port,
            start_time,
        )
        pool.submit(
            load_redis_network,
            storage,
            "redis_io_read",
            os.path.join(node_dir, "io-read-all.csv"),
            run_id,
            port,
            start_time,
        )

        pool.submit(
            load_wf_log,
            storage,
            os.path.join(node_dir, wf_log_name),
            run_id,
            port,
            start_time,
        )
        pool.submit(
            load_redis_log_bgsave,
            storage,
            os.path.join(node_dir, "redis.stdout.log"),
            run_id,
            port,
            start_time,
        )
        pool.submit(
            load_redis_log_failover,
            storage,
            os.path.join(node_dir, "redis.stdout.log"),
            run_id,
            port,
            start_time,
        )
        pool.submit(
            load_redis_log_restart,
            storage,
            os.path.join(node_dir, "redis.stdout.log"),
            run_id,
            port,
            start_time,
        )
        # load_wf_log(
        # storage, os.path.join(node_dir, wf_log_name), run_id, port, start_time
        # )
        pool.submit(
            load_rdb_file,
            storage,
            os.path.join(node_dir, "dump.rdb"),
            run_id,
            port,
        )
    # load_rdb_file(storage, os.path.join(node_dir, "dump.rdb"), run_id, port)
    pool.shutdown()

    patches_dir: str = os.path.join(run_dir, "cluster", "patches")
    # Patches are all the same. We can just take the last directory...
    patch_files = [
        os.path.join(patches_dir, f)
        for f in (os.listdir(patches_dir) if os.path.exists(patches_dir) else [])
        if f.endswith(".o")
    ]

    if len(patch_files) == 0:
        # patch files may be empty, if skip_patch_files is set to TRUE when benchmarking
        return

    patch_data = pd.DataFrame(
        {
            "name": [os.path.basename(file) for file in patch_files],
            "size_bytes": [os.path.getsize(file) for file in patch_files],
        }
    )
    patch_data["run_id"] = run_id
    storage.insert("patch_file", patch_data)

    elf_data = patch_elf.read_patch_elf(patch_files)
    elf_data["run_id"] = run_id
    storage.insert("patch_elf", elf_data)


def load_cluster_status(
    storage: Storage, cluster_status_file: str, run_id: str, start_time: int
) -> None:
    print("Loading cluster status")
    cluster_status_data = cluster_status.read_cluster_status_data(cluster_status_file)
    cluster_status_data["time_s"] = cluster_status_data["time"] - start_time

    cluster_status_data["run_id"] = run_id
    storage.insert("cluster_status", cluster_status_data)


def load_run_info_data(storage: Storage, run_objects_file: str) -> str:
    print("Loading run info data")
    run_data = experiment_info.read_run_info_data(run_objects_file)
    run_data["benchmark_framework_end_time_s"] = (
        run_data["benchmark_framework_end_time"]
        - run_data["benchmark_framework_start_time"]
    )

    run_hash = hashlib.sha512()
    reduce(
        lambda _, value: run_hash.update(str(value).encode("UTF-8")),
        run_data.iloc[0].values.flatten().tolist(),
        None,  # We need this to also have the first value considered of our list.
    )
    if USE_RANDOM_RUN_ID:
        run_hash.update(f"{random.random()}".encode("UTF-8"))
    run_id = run_hash.hexdigest()

    run_data["run_id"] = run_id
    storage.insert("run", run_data)
    return run_id, run_data["benchmark_framework_start_time"][0]


def load_failover(
    storage: Storage, failover_file: str, run_id: str, start_time: float
) -> None:
    if not os.path.exists(failover_file):
        return
    failover_data = failover.read_failover_data(failover_file, start_time)
    failover_data["run_id"] = run_id
    storage.insert("failover", failover_data)


def load_latencies(
    storage: Storage, latencies_file: str, run_id: str, start_time: float
) -> None:
    if not os.path.exists(latencies_file):
        return
    latencies_data = latencies.read_latencies(latencies_file)
    latencies_data["run_id"] = run_id
    latencies_data["time_s"] = latencies_data["time"] - start_time
    storage.insert("latencies", latencies_data)


def load_latencies_info(storage: Storage, latencies_file: str, run_id: str) -> float:
    if not os.path.exists(latencies_file):
        print("No latencies file. Using '0' as start time.")
        return 0

    print("Loading latencies")
    latencies_info_data = latencies.read_info(latencies_file)
    latencies_info_data["run_id"] = run_id
    storage.insert("latencies_info", latencies_info_data)

    start_time: float = latencies_info_data.iloc[0].start_time
    return start_time


def load_rdb_file(storage: Storage, rdb_file: str, run_id: str, port: int) -> None:
    if not os.path.exists(rdb_file):
        return
    rdb_data = db_keys.read_redis_rdb_keys(
        rdb_file,
        os.path.join(SCRIPT_DIR, "redis-server"),
        os.path.join(SCRIPT_DIR, "redis-cli"),
    )
    if rdb_data.empty:
        return
    rdb_data["port"] = port
    rdb_data["run_id"] = run_id

    storage.insert("rdb", rdb_data)


def load_redis_network(
    storage: Storage,
    table: str,
    network_file: str,
    run_id: str,
    port: int,
    start_time: int,
) -> None:
    if not os.path.exists(network_file):
        return
    data = redis_network.read_redis_network(network_file)

    data["run_id"] = run_id
    data["port"] = port
    data["time_s"] = data["time"] - start_time
    storage.insert(table, data)


def load_redis_network_summary(
    storage: Storage, network_file: str, run_id: str, port: int, start_time: int
) -> None:
    if not os.path.exists(network_file):
        return
    data = redis_network_summary.read_redis_network(network_file)

    data["run_id"] = run_id
    data["port"] = port
    storage.insert("redis_network_summary", data)


def load_redis_log_restart(
    storage: Storage, redis_log_file: str, run_id: str, port: int, start_time: int
) -> None:
    if not os.path.exists(redis_log_file):
        return
    print("Loading redis log")

    data = redis_log.read_redis_restart(redis_log_file)
    if data is None:
        return

    data["run_id"] = run_id
    data["start_time_s"] = data["start_time"] - start_time
    data["end_time_s"] = data["end_time"] - start_time
    data["port"] = port
    storage.insert("redis_restart", data)


def load_redis_log_failover(
    storage: Storage, redis_log_file: str, run_id: str, port: int, start_time: int
) -> None:
    if not os.path.exists(redis_log_file):
        return
    print("Loading redis log")

    data = redis_log.read_redis_failover(redis_log_file)
    if data is None:
        return

    data["run_id"] = run_id
    data["start_time_s"] = data["start_time"] - start_time
    data["end_time_s"] = data["end_time"] - start_time
    data["port"] = port
    storage.insert("redis_failover", data)


def load_redis_log_bgsave(
    storage: Storage, redis_log_file: str, run_id: str, port: int, start_time: int
) -> None:
    if not os.path.exists(redis_log_file):
        return
    print("Loading redis log")

    data = redis_log.read_redis_log(redis_log_file)

    data["run_id"] = run_id
    data["time_s"] = data["time"] - start_time
    data["port"] = port
    storage.insert("redis_bgsaves", data)


def load_wf_log(
    storage: Storage, wf_log_file: str, run_id: str, port: int, start_time: int
) -> None:
    if not os.path.exists(wf_log_file):
        return

    print("Loading WfPatch log")

    def insert(
        table: str,
        data_fn: Callable[[str], pd.DataFrame],
        do_count: Optional[bool] = None,
        time_division: int = 1,
    ) -> None:
        try:
            data = data_fn(wf_log_file)
        except Exception as e:
            print(f"Error while loading data for table {table}")
            print(e)
            exit(1)
        data["run_id"] = run_id
        data["port"] = port
        if "time" in data:
            # Time is given in ms and not s..
            data["time"] /= float(time_division)
            data["time_s"] = data["time"] - start_time
        if do_count:
            data["entry_counter"] = range(len(data))
        storage.insert(table, data)

    insert("wf_l_birth", wf_log.read_birth_data)
    insert("wf_l_death", wf_log.read_death_data)
    insert("wf_l_apply", wf_log.read_apply_data, True)
    insert("wf_l_finished", wf_log.read_finished_data, True)
    insert("wf_l_migrated", wf_log.read_migrated_data, True)
    insert("wf_l_quiescence", wf_log.read_quiescence_data, True)
    insert("wf_l_reach_quiescence", wf_log.read_reach_quiescence_data, True)
    insert("wf_l_as_new", wf_log.read_as_new_data, True)
    insert("wf_l_as_delete", wf_log.read_as_delete_data, True)
    insert("wf_l_as_switch", wf_log.read_as_switch_data, True)
    insert("wf_l_patched", wf_log.read_patched_data, True)
    insert("wf_l_pte", wf_log.read_pte_data, True)
    insert("wf_l_vma", wf_log.read_vma_data, True)
    insert("wf_l_e2e_patched", wf_log.read_e2e_patched_data, True)

    insert("wf_r_new_patch", wf_log_redis.read_new_patch, True, 1000)
    insert("wf_r_patch_applied", wf_log_redis.read_patch_applied, True, 1000)
    insert("wf_r_patch_signaled", wf_log_redis.read_patch_signaled, True, 1000)
    insert("wf_r_patch_received", wf_log_redis.read_patch_received, True, 1000)
    insert("wf_r_patch_sent", wf_log_redis.read_patch_sent, True, 1000)
    insert("wf_r_patch_request", wf_log_redis.read_patch_request, True, 1000)


def _parse_arguments(input_args: List[str]) -> Namespace:
    parser = ArgumentParser(
        "BEnchmark Data analyzER (BEDER) - a framework to analyze benchmark data based on the data "
        "generated by BenchBase."
    )
    experiment_dir_parser = parser.add_mutually_exclusive_group(required=True)
    experiment_dir_parser.add_argument(
        "--experiment", help="The directory containing the experiment."
    )
    experiment_dir_parser.add_argument(
        "--benchmark", help="The directory containing all the experiment directories."
    )

    parser.add_argument(
        "--root-workers",
        help="The number of concurrent experiment workers",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--sub-workers",
        help="The number of sub-workers for loading experiment data",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--success-only",
        help="Read only data for successful benchmark runs",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--run-objects-name",
        help="The name of the file containing the objects of the benchmark.",
        default="experiment_objects.yaml",
    )
    parser.add_argument(
        "--wf-log-name",
        help="The name of the WfPatch log file.",
        default="redis.stderr.log",
    )
    parser.add_argument(
        "--random-run-id",
        help="Use a random run id in case of the same experiment has to be loaded multiple times",
        action="store_true",
        default=USE_RANDOM_RUN_ID,
    )

    parser.add_argument(
        "--output",
        help="The DuckDB database file to which the benchmark data should be added. If the database "
        "file does not exist, a new database file is be created. If the database already exists, "
        "the benchmark data is added.",
        required=True,
        type=str,
    )

    return parser.parse_args(input_args)


def main(input_args: List[str]):
    args = _parse_arguments(input_args)

    global USE_RANDOM_RUN_ID
    USE_RANDOM_RUN_ID = args.random_run_id

    # We create a shared queue.
    m = multiprocessing.Manager()
    db_input_queue = m.JoinableQueue()

    # Shared queue is used to insert data into DUckDB. This class runs in a background thread.
    storage = DuckDBStorageThread(args.output, db_input_queue)
    conn: DuckDBStorage = storage.connect()
    conn.create_tables()

    if args.experiment:
        experiments = [args.experiment]
    else:
        experiments = [
            os.path.join(args.benchmark, experiment)
            for experiment in os.listdir(args.benchmark)
            if os.path.isdir(os.path.join(args.benchmark, experiment))
        ]
    experiments = [os.path.realpath(exp) for exp in experiments]

    data_dirs = [
        (experiment_dir, run_dir)
        for experiment_dir in experiments
        for run_dir in get_run_dirs(experiment_dir, args.success_only)
    ]
    # Wrapper object to wrap the shared queue and that collects the intems in the queue
    collector: Storage = StorageProcessCollector(db_input_queue)

    # pool = ThreadPoolExecutor(max_workers=6)
    # pool = ProcessPoolExecutor(max_workers=3)
    pool = FutureCollector(ProcessPoolExecutor(max_workers=args.root_workers))
    for experiment_dir, run_dir in data_dirs:
        pool.submit(
            load_run,
            collector,
            run_dir,
            args.run_objects_name,
            args.wf_log_name,
            experiment_dir,
            args.sub_workers,
        )
    print("Done loading tasks.. Waiting for finish")
    pool.shutdown()
    print("Tasks finished, waiting for data to get loaded into DuckDB")
    conn.close()
    print("Done loading data into DuckDB :-)")


if __name__ == "__main__":
    main(sys.argv[1:])
