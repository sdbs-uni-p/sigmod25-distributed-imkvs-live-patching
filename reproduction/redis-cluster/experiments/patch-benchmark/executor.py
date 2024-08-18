import datetime
import io
import os
import shutil
import subprocess
import sys
import tempfile
import time
from argparse import ArgumentParser, Namespace
from multiprocessing.pool import ThreadPool
from typing import Any, Dict, List, Optional

import yaml

import crc16
import process
from config import parse_config
from model import Experiment

VERBOSE = False

log_buffer: List[str] = []


def _cluster_status_master_ports(status: Dict) -> List[int]:
    current = sorted(status.keys())[-1]
    return [
        port
        for port, value_dict in status[current].items()
        if value_dict["role"] == "master"
    ]


def log(msg: Any):
    msg = f"{datetime.datetime.now().isoformat()} {msg}"
    print(msg)
    log_buffer.append(msg)


def write_log(file: str):
    with open(file, "a") as f:
        f.write("\n".join(log_buffer))
        f.write("\n")
    log_buffer.clear()


def touch(path):
    with open(path, "a"):
        os.utime(path, None)


def get_path_from_root(paths: List[str]) -> str:
    return os.path.realpath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", *paths)
    )


def get_script(name: str) -> str:
    return os.path.realpath(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "scripts", name)
    )


def _wfpatch_env(
    experiment: Experiment, experiment_specific_benchmark_result_dir: str
) -> Dict:
    # This is only needed when we apply a patch!
    if not experiment.patch:
        return {}
    return {
        "WF_SKIP_PATCH_APPLICATION": int(experiment.patch.skip_patch_application),
        "WF_MEASURE_VMA": int(experiment.patch.measure_vma),
        "WF_MEASURE_PTE": int(experiment.patch.measure_pte),
        "WF_LOCAL_SINGLE_AS": int(experiment.patch.single_as_for_each_thread),
        "WF_PATCH_ONLY_ACTIVE": int(experiment.patch.patch_only_active),
    }


def _cluster_work_dir(experiment: Experiment) -> str:
    return os.path.join(
        experiment.redis_cluster.output,
        os.path.splitext(os.path.basename(experiment.redis_cluster.config))[0],
    )


def _cluster_command(experiment: Experiment) -> List[str]:
    return [
        "pipenv",
        "run",
        "python",
        get_path_from_root(["redis-cluster-manager"]),
        "--work-dir",
        _cluster_work_dir(experiment),
    ]


def build(experiment: Experiment) -> None:
    command = [
        get_path_from_root(["redis-build-utils", "build"]),
        "--commit",
        experiment.commit,
        "--build-dir",
        experiment.build.dir,
        "--git-name",
        experiment.build.git_dir_name,
        "--output-dir",
        experiment.build.output,
    ]

    log(f"[BUILD] {command}")
    process.run2(command, verbose=VERBOSE)
    experiment.build.bin_dir = os.path.join(experiment.build.output, "build-new")


def create_patches(experiment: Experiment) -> bool:
    if not experiment.patch_generation:
        return False
    if experiment.patch.skip_patch_application:
        # In case we skip patch application, we also just use non-existing patches..
        # Otherwise, it just puts unneccessary load on the machines to generate patches..
        experiment.patch_generation.patch_paths = [
            f"dummy-{i}.patch" for i in range(experiment.patch_generation.patches)
        ]
        return False
    if experiment.patch.skip_patch_file:
        experiment.patch_generation.patch_paths = []
        return False

    command = [
        get_path_from_root(
            ["patches", "generate-redis-getPatch-patches", "generate-patches-build"]
        ),
        f"{experiment.patch_generation.patches}",
        f"{experiment.commit}",
    ]

    log(f"[CREATE PATCHES] {command}")
    process.run2(command, verbose=VERBOSE)
    # We use the binary of the first compiled patch as binary for execution
    experiment.build.bin_dir = get_path_from_root(
        [
            "patches",
            "generate-redis-getPatch-patches",
            f"compiled-patches-{experiment.commit}",
            "livepatch-1",
            "build",
        ]
    )

    experiment.patch_generation.patch_paths = [
        os.path.join(
            get_path_from_root(
                [
                    "patches",
                    "generate-redis-getPatch-patches",
                    f"patches-{experiment.commit}",
                ]
            ),
            f,
        )
        for f in os.listdir(
            get_path_from_root(
                [
                    "patches",
                    "generate-redis-getPatch-patches",
                    f"patches-{experiment.commit}",
                ]
            )
        )
    ][0 : experiment.patch_generation.patches]

    # Copy all patches to the cluster directory
    experiment.redis_cluster.patches_dir = os.path.join(
        _cluster_work_dir(experiment), "patches"
    )
    os.mkdir(experiment.redis_cluster.patches_dir)
    for patch in experiment.patch_generation.patch_paths:
        shutil.copy(patch, experiment.redis_cluster.patches_dir)
    return True


def prepare_cluster(experiment: Experiment) -> None:
    command = [
        *_cluster_command(experiment),
        "Create",
        "--config",
        experiment.redis_cluster.config,
    ]
    log(f"[PREPARE] {command}")
    process.run2(command, verbose=VERBOSE)


def start_cluster(
    experiment: Experiment, experiment_specific_benchmark_result_dir: str
) -> subprocess.Popen:
    command = [
        *_cluster_command(experiment),
        "Start",
        "--bin",
        experiment.build.bin_dir,
        "--join",
    ]
    log(f"[Start Cluster] {command}")

    cluster_proc = process.run_async(
        command,
        stdout_pipe=subprocess.PIPE,
        stderr_pipe=None if VERBOSE else subprocess.DEVNULL,
        env=_wfpatch_env(experiment, experiment_specific_benchmark_result_dir),
    )

    cluster_output = io.BytesIO()
    for c in iter(lambda: cluster_proc.stdout.read(1), b""):
        cluster_output.write(c)
        if "[OK] All 16384 slots covered." in cluster_output.getvalue().decode("UTF-8"):
            break
    cluster_output.close()

    print("Cluster is ready. We start in 10s..")
    time.sleep(10)

    return cluster_proc


def stop_cluster(experiment: Experiment) -> subprocess.Popen:
    command = [*_cluster_command(experiment), "Stop"]
    if experiment.benchmark.dump_db_at_stop:
        command += ["--dump"]
    log(f"[STOP CLUSTER] {command}")
    process.run2(command, verbose=VERBOSE)


def cluster_status(experiment: Experiment) -> Dict:
    # 1691487518.290895:
    #   7000:
    #       master_port: null
    #       role: master
    #       hash_slots: [0, 5400]
    command = [
        *_cluster_command(experiment),
        "Status",
        "--output-file",
        os.path.join(
            _cluster_work_dir(experiment), experiment.redis_cluster.status_file
        ),
    ]

    process.run2(command, verbose=VERBOSE)
    with open(
        os.path.join(
            _cluster_work_dir(experiment), experiment.redis_cluster.status_file
        )
    ) as f:
        return yaml.safe_load(f)


def load_data(experiment: Experiment, ports: List[int], cluster_status: Dict) -> None:
    if experiment.data.max_memory_usage_gb <= 0:
        return
    max_keyspace = calculate_maximum_keyspace(
        cluster_status, experiment.data.max_memory_usage_gb
    )
    # We use max_keyspace as max_keysapce and also as number of requests.
    # As we set --incr-key-only, every single key is added to the cluster.
    command = (
        process.memtier_benchmark_cmd(
            get_path_from_root(["memtier_benchmark"]),
            max_keyspace,
            experiment.data.data_size,
            1,
            20,
            ports[0],
            "load-",
        )
        + process.memtier_requests_addition(
            int(
                max_keyspace / 20
            )  # Requests PER client PER thread. This should be really the total number of requests!
        )
        + process.memtier_load_addition()
        + process.memtier_no_latency_recording()
        + process.memtier_incr_key_addition()  # Every key should be created..
    )
    # if experiment.benchmark.incr_key_only:
    # command += process.memtier_incr_key_addition()
    log(f"[DATA - Load Data] {command}")
    process.run2(command, verbose=VERBOSE)

    sleep_time = max(len(ports) / 3, 5)
    print(f"Cluster is loaded.. We start in {sleep_time}s..")
    time.sleep(sleep_time)


def failover(
    experiment: Experiment,
    experiment_specific_benchmark_result_dir: str,
    failover_done_notification: Optional[str],
) -> subprocess.Popen:
    experiment.failover.output_file = os.path.join(
        experiment_specific_benchmark_result_dir, "failover.yaml"
    )
    command = [
        *_cluster_command(experiment),
        "Failover",
        "--bin",
        experiment.build.bin_dir,
        "--output-file",
        f"{experiment.failover.output_file}",
    ]
    if experiment.failover.snapshot:
        command += ["--snapshot"]
    if failover_done_notification:
        command += ["--delete-file-on-finish", f"{failover_done_notification}"]
    if experiment.failover.skip_failover:
        command += ["--skip-failover"]
    if experiment.failover.skip_restart_master:
        command += ["--skip-restart-master"]
    if experiment.failover.skip_restart_replica:
        command += ["--skip-restart-replica"]
    if experiment.failover.all_nodes:
        command += ["--all-nodes"]

    log(f"[FAILOVER] {command}")
    return process.run2_async(command, verbose=VERBOSE)


def apply_patch(experiment: Experiment) -> None:
    if len(experiment.patch_generation.patch_paths) == 0:
        return

    command = [
        *_cluster_command(experiment),
        "Patch",
        "--sleep-between-versions",
        f"{experiment.patch.sleep_between_versions}",
        "--sleep-bias",
        f"{experiment.patch.sleep_bias}",
        "--method",
        experiment.patch.method,
        "--distribution",
        experiment.patch.distribution,
        "--patches",
        *[
            f"../patches/{os.path.basename(p)}"
            for p in experiment.patch_generation.patch_paths
        ],
    ]
    if experiment.patch.reverse_version:
        command += ["--reverse-version"]
    log(f"[APPLY PATCH] {command}")
    process.run2(command, verbose=VERBOSE)


def _get_masters_replicas(cluster_status: Dict) -> (int, int):
    initial_state = cluster_status[sorted(cluster_status.keys())[0]]
    masters = [
        port
        for port, value_dict in initial_state.items()
        if value_dict["role"] == "master"
    ]
    replicas = [
        port
        for port, value_dict in initial_state.items()
        if value_dict["role"] == "slave"
    ]
    return (masters, replicas)


def calculate_maximum_keyspace(cluster_status: Dict, max_mem_usage_gb: int) -> int:
    masters, replicas = _get_masters_replicas(cluster_status)
    replicas_per_master = len(replicas) / len(masters)

    # Data size of 4096 Bytes produces 5200 as KEY/VALUE data (see MEMORY USAGE <KEY> command)

    # +1 because of the master also holds data. Replicas replicates it
    return int(
        ((max_mem_usage_gb * 1024 * 1024 * 1024) / 5200.0) / (1.0 + replicas_per_master)
    )


def benchmark(
    experiment: Experiment,
    cluster_proc: subprocess.Popen,
    ports: List[int],
    cluster_status: Dict,
    experiment_specific_benchmark_result_dir: str,
) -> str:
    recent_cluster_state = cluster_status[sorted(cluster_status.keys())[-1]]
    # Port, Latencies File, Benchmark Log File, Key Tag
    benchmark_data: [Tuple[int, str, Any, str]] = None
    if experiment.benchmark.per_master:
        masters, *_ = _get_masters_replicas(cluster_status)
        benchmark_data = [
            (
                port,
                os.path.join(
                    experiment_specific_benchmark_result_dir, f"latencies.{port}.csv"
                ),
                open(
                    os.path.join(
                        experiment_specific_benchmark_result_dir,
                        f"benchmark_log.{port}",
                    ),
                    "w",
                ),
                crc16.find_hashslot(recent_cluster_state[port]["hash_slots"], ""),
            )
            for port in masters
        ]
    else:
        benchmark_data = [
            (
                os.path.join(experiment_specific_benchmark_result_dir, "latencies.csv"),
                open(
                    os.path.join(
                        experiment_specific_benchmark_result_dir, "benchmark_log"
                    ),
                    "w",
                ),
                None,
            )
        ]

    benchmark_commands = []
    for port, latencies_file, log_file, key_tag in benchmark_data:
        allowed_ports = []
        if key_tag:
            allowed_ports = [port] + [
                replica_port
                for replica_port, data in recent_cluster_state.items()
                if data["master_port"] == port
            ]

        recent_cluster_state
        cmd = (
            process.taskset_cmd(
                experiment.benchmark.taskset_start,
                experiment.benchmark.taskset_end,
                experiment.benchmark.taskset_step,
            )
            + process.memtier_benchmark_cmd(
                get_path_from_root(["memtier_benchmark"]),
                calculate_maximum_keyspace(
                    cluster_status, (experiment.benchmark.max_memory_usage_gb / (len(masters) if experiment.benchmark.per_master else 1))
                ),
                experiment.benchmark.data_size,
                experiment.benchmark.clients,
                experiment.benchmark.threads,
                port,
            )
            + process.memtier_command_addition(experiment.benchmark.name)
            + process.memtier_result_addition(latencies_file)
            + process.memtier_key_tag_addition(key_tag, allowed_ports)
        ) + (
            process.memtier_requests_addition(experiment.benchmark.requests)
            if experiment.benchmark.requests
            else process.memtier_time_addition(experiment.benchmark.time_s)
        )

        if experiment.benchmark.incr_key_only:
            cmd += process.memtier_incr_key_addition()
        if experiment.benchmark.no_latency_recording:
            cmd += process.memtier_no_latency_recording()

        benchmark_commands.append((cmd, log_file))

    # Start Benchmark

    # We need this to run in the background because its exeuction must be delayed
    # using the idle before value.
    def exec_benchmark_in_background():
        log(f"[BENCHMARK - Sleep] {experiment.benchmark.idle_before}")
        time.sleep(experiment.benchmark.idle_before)
        if experiment.benchmark.time_s > 0:
            for benchmark_command, *_ in benchmark_commands:
                log(f"[BENCHMARK - Benchmark Command] {benchmark_command}")
            benchmark_procs = [
                process.run_async(
                    benchmark_command,
                    # exception_on_error=False,
                    cwd=experiment.benchmark.dir,
                    stdout_pipe=benchmark_log_file,
                    stderr_pipe=benchmark_log_file,
                )
                for benchmark_command, benchmark_log_file in benchmark_commands
            ]
            return benchmark_procs
        return [None]

    if "network" in experiment.commit.split("-"):
        # Enable network recording before we start the benchmark!
        command = [
            *_cluster_command(experiment),
            "NetworkRecorder",
        ]
        log(f"[ENABLE NETWORK RECORDING] {command}")
        process.run2(command, verbose=VERBOSE)
        os.environ["ENABLE_NETWORK_RECORDING"] = "1"

    benchmark_th_pool = ThreadPool(processes=1)
    benchmark_th = benchmark_th_pool.apply_async(exec_benchmark_in_background)
    experiment.benchmark.framework_start_time = time.time()

    cluster_procs = [cluster_proc]
    if experiment.patch and experiment.patch.apply_patch_after_s:
        time.sleep(experiment.patch.apply_patch_after_s)
        apply_patch(experiment)
    if experiment.failover and experiment.failover.failover_after_s:
        time.sleep(experiment.failover.failover_after_s)

        failover_done_notification = tempfile.NamedTemporaryFile()
        failover_proc = failover(
            experiment,
            experiment_specific_benchmark_result_dir,
            failover_done_notification.name,
        )
        while os.path.exists(failover_done_notification.name):
            # Wait until failover is done!
            time.sleep(0.1)
        # Just call this to not get some strange excepetion...
        try:
            failover_done_notification.close()
        except FileNotFoundError:
            pass

        if (
            experiment.failover.skip_restart_master
            and experiment.failover.skip_restart_replica
        ):
            # No new process is started. We don't need to add the proc because it is gone
            pass
        else:
            if experiment.failover.all_nodes:
                # Original process is not running anymore after failover.
                cluster_procs = [failover_proc]
            else:
                # We do not failover all nodes; So both processes are running after failover
                cluster_procs.append(failover_proc)

    print("Wait benchmark proc")
    benchmark_procs = benchmark_th.get()
    for benchmark_proc in benchmark_procs:
        if benchmark_proc is not None:
            benchmark_proc.wait()

    print("Wait end benchmark proc")
    for _, benchmark_log_file in benchmark_commands:
        benchmark_log_file.close()

    experiment.benchmark.framework_end_time = time.time()
    experiment.success = False
    experiment.db_failure = False
    experiment.benchmark_failure = False
    experiment.benchmark_timeout = False

    result: str
    if all(
        (benchmark_proc is None or benchmark_proc.returncode == 0)
        for benchmark_proc in benchmark_procs
    ):
        if all(c.poll() is None for c in cluster_procs):
            # Cluster still alive..
            result = "SUCCESS"
            experiment.success = True
        else:
            # Cluster failure
            result = "DB_FAILURE"
            experiment.db_failure = True
    else:
        if any(benchmark_proc.returncode == 124 for benchmark_proc in benchmark_procs):
            result = "BENCHMARK_TIMEOUT"
            experiment.benchmark_timeout = True
        else:
            result = "BENCHMARK_FAILURE"
            experiment.benchmark_failure = True

    # Store benchmark result
    touch(os.path.join(experiment_specific_benchmark_result_dir, result))
    # Store experiment object
    with open(
        os.path.join(
            experiment_specific_benchmark_result_dir, "experiment_objects.yaml"
        ),
        "w",
    ) as f:
        yaml.dump(experiment.to_dict(), f, sort_keys=False)

    # Store cluster
    return experiment_specific_benchmark_result_dir


def store_metadata(experiment: Experiment, experiment_result_dir: str) -> None:
    output = experiment.benchmark.output

    if experiment.benchmark.dump_db_at_stop:
        shutil.copytree(
            _cluster_work_dir(experiment),
            os.path.join(experiment_result_dir, "cluster"),
        )
    else:
        shutil.copytree(
            _cluster_work_dir(experiment),
            os.path.join(experiment_result_dir, "cluster"),
            ignore=shutil.ignore_patterns("*.rdb"),
        )


def notify(message: str) -> None:
    command = [get_script("notify"), message]

    log(f"[NOTIFY] {command}")
    process.run2(command, verbose=VERBOSE, exception_on_error=False)


def prepare_system():
    # Prepare system for benchmark
    prepare_benchmark_command = [get_script("prepare_benchmark")]
    log(f"[EXPERIMENT - Prepare] {prepare_benchmark_command}")
    process.run2(prepare_benchmark_command, verbose=VERBOSE, exception_on_error=False)


def teardown_system():
    # Teardown system after benchmarking
    teardown_benchmark_command = [get_script("teardown_benchmark")]
    log(f"[BENCHMARK - Teardown] {teardown_benchmark_command}")
    process.run2(teardown_benchmark_command, verbose=VERBOSE, exception_on_error=False)


def execute(experiment: Experiment) -> None:
    # Prepare system before we create a cluster
    prepare_system()
    # 1. Create cluster (prepares directory etc.)
    prepare_cluster(experiment)

    # 2. Compile binary
    # We use the binary of patch generation, as patches are compiled exactly for this binary.
    # If we do not have a binary of patch application, we compile our own binary...
    if not create_patches(experiment):
        build(experiment)

    # Get next free index for a result directory
    specific_experiment_dir_counter = 0
    if os.path.exists(experiment.benchmark.output):
        while True:
            if any(
                file.startswith(str(specific_experiment_dir_counter))
                for file in os.listdir(experiment.benchmark.output)
            ):
                specific_experiment_dir_counter += 1
            else:
                break

    experiment_specific_benchmark_result_dir = os.path.join(
        experiment.benchmark.output, (f"{specific_experiment_dir_counter}")
    )
    os.makedirs(experiment_specific_benchmark_result_dir)

    # 4. Start cluster
    cluster_proc = start_cluster(experiment, experiment_specific_benchmark_result_dir)

    # 5. Get status of cluster (master/replicas)
    status = cluster_status(experiment)

    # Get one master port
    ports = _cluster_status_master_ports(status)

    # 6. Load data into cluster
    # Use the first master port
    load_data(experiment, ports, status)

    # 7. Benchmark
    experiment_result_dir = benchmark(
        experiment,
        cluster_proc,
        ports,
        status,
        experiment_specific_benchmark_result_dir,
    )

    print("Benchmark done")

    # 8. Stop cluster
    stop_cluster(experiment)

    # Teardown system after benchmarking
    teardown_system()

    # 9. Store some meta data
    store_metadata(experiment, experiment_result_dir)

    write_log(os.path.join(experiment.benchmark.output, "log"))


def parse_args(input_args: List[str]) -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("-v", "--verbose", help="Verbose output", action="store_true")
    parser.add_argument(
        "--dry-run", help="Dry-Run. Do not perform any action.", action="store_true"
    )
    parser.add_argument("--config", help="The configuration file", required=True)
    parser.add_argument(
        "--overwrite",
        help="Allows to overwrite settings ot the configuration file. Format: path=value. "
        "Example: build.output='new-output'",
        nargs="+",
        required=False,
    )
    return parser.parse_args(input_args)


def main(input_args: List[str]) -> None:
    args = parse_args(input_args)

    global VERBOSE
    VERBOSE = args.verbose

    experiments: List[Experiment] = parse_config(args.config, args.overwrite)

    print(f"In total {len(experiments)} experiments will be executed.")

    def prepare_experiment_directories(experiment: Experiment) -> None:
        experiment.build.output = os.path.join(
            experiment.build.output, experiment.commit
        )
        experiment.benchmark.output = os.path.join(
            experiment.benchmark.output,
            f"{experiment.commit}-{experiment.benchmark.output_name}",
        )

    counter = 1
    for experiment in experiments:
        prepare_experiment_directories(experiment)
        log(yaml.safe_dump(experiment.to_dict(), sort_keys=False))
        log(f"Executing experiment {counter}/{len(experiments)}")
        if not args.dry_run:
            notify(
                f"{counter}/{len(experiments)} {experiment.benchmark.output_name} {experiment.benchmark.output_name} {experiment.commit} Start"  # noqa: E501
            )
            try:
                execute(experiment)
            except Exception as e:
                notify(
                    f"{counter}/{len(experiments)} {experiment.benchmark.output_name} {experiment.benchmark.output_name} {experiment.commit} ERROR"  # noqa: E501
                )
                raise e
            notify(
                f"{counter}/{len(experiments)} {experiment.benchmark.output_name} {experiment.benchmark.output_name} {experiment.commit} End"  # noqa: E501
            )
        counter += 1


if __name__ == "__main__":
    main(sys.argv[1:])
