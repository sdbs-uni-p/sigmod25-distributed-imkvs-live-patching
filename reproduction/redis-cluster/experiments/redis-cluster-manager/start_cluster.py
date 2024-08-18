#!/usr/bin/env python3

import multiprocessing
import os
import subprocess
import time
from argparse import ArgumentParser
from typing import Any, Dict, List, Optional, Tuple

import yaml

import model
import utils
from model import Cluster, Node

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def start_node(node: Node, bin_dir: str) -> subprocess.Popen:
    cmd = []
    if node.taskset_start is not None:
        cmd += [
            "taskset",
            "-c",
            f"{node.taskset_start}-{node.taskset_end}:{node.taskset_step}",
        ]
    cmd += [os.path.join(bin_dir, "src", "redis-server"), node.config()]

    def set_output(f: Optional[str]) -> Optional[int]:
        if f is None:
            return None
        elif f == "/dev/null":
            return subprocess.DEVNULL
        return open(f, "a") if node.direct_pipe else subprocess.PIPE

    stdout = set_output(node.log_stdout_file())
    stderr = set_output(node.log_stderr_file())

    print(cmd)
    return subprocess.Popen(cmd, cwd=node.directory(), stdout=stdout, stderr=stderr)


def wait_end_nodes(procs: List[Tuple[subprocess.Popen, Node]]) -> None:
    def write_log(dst: str, content: bytes) -> None:
        if dst and not node.direct_pipe:
            with open(dst, "ab") as f:
                f.write(content)

    for proc, node in procs:
        proc.wait()
        stdout, stderr = proc.communicate()
        write_log(node.log_stdout_file(), stdout)
        write_log(node.log_stderr_file(), stderr)


def start_cluster(cluster: Cluster, bin_dir: str, join: bool) -> None:
    # Default is, e.g., 1024, which is too less when creating 2k redis instances.
    os.system(f"ulimit -n {len(cluster.nodes)*3}")
    if any([os.path.exists(node.pid_file()) for node in cluster.nodes]):
        print(
            "Some cluster instance is already running... Please shutdown before starting a new cluster!"
        )
        exit(1)

    cpu_cores = [node.taskset_start for node in cluster.nodes] + [
        node.taskset_end for node in cluster.nodes
    ]
    if min(cpu_cores) < 0 or max(cpu_cores) >= multiprocessing.cpu_count():
        print(
            f"Invalid taskset. Min. taskset is {min(cpu_cores)}; Max. taskset is {max(cpu_cores)}."
        )
        print(f"But taskset has the range of 0 - {multiprocessing.cpu_count() - 1}.")
        exit(1)

    procs: List[Tuple[subprocess.Popen, Node]] = [
        (start_node(node, bin_dir), node) for node in cluster.nodes
    ]

    # Nodes are started. Create a cluster
    def join_cluster() -> subprocess.Popen:
        cmd = (
            [os.path.join(bin_dir, "src", "redis-cli"), "--cluster", "create"]
            + [f"127.0.0.1:{node.port}" for node in cluster.nodes]
            + ["--cluster-replicas", f"{cluster.replicas_per_master}", "--cluster-yes"]
        )
        print(cmd)
        return subprocess.Popen(cmd, cwd=cluster.directory())

    if join:
        time.sleep(2)
        join_proc = join_cluster()
        join_proc.wait()
        print("Cluster created and joined. Now we wait for its shutdown ;-)")

    wait_end_nodes(procs)


def parser_args(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--bin", help="The path to the binary directory.", required=True
    )
    parser.add_argument(
        "--join", help="Create the cluster, i.e. join the nodes", action="store_true"
    )


def main(cluster: Cluster, args: Dict[str, Any]) -> None:
    bin_dir = utils.abs_path(args.bin)

    start_cluster(cluster, bin_dir, args.join)
