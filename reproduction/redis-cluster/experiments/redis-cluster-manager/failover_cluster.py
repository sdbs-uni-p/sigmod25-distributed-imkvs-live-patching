#!/usr/bin/env python3
import math
import os
import subprocess
import sys
import time
from argparse import ArgumentParser
from contextlib import contextmanager
from itertools import accumulate
from typing import Any, Callable, Dict, List, Optional

import psutil
import redis
import yaml
from redis.exceptions import BusyLoadingError, ConnectionError, ResponseError

import cluster_status as status
import crc16
import model
import start_cluster as start
import utils
from model import Cluster, Node

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

OUTPUT = None

REDIS_RDB_SAVE_B_SEC = 350 * 1024 * 1024  # 350 MB


class LogEntry:
    def __init__(self, name: str, node: Node):
        self.name: str = name
        self.port: int = node.port

        self.start_time: float = time.time()
        self.end_time: float

        self.actions: List[Tuple[float, str]] = []

    def log_action(self, action: str) -> None:
        self.actions.append((time.time(), action))

    def end(self) -> None:
        self.end_time = time.time()

    def to_dict(self) -> str:
        actions_with_latency = list(
            accumulate(
                self.actions,
                func=lambda x, y: (*y, (y[0] - x[0]) * 1000),
                initial=(self.start_time, ""),
            )
        )[
            1:
        ]  # Remove first entry as this is the initial value

        return {
            "name": self.name,
            "port": self.port,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": (self.end_time - self.start_time) * 1000,
            "actions": [
                {"action": a, "action_time": t, "action_duration_ms": l}
                for t, a, l in actions_with_latency
            ],
        }


def log(log_entry: LogEntry):
    yaml.dump({time.time(): log_entry.to_dict()}, OUTPUT)


def retry_on_error(fn: Callable, sleep=0.5, break_on_exception: List = None) -> Any:
    while True:
        try:
            return fn()
        except (BusyLoadingError, ConnectionError) as e:
            if break_on_exception and type(e) in break_on_exception:
                break
            time.sleep(sleep)


def failover_node(node: Node, force_after_tries: int) -> None:
    con = redis.Redis(host="127.0.0.1", port=node.port)
    # retry_on_error(lambda: con.execute_command("CLUSTER FAILOVER"))

    failover_counter = 0
    log_entry = LogEntry("failover", node)
    while retry_on_error(lambda: status.node_status(node, con)["role"]) == "slave":
        if failover_counter > force_after_tries:
            log_entry.log_action("failover takeover start")
            retry_on_error(
                lambda: con.execute_command("CLUSTER FAILOVER TAKEOVER"),
                break_on_exception=[ResponseError],
            )
            log_entry.log_action("failover takeover end")
        if failover_counter == force_after_tries:
            log_entry.log_action("failover force start")
            retry_on_error(
                lambda: con.execute_command("CLUSTER FAILOVER FORCE"),
                break_on_exception=[ResponseError],
            )
            log_entry.log_action("failover force end")
        else:
            log_entry.log_action("failover start")
            retry_on_error(
                lambda: con.execute_command("CLUSTER FAILOVER"),
                break_on_exception=[ResponseError],
            )
            log_entry.log_action("failover end")

        # CLUSTER_MF_TIMEOUT in cluster.h of REDIS is set to 5000ms hardcoded. We use 100ms as buffer
        failover_timeout = 5 + 0.5
        sleep_between_checks = 0.5
        for _ in range(math.ceil(failover_timeout / sleep_between_checks)):
            if (
                retry_on_error(lambda: status.node_status(node, con)["role"])
                == "master"
            ):
                break
            time.sleep(sleep_between_checks)

        failover_counter += 1

    log_entry.end()
    log(log_entry)

    con.close()


@contextmanager
def set_repl_backlog_size(node: Node, snapshot: bool) -> None:
    if not snapshot:
        # Ignore the commands below... We do not change the repl-backlog-size!
        try:
            yield
        finally:
            pass
        return

    con = redis.Redis(host="127.0.0.1", port=node.port)
    size = 30 * 1024 * 1024 * 1024  # 30 GiB
    con.execute_command(f"CONFIG SET repl-backlog-size {size}")
    # Give the backlog 1s time to get filled...
    time.sleep(1)

    try:
        yield
    finally:
        # 1mib = 1048576
        con.execute_command("CONFIG SET repl-backlog-size 1048576")
        con.close()


def off_on_node(
    node: Node, master: Node, bin_dir: str, snapshot: bool
) -> subprocess.Popen:
    with open(node.pid_file()) as f:
        pid = [int(l.strip()) for l in f.readlines() if len(l.strip()) > 0][0]
    proc = psutil.Process(pid)

    master_con = redis.Redis(host="127.0.0.1", port=master.port)
    master_hash_slots = status.node_hash_slots(master, master_con)
    cluster_tag = crc16.find_hashslot(master_hash_slots)

    log_entry = LogEntry("restart", node)
    log_entry.log_action("connection start")
    con = redis.Redis(host="127.0.0.1", port=node.port)
    log_entry.log_action("connection end")

    # if snapshot:
    #    log_entry.log_action("bgsave start")
    #    con.execute_command("config set appendonly yes")
    #    time.sleep(0.5)
    #    while con.info("persistence")["aof_rewrite_in_progress"] == 1:
    #        time.sleep(0.5)
    #    log_entry.log_action("bgsave end")

    log_entry.log_action("shutdown start")
    retry_on_error(lambda: con.shutdown(save=snapshot))
    log_entry.log_action("shutdown end")
    con.close()

    # Patch offline (we don't exchange the binary..)

    # Wait until server is shutdown
    try:
        while psutil.pid_exists(pid) and proc.status() != psutil.STATUS_ZOMBIE:
            # PID exists. Redis is still alive...
            time.sleep(0.01)  # To lower cpu consumption
    except psutil.NoSuchProcess:
        # Process is gone
        pass
    log_entry.log_action("shutdown complete")

    def start_node():
        # Start node again
        log_entry.log_action("start start")
        start_proc = start.start_node(node, bin_dir)
        log_entry.log_action("start end")
        con = retry_on_error(lambda: redis.Redis(host="127.0.0.1", port=node.port))
        while retry_on_error(lambda: status.node_status(node, con)["role"]) != "slave":
            time.sleep(0.5)
            pass  # Just wait until node is up and a slave..
        log_entry.log_action("node process started")

        log_entry.log_action("node sync start")
        while (
            retry_on_error(lambda: status.node_status(node, con)["master_link_status"])
            != "up"
        ):
            time.sleep(0.5)
            pass
        while (
            retry_on_error(
                lambda: status.node_status(node, con)["master_sync_in_progress"]
            )
            != 0
        ):
            time.sleep(0.5)
            pass
        log_entry.log_action("node sync end")

        log_entry.log_action("node catchup start")
        con.readonly()
        while True:
            current_time = time.time()
            set_cmd = f"SET sync.time:{{{cluster_tag}}} {current_time}"
            get_cmd = f"GET sync.time:{{{cluster_tag}}}"
            master_con.execute_command(set_cmd)
            replica_time = con.execute_command(get_cmd)
            if replica_time == None:
                continue
            replica_time = float(replica_time)
            if current_time - replica_time <= 1:
                # less than 10s offset between master and replica. This may be ok..
                break
            time.sleep(0.5)
        log_entry.log_action("node catchup end")

        return start_proc

    start_proc = start_node()

    log_entry.end()
    log(log_entry)

    return start_proc


def failover_cluster(
    cluster: Cluster,
    bin_dir: str,
    force_after_tries: int,
    all_nodes: bool,
    delete_file_on_finish: Optional[str],
    skip_restart_master: bool,
    skip_failover: bool,
    skip_restart_replica: bool,
    snapshot: bool,
) -> None:
    # Default is, e.g., 1024, which is too less when creating 2k redis instances.
    os.system(f"ulimit -n {len(cluster.nodes)*3}")

    procs: List[Tuple[subprocess.Popen, Node]] = []
    status_nodes = [(node, status.node_status(node)) for node in cluster.nodes]

    groups: List[List[Node, List[Node]]] = [
        [
            master_node,
            [
                replica_node
                for replica_node, replica_status in status_nodes
                if replica_status["master_port"] == master_node.port
            ],
        ]
        for master_node, master_status in status_nodes
        if master_status["role"] == "master"
    ]
    # groups: List[Node, List[Node]] = [master_node,
    #        [replica_node for repilca_node, replica_status in status_nodes if repilca_status["master_port"] == master_node.port]
    #        for master_node, master_status in status_nodes if master_status["role"] == "master"]

    if not all_nodes:
        groups = [groups[0]]

    def failover_group(master: Node, replicas: List[Node]):
        if not skip_restart_replica:
            with set_repl_backlog_size(master, snapshot):
                for replica in replicas:
                    proc = off_on_node(replica, master, bin_dir, snapshot)
                    time.sleep(20)
                    procs.append((proc, replica))

        if not skip_failover:
            failover_node(replicas[0], force_after_tries)
            time.sleep(20)

        if not skip_restart_master:
            # 2.2 Shutdown master node
            with set_repl_backlog_size(replicas[0], snapshot):
                proc = off_on_node(master, replicas[0], bin_dir, snapshot)
                time.sleep(20)
                procs.append((proc, master))

    for master, replicas in groups:
        failover_group(master, replicas)

    print("Failover done. Now we wait for its shutdown ;-)")
    if delete_file_on_finish:
        os.remove(delete_file_on_finish)
    start.wait_end_nodes(procs)


def parser_args(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--bin", help="The path to the binary directory.", required=True
    )
    parser.add_argument(
        "--force-after-tries", help="Force failover after X tries.", default=10
    )
    parser.add_argument(
        "--output-file", help="Print all information to a file.", default=None
    )
    parser.add_argument(
        "--all-nodes",
        help="Perform the failover scenario for *all* nodes. Otherwise, it is performed for *one* master and its replica nodes.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--skip-restart-master",
        help="Skip restart master",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--skip-failover",
        help="Skip failover.",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--skip-restart-replica",
        help="Skip replica restart",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--delete-file-on-finish",
        type=str,
        help="Once the failover process is done, the file will be deleted (used for communication/notifiction)",
    )
    parser.add_argument(
        "--snapshot", action="store_true", help="Perform a snapshot before shutdown?"
    )


def main(cluster: Cluster, args: Dict[str, Any]) -> None:
    bin_dir = utils.abs_path(args.bin)

    global OUTPUT
    if args.output_file:
        OUTPUT = open(args.output_file, "a")
    else:
        OUTPUT = sys.stdout

    failover_cluster(
        cluster,
        bin_dir,
        args.force_after_tries,
        args.all_nodes,
        args.delete_file_on_finish,
        args.skip_restart_master,
        args.skip_failover,
        args.skip_restart_replica,
        args.snapshot,
    )
