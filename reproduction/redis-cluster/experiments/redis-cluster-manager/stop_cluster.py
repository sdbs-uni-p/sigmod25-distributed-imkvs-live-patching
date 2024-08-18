#!/usr/bin/env python3
import os
from argparse import ArgumentParser
from typing import Any, Dict

import redis
import yaml

import model
import utils
from model import Cluster, Node

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def stop_cluster(cluster: Cluster, dump: bool) -> None:
    # def get_pid(node: Node) -> Optional[str]:
    #     try:
    #         with open(node.pid_file()) as f:
    #             return int(f.read().strip())
    #     except FileNotFoundError:
    #         return None

    # def send_signal(pid: int, signal: int) -> bool:
    #     try:
    #         os.kill(pid, signal)
    #         return True
    #     except OSError:
    #         return False

    # def stop(pid: int) -> bool:
    #     return send_signal(pid, signal.SIGINT)

    # def kill(pid: int) -> bool:
    #     return send_signal(pid, signal.SIGKILL)

    # def pid_alive(pid: int) -> bool:
    #     return send_signal(pid, 0)

    def shutdown(node: Node) -> bool:
        print(f"Shutdown {node.port}")
        con = redis.Redis(host="127.0.0.1", port=node.port)
        con.shutdown(save=dump)
        # con.close()
        print(f"Shutdown {node.port} done")

    for node in cluster.nodes:
        shutdown(node)

    # pids = [get_pid(node) for node in cluster.nodes if get_pid(node) is not None]
    # for pid in pids:
    #     stop(pid)

    # any_node_alive: bool
    # sleep = 0.1
    # for _ in range(math.ceil(delay_before_kill / sleep)):
    #     any_node_alive = any([pid_alive(pid) for pid in pids])
    #     if not any_node_alive:
    #         # No more node is alive.
    #         break
    #     time.sleep(sleep)

    # if any_node_alive:
    #     # Node is still alive.. We kill it now!
    #     for pid in pids:
    #         kill(pid)

    # # Cleanup all pid files...
    # for pid_file in [
    #     node.pid_file() for node in cluster.nodes if os.path.exists(node.pid_file())
    # ]:
    #     os.remove(pid_file)


def parser_args(parser: ArgumentParser) -> None:
    # parser.add_argument(
    #    "--delay-before-kill",
    #    help="Shutdown is sent to each node. If the node has not shutdown sucessfully, it is killed after 'X' seconds",
    #    default=60,
    # )
    parser.add_argument("--dump", help="Dump data on shutdown", action="store_true")


def main(cluster: Cluster, args: Dict[str, Any]) -> None:
    stop_cluster(cluster, args.dump)
