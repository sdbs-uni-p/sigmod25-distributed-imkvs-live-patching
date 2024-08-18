#!/usr/bin/env python3

import enum
import os
import random
import time
from argparse import ArgumentParser
from typing import Any, Dict, List

import redis
import yaml

import cluster_status as status
import model
import utils
from model import Cluster, Node

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
random.seed(42)


class PatchMethod(enum.Enum):
    LAZY = "lazy"
    LAZY_SYNC = "lazy-sync"
    EAGER = "eager"
    EAGER_SYNC = "eager-sync"

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return str(self)

    @staticmethod
    def argparse(s):
        try:
            return PatchMethod[s.upper()]
        except KeyError:
            return s


class PatchDistribution(enum.Enum):
    # All nodes
    ROUND_ROBIN = 0
    # Master nodes only
    MASTER_ROUND_ROBIN = 1
    # Replica nodes only
    REPLICA_ROUND_ROBIN = 2
    # Single master node
    MASTER_SINGLE = 3

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return str(self)

    @staticmethod
    def argparse(s):
        try:
            return PatchDistribution[s.upper()]
        except KeyError:
            return s


def patch_cluster(
    cluster: Cluster,
    patches: List[str],
    distribution: PatchDistribution,
    method: PatchMethod,
    reverse_version: bool,
    start_version: int,
    sleep_between_versions: float,
    sleep_bias: float,
) -> None:
    if len(patches) == 0:
        return

    node_filter: Callable[[Node], bool] = None
    nodes: List[Node] = None
    if distribution == PatchDistribution.ROUND_ROBIN:
        nodes = cluster.nodes
    if distribution == PatchDistribution.MASTER_SINGLE:
        nodes = [
            node
            for node in cluster.nodes
            if status.node_status(node)["role"] == "master"
        ]
        nodes = [nodes[0]]
    if distribution == PatchDistribution.MASTER_ROUND_ROBIN:
        nodes = [
            node
            for node in cluster.nodes
            if status.node_status(node)["role"] == "master"
        ]
    if distribution == PatchDistribution.REPLICA_ROUND_ROBIN:
        nodes = [
            node
            for node in cluster.nodes
            if status.node_status(node)["role"] == "slave"
        ]

    version = start_version
    if reverse_version:
        version = len(patches) + start_version - 1
        patches.reverse()

    def next_version() -> None:
        nonlocal version
        if reverse_version:
            version -= 1
        else:
            version += 1

    node_connections: List[redis.Redis] = [
        redis.Redis(host="127.0.0.1", port=n.port) for n in nodes
    ]
    for idx, patch in enumerate(patches):
        conn: redis.Redis = node_connections[idx % len(nodes)]
        conn.execute_command(f"PATCH {method.value} {version} {patch}")
        next_version()
        time.sleep(sleep_between_versions + random.uniform(-sleep_bias, sleep_bias))

    for con in node_connections:
        con.close()


def parser_args(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--patches", nargs="+", help="A list of patches to apply.", required=True
    )
    parser.add_argument(
        "--method", type=PatchMethod.argparse, choices=list(PatchMethod), required=True
    )
    parser.add_argument(
        "--distribution",
        type=PatchDistribution.argparse,
        choices=list(PatchDistribution),
        required=True,
    )
    parser.add_argument(
        "--reverse-version",
        action="store_true",
        help="Use a reverse version, i.e. set patch versions from X to 1",
    )
    parser.add_argument("--start-version", default=1, help="Start with this version.")
    parser.add_argument(
        "--sleep-between-versions",
        default=0.0,
        type=float,
        help="The time this thread sleeps between patch application.",
    )
    parser.add_argument(
        "--sleep-bias",
        default=0.0,
        type=float,
        help="This time is added or subtractet to '--sleep-between-versions'",
    )


def main(cluster: Cluster, args: Dict[str, Any]) -> None:
    patch_cluster(
        cluster,
        args.patches,
        args.distribution,
        args.method,
        args.reverse_version,
        args.start_version,
        args.sleep_between_versions,
        args.sleep_bias,
    )
