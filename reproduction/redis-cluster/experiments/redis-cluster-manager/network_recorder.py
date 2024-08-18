#!/usr/bin/env python3

import os
from argparse import ArgumentParser
from typing import Any, Dict

import redis
import yaml

import cluster_status as status
import model
import utils
from model import Cluster, Node

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def record_network(
    cluster: Cluster,
) -> None:
    for node in cluster.nodes:
        conn = redis.Redis(host="127.0.0.1", port=node.port)
        conn.execute_command(f"networkcount")
        conn.close()


def parser_args(parser: ArgumentParser) -> None:
    pass


def main(cluster: Cluster, args: Dict[str, Any]) -> None:
    record_network(cluster)
