#!/usr/bin/env python3

import os
import time
from argparse import ArgumentParser
from typing import Any, Dict

import redis
import yaml

import model
import utils
from model import Cluster, Node

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def node_hash_slots(node: Node, con: redis.Redis = None) -> (int, int):
    close = False
    if not con:
        con = redis.Redis(host="127.0.0.1", port=node.port, decode_responses=True)
        close = True

    hash_slots = None
    cluster_node_info = con.execute_command("CLUSTER NODES")[f"127.0.0.1:{node.port}"]
    if len(cluster_node_info['slots']) > 0:
        hash_slots = [int(slot) for slot in cluster_node_info["slots"][0]]

    if close:
        con.close()
    return hash_slots


def node_status(node: Node, con: redis.Redis = None) -> Dict[str, Any]:
    close = False
    if not con:
        con = redis.Redis(host="127.0.0.1", port=node.port, decode_responses=True)
        close = True

    info = con.info("replication")
    role = info["role"]
    master_link_status = info.get("master_link_status")
    master_port = info.get("master_port")
    master_sync_in_progress = info.get("master_sync_in_progress")
    hash_slots = []
    if role == 'master':
        hash_slots = node_hash_slots(node, con)

    if close:
        con.close()
    return {
        "role": role,
        "master_port": master_port,
        "master_link_status": master_link_status,
        "master_sync_in_progress": master_sync_in_progress,
        "hash_slots": hash_slots,
    }


def cluster_status(cluster: Cluster, output_file: str) -> None:
    info = {node.port: node_status(node) for node in cluster.nodes}

    utils.dump_yaml_object({time.time(): info}, output_file, append=True)


def parser_args(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--output-file", help="The path to the output file.", required=True
    )


def main(cluster: Cluster, args: Dict[str, Any]) -> None:
    cluster_status(cluster, args.output_file)
