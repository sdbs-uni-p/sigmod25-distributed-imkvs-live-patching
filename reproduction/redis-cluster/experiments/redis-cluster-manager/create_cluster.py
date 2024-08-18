#!/usr/bin/env python3

import os
from argparse import ArgumentParser
from typing import Any, Dict, List, TypeVar

import yaml

import model
import utils
from model import Cluster, Node

T = TypeVar("T")

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def create_cluster(cluster: Cluster, redis_conf_template: str) -> None:
    utils.clean_dir(cluster.directory())

    def create_node(node: Node):
        template = redis_conf_template.format(port=node.port)

        # Create directory for master and add template
        utils.mkdir(node.directory())
        with open(node.config(), "w") as f:
            f.write(template)

    for node in cluster.nodes:
        create_node(node)

    utils.dump_yaml_object(cluster, cluster.config())


def _transform(config: Dict[str, Any]) -> Cluster:
    if "cluster" not in config:
        print("No cluster configuration found.")
        exit(1)

    cluster_config = config["cluster"]

    def set_attributes(
        attributes: Dict[str, Any],
        obj: T,
        ignore_keys: List[str] = None,
        overwrite: bool = True,
    ) -> T:
        for key, value in attributes.items():
            if ignore_keys and key in ignore_keys:
                continue
            if overwrite or getattr(obj, key) is None:
                setattr(obj, key, value)

        return obj

    def transform_node(node_config: Dict[str, Any]) -> Node:
        def set_own_attributes(node: Node) -> Node:
            setattr(node, "_run_dir", f"{node.port}")

        # Create master node
        node: Node = set_attributes(node_config, Node())
        set_own_attributes(node)

        return node

    cluster: Cluster = Cluster()
    # Create from an high level description of the cluster a detailed cluster configuration
    masters: int = cluster_config["masters"]
    replicas_per_master: int = cluster_config["replicas_per_master"]
    start_port: int = cluster_config["start_port"]

    cluster.replicas_per_master = replicas_per_master

    for port in range(start_port, start_port + masters + masters * replicas_per_master):
        node: Node = transform_node({"port": port})
        cluster.nodes.append(node)

    # Add settings
    settings_config = config.get("settings", {})
    ##########################
    ######## TASKSET #########
    ##########################
    taskset_config = settings_config.get("taskset")
    if taskset_config:

        def get_next_taskset() -> Dict[str, Any]:
            # This method has side effects!
            start: int = taskset_config.get("start", 0)
            steps: int = taskset_config.get("steps", 1)
            cores: int = taskset_config.get("cores", 1)

            end: int = start + ((cores - 1) * steps)
            # Prepare for next run. SIDE-EFFECT!
            if not taskset_config.get("fixed"):
                taskset_config["start"] = end + steps

            return {"taskset_start": start, "taskset_step": steps, "taskset_end": end}

        for node in cluster.nodes:
            set_attributes(get_next_taskset(), node, overwrite=False)

    ##########################
    ########## LOG ###########
    ##########################
    log_config = settings_config.get("log")
    for node in cluster.nodes:
        set_attributes(
            {
                "_log_stdout": log_config.get("stdout"),
                "_log_stderr": log_config.get("stderr"),
                "direct_pipe": log_config.get("direct_pipe"),
            },
            node,
        )

    # Validate cluster if all settings are valid, e.g. no overlapping ports, taskset etc.
    def validate() -> None:
        ports = [node.port for node in cluster.nodes]
        if len(ports) != len(set(ports)):
            print("Some port is used twice.. Please fix!")
            exit(1)

        def all_or_nothing_none(keys: List[str], node: Node) -> None:
            attrs = [getattr(node, key) for key in keys]
            if all([attr is None for attr in attrs]) or all(
                [attr is not None for attr in attrs]
            ):
                # All Ok
                return
            print(f"Some value are none for {keys}")
            print(node)

        for node in cluster.nodes:
            all_or_nothing_none(["taskset_start", "taskset_end", "taskset_step"], node)

    validate()
    return cluster


def parser_args(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        help="The configuration file from which to create a cluster.",
        required=True,
    )
    parser.add_argument(
        "---template",
        help="The template of the redis.conf file",
        default="template-redis.conf",
    )


def main(args: Dict[str, Any]) -> None:
    with open(args.config) as f:
        config = yaml.safe_load(f)
    cluster: Cluster = _transform(config)

    with open(utils.abs_path(args.template)) as f:
        template = f.read()

    create_cluster(cluster, template)
