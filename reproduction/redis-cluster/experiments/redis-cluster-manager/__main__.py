#!/usr/bin/env python3
from argparse import ArgumentParser

import yaml

import cluster_status as status
import create_cluster as create
import failover_cluster as failover
import model
import network_recorder as network_recorder
import patch_cluster as patch
import start_cluster as start
import stop_cluster as stop
import utils
from model import Cluster


def main():
    parser = ArgumentParser()

    parser.add_argument(
        "--work-dir",
        help="The work directory in which the cluster is created.",
        required=True,
    )

    sub_parser = parser.add_subparsers(
        help="Cluster Management", required=True, dest="parser"
    )

    create_parser = sub_parser.add_parser("Create", help="Create a new redis-cluster")
    create.parser_args(create_parser)

    start_parser = sub_parser.add_parser("Start", help="Start a redis-cluster")
    start.parser_args(start_parser)

    stop_parser = sub_parser.add_parser("Stop", help="Stop a redis-cluster")
    stop.parser_args(stop_parser)

    status_parser = sub_parser.add_parser("Status", help="Status of a redis-cluster")
    status.parser_args(status_parser)

    patch_parser = sub_parser.add_parser("Patch", help="Patch a redis-cluster")
    patch.parser_args(patch_parser)

    failover_parser = sub_parser.add_parser("Failover", help="Failover a redis-cluster")
    failover.parser_args(failover_parser)

    network_recorder_parser = sub_parser.add_parser(
        "NetworkRecorder", help="Start network recording of all packets"
    )
    network_recorder.parser_args(network_recorder_parser)

    args = parser.parse_args()

    model.ROOT_DIR = utils.abs_path(args.work_dir)

    match args.parser:
        case "Create":
            create.main(args)
        case _:
            # The Cluster().config() is only needed to get the proper file name.
            cluster: Cluster = utils.load_yaml_object(Cluster().config())
            match args.parser:
                case "Start":
                    start.main(cluster, args)
                case "Stop":
                    stop.main(cluster, args)
                case "Status":
                    status.main(cluster, args)
                case "Patch":
                    patch.main(cluster, args)
                case "Failover":
                    failover.main(cluster, args)
                case "NetworkRecorder":
                    network_recorder.main(cluster, args)


if __name__ == "__main__":
    main()
