#!/usr/bin/env python

import sys
from typing import List
from argparse import ArgumentParser

TEMPLATE = """
settings:
  taskset:
    start: 1
    cores: 24
    steps: 2
    fixed: True
  log:
    stdout: redis.stdout.log
    stderr: redis.stderr.log
    direct_pipe: True

cluster:
  masters: {masters}
  replicas_per_master: {replicas}
  start_port: 7000
"""

def main(argv: List[str]):
    parser = ArgumentParser()
    parser.add_argument("--start-masters", type=int, required=True)
    parser.add_argument("--end-masters", type=int, required=True)
    
    parser.add_argument("--start-replicas", type=int, required=True)
    parser.add_argument("--end-replicas", type=int, required=True)

    args = parser.parse_args(argv)

    for masters in range(args.start_masters, args.end_masters + 1):
        for replicas in range(args.start_replicas, args.end_replicas + 1):
            with open(f"cluster-{masters}-{replicas}.yaml", "w") as f:
                f.write(TEMPLATE.format(masters=masters, replicas=replicas))

if __name__ == "__main__":
    main(sys.argv[1:])
