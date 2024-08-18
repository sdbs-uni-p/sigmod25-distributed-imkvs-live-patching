from __future__ import annotations

import os
from abc import ABC
from typing import List, Optional

import yaml

ROOT_DIR: str = None


class _Print(ABC):
    def to_dict(self) -> dict:
        return {
            key: (value.to_dict() if isinstance(value, _Print) else value)
            for key, value in vars(self).items()
        }

    def __str__(self):
        attributes = "\n\t".join(
            f"{attr}: {getattr(self, attr)}" for attr in vars(self)
        )
        return f"{type(self).__name__}\n\t{attributes}"


class Base(_Print):
    yaml_loader = yaml.SafeLoader

    def __init__(self, config_name: str) -> Base:
        # Relative path to the directory; None measns "root" directory
        self._run_dir: str = None
        self._config_name: str = config_name

    def config(self) -> str:
        return os.path.join(self.directory(), self._config_name)

    def directory(self) -> str:
        return (
            ROOT_DIR if self._run_dir is None else os.path.join(ROOT_DIR, self._run_dir)
        )


class Node(Base):
    def __init__(self) -> Node:
        super().__init__("redis.conf")
        self.port: int = None

        # taskset
        self.taskset_start: int = None
        self.taskset_end: int = None
        self.taskset_step: int = None

        # log
        self._log_stdout: str = None
        self._log_stderr: str = None
        self.direct_pipe: bool = None

    def pid_file(self) -> str:
        return os.path.join(self.directory(), "redis.pid")

    def _log_file(self, f: str) -> Optional[str]:
        return (
            (f if os.path.isabs(f) else os.path.join(self.directory(), f))
            if f
            else None
        )

    def log_stdout_file(self) -> Optional[str]:
        return self._log_file(self._log_stdout)

    def log_stderr_file(self) -> Optional[str]:
        return self._log_file(self._log_stderr)


class Cluster(Base):
    def __init__(self) -> Cluster:
        super().__init__("cluster.yaml")
        self.nodes: List[Node] = []
        self.replicas_per_master: int = None
