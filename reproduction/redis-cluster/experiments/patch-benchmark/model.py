from __future__ import annotations

from abc import ABC
from typing import List


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


class Experiment(_Print):
    def __init__(
        self,
        name: str,
        commit: str,
        build: Build,
        patch_generation: PatchGeneration,
        data: Data,
        benchmark: Benchmark,
        redis_cluster: RedisCluster,
        patch: Patch,
        failover: Failover,
    ):
        # Input
        self.name: str = name
        self.commit: str = commit
        self.build: Build = build
        self.patch_generation: PatchGeneration = patch_generation
        self.data: Data = data
        self.benchmark: Benchmark = benchmark
        self.redis_cluster = redis_cluster
        self.patch: Patch = patch
        self.failover: Failover = failover

        # Output
        self.success: bool = None
        self.benchmark_failure: bool = None
        self.benchmark_timeout: bool = None


class Build(_Print):
    def __init__(self):
        # Input
        self.dir: str = None
        self.git_dir_name: str = None
        self.output: str = None

        # Output
        self.bin_dir: str = None


class Data(_Print):
    def __init__(self):
        # Input
        self.max_memory_usage_gb: int = None
        self.data_size: int = 4096


class Benchmark(_Print):
    def __init__(self):
        # Input
        self.dir: str = None
        self.output: str = None
        self.taskset_start: int = None
        self.taskset_end: int = None
        self.taskset_step: int = None

        self.name: str = None
        self.idle_before: int = None
        self.output_name: str = None
        self.requests: int = None
        self.time_s: int = None
        self.max_memory_usage_gb: int = None
        self.data_size: int = 4096
        self.clients: int = None
        self.threads: int = None
        self.per_master: bool = None

        self.incr_key_only: bool = None
        self.dump_db_at_stop: bool = None
        self.no_latency_recording: bool = None

        # Output
        self.framework_start_time: float = None
        self.framework_end_time: float = None


class RedisCluster(_Print):
    def __init__(self):
        # Input
        self.config: str = None
        self.output: str = None
        self.status_file: str = None

        # Output
        self.patches_dir: str = None


class PatchGeneration(_Print):
    def __init__(self):
        # input
        self.patches: int = None

        # output
        self.patch_paths: List[str] = None


class Patch(_Print):
    def __init__(self):
        # Input
        self.method: str = None
        self.distribution: str = None
        self.reverse_version: bool = None
        self.apply_patch_after_s: float = None
        self.sleep_between_versions: float = None
        self.sleep_bias: float = None

        # WfPatch
        self.measure_vma: bool = None
        self.measure_pte: bool = None
        self.patch_only_active: bool = None
        self.apply_all_patches_at_once: bool = None
        self.single_as_for_each_thread: bool = None
        self.skip_patch_file: bool = None
        self.skip_patch_application: bool = None


class Failover(_Print):
    def __init__(self):
        # Input
        self.failover_after_s: float = None
        self.skip_failover: bool = None
        self.skip_restart_master: bool = None
        self.skip_restart_replica: bool = None
        self.all_nodes: bool = None
        self.snapshot: bool = None

        # Output
        self.output_file: str = None
