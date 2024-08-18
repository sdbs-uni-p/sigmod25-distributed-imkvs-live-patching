import os
from copy import deepcopy
from functools import reduce
from itertools import product
from typing import Any, Dict, List, Optional, Tuple, TypeVar

import yaml

from model import (
    Benchmark,
    Build,
    Data,
    Experiment,
    Failover,
    Patch,
    PatchGeneration,
    RedisCluster,
)

T = TypeVar("T")

_PATHS_TO_RESOLVE = [
    ("build", "dir"),
    ("build", "output"),
    ("redis_cluster", "config"),
    ("redis_cluster", "output"),
    ("benchmark", "dir"),
    ("benchmark", "output"),
]


def _resolve_path(base_dir: str, path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.realpath(os.path.join(base_dir, path))


def _weak_dict_merge(
    base: Dict[str, Any], specialization: Dict[str, Any]
) -> Dict[str, Any]:
    for key, value in specialization.items():
        if isinstance(value, dict):
            if key not in base:
                base[key] = {}
            _weak_dict_merge(base[key], value)
        else:
            base[key] = value
    return base


def _resolve_iterations(config: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    result = []
    to_resolve = [config]
    while len(to_resolve) > 0:
        resolve_config = to_resolve.pop()
        res = _resolve_iterations_recursive(resolve_config)
        if res is None:
            result.append(resolve_config)
        else:
            to_resolve.extend(res)
    return result


def _resolve_iterations_recursive(
    config: Dict[str, Any]
) -> Optional[List[Dict[str, Any]]]:
    iterations_key = "_iterations"

    def find_iterations_recursive(
        current_config: Dict[str, Any] = None, path: List[str] = None
    ) -> List[Tuple[str, List[Any]]]:
        # Start of recursion
        if current_config is None:
            current_config = config
        if path is None:
            path = []

        result = []
        if (
            iterations_key in current_config
            and current_config[iterations_key] != "None"
        ):
            if not isinstance(current_config[iterations_key], list):
                raise ValueError(f"{iterations_key} must define a list! Path: {path}")
            result.append((path[:], current_config[iterations_key]))

        for key, value in current_config.items():
            if isinstance(value, dict):
                result.extend(find_iterations_recursive(value, path + [key]))
        return result

    iterations = find_iterations_recursive()
    if len(iterations) == 0:
        return None

    # Example:
    # _iterations:
    #   - database: ...
    #   - database: ...
    #   - database:
    #       _iterations: # <- this iteration is resolved in a recursive fashion by executing this function again!
    #           - ...
    #           - ...
    # benchmark:
    #   _iterations:
    #       - hallo: ...
    #       - hallo: ...

    # iterations = [([], [-database..., -database..., -database...]), (["benchmark"], [-hallo..., -hallo...])]
    # product (product of the iteration configs):
    # 0 0
    # 0 1
    # 1 0
    # 1 1
    # 2 0
    # 2 1
    # We want to use every combination of different _iterations
    new_configs = []
    for indices in product(
        *[range(len(iteration_configs)) for path, iteration_configs in iterations]
    ):
        # Use deepcopy because it's a nested dict
        new_config = deepcopy(config)
        for iterations_idx, idx in enumerate(indices):
            path, it_config = iterations[iterations_idx]
            # Get place in which we want to insert our 'iterations config'
            insertion = reduce(lambda dic, key: dic[key], path, new_config)
            # Remove _iterations as it is "replaced" now.
            del insertion[iterations_key]
            # Insert (merge) the new configuration into the place of insertion
            _weak_dict_merge(insertion, deepcopy(it_config[idx]))
        new_configs.append(new_config)

    return new_configs


def _resolve_reference(config: Dict[str, Any], base_dir: str) -> List[Dict[str, Any]]:
    configs = [config]
    reference_key = "_reference"
    if reference_key in config:
        ref = config[reference_key]
        del config[reference_key]  # Delete reference
        # For reversed: see example in def _config_from_file
        for reference_config_file in reversed(ref if isinstance(ref, list) else [ref]):
            with open(_resolve_path(base_dir, reference_config_file)) as f:
                reference_config = yaml.safe_load(f)
                reference_configs = _resolve_reference(reference_config, base_dir)
                configs.extend(reference_configs)
    return configs


def _config_from_file(config_file: str, base_dir: str) -> Dict[str, Any]:
    with open(config_file) as f:
        config = yaml.safe_load(f)
    configs = _resolve_reference(config, base_dir)

    # Example:
    # a -> bc # order: acb | a is most specific, followed by c, base is b
    # b -> d # order: bd | b is most specific; d is base
    # c -> ef # order: cfe | c is most specific, followed by f, base is e
    # Total order: acfebd

    merged_config = configs.pop(-1)
    for c in reversed(configs):
        merged_config = _weak_dict_merge(merged_config, c)
    return merged_config


def _config_from_arguments(arguments: List[str]) -> Dict[str, Any]:
    # arg: build.output='new-output'
    # 1. Split by = (build.ouptut, 'new-output')
    # 2. Split the path by . ([build, output], 'new-output')
    pairs = [
        (path.split("."), value)
        for arg in arguments
        for path, value in [arg.split("=", 1)]
    ]

    result = {}
    for paths, value in pairs:
        tmp = {paths[-1]: value}
        for path in reversed(paths[:-1]):
            tmp = {path: tmp}
        result = _weak_dict_merge(result, tmp)
    if "data" in result and "max_memory_usage_gb" in result["data"]:
        result["data"]["max_memory_usage_gb"] = int(
            result["data"]["max_memory_usage_gb"]
        )
    if "benchmark" in result and "idle_before" in result["benchmark"]:
        result["benchmark"]["idle_before"] = int(result["benchmark"]["idle_before"])
    if "patch_generation" in result and "patches" in result["patch_generation"]:
        result["patch_generation"]["patches"] = int(
            result["patch_generation"]["patches"]
        )

    return result


def config_to_experiment(config: Dict[str, Any], base_dir: str) -> Experiment:
    # Resolve all paths
    for paths in _PATHS_TO_RESOLVE:
        tmp = config
        for path in paths[:-1]:
            tmp = tmp.get(path, {})
        tmp[paths[-1]] = _resolve_path(base_dir, tmp[paths[-1]])

    def set_attributes(
        attributes: Dict[str, Any], obj: T, ignore_keys: List[str] = None
    ) -> T:
        if attributes is None:
            return None
        for key, value in attributes.items():
            if ignore_keys and key in ignore_keys:
                continue
            setattr(obj, key, value)
        return obj

    build = set_attributes(config["build"], Build())
    data = set_attributes(config["data"], Data())
    benchmark = set_attributes(config["benchmark"], Benchmark())
    if benchmark.requests and benchmark.time_s:
        print("It is not allowed to set requests and time_s for 'benchmark'!")
        exit(1)

    # Either patch is defined or failover. Both together is not allowed
    patch = set_attributes(config.get("patch"), Patch())
    patch_generation = set_attributes(config.get("patch_generation"), PatchGeneration())
    failover = set_attributes(config.get("failover"), Failover())
    if (patch and not patch_generation) or (not patch and patch_generation):
        print("'patch' and 'patch_generation' must be specified! One is missing")
        exit(1)
    if patch and failover:
        print("'patch' and 'failover' is specified! Only one is allowed!")
        exit(1)
    if not patch and not failover:
        print("One of 'patch' and 'failover' must be specified! None of both is given!")
        exit(1)

    redis_cluster = set_attributes(config.get("redis_cluster", {}), RedisCluster())

    return Experiment(
        name=config["name"],
        commit=config["commit"],
        build=build,
        patch_generation=patch_generation,
        data=data,
        benchmark=benchmark,
        patch=patch,
        redis_cluster=redis_cluster,
        failover=failover,
    )


def _resolve_repeat(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    repeat_key = "_repeat"
    if repeat_key in config:
        repeat = config[repeat_key]
        del config[repeat_key]
        return [deepcopy(config) for _ in range(repeat)]
    return [config]


def parse_config(config_file: str, arguments: Optional[List[str]]) -> List[Experiment]:
    base_dir = os.path.dirname(os.path.realpath(config_file))

    # Parse configs and merge them to one
    file_config = _config_from_file(config_file, base_dir)
    overwrite_config = _config_from_arguments(arguments) if arguments else {}
    config = _weak_dict_merge(file_config, overwrite_config)

    # Resolve iterations
    configs = _resolve_iterations(config)
    configs = [
        repeat_config for config in configs for repeat_config in _resolve_repeat(config)
    ]
    return [config_to_experiment(c, base_dir) for c in configs]
