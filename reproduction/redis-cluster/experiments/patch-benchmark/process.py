import os
import subprocess
from typing import Any, Dict, List, Optional


def taskset_cmd(start: int, end: int, step: int) -> List[str]:
    return ["taskset", "-c", f"{start}-{end}:{step}"]


def timeout_cmd(timeout_min: int) -> List[str]:
    return ["timeout", "-k", "30s", f"{timeout_min}m"]


def memtier_load_addition() -> List[str]:
    return [
        "--command",
        "set __key__ __data__",
        "--out-file-result",
        "/dev/null",
        "--random-data",
    ]


def memtier_key_tag_addition(tag: str, allowed_ports: List[int]) -> List[str]:
    if not tag:
        return []

    res = ["--key-tag", f"{tag}"]
    if len(allowed_ports) > 0:
        res += ["--only-ports", f"{' '.join(str(p) for p in allowed_ports)}"]
    return res


def memtier_incr_key_addition() -> List[str]:
    return ["--incr-key-only"]


def memtier_no_latency_recording() -> List[str]:
    return ["--no-latency-recording"]


def memtier_requests_addition(requests: int) -> List[str]:
    return ["-n", f"{requests}"]


def memtier_time_addition(time_s: int) -> List[str]:
    return ["--test-time", f"{time_s}"]


def memtier_command_addition(command: str) -> List[str]:
    return ["--command", f"{command}"]


def memtier_result_addition(result_file: str) -> List[str]:
    return ["--out-file-result", f"{result_file}"]


def memtier_benchmark_cmd(
    bin_dir: str,
    keyspace: int,
    data_size: int,
    clients: int,
    threads: int,
    port: int,
    key_prefix: str = "",
) -> List[str]:
    return [
        os.path.join(bin_dir, "memtier_benchmark"),
        "--key-maximum",
        f"{keyspace}",
        "-d",
        f"{data_size}",
        "--clients",
        f"{clients}",
        "--threads",
        f"{threads}",
        "-p",
        f"{port}",
        "--out-file",
        "/dev/null",
        "--cluster-mode",
        "--hide-histogram",
        "--distinct-client-seed",
        "--key-prefix",
        f"{key_prefix}",
    ]


def _run_kwargs(
    cwd: str = None,
    env: Dict[str, Any] = None,
    stdout_pipe: Optional[int] = None,
    stderr_pipe: Optional[int] = None,
    shell: bool = False,
) -> Dict[str, Any]:
    kwargs = {"cwd": cwd, "stdout": stdout_pipe, "stderr": stderr_pipe, "shell": shell}
    if env is not None:
        os_env = os.environ.copy()
        os_env.update({key: str(value) for key, value in env.items()})
        kwargs.update({"env": os_env})
    return kwargs


def run2(
    command: List[str],
    cwd: str = None,
    exception_on_error: bool = True,
    env: Dict[str, Any] = None,
    verbose: bool = False,
) -> subprocess.CompletedProcess:
    return run(
        command,
        cwd,
        exception_on_error,
        env,
        None if verbose else subprocess.DEVNULL,
        None if verbose else subprocess.DEVNULL,
    )


def run(
    command: List[str],
    cwd: str = None,
    exception_on_error: bool = True,
    env: Dict[str, Any] = None,
    stdout_pipe: Optional[int] = None,
    stderr_pipe: Optional[int] = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        check=exception_on_error,
        **_run_kwargs(cwd, env, stdout_pipe, stderr_pipe),
    )


def run2_async(
    command: List[str],
    cwd: str = None,
    env: Dict[str, Any] = None,
    verbose: bool = False,
    shell: bool = False,
) -> subprocess.Popen:
    return run_async(
        command,
        cwd,
        env,
        None if verbose else subprocess.DEVNULL,
        None if verbose else subprocess.DEVNULL,
        shell=shell,
    )


def run_async(
    command: List[str],
    cwd: str = None,
    env: Dict[str, Any] = None,
    stdout_pipe: Optional[int] = None,
    stderr_pipe: Optional[int] = None,
    shell: bool = False,
) -> subprocess.Popen:
    return subprocess.Popen(
        command, **_run_kwargs(cwd, env, stdout_pipe, stderr_pipe, shell)
    )
