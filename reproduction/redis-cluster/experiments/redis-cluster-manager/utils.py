import os
import shutil
from typing import Any, Optional, Union

import yaml

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def dump_yaml_object(obj: Any, dst: str, append: bool = False) -> None:
    with open(dst, "a") as f:
        yaml.dump(obj, f)


def load_yaml_object(src: str) -> Any:
    with open(src, "r") as f:
        return yaml.load(f.read(), Loader=yaml.Loader)


def mkdir(directory: str, name: Optional[Union[str, int]] = None) -> str:
    path = os.path.join(directory, name) if name else directory

    os.makedirs(path)
    return path


def delete(file_or_directory: str) -> None:
    try:
        if os.path.isfile(file_or_directory) or os.path.islink(file_or_directory):
            os.unlink(file_or_directory)
        elif os.path.isdir(file_or_directory):
            shutil.rmtree(file_or_directory)
    except Exception as e:
        print("Failed to delete %s. Reason: %s" % (file_or_directory, e))


def clean_dir(directory: str) -> None:
    if not os.path.exists(directory) or not os.path.isdir(directory):
        return

    for f in [os.path.join(directory, f) for f in os.listdir(directory)]:
        delete(f)


def abs_path(path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(SCRIPT_DIR, path)
