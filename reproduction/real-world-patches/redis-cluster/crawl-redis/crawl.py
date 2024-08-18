#!/usr/bin/env python

from collections import defaultdict
import os
import git

import re


GIT_URL = "https://github.com/redis/redis.git"
GIT_DIR = "redis"

START_TAG = 2
END_TAG = 7

OUTPUT_FILE = "crawled-commits.txt"

if not os.path.isdir(GIT_DIR):
    git.Repo.clone_from(GIT_URL, GIT_DIR)

repo = git.Repo(GIT_DIR)

def check_tag(tag) -> str:
    if re.fullmatch(r'\d+\.\d+\.\d+', tag.name):
        return True
    return False

tags = [tag for tag in repo.tags if check_tag(tag)]

groups = defaultdict(list)

for tag in tags:
    major, minor, patch = tag.name.split('.')
    major = int(major)
    minor = int(minor)
    patch = int(patch)
    groups[(major, minor)].append(patch)

for key, val in groups.items():
    val.sort()
    groups[key] = [val[0], val[-1]]

with open(OUTPUT_FILE, "w") as f:
    out = ""
    for key, val in groups.items():
        out += f"{key[0]}.{key[1]}.{val[0]}..{key[0]}.{key[1]}.{val[1]}\n"
    f.write(out.strip())

