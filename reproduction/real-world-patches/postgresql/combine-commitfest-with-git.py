#!/usr/bin/env python

import os
import git

from common import parse_message_id

GIT_URL = "https://github.com/postgres/postgres.git"
GIT_DIR = "postgres"
GIT_BRANCH = "master"

if not os.path.isdir(GIT_DIR):
    git.Repo.clone_from(GIT_URL, GIT_DIR)

repo = git.Repo(GIT_DIR)
commits = list(repo.iter_commits(GIT_BRANCH))

with open("postgresql-message-ids.txt", 'r') as f:
    crawled_ids = [line.strip() for line in f.readlines()]

found_commits = set()
for commit in commits:
    for line in commit.message.split("\n"):
        if line.startswith("Discussion"):
            message_id = parse_message_id(line[line.rfind("/") + 1:])
            if message_id in crawled_ids:
                print(message_id)
                found_commits.add(commit.hexsha)

print(f"Found {len(found_commits)} commits.\nTotal commits: {len(commits)}\nTotal commitfest message ids: {len(crawled_ids)}")
with open("bug-fix-commits.txt", "w") as f:
    f.write("\n".join(found_commits))

