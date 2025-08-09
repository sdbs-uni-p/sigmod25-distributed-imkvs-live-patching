# Redis Real-World Patches

> **_NOTE:_** The experiments have to be performed with the MMView Linux kernel (`cd ~ && ./kernel-mmview && sudo reboot` in the QEMU VM).
> **_DISCLAIMER:_** This toolchain relies on the current state of the Redis git history, so reproducing the results may lead to variations. For reference, the data we collected is available in the [original-data](original-data) folder.

For Redis, we not only crawled real-world patches from the git development history but also performed experiments to measure the patch application time. We first analyzed the git development history of Redis across various versions, focusing on "patch versions" that included only patches or bug fixes (e.g., between versions 7.0.0 and 7.0.1, but not between 7.0.0 and 7.1.0). For each of these versions, we attempted to generate a patch.

After obtaining a list of live-patchable commits, we ran experiments on them. For each live-patchable commit, we tried to automatically apply the source code changes that enable live patching capabilities for a Redis instance. To increase the success rate, we created specific *git source code patches* for different Redis versions, available in the [git-patches](git-patches) directory.

If the source code changes were successfully applied, we conducted experiments to measure the patch application time. After running the experiments, the data can be visualized, as described in the parent-parent directory ([../../README.md](../../README.md)).

```
# 0. Prepare
./setup

# 1. Crawl Redis git history
# Output: real-world.commits.success
cd crawl-redis
./do

# 2. Perform experiment
# Output: realworldpatches.csv
cd ..
./experiment
```

As the script crawls the current git development history - which may differ from the state when we initially collected the data — we have provided our original output files in the [original-data](original-data) directory.
The `info-*` files contain the complete output of the respective scripts for reference.
The [original-data/crawled-commits.txt](original-data/crawled-commits.txt) file contains a list of the git versions that were available and used at the time we conducted these experiments.

For the share of found and live patchable commits, please see the `real-world-patches-redis.txt` file in the  [../../data](../../data) directory (and the README of the `data` directory on how to read the file). 
