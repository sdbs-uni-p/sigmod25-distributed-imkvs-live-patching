# Redis Cluster - Experiments

This directory contains all scripts for the Redis Cluster experiments (Section 6), except the experiment using real-world patches (Figure 7).
Please see the [../real-world][../real-world] directory for the scripts to crawl and benchmark Redis real-world patches.

## Artifacts

### Source Code Modifications

The directory [experiments/patches](experiments/patches) contains the source code modifications we made to external software for our Redis Cluster experiments.
The changes are git patches (https://git-scm.com/docs/git-format-patch).

- [memtier_benchmark.patch](experiments/patches/memtier_benchmark.patch) includes the changes to the memtier benchmark framework (e.g. single latency measurement etc.).
- [redis-live-patch.patch](experiments/patches/redis-live-patch.patch) contains all changes for our live patching Redis Cluster prototype - it equips Redis Cluster with live patching and patch distribution capabilities.
- [redis-network-single-latencies.patch](experiments/patches/redis-network-single-latencies.patch) contains the additions to the Redis Cluster to measure sent bytes (for the experiments in Section 6.7).


### Synthetic Patch

The directory [experiments/patches/generate-redis-getPatch-patches](experiments/patches/generate-redis-getPatch-patches) stores the synthetic patch along with scripts for generating successive patches of any length.
The [base.patch](experiments/patches/generate-redis-getPatch-patches/base.patch) file marks the starting point of the synthetic patch.
Notably, the function and file that the synthetic patch modifies are introduced through our live patching prototype [redis-live-patch.patch](experiments/patches/redis-live-patch.patch).

## Experiments

### Execution

> **_NOTE:_** The experiments have to be performed with the MMView Linux kernel (`cd ~ && ./kernel-mmview && sudo reboot` in the QEMU VM).

> **_NOTE:_** Our experiments use CPU pinning tailored to the specific number of CPU cores in our hardware. You may need to adjust the pinning to match your hardware configuration.
> - [experiments/config-common.yaml](experiments/config-common.yaml): The basic configuration file used for the Redis Cluster experiments (Section 6 in the paper). `taskset_start`, `taskset_end` and `taskset_step` define the taskset used for the **benchmark framework**.
> - [experiments/cluster-configs/generate_configs.py](experiments/cluster-configs/generate_configs.py): The variable `TEMPLATE` defines the taskset for the **Redis Cluster**:
>   - `start`: The first core which is used for taskset.
>   - `cores`: The total number of cores that can be used.
>   - `steps`: The taskset step (i.e. a value of 2 means only every second CPU core is used).
>   - If values are modified, the [experiments/setup-configs](experiments/setup-configs) has to be executed again.


We walk through the different directories in [experiments](experiments) by the example of executing an experiment.

1. We select one of the experiments in the [experiments/experiments](experiments/experiments) directory: [teaser](experiments/experiments/teaser) contains experiments for Figure 1, [synchronization-time](experiments/experiments/synchronization-time) for Section 6.4 (the term synchronization-time is used in this reproduction package for the term update-lag used in the paper. Both terms are synonymously), [qps-latencies](experiments/experiments/qps-latencies) for Section 6.5 and 6.6 and [network](experiments/experiments/network) for Section 6.7.
2. Each experiment references to an experiment configuration (`config-*.yaml` files in the root [experiments](experiments) directory). These yaml files define the experiment, for example, which benchmark workload to execute, what CPU pinning to use, where to store the result files, how long a benchmark should be executed, what memory state to use etc. This configuration is a custom crafted format and used for our implemented experiment execution platform.
3. The [experiments/patch-benchmark](experiments/patch-benchmark) tool defines our experiment execution platform; it parses the configuration file and executes the experiment.  It is responsible for all tasks like spinning up the Redis Cluster, scheduling the benchmark framework, scheduling conventional/live patching etc. It makes use of the [experiments/redis-build-utils](experiments/redis-build-utils) for compiling the Redis Cluster source code (this directory also contains the scripts to generate a live patch using Kpatch). Furthermore, the execution platform also makes use of the scripts bundeled in [experiments/redis-cluster-manager](experiments/redis-cluster-manager): These scripts are responsible for (1) spinning up a Redis Cluster, (2) stopping a Redis Cluster, (3) requesting the cluster status, (4) to apply a live patch to the a node or (5) to perform the conventional patching (restating each replica; performing a failover; restarting the former master etc.). The cluster configuration is controlled based on a configuration file stored in [experiments/cluster-configs](experiments/cluster-configs). The cluster configurations are generated by the [experiments/cluster-configs/generate_configs.py](experiments/cluster-configs/generate_configs.py) script. To modify cluster settings like taskset, modify the `generate_configs.py` script and generate the configuration files again using the [experiments/setup-configs] script.
4. The raw benchmark data of an experiment is stored in the [../data](../data/) directory. A separate directory is created for each experiment (i.e. for each script in [experiments/experiments/...](experiments/experiments/)).

### Transformation

> **_NOTE:_** The transformation of raw benchmark data into DuckDB files has to be performed with the regular Linux kernel (`cd ~ && ./kernel-regular && sudo reboot` in the QEMU VM).

Once an experiment has been executed (see previous section), the benchmark data needs to be prepared for further analysis.
To efficiently analyze benchmark data, we make use of DuckDB (https://duckdb.org): The scripts to load the benchmark data and transform it into a DuckDB database are contained the directory [transformation/beder2](transformation/beder2) (**be**nachmark **d**ata analyz**er**). The schema of the data is stored in [transformation/beder2/queries/create.sql](transformation/beder2/queries/create.sql). The [transformation/duckdb-utils](transformation/duckdb-utils) directory contains utility-scripts for DuckDB, e.g. to download a specific version, or to merge two DuckDB database files.

The resulting DuckDB database files are stored in the [../data](../data) directory.

### Analysis

Please see the previous [README.md][../README.md] for further analysis of the data.

### Commands

```
pwd
# Should return:
# <SOME-PATH>/sigmod25-distributed-imkvs-live-patching/reproduction/redis-cluster

DIR=$(pwd)

###########################
# Use MMView Linux Kernel #
###########################

# 1. Experiments
# 1.1. Prepare Experiments
cd $DIR/experiments
./setup

# 1.2. Perform Experiments
cd $DIR/experiments/experiments
# Execute all Redis Cluster experiments (except real-world patches experiment).
# Result data is stored in the $DIR/data directory.
./do-all

############################
# Use regular Linux Kernel #
############################

# 2. Transformation
# 2.1. Prepare Transformation
cd $DIR/transformation
./setup

# 2.2. Perform Transformation
# Transform all raw benchmark data into DuckDB database files.
# DuckDB database files are stored in the $DIR/data directory (in the respective experiment directory).
./do-all

# All experiments are performed and data is prepared. See README of the parent directory.
```
