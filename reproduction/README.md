# Reproduction

This directory contains all scripts to reproduce our results.

Please note that the term "synchronization time" refers to "update lag."

## Hardware Requirements

To be able to execute this reproduction package, the following hardware is recommended:

- At least 700 GB of free disk space
- At least 384 GB of main memory
- At least 22 CPU cores

Reproducing the experiments takes about **130 hours** and will generate about **685 GB** of data.

## Hardware

We conducted our experiments on a Dell PowerEdge R640 server equipped with:

- CPU: 2x Intel Xeon Gold 6248R
- 24 cores/48 threads per CPU
- 3.0 GHz
- Main Memory: 384 GB (12x 32 GB)
- Disk: 3.0 TB SSD

> **_NOTE:_** Our experiments use CPU pinning tailored to the specific number of CPU cores in our hardware. You may need to adjust the pinning to match your hardware configuration.
> - [redis-cluster/experiments/config-common.yaml](redis-cluster/experiments/config-common.yaml): The basic configuration file used for the Redis Cluster experiments (Section 6 in the paper). `taskset_start`, `taskset_end` and `taskset_step` define the taskset used for the **benchmark framework**.
> - [redis-cluster/experiments/cluster-configs/generate_configs.py](redis-cluster/experiments/cluster-configs/generate_configs.py): The variable `TEMPLATE` defines the taskset for the **Redis Cluster**:
>   - `start`: The first core which is used for taskset.
>   - `cores`: The total number of cores that can be used.
>   - `steps`: The taskset step (i.e. a value of 2 means only every second CPU core is used).
>   - If values are modified, the [redis-cluster/experiments/setup-configs](redis-cluster/experiments/setup-configs) has to be executed again.

To easily reproduce the experiments, the following scripts can be executed:

**Experiments - Redis Cluster Live Patching:**

Executes all experiments of Redis Cluster. You may have to adjust the CPU pinning before running this script (see above).

Estimated duration: 100 hours.

```
# IMPORTANT: This script has to be executed with the **MMView** Linux Kernel.
# IMPORTANT: Please see the note above about CPU pinning before running the experiments.
# 					 Otherwise, experiments may abort unexpectedly.
cd ~
./kernel-mmview
sudo reboot

cd distributed-imkvs-live-patching/reproduction
./reproduce-experiments
```

**Experiments - Real-World Patch Crawling:**

**_DISCLAIMER:_ Real-World patch crawling is done based on the *current state* of the respective git history (Redis/PostgreSQL), so reproducing the results *may lead to variations*. For reference, the data we collected is available in a dedicated directory (see below).**

*Redis:*

Our collected data is available in the [real-world-patches/redis-cluster/original-data](real-world-patches/redis-cluster/original-data) folder. Please see the README in [real-world-patches/redis-cluster](real-world-patches/redis-cluster) for details.

Estimated duration: 12 hours.

```
# IMPORTANT: This script has to be executed with the **MMView** Linux Kernel.
cd ~
./kernel-mmview
sudo reboot

cd distributed-imkvs-live-patching/reproduction
./reproduce-crawl-real-world-patches-redis
```



*PostgreSQL:*

Our collected data is available in the [real-world-patches/postgresql/original-data](real-world-patches/postgresql/original-data) folder. Please see the README in [real-world-patches/postgresql](real-world-patches/postgresql) for details.

**_IMPORTANT:_** This toolchain relies on the *current state* of the PostgreSQL commitfest website and the PostgreSQL git history, so reproducing the results *may lead to variations*. Since this toolchain uses web scraping, it is also possible that this toolchain may fail (e.g., if the HTML structure of the website has changed). 

Estimated duration: 12 hours.

```
# This script does not require a specific Linux kernel.

cd distributed-imkvs-live-patching/reproduction
./reproduce-crawl-real-world-patches-postgresql
```

**Experiment Analysis:**

Transforms the raw experiment data into DuckDB files and performs the subsequent analysis.

Estimated duration: 6 hours.

```
# IMPORTANT: This script has to be executed with the **regular** Linux Kernel.
cd ~
./kernel-regular
sudo reboot

cd distributed-imkvs-live-patching/reproduction
./reproduce-analysis
```



We make use of DuckDB for data anlaysis. However, we have encountered some issues when using DuckDB with the MMView Linux kernel. Therefore, it is important to use the regular Linux kernel when transforming or analyzing the data (i.e. when DuckDB is used).

Troubleshooting:
In case one of the analysis scripts crashes (e.g., due to an out-of-memory error caused by DuckDB), please delete all DuckDB database files (`cd data && find . | grep duckdb | xargs rm`), reboot the system (`sudo reboot`) and start the `reproduce-analysis` script again.

**Analysis:**

- All generated plots will be stored in the `data/output` directory. Please see the [data](data) directory of a mapping of file name to the figure referenced in the paper.

---

The steps above are a convenient way to reproduce the experiments. A detailed description of the individual steps can be found below.

## Experiments - Detailed Steps

> **_NOTE:_** The experiments have to be performed with the MMView Linux kernel (`cd ~ && ./kernel-mmview && sudo reboot` in the QEMU VM).

### Redis Cluster

Please refer to the [redis-cluster](redis-cluster) directory for the Redis Cluster experiments described in Section 6.

### Real-World Patches

Please refer to the [real-world-patches](real-world-patches) directory for scripts that crawl real-world PostgreSQL and Redis patches.

## Transformation

> **_NOTE:_** The transformation of raw benchmark data into DuckDB files has to be performed with the regular Linux kernel (`cd ~ && ./kernel-regular && sudo reboot` in the QEMU VM).

Once the experiments are complete, the raw benchmark data (especially from the Redis Cluster experiments) has to be transformed into DuckDB database files (these are used for the subsequent analysis).
Please refer to the [redis-cluster/transformation](redis-cluster/transformation) directory for all deteails.

## Analaysis

> **_NOTE:_** The analysis has to be performed with the regular Linux kernel (`cd ~ && ./kernel-regular && sudo reboot` in the QEMU VM).

Once the raw benchmark data got transformed into DuckDB database files, the data can be analyzed using the R scripts located in the plots directory.
The `do-*` scripts offer convenient functionality for easily generating plots, as they already include the paths to the data and the corresponding R script.
Before plots can be generated, the required software has to be prepared by executing the [plots/setup](plots/setup) script.

Some of the scripts also display raw benchmark data in the console for easier and detailed analysis.

Below is a mapping between the figures used in the paper and the scripts that generate the respective plots.

- Figure 1: [do-teaser-line](plots/do-teaser-line)
- Figure 5: [do-synchronization-time-failover-line](plots/do-synchronization-time-failover-line)
- Figure 6: [do-synchronization-time-patch-boxplot-idle](plots/do-synchronization-time-patch-boxplot-idle)
- Figure 7: [do-all-patches](plots/do-all-patches)
- Figure 8: [do-rps-time](plots/do-rps-time)
- Figure 9: [do-rps-time-single-details](plots/do-rps-time-single-details)
- Figure 10: [do-latencies-single-details](plots/do-latencies-single-details)

The plots are stored in the `data/output` directory. Please also see the [data](data) directory for a mapping between the final file name (the file generated by the `do-*` script) and the figure in the paper.

### Steps

```
# 0. Prepare
cd plots
./setup

# 1. Create plot
./do-....

# Plot is stored in the data/output directory.
```

## Original Data

The [original-plots](original-plots) directory contains the original plots used in the paper.
For easy comparison of our original plots and the reproduced plots, the [plot-comparison](plot-comparison) directory provides a script that starts a webserver which allows for easier visual inspection of the original plot and the reproduced plots. See the [plot-comparison](plot-comparison) directory for all details.

The raw and transformed data from our research can also be downloaded (see previous [../README.md](../README.md)) and further analyzed.
To do so, download the data and move the unpacked data into the [data](data) directory.
The [data](data) directory should have a layout like this:

```
data/
  latencies-failover
    latencies-failover.duckdb # transformed benchmark data
    7.0.11-get/               # raw benchmark data
      ...
    7.0.11-set/               # raw benchmark data
      ...
  ...
```

The [plots](plots) scripts exclusively uses the transformed data, so downloading only the transformed data is sufficient.
