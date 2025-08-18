# Data

This directory will store in the `output` sub-directory the generated plots. 

The following files correspond to the following figures and statements in the paper:

**Statements:**

- **Section 8.3. Assessing Real-World Patches**
  - **Redis**: `real-world-patches-redis.txt`
    - The line `Total commits considered: ` shows the number of bug fixes.
    - The line `Success:` shows the number of successfully generated live patches.
    - We found `1554` bug fixes, and successfully generated a live patch for `1228` of them. **Please note that the numbers may deviate when reproducing the experiments, as the *current state* (git repository) is crawled.**
  - **PostgreSQL**: `real-world-patches-postgresql.txt`
    - The line `Backend commits:` shows the number of bug fixes.
    - The line `Success:` shows the number of successfully generated live patches.
    - We found `412` bug fixes, and successfully generated a live patch for `350` of them. **Please note that the numbers may deviate when reproducing the experiments, as the *current state* (PostgreSQL Commitfest Website and git repository) is crawled.**

**Figures:**

- Figure 1: `Synchronization-Time-Teaser.pdf`
- Figure 5: `Synchronization-Time-Failover-Time.pdf`
- Figure 6: `Synchronization-Time-Patch-Boxplot-Idle.pdf`
- Figure 7: `Patch-Duration-RealWorld.pdf`
  - Please note: Redis real-world patches are crawled based on the *current* git history. Therefore, there may be deviations in this chart. 

- Figure 8: `RPS-Time.pdf`
- Figure 9: `RPS-Time-Single-Details.pdf`
- Figure 10: `Latencies-Single-Details.pdf`

**Other Files**: All other figures are referenced in the [README](../../README.md) of this reproduction package. 
