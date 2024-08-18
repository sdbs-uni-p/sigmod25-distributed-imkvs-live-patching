# WfPatch User Space Library Extensions

The artifacts from our earlier research [1] are available here:
- Website: https://github.com/sdbs-uni-p/vldb25-dbms-live-patching

The artifacts from the research by Rommel et al. [2] are available here:
- Website: https://www.sra.uni-hannover.de/Publications/2020/WfPatch/index.html
- Direct link to the artifacts (a QEMU VM): https://www.sra.uni-hannover.de/Publications/2020/WfPatch/artifact-vm.tar.xz

> **_NOTE:_** This directory includes the WfPatch user space library from our earlier research [1], from Rommel et al. [2], and our version used for this research.
To ensure clarity regarding our contributions, we have created this section. Here, we aim to explicitly outline the nature and extent of our modifications, as it is not always feasible to differentiate these changes directly within the reproduction package. This section serves to provide transparency and facilitate a clear understanding of the enhancements and adjustments we have implemented.

## User Space Library

The files `wf-userland.c` and `wf-userland.h` define the user space library of the WfPatch live patching framework by Rommel et al. [2]. The files in this directory represent ***our*** modified version of the user space library.
As a starting point, we used the WfPatch user space library files from our previous research [1]. The files from our previous research are located in the `fruth/` directory.
To illustrate our modifications to the user space library done in this research, we provide a git patch detailing the differences between our version used for this research and the previous research version.

For completeness, we also provide the original WfPatch user space library of Rommel et al. [2] in the `rommel/` directory and provide the difference to our used for version in `rommel-diff/`.

## create-patch and kpatch

The `create-patch` script is a utility for generating patches and we used the version from our earlier research [1] (this version is slightly modified to the version provided by Rommel et al. [2]; see [1] for details).
The same applies to `kpatch`.

---

[1] Michael Fruth and Stefanie Scherzinger. 2024.
The Case for DBMS Live Patching. In Proc. VLDB Endow. 17. Reproduction Package: https://github.com/sdbs-uni-p/vldb25-dbms-live-patching

[2] Florian Rommel, Christian Dietrich, Daniel Friesel, Marcel Köppen, ChristophBorchert, Michael Müller, Olaf Spinczyk, and Daniel Lohmann. 2020. *From Global to Local Quiescence: Wait-Free Code Patching of Multi-Threaded Processes*. In Proc. OSDI. 651–666.
