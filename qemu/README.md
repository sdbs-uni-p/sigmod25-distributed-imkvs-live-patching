# QEMU Virtual Machine (VM)

We provide a QEMU VM which is equipped (1) with the MMView Linux kernel and (2) all necessary software and libraries installed required for executing our scripts/tools. All steps of the reproduction package can be executed within the VM.

> **NOTE: Our experiments were conducted directly on a prepared server, avoiding the overhead of virtualization. When replicating the results in the VM, we observed a 25% performance degradation compared to the direct server execution. This degradation introduces significant system noise, obscuring some measured effects. Consequently, we recommend conducting the experiments directly on a prepared server for more accurate results. However, we provide the VM for a quick start and ease of reproducibility.**

You can reproduce our results either by using our pre-defined [QEMU VM](#qemu-vm) or by preparing a [new server](#prepare-own-server) to execute experiments directly on it.

## QEMU VM

You need to install QEMU and KVM (for best performance) on your system.

The following commands get you started using the VM:

```
# 1. Clone Repository
git clone https://github.com/sdbs-uni-p/sigmod25-distributed-imkvs-live-patching.git distributed-imkvs-live-patching

# 2. Go into the qemu directory
cd distributed-imkvs-live-patching/qemu

# 3. Download and extract QEMU VM
./download-vm

# 4. Run VM
# Note: The script assigns
#   - all available main memory except for 20 GiB and
#   - all CPU cores except for one core per CPU socket
# of the host system to the VM.
./run-vm

# 5. SSH into VM (see credentials below)
ssh repro@127.0.0.1 -p 2222

# 6. Inside the VM:
# Download the repository
./setup

# 7. Check current Linux kernel
uname -a

# Unmodified Linux kernel:
# Linux debian 5.15.0-0.bpo.3-amd64 #1 SMP Debian 5.15.15-2~bpo11+1 (2022-02-03) x86_64 GNU/Linux

# MMView Linux kernel:
# Linux debian 5.15.0-mmview-min #2 SMP Sun May 19 22:47:40 CEST 2024 x86_64 GNU/Linux
```

### Credentials (username:password):

- repro:repro
- root:root

> **_NOTE:_** In order to enable easy reproduction, security best practices were neglected. Both users are in the `sudo` group without the need for a password (`NOPASSWD` in `/etc/sudoers`).

### Switching Kernel

To reproduce our results, we require both the unmodified Linux kernel and the MMView Linux kernel. The VM includes scripts facilitating the easy switching of kernels:

```
cd ~

# Enable MMView Linux kernel
./kernel-mmview
sudo reboot

# Enable unmodified Linux kernel
./kernel-regular
sudo reboot
```

## Prepare own Server

In case you want to prepare a new server for the execution of all scripts, i.e. reproducing the results, please see the notes below on how to compile the MMView Linux kernel and what software is required. It is required to use ***Debian 11 (bullseye)*** as distribution (each distribution ships a different version of `gcc` and patch generation is sensitive to the `gcc` version used).

### VM Preparation Notes

The following is a brief summary about the steps performed to prepare the VM. These steps can be used as a basis for preparing a new server for our reproduction pipeline.

```
# Install software
apt-get update -y
apt-get install -y \
    sudo \
    vim \
    git \
    gcc \
    tmux \
    build-essential \
    flex \
    bison \
    libssl-dev \
    bc \
    elfutils \
    libelf-dev \
    dwarves \
    libncurses-dev \
    curl \
    zsh \
    openjdk-17-jdk \
    unzip \
    ccache \
    ninja-build \
    rsync \
    r-base \
    libmariadb-dev \
    libffi-dev
apt-get install -y --no-install-recommends\
    ca-certificates \
    cmake \
    gnutls-dev \
    libboost-all-dev \
    libevent-dev \
    libncurses5-dev \
    libpcre2-dev \
    libxml2-dev \
    pkg-config \
    psmisc \
    wget \
    zlib1g-dev \
    libcurl4-openssl-dev \
    libfreetype6-dev \
    libfontconfig1-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    libharfbuzz-dev \
    libfribidi-dev


# Add NOPASSWD to sudo group
vim /etc/sudoers

# Add repro user to sudo group:
usermoad -aG sudo repro

# Map /tmp to tmpfs
sudo vim /etc/fstab
# Add line:
tmpfs   /tmp    tmpfs   size=100%,mode=1777,nosuid,nodev   0   0

# Install Docker
# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker repro

# Create script "cpupower" in "/bin" as this command cannot be used inside the VM. The experimet scripts use this command heavly and should not fail because it is missing.
cd /bin
touch cpupower
chmod +x cpupower

# Install pyenv
curl https://pyenv.run | bash
# ...
# Update .bashrc and .profile of user repro based on output of pyenv installation.

# Install Python 3.10.
pyenv install 3.10.1
pyenv global 3.10.1
pip install pipenv
```

#### Linux Kernel Compilation

```
# Compile Linux Kernel (MMView)
git clone https://github.com/luhsra/linux-mmview.git
cd linux-mmview
git checkout ecfcf9142ada6047b07643e9fa2afe439b69a5f0

# When compiling the kernel, disable the following options (list may not be complete):
# - CONFIG_USERFAULTFD
# - CONFIG_KSM
# - CONFIG_TRANSPARENT_HUGEPAGE
# - CONFIG_ACPI_NFIT
# - CONFIG_X86_PMEM_LEGACY
# - CONFIG_LIBNVDIMM

# ################################
# Compile kernel without modules #
# ################################
make localmodconfig
make menuconfig
# Disable "enable loadable module support"
# Exit and save .config

# Set in .config
# - CONFIG_LOCALVERSION="-mmview-min"
# Disable the CONFIG_ options mentioned above

make -j
sudo make install

# ###################################
# Compile kernel with modules       #
# (not done but here for reference) #
# ###################################
make oldconfig

# Set in .config
# - CONFIG_LOCALVERSION="-mmview"
# Disable the CONFIG_ options mentioned above

make -j
sudo make modules_install
sudo make install
```
