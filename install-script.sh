#!/usr/bin/env bash
set -euo pipefail

# ======================================================
# ======================================================

if [ ! -d "$HOME/cascade-cbdc" ]; then
    git clone https://github.com/Derecho-Project/cascade-cbdc.git
fi
if [ ! -d "$HOME/cascade" ]; then
    git clone https://github.com/Derecho-Project/cascade.git
fi
if [ ! -d "$HOME/derecho" ]; then
    git clone https://github.com/Derecho-Project/derecho.git
fi
if [ ! -d "$HOME/wanagent" ]; then
    git clone https://github.com/Derecho-Project/wanagent.git
fi

echo "[*] Updating system packages..."
sudo apt-get update

echo "[*] Installing system dependencies..."
sudo apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    wget \
    pkg-config \
    libfmt-dev \
    libreadline-dev \
    libboost-all-dev \
    ragel \
    libnuma-dev \
    libssl-dev \
    libibverbs-dev \
    librdmacm-dev \
    libspdlog-dev \
    pkg-config \
    libgzstream-dev zlib1g-dev \
    ca-certificates \
    libssl-dev \
    nlohmann-json3-dev

# Wanagent Install
cd wanagent
mkdir build
cd build
cmake ..
cmake --build .
sudo make install
cd ../../

# Spdlog install
install_spdlog() {
    local SPDLOG_VER="v1.13.0" # >= 1.12 required; bump if you want
    local PREFIX="/usr/local"

    echo "[*] Installing spdlog ${SPDLOG_VER} from source with -fPIC..."

    # If a bad static lib exists, it can get picked up first by CMake/linker.
    if [ -f "${PREFIX}/lib/libspdlog.a" ]; then
        echo "    removing ${PREFIX}/lib/libspdlog.a (likely non-PIC)"
        sudo rm -f "${PREFIX}/lib/libspdlog.a"
    fi

    if [ ! -d "$HOME/spdlog" ]; then
        git clone https://github.com/gabime/spdlog.git "$HOME/spdlog"
    fi
    cd "$HOME/spdlog"
    git fetch --tags -q
    git checkout -q "${SPDLOG_VER}"

    rm -rf build
    mkdir -p build
    cd build
    cmake .. -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_CXX_FLAGS="-fPIC"
    make -j"$(nproc)"
    sudo make install
    sudo ldconfig
}

install_spdlog
cd ../..

# Derecho install

cd derecho

need_cmd() { command -v "$1" >/dev/null 2>&1; }

# CMake: README wants 3.15.4+ (Ubuntu 18.04 default is too old).

echo "[*] Running Derecho prerequisite scripts..."
#chmod +x scripts/prerequisites/*.sh

# libfabric v1.12.1 (required, do NOT use distro version)
sudo -E ./scripts/prerequisites/install-libfabric.sh

# mutils + mutils-containers
sudo -E ./scripts/prerequisites/install-mutils.sh
sudo -E ./scripts/prerequisites/install-mutils-containers.sh

echo "[*] Refreshing linker cache..."
sudo ldconfig

echo "[✓] Derecho prerequisites installed."
echo "Derecho installation:"
mkdir -p build && cd build
cmake ..
cmake --build .
sudo make install

cd ../../

# Cascade Install

cd cascade
mkdir build
cd build

git checkout cascade_chain

echo "[*] Installing Python (optional)..."
sudo apt-get install -y python3 python3-dev python3-pip
pip3 install --upgrade pip
pip3 install pybind11

echo "[*] Installing Java (optional)..."
sudo apt-get install -y openjdk-11-jdk

echo "[*] Installing FUSE (optional)..."
sudo apt-get install -y libfuse3-dev

echo "[*] Installing rpclib via repo script..."
sudo bash "$HOME/cascade"/scripts/prerequisites/install-rpclib.sh

echo "[*] Installing Hyperscan via repo script..."
sudo bash "$HOME/cascade"/scripts/prerequisites/install-hyperscan.sh

echo "[*] Installing libwsong via repo script..."
sudo bash "$HOME/cascade"/scripts/prerequisites/install-libwsong.sh

echo "[*] Installing boolinq (optional)..."
if [ ! -d external/boolinq ]; then
    git clone https://github.com/k06a/boolinq.git external/boolinq
fi
sudo cp -r external/boolinq/include/boolinq /usr/local/include/

sudo ldconfig

echo "[✓] All Cascade prerequisites installed."
echo "Installing cascade"
cmake ..
cmake --build .
sudo make install

echo "Installing cascade-cbdc"
cd ../../cascade-cbdc
git checkout cascadechain-addition
mkdir build
cd build
cmake ..
cmake --build .
