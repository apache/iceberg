#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Sets up libcudf from NVIDIA's Python wheels (no conda, no Python environment): downloads the
# libcudf wheels and their dependencies once into DIR, unpacks them, and exports CUDF_HOME and
# RMM_HOME (to build TornadoVM's cuDF shim) and LD_LIBRARY_PATH (to run it). For CUDA 13 it also
# unpacks NVIDIA's CUDA runtime and NVRTC wheels, so that only the GPU driver is needed.
#
#   source tornadovm/cudf-env.sh [DIR]       DIR defaults to ~/.iceberg-tornadovm/cudf
#
# CUDF_VERSION picks the release (default 26.8.*, the one this module is tested with);
# CUDF_PACKAGE the CUDA build: libcudf-cu13 by default (TornadoVM's released CUDA SDKs from 8.1.0
# link CUDA 13), libcudf-cu12 for a TornadoVM built from source against CUDA 12.

CUDF_VERSION="${CUDF_VERSION:-26.8.*}"
CUDF_PACKAGE="${CUDF_PACKAGE:-libcudf-cu13}"
CUDF_DIR="${1:-$HOME/.iceberg-tornadovm/cudf}"

if [ ! -s "$CUDF_DIR/ld_library_path" ]; then
    echo "Installing $CUDF_PACKAGE $CUDF_VERSION into $CUDF_DIR" >&2
    mkdir -p "$CUDF_DIR/wheels"
    python3 -m pip download --quiet --timeout 120 --retries 10 --only-binary=:all: --python-version 3.12 \
        --platform manylinux_2_28_x86_64 --platform manylinux_2_27_x86_64 --platform manylinux_2_17_x86_64 --platform manylinux2014_x86_64 \
        --extra-index-url https://pypi.nvidia.com -d "$CUDF_DIR/wheels" "$CUDF_PACKAGE==$CUDF_VERSION" \
        || { echo "cudf-env.sh: downloading the libcudf wheels failed" >&2; return 1 2>/dev/null || exit 1; }
    if [ "$CUDF_PACKAGE" = libcudf-cu13 ]; then
        python3 -m pip download --quiet --timeout 120 --retries 10 --only-binary=:all: --no-deps --python-version 3.12 \
            --platform manylinux_2_28_x86_64 --platform manylinux_2_17_x86_64 --platform manylinux2014_x86_64 \
            -d "$CUDF_DIR/wheels" "nvidia-cuda-runtime==13.*" "nvidia-cuda-nvrtc==13.*" \
            || { echo "cudf-env.sh: downloading the CUDA 13 runtime wheels failed" >&2; return 1 2>/dev/null || exit 1; }
    fi
    for wheel in "$CUDF_DIR"/wheels/*.whl; do
        python3 -m zipfile -e "$wheel" "$CUDF_DIR"
    done
    rm -rf "$CUDF_DIR/wheels"
    nvrtc=$(find "$CUDF_DIR" -name 'libnvrtc.so.13' | head -1)
    if [ -n "$nvrtc" ]; then
        ln -sf libnvrtc.so.13 "$(dirname "$nvrtc")/libnvrtc.so"
        ln -sf libcudart.so.13 "$(dirname "$nvrtc")/libcudart.so"
    fi
    find "$CUDF_DIR" -name '*.so*' -type f -printf '%h\n' | sort -u | paste -sd: > "$CUDF_DIR/ld_library_path"
fi

export CUDF_HOME="$CUDF_DIR/libcudf"
export RMM_HOME="$CUDF_DIR/librmm"
export LD_LIBRARY_PATH="$(cat "$CUDF_DIR/ld_library_path")${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
