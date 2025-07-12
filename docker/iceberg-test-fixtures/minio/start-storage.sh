#!/bin/bash
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

export MINIO_DOMAIN=localhost
export MINIO_ROOT_USER="${MINIO_ROOT_USER:-admin}"
export MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-password}"
export MINIO_DATA_DIR="${MINIO_DATA_DIR:-/data}"
export MINIO_PORT="${MINIO_PORT:-9000}"
export MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"

mkdir -p $MINIO_DATA_DIR

minio server "${MINIO_DATA_DIR}" \
  --address ":${MINIO_PORT}" \
  --console-address ":${MINIO_CONSOLE_PORT}" \
  > /tmp/minio.log 2>&1 &

# Wait for MinIO to start
echo "Waiting for MinIO to be ready..."
until curl -s "http://localhost:${MINIO_PORT}/minio/health/ready" > /dev/null; do
  sleep 1
done
echo "MinIO is up."

# Setup alias for AWS s3 client Virtual Hosted-Style access
echo "127.0.0.1 warehouse.localhost" >> /etc/hosts

# Create default bucket using mc
echo "Configuring mc and creating default bucket: warehouse..."
mc alias set minio "http://localhost:${MINIO_PORT}" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb minio/warehouse || true
mc policy set public minio/warehouse