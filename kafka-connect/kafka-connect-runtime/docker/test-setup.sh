#!/usr/bin/env bash

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
# specific language governed permissions and limitations
# under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yaml"
CONNECT_URL="http://localhost:8083"
MINIO_ALIAS="minio"
MINIO_URL="http://localhost:9000"
BUCKET="bucket"
WAREHOUSE="s3://bucket/warehouse/"
TOPIC="test-topic"
CONNECTOR_NAME="test-iceberg-sink"
TEST_DATA='{"id": 1, "name": "test-record"}'


# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log() {
  echo -e "${GREEN}[INFO]${NC} $1"
}

error() {
  echo -e "${RED}[ERROR]${NC} $1" >&2
  exit 1
}

cleanup() {
  log "Cleaning up..."
  docker compose -f "${COMPOSE_FILE}" down -v || true
  docker run -it --rm minio/mc alias rm "${MINIO_ALIAS}" >/dev/null 2>&1 || true
}
