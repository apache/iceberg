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

trap cleanup EXIT

# Parse args
if [[ "${1:-}" == "--smoke" ]]; then
  SMOKE=true
else
  SMOKE=false
fi

if [[ "${SMOKE}" == true ]]; then
  log "Running smoke test (minimal verification)"
fi

log "Starting Docker Compose services..."
docker compose -f "${COMPOSE_FILE}" up -d
sleep 10  # Initial wait for startup

# Configure MinIO alias
docker run --rm -v "${SCRIPT_DIR}:/scripts" \
  minio/mc alias set "${MINIO_ALIAS}" "${MINIO_URL}" minioadmin minioadmin || true

# Verify bucket exists (from create-bucket service)
log "Verifying bucket..."
if ! docker run --rm -v "${SCRIPT_DIR}:/scripts" \
  minio/mc ls "${MINIO_ALIAS}/${BUCKET}" >/dev/null 2>&1; then
  error "Bucket ${BUCKET} not created"
fi

# Wait for Kafka Connect to be healthy
log "Waiting for Kafka Connect..."
timeout=60
for i in $(seq 1 $timeout); do
  if curl -f "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    log "Kafka Connect ready"
    break
  fi
  sleep 1
done
[[ $i -eq $timeout ]] && error "Kafka Connect not ready after ${timeout}s"

# Create topic if not exists (using Kafka container)
log "Creating Kafka topic ${TOPIC}..."
docker exec kafka kafka-topics --create --topic "${TOPIC}" --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true

# Deploy minimal Iceberg sink connector
log "Deploying ${CONNECTOR_NAME} connector..."
cat > /tmp/connector.json << EOF
{
  "name": "${CONNECTOR_NAME}",
  "config": {
    "connector.class": "org.apache.iceberg.kafka.connect.IcebergSinkConnector",
    "topics": "${TOPIC}",
    "iceberg.catalog.type": "rest",
    "iceberg.rest.uri": "http://iceberg:8181",
    "iceberg.tables": "${TOPIC}",
    "iceberg.tables.auto-create": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "tasks.max": "1"
  }
}
EOF

curl -X POST -H "Content-Type: application/json" \
  --data @/tmp/connector.json "${CONNECT_URL}/connectors" || error "Failed to deploy connector"

# Produce test message
log "Producing test message to ${TOPIC}..."
echo "${TEST_DATA}" | docker exec -i kafka kafka-console-producer \
  --topic "${TOPIC}" --bootstrap-server localhost:9092 >/dev/null 2>&1

# Wait for sink to process
sleep 5

# Verify data in Iceberg table (check metadata files in S3)
log "Verifying data in Iceberg table..."
TABLE_PATH="${BUCKET}/warehouse/${TOPIC}"
if ! docker run --rm -v "${SCRIPT_DIR}:/scripts" \
  minio/mc ls --recursive "${MINIO_ALIAS}/${TABLE_PATH}/metadata/" | grep -q "v1.metadata.json"; then
  error "No Iceberg metadata created for table ${TOPIC}"
fi



log "Test passed! Stack is healthy."
