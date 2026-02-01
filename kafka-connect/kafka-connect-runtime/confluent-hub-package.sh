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
# Builds and repackages the connector for Confluent Hub submission.
# Usage: ./confluent-hub-package.sh [TAG]
#   TAG: git tag to build (default: latest tag)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/../.."

# Determine tag
git fetch --tags 2>/dev/null || true
if [[ -n "${1:-}" ]]; then
    TAG="$1"
else
    TAG=$(git describe --tags "$(git rev-list --tags --max-count=1)")
    echo "Using latest tag: $TAG"
fi

git checkout "$TAG"

# Extract version from tag (apache-iceberg-X.Y.Z -> X.Y.Z)
VERSION="${TAG#apache-iceberg-}"

# Create version.txt to override gradle's SNAPSHOT versioning
echo "$VERSION" > "${PROJECT_ROOT}/version.txt"
trap "rm -f '${PROJECT_ROOT}/version.txt'" EXIT

# Build (clean first to remove old artifacts)
"${PROJECT_ROOT}/gradlew" clean :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip \
    -x test -x integrationTest --quiet

# Extract
DIST_DIR="${SCRIPT_DIR}/build/distributions"
MAIN_ZIP="${DIST_DIR}/iceberg-kafka-connect-runtime-${VERSION}.zip"
[[ -f "${MAIN_ZIP}" ]] || { echo "Distribution not found: ${MAIN_ZIP}"; exit 1; }
PACKAGE_NAME="apache-iceberg-kafka-connect-${VERSION}"
WORK_DIR="${SCRIPT_DIR}/build/confluent-hub-work"

rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
unzip -q "${MAIN_ZIP}" -d "${WORK_DIR}"
mv "${WORK_DIR}/iceberg-kafka-connect-runtime-${VERSION}" "${WORK_DIR}/${PACKAGE_NAME}"
PKG="${WORK_DIR}/${PACKAGE_NAME}"

# Update manifest owner
sed -i.bak 's/"username": "iceberg"/"username": "apache"/' "${PKG}/manifest.json"
rm -f "${PKG}/manifest.json.bak"

# Add README
cat > "${PKG}/doc/README.md" << EOF
# Apache Iceberg Sink Connector

A Kafka Connect sink connector for writing data from Apache Kafka into Apache Iceberg tables.

Documentation: https://iceberg.apache.org/docs/${VERSION}/kafka-connect/

License: Apache License 2.0
EOF

# Add example config
mkdir -p "${PKG}/etc"
cat > "${PKG}/etc/iceberg-sink.properties.example" << 'EOF'
# Iceberg Sink Connector - Example Configuration
# https://iceberg.apache.org/docs/latest/kafka-connect/
name=iceberg-sink
connector.class=org.apache.iceberg.connect.IcebergSinkConnector
tasks.max=1
topics=events
iceberg.tables=db.table_name
iceberg.catalog.type=rest
iceberg.catalog.uri=https://your-catalog-uri
iceberg.catalog.warehouse=your-warehouse
EOF

# Add logo
mkdir -p "${PKG}/assets"
cp "${SCRIPT_DIR}/src/main/resources/iceberg.png" "${PKG}/assets/"

# Security scan
docker run --rm -v "${PKG}:/scan:ro" aquasec/trivy rootfs --severity HIGH,CRITICAL /scan

# Create archive
OUTPUT="${DIST_DIR}/${PACKAGE_NAME}.zip"
(cd "${WORK_DIR}" && zip -rq "${OUTPUT}" "${PACKAGE_NAME}")
rm -rf "${WORK_DIR}"

echo "Created: ${OUTPUT}"
