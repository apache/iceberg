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

# Constants are referenced from sibling scripts via `source`, so shellcheck
# can't see their use sites when analyzing this file in isolation.
# shellcheck disable=SC2034
[[ -n "${_CONSTANTS_LOADED:-}" ]] && return 0 2>/dev/null || true
_CONSTANTS_LOADED=1

# Tag prefix for git tags. Iceberg uses "apache-iceberg-X.Y.Z" and
# "apache-iceberg-X.Y.Z-rcN".
TAG_PREFIX="apache-iceberg-"

# Branch suffix convention. Iceberg uses bare "X.Y.x" branches
# (e.g. "1.10.x") for patch releases. New minor versions are typically
# cut from main.
BRANCH_SUFFIX=".x"

# Apache distribution SVN paths.
APACHE_DIST_URL=${APACHE_DIST_URL:-"https://dist.apache.org/repos/dist"}
APACHE_DIST_DEV_PATH="/dev/iceberg"
APACHE_DIST_RELEASE_PATH="/release/iceberg"

# Apache Nexus staging API.
NEXUS_BASE_URL=${NEXUS_BASE_URL:-"https://repository.apache.org/service/local"}
NEXUS_STAGING_GROUP_URL="https://repository.apache.org/content/groups/staging/org/apache/iceberg/"

# Iceberg Maven coordinates group used to locate the open Nexus staging repo.
NEXUS_STAGING_PROFILE="org.apache.iceberg"

# Default to dry-run so a misconfigured workflow cannot accidentally publish.
DRY_RUN=${DRY_RUN:-1}

# Version regexes.
VERSION_REGEX="([0-9]+)\.([0-9]+)\.([0-9]+)"
VERSION_REGEX_GIT_TAG="^${TAG_PREFIX}${VERSION_REGEX}-rc([0-9]+)$"
VERSION_REGEX_FINAL_TAG="^${TAG_PREFIX}${VERSION_REGEX}$"
BRANCH_VERSION_REGEX="^([0-9]+)\.([0-9]+)\.x$"

GITHUB_REPO=${GITHUB_REPOSITORY:-"apache/iceberg"}

# ---------------------------------------------------------------------------
# Convenience binary build matrix
# ---------------------------------------------------------------------------
#
# Sourced directly from gradle.properties so the release pipeline cannot
# drift from the build's own notion of supported versions. Update
# gradle.properties; this script picks up the change automatically.
GRADLE_PROPERTIES_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/gradle.properties"

# Reads a single property from gradle.properties. Accepts either
# `<key>=<value>` or `systemProp.<key>=<value>` forms; comments and leading
# whitespace are tolerated. Echoes the trimmed value on success; returns 1
# if the key is absent or the file is unreadable.
function _parse_gradle_property {
  local key="$1"
  local file="${2:-${GRADLE_PROPERTIES_FILE}}"
  if [[ ! -r "${file}" ]]; then
    return 1
  fi

  local line
  line=$(grep -E "^[[:space:]]*(systemProp\.)?${key}[[:space:]]*=" "${file}" | head -n1)
  if [[ -z "${line}" ]]; then
    return 1
  fi

  local value="${line#*=}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s\n' "${value}"
}

# Like _parse_gradle_property but exits if the key is missing or empty.
# Used at module-load time to fail fast on a misconfigured build.
function _require_gradle_property {
  local key="$1"
  local value
  if ! value=$(_parse_gradle_property "${key}"); then
    echo "ERROR: required key '${key}' not found in ${GRADLE_PROPERTIES_FILE}" >&2
    exit 1
  fi
  if [[ -z "${value}" ]]; then
    echo "ERROR: key '${key}' in ${GRADLE_PROPERTIES_FILE} has empty value" >&2
    exit 1
  fi
  printf '%s\n' "${value}"
}

# Full known matrix mirrored from systemProp.knownXxxVersions.
KNOWN_SCALA_VERSIONS=$(_require_gradle_property knownScalaVersions)
KNOWN_FLINK_VERSIONS=$(_require_gradle_property knownFlinkVersions)
KNOWN_SPARK_VERSIONS=$(_require_gradle_property knownSparkVersions)
KNOWN_KAFKA_VERSIONS=$(_require_gradle_property knownKafkaVersions)

# Primary Scala for the main publish pass: the build's default Scala.
SCALA_PRIMARY=$(_require_gradle_property defaultScalaVersion)

# Secondary Scala = the other entry in knownScalaVersions. The release
# pipeline runs a follow-up Spark-only pass against this Scala for the
# Spark versions that ship dual-Scala builds.
SCALA_SECONDARY=""
for _v in $(echo "${KNOWN_SCALA_VERSIONS}" | tr ',' ' '); do
  if [[ "${_v}" != "${SCALA_PRIMARY}" ]]; then
    SCALA_SECONDARY="${_v}"
    break
  fi
done
unset _v
if [[ -z "${SCALA_SECONDARY}" ]]; then
  echo "ERROR: knownScalaVersions (${KNOWN_SCALA_VERSIONS}) must contain a non-default entry to use as the secondary Scala" >&2
  exit 1
fi

# Spark versions to include in the primary-Scala main publish pass. Spark
# 4.x dropped Scala 2.12 support, so a `-DscalaVersion=2.12 -DsparkVersions=4.x`
# invocation either fails the build or silently ignores the requested
# Scala (Iceberg's spark/v4.x/build.gradle hardcodes scalaVersion='2.13'
# internally). Filter Spark to those versions that actually support the
# primary Scala. Today: Spark X.Y where X < 4.
SPARK_VERSIONS_PRIMARY_SCALA=""
for _v in $(echo "${KNOWN_SPARK_VERSIONS}" | tr ',' ' '); do
  if [[ "${_v%%.*}" -lt 4 ]]; then
    if [[ -n "${SPARK_VERSIONS_PRIMARY_SCALA}" ]]; then
      SPARK_VERSIONS_PRIMARY_SCALA="${SPARK_VERSIONS_PRIMARY_SCALA},"
    fi
    SPARK_VERSIONS_PRIMARY_SCALA="${SPARK_VERSIONS_PRIMARY_SCALA}${_v}"
  fi
done
unset _v

# Spark versions to publish against the secondary Scala. Every Spark
# version in the known matrix ships a Scala 2.13 build, so the secondary
# pass covers all of them: Spark 3.x to add the 2.13 variant alongside
# 2.12, and Spark 4.x because the primary pass intentionally excludes it.
SPARK_VERSIONS_SECONDARY_SCALA="${KNOWN_SPARK_VERSIONS}"
