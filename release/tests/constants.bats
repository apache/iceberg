#!/usr/bin/env bats
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

setup() {
  load test_helper/common
  source "${LIBS_DIR}/_constants.sh"
}

@test "TAG_PREFIX is apache-iceberg-" {
  [ "$TAG_PREFIX" = "apache-iceberg-" ]
}

@test "BRANCH_SUFFIX is .x" {
  [ "$BRANCH_SUFFIX" = ".x" ]
}

@test "APACHE_DIST_URL has correct default" {
  [ "$APACHE_DIST_URL" = "https://dist.apache.org/repos/dist" ]
}

@test "APACHE_DIST_DEV_PATH is /dev/iceberg" {
  [ "$APACHE_DIST_DEV_PATH" = "/dev/iceberg" ]
}

@test "APACHE_DIST_RELEASE_PATH is /release/iceberg" {
  [ "$APACHE_DIST_RELEASE_PATH" = "/release/iceberg" ]
}

@test "NEXUS_BASE_URL has correct default" {
  [ "$NEXUS_BASE_URL" = "https://repository.apache.org/service/local" ]
}

@test "NEXUS_STAGING_PROFILE is org.apache.iceberg" {
  [ "$NEXUS_STAGING_PROFILE" = "org.apache.iceberg" ]
}

@test "DRY_RUN defaults to 1" {
  [ "$DRY_RUN" = "1" ]
}

@test "VERSION_REGEX matches semver components" {
  [[ "1.10.0" =~ ^${VERSION_REGEX}$ ]]
  [ "${BASH_REMATCH[1]}" = "1" ]
  [ "${BASH_REMATCH[2]}" = "10" ]
  [ "${BASH_REMATCH[3]}" = "0" ]
}

@test "VERSION_REGEX_GIT_TAG matches RC tag" {
  [[ "apache-iceberg-1.10.0-rc3" =~ ${VERSION_REGEX_GIT_TAG} ]]
  [ "${BASH_REMATCH[1]}" = "1" ]
  [ "${BASH_REMATCH[4]}" = "3" ]
}

@test "VERSION_REGEX_GIT_TAG rejects final tag" {
  [[ ! "apache-iceberg-1.10.0" =~ ${VERSION_REGEX_GIT_TAG} ]]
}

@test "VERSION_REGEX_FINAL_TAG matches final tag" {
  [[ "apache-iceberg-1.10.0" =~ ${VERSION_REGEX_FINAL_TAG} ]]
}

@test "VERSION_REGEX_FINAL_TAG rejects RC tag" {
  [[ ! "apache-iceberg-1.10.0-rc3" =~ ${VERSION_REGEX_FINAL_TAG} ]]
}

@test "BRANCH_VERSION_REGEX matches 1.10.x" {
  [[ "1.10.x" =~ ${BRANCH_VERSION_REGEX} ]]
  [ "${BASH_REMATCH[1]}" = "1" ]
  [ "${BASH_REMATCH[2]}" = "10" ]
}

@test "BRANCH_VERSION_REGEX rejects 1.10.0" {
  [[ ! "1.10.0" =~ ${BRANCH_VERSION_REGEX} ]]
}

@test "BRANCH_VERSION_REGEX rejects iceberg-1.10.x" {
  [[ ! "iceberg-1.10.x" =~ ${BRANCH_VERSION_REGEX} ]]
}

@test "BRANCH_VERSION_REGEX rejects release/1.10.x" {
  [[ ! "release/1.10.x" =~ ${BRANCH_VERSION_REGEX} ]]
}

@test "GITHUB_REPO has correct default" {
  [ "$GITHUB_REPO" = "apache/iceberg" ]
}

@test "Convenience binary matrix is sourced from gradle.properties" {
  # Re-parse gradle.properties independently of _constants.sh and assert
  # _constants.sh exposes the same values. If _parse_gradle_property
  # regresses (e.g. trims wrong) this test fails before any release runs.
  local gp
  gp=$(cd "${LIBS_DIR}/../.." && pwd)/gradle.properties
  [ -f "${gp}" ] \
    || { echo "gradle.properties not found at ${gp}"; return 1; }

  _from_gp() {
    awk -F= -v k="$1" '
      /^[[:space:]]*#/ { next }
      {
        sub(/^[[:space:]]+/, "", $1)
        if ($1 == "systemProp." k || $1 == k) {
          sub(/^[^=]*=/, "")
          sub(/[[:space:]]+$/, "")
          sub(/^[[:space:]]+/, "")
          print
          exit
        }
      }' "${gp}"
  }

  [ "${KNOWN_SCALA_VERSIONS}" = "$(_from_gp knownScalaVersions)" ]
  [ "${KNOWN_FLINK_VERSIONS}" = "$(_from_gp knownFlinkVersions)" ]
  [ "${KNOWN_SPARK_VERSIONS}" = "$(_from_gp knownSparkVersions)" ]
  [ "${KNOWN_KAFKA_VERSIONS}" = "$(_from_gp knownKafkaVersions)" ]
  [ "${SCALA_PRIMARY}"        = "$(_from_gp defaultScalaVersion)" ]
}

@test "SCALA_SECONDARY is the non-default entry of KNOWN_SCALA_VERSIONS" {
  [[ ",${KNOWN_SCALA_VERSIONS}," == *",${SCALA_SECONDARY},"* ]]
  [ "${SCALA_SECONDARY}" != "${SCALA_PRIMARY}" ]
}

@test "SPARK_VERSIONS_PRIMARY_SCALA filters KNOWN_SPARK_VERSIONS to Spark 3.x (Spark 4+ dropped Scala 2.12)" {
  IFS=',' read -ra known <<< "${KNOWN_SPARK_VERSIONS}"
  IFS=',' read -ra primary <<< "${SPARK_VERSIONS_PRIMARY_SCALA}"

  for v in "${primary[@]}"; do
    local seen=0 k
    for k in "${known[@]}"; do
      if [[ "${k}" = "${v}" ]]; then seen=1; break; fi
    done
    [ "${seen}" -eq 1 ] \
      || { echo "${v} not in KNOWN_SPARK_VERSIONS=${KNOWN_SPARK_VERSIONS}"; return 1; }
    [ "${v%%.*}" -lt 4 ] \
      || { echo "${v} should not be in SPARK_VERSIONS_PRIMARY_SCALA (major >= 4 dropped Scala 2.12)"; return 1; }
  done

  # Every Spark 3.x in KNOWN_SPARK_VERSIONS must appear in the primary set.
  for k in "${known[@]}"; do
    if [[ "${k%%.*}" -lt 4 ]]; then
      [[ ",${SPARK_VERSIONS_PRIMARY_SCALA}," == *",${k},"* ]] \
        || { echo "Spark ${k} missing from SPARK_VERSIONS_PRIMARY_SCALA=${SPARK_VERSIONS_PRIMARY_SCALA}"; return 1; }
    fi
  done
}

@test "SPARK_VERSIONS_SECONDARY_SCALA covers every Spark version in KNOWN_SPARK_VERSIONS" {
  # Every Spark version in the known matrix must be in the secondary
  # sweep: Spark 3.x adds a 2.13 variant alongside 2.12, and Spark 4.x is
  # published only via this pass.
  [ "${SPARK_VERSIONS_SECONDARY_SCALA}" = "${KNOWN_SPARK_VERSIONS}" ]
}

@test "_parse_gradle_property: returns trimmed value for systemProp.* keys" {
  TMP=$(mktemp)
  cat >"${TMP}" <<'EOF'
# leading comment
   systemProp.knownSparkVersions = 3.4,3.5,4.0,4.1
systemProp.defaultScalaVersion=2.12
plain.key=hello
EOF
  [ "$(_parse_gradle_property knownSparkVersions "${TMP}")" = "3.4,3.5,4.0,4.1" ]
  [ "$(_parse_gradle_property defaultScalaVersion "${TMP}")" = "2.12" ]
  [ "$(_parse_gradle_property plain.key "${TMP}")" = "hello" ]
  rm -f "${TMP}"
}

@test "_parse_gradle_property: returns nonzero for missing key" {
  TMP=$(mktemp)
  cat >"${TMP}" <<'EOF'
systemProp.knownSparkVersions=3.4
EOF
  run _parse_gradle_property doesNotExist "${TMP}"
  [ "$status" -ne 0 ]
  rm -f "${TMP}"
}

@test "_parse_gradle_property: returns nonzero when file missing" {
  run _parse_gradle_property anyKey /nonexistent/path/gradle.properties
  [ "$status" -ne 0 ]
}
