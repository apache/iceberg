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
  source "${LIBS_DIR}/_gradle.sh"
  TEST_TMPDIR=$(mktemp -d)
}

teardown() {
  rm -rf "${TEST_TMPDIR}"
}

# ---- build_source_tarball ----

@test "build_source_tarball: dry-run does not invoke gradle or gpg" {
  DRY_RUN=1
  run build_source_tarball "${TEST_TMPDIR}" "apache-iceberg-1.10.0" "deadbeef" "1.10.0" ""
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run"* ]]
  [ ! -f "${TEST_TMPDIR}/version.txt" ]
  [ ! -f "${TEST_TMPDIR}/iceberg-build.properties" ]
  [ ! -f "${TEST_TMPDIR}/apache-iceberg-1.10.0.tar.gz" ]
}

# ---- stage_convenience_binaries ----

@test "stage_convenience_binaries: dry-run prints all expected gradle invocations" {
  DRY_RUN=1
  export NEXUS_USERNAME="testuser"
  export NEXUS_PASSWORD="testpass"
  run stage_convenience_binaries
  [ "$status" -eq 0 ]
  [[ "$output" == *"publishApachePublicationToMavenRepository"* ]]
  [[ "$output" == *"-Prelease"* ]]
  [[ "$output" == *"--no-parallel"* ]]
  [[ "$output" == *"--no-configuration-cache"* ]]
  [[ "$output" == *"-DscalaVersion=2.12"* ]]
  [[ "$output" == *"-DscalaVersion=2.13"* ]]
  [[ "$output" == *"-DflinkVersions=1.20,2.0,2.1"* ]]
  [[ "$output" == *"-DkafkaVersions=3"* ]]
  unset NEXUS_USERNAME NEXUS_PASSWORD
}

@test "stage_convenience_binaries: main matrix excludes Spark 4.x (which dropped Scala 2.12)" {
  DRY_RUN=1
  export NEXUS_USERNAME="testuser"
  export NEXUS_PASSWORD="testpass"
  run stage_convenience_binaries
  [ "$status" -eq 0 ]
  [[ "$output" == *"-DsparkVersions=3.4,3.5"* ]] \
    || { echo "expected primary-Scala main matrix to use only Spark 3.x"; printf '%s\n' "$output"; return 1; }
  # Inspect only the gradle invocation lines carrying the primary Scala. A
  # whole-output glob would falsely match Spark 4.x from the later 2.13 sweep.
  local line
  while IFS= read -r line; do
    [[ "${line}" != *"-DscalaVersion=2.12"* ]] && continue
    [[ "${line}" != *"sparkVersions="*"4.0"* ]] \
      || { echo "Spark 4.0 must not appear in primary-Scala invocation: ${line}"; return 1; }
    [[ "${line}" != *"sparkVersions="*"4.1"* ]] \
      || { echo "Spark 4.1 must not appear in primary-Scala invocation: ${line}"; return 1; }
  done <<< "$output"
  unset NEXUS_USERNAME NEXUS_PASSWORD
}

@test "stage_convenience_binaries: secondary-Scala sweep includes Spark 3.4" {
  DRY_RUN=1
  export NEXUS_USERNAME="testuser"
  export NEXUS_PASSWORD="testpass"
  run stage_convenience_binaries
  [[ "$output" == *":iceberg-spark:iceberg-spark-3.4_2.13:publishApachePublicationToMavenRepository"* ]]
  [[ "$output" == *":iceberg-spark:iceberg-spark-extensions-3.4_2.13:publishApachePublicationToMavenRepository"* ]]
  [[ "$output" == *":iceberg-spark:iceberg-spark-runtime-3.4_2.13:publishApachePublicationToMavenRepository"* ]]
  unset NEXUS_USERNAME NEXUS_PASSWORD
}

@test "stage_convenience_binaries: secondary-Scala sweep includes Spark 3.5" {
  DRY_RUN=1
  export NEXUS_USERNAME="testuser"
  export NEXUS_PASSWORD="testpass"
  run stage_convenience_binaries
  [[ "$output" == *":iceberg-spark:iceberg-spark-3.5_2.13:publishApachePublicationToMavenRepository"* ]]
  [[ "$output" == *":iceberg-spark:iceberg-spark-extensions-3.5_2.13:publishApachePublicationToMavenRepository"* ]]
  [[ "$output" == *":iceberg-spark:iceberg-spark-runtime-3.5_2.13:publishApachePublicationToMavenRepository"* ]]
  unset NEXUS_USERNAME NEXUS_PASSWORD
}

@test "stage_convenience_binaries: secondary-Scala sweep includes Spark 4.x (only published here)" {
  DRY_RUN=1
  export NEXUS_USERNAME="testuser"
  export NEXUS_PASSWORD="testpass"
  run stage_convenience_binaries
  [[ "$output" == *":iceberg-spark:iceberg-spark-4.0_2.13:publishApachePublicationToMavenRepository"* ]]
  [[ "$output" == *":iceberg-spark:iceberg-spark-extensions-4.0_2.13:publishApachePublicationToMavenRepository"* ]]
  [[ "$output" == *":iceberg-spark:iceberg-spark-runtime-4.0_2.13:publishApachePublicationToMavenRepository"* ]]
  [[ "$output" == *":iceberg-spark:iceberg-spark-4.1_2.13:publishApachePublicationToMavenRepository"* ]]
  [[ "$output" == *":iceberg-spark:iceberg-spark-extensions-4.1_2.13:publishApachePublicationToMavenRepository"* ]]
  [[ "$output" == *":iceberg-spark:iceberg-spark-runtime-4.1_2.13:publishApachePublicationToMavenRepository"* ]]
  unset NEXUS_USERNAME NEXUS_PASSWORD
}

@test "stage_convenience_binaries: passes credentials via ORG_GRADLE_PROJECT_* env vars (not in argv)" {
  DRY_RUN=1
  export NEXUS_USERNAME="visible-user"
  export NEXUS_PASSWORD="secret-password-xyz"
  run stage_convenience_binaries
  [[ "$output" != *"secret-password-xyz"* ]] \
    || { echo "password leaked into dry-run output"; printf '%s\n' "$output"; return 1; }
  [[ "$output" != *"-PmavenUser="* ]] \
    || { echo "should not pass -PmavenUser= on argv"; printf '%s\n' "$output"; return 1; }
  [[ "$output" != *"-PmavenPassword="* ]] \
    || { echo "should not pass -PmavenPassword= on argv"; printf '%s\n' "$output"; return 1; }
  unset NEXUS_USERNAME NEXUS_PASSWORD ORG_GRADLE_PROJECT_mavenUser ORG_GRADLE_PROJECT_mavenPassword
}

@test "stage_convenience_binaries: exports ORG_GRADLE_PROJECT_mavenUser/Password to the gradle invocation env" {
  DRY_RUN=1
  export NEXUS_USERNAME="release-mgr"
  export NEXUS_PASSWORD="trace-canary-1234"
  unset ORG_GRADLE_PROJECT_mavenUser ORG_GRADLE_PROJECT_mavenPassword

  run stage_convenience_binaries
  [ "$status" -eq 0 ]

  # In a sub-shell-style `run`, exports made by the function don't survive,
  # so we re-invoke directly to inspect environment side-effects.
  stage_convenience_binaries >/dev/null 2>&1
  [ "${ORG_GRADLE_PROJECT_mavenUser}" = "release-mgr" ]
  [ "${ORG_GRADLE_PROJECT_mavenPassword}" = "trace-canary-1234" ]
  unset NEXUS_USERNAME NEXUS_PASSWORD ORG_GRADLE_PROJECT_mavenUser ORG_GRADLE_PROJECT_mavenPassword
}

@test "stage_convenience_binaries: warns when NEXUS_USERNAME unset" {
  DRY_RUN=1
  unset NEXUS_USERNAME NEXUS_PASSWORD
  run stage_convenience_binaries
  [[ "$output" == *"NEXUS_USERNAME or NEXUS_PASSWORD not set"* ]]
}

# ---- verify_jdk_17 ----
#
# verify_jdk_17 parses the output of `java -version`. We stub `java` and
# `command` so we can exercise the parser without depending on the JDK
# installed in the test environment.

@test "verify_jdk_17: passes for OpenJDK 17 version string" {
  command() {
    if [[ "$1" == "-v" && "$2" == "java" ]]; then return 0; fi
    builtin command "$@"
  }
  java() {
    echo 'openjdk version "17.0.10" 2024-01-16' >&2
  }
  export -f command java
  run verify_jdk_17
  [ "$status" -eq 0 ]
}

@test "verify_jdk_17: fails for Java 11" {
  command() {
    if [[ "$1" == "-v" && "$2" == "java" ]]; then return 0; fi
    builtin command "$@"
  }
  java() {
    echo 'openjdk version "11.0.21" 2023-10-17' >&2
  }
  export -f command java
  run verify_jdk_17
  [ "$status" -ne 0 ]
  [[ "$output" == *"must be built with Java 17"* ]]
}

@test "verify_jdk_17: fails for Java 21" {
  command() {
    if [[ "$1" == "-v" && "$2" == "java" ]]; then return 0; fi
    builtin command "$@"
  }
  java() {
    echo 'openjdk version "21.0.2" 2024-01-16' >&2
  }
  export -f command java
  run verify_jdk_17
  [ "$status" -ne 0 ]
  [[ "$output" == *"must be built with Java 17"* ]]
}

@test "verify_jdk_17: handles 1.x version string for old JDKs" {
  command() {
    if [[ "$1" == "-v" && "$2" == "java" ]]; then return 0; fi
    builtin command "$@"
  }
  java() {
    echo 'java version "1.8.0_392"' >&2
  }
  export -f command java
  run verify_jdk_17
  [ "$status" -ne 0 ]
  [[ "$output" == *"must be built with Java 17"* ]]
}

@test "verify_jdk_17: fails when java is missing from PATH" {
  command() {
    if [[ "$1" == "-v" && "$2" == "java" ]]; then return 1; fi
    builtin command "$@"
  }
  export -f command
  run verify_jdk_17
  [ "$status" -ne 0 ]
  [[ "$output" == *"java not found"* ]]
}

# ---- drift guard: stage_convenience_binaries argv matches gradle.properties ----
#
# The convenience-binary publish matrix is sourced from gradle.properties
# at module-load time (see _constants.sh). This guard runs the dry-run
# path of stage_convenience_binaries and asserts the resulting Gradle
# argv carries exactly the matrix gradle.properties advertises, so we
# fail loudly if the parser ever drops or mis-trims a value.

@test "drift guard: stage_convenience_binaries publishes the matrix declared in gradle.properties" {
  DRY_RUN=1
  export NEXUS_USERNAME="testuser"
  export NEXUS_PASSWORD="testpass"

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

  local gp_default_scala gp_known_flink gp_known_spark gp_known_kafka gp_known_scala
  gp_default_scala=$(_from_gp defaultScalaVersion)
  gp_known_scala=$(_from_gp knownScalaVersions)
  gp_known_flink=$(_from_gp knownFlinkVersions)
  gp_known_spark=$(_from_gp knownSparkVersions)
  gp_known_kafka=$(_from_gp knownKafkaVersions)

  run stage_convenience_binaries
  [ "$status" -eq 0 ]

  # Main pass: primary Scala + full Flink/Kafka + Spark < 4 only.
  local primary_spark=""
  local v
  for v in $(echo "${gp_known_spark}" | tr ',' ' '); do
    if [[ "${v%%.*}" -lt 4 ]]; then
      if [[ -n "${primary_spark}" ]]; then primary_spark="${primary_spark},"; fi
      primary_spark="${primary_spark}${v}"
    fi
  done

  [[ "$output" == *"-DscalaVersion=${gp_default_scala}"* ]] \
    || { echo "main matrix is missing -DscalaVersion=${gp_default_scala}"; printf '%s\n' "$output"; return 1; }
  [[ "$output" == *"-DflinkVersions=${gp_known_flink}"* ]] \
    || { echo "main matrix is missing -DflinkVersions=${gp_known_flink}"; printf '%s\n' "$output"; return 1; }
  [[ "$output" == *"-DsparkVersions=${primary_spark}"* ]] \
    || { echo "main matrix should use primary-Scala Spark subset (${primary_spark}); got"; printf '%s\n' "$output"; return 1; }
  [[ "$output" == *"-DkafkaVersions=${gp_known_kafka}"* ]] \
    || { echo "main matrix is missing -DkafkaVersions=${gp_known_kafka}"; printf '%s\n' "$output"; return 1; }

  # Secondary-Scala sweep: every Spark version in knownSparkVersions
  # (including 4.x) must get an explicit invocation.
  local secondary_scala
  secondary_scala=$(echo "${gp_known_scala}" | tr ',' '\n' | grep -v "^${gp_default_scala}$" | head -n1)
  [ -n "${secondary_scala}" ] \
    || { echo "no secondary Scala available in knownScalaVersions=${gp_known_scala}"; return 1; }

  for v in $(echo "${gp_known_spark}" | tr ',' ' '); do
    [[ "$output" == *":iceberg-spark:iceberg-spark-${v}_${secondary_scala}:publishApachePublicationToMavenRepository"* ]] \
      || { echo "Spark ${v}_${secondary_scala} missing from secondary-Scala sweep"; printf '%s\n' "$output"; return 1; }
    [[ "$output" == *":iceberg-spark:iceberg-spark-extensions-${v}_${secondary_scala}:publishApachePublicationToMavenRepository"* ]] \
      || { echo "Spark extensions ${v}_${secondary_scala} missing from secondary-Scala sweep"; printf '%s\n' "$output"; return 1; }
    [[ "$output" == *":iceberg-spark:iceberg-spark-runtime-${v}_${secondary_scala}:publishApachePublicationToMavenRepository"* ]] \
      || { echo "Spark runtime ${v}_${secondary_scala} missing from secondary-Scala sweep"; printf '%s\n' "$output"; return 1; }
  done

  unset NEXUS_USERNAME NEXUS_PASSWORD
}

@test "drift guard: Spark 4.x is never combined with the primary Scala on argv" {
  DRY_RUN=1
  export NEXUS_USERNAME="testuser"
  export NEXUS_PASSWORD="testpass"

  run stage_convenience_binaries
  [ "$status" -eq 0 ]

  # Walk every Gradle invocation line in the dry-run output. For each one,
  # if it carries -DscalaVersion=2.12 (the primary Scala), assert it does
  # not also carry any -DsparkVersions=...4.x entry. This catches future
  # regressions where SPARK_VERSIONS_PRIMARY_SCALA accidentally pulls in
  # Spark 4+.
  local line
  while IFS= read -r line; do
    if [[ "${line}" == *"-DscalaVersion=2.12"* ]]; then
      [[ "${line}" != *"sparkVersions="*"4.0"* ]] \
        || { echo "primary Scala invocation must not include Spark 4.0: ${line}"; return 1; }
      [[ "${line}" != *"sparkVersions="*"4.1"* ]] \
        || { echo "primary Scala invocation must not include Spark 4.1: ${line}"; return 1; }
    fi
  done <<< "$output"

  unset NEXUS_USERNAME NEXUS_PASSWORD
}
