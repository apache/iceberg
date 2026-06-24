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

set -euo pipefail

plan_output=""

run_plan() {
  local name="$1"
  shift
  local changed_files

  changed_files="$(mktemp)"
  plan_output="$(mktemp)"
  printf '%s\n' "$@" > "${changed_files}"

  GITHUB_EVENT_NAME=pull_request \
    CHANGED_FILES_FILE="${changed_files}" \
    GITHUB_OUTPUT="${plan_output}" \
    bash .github/scripts/plan-pr-ci.sh > "/tmp/${name}-plan.log" 2>&1

  rm -f "${changed_files}"
}

output_value() {
  local name="$1"
  sed -n "s/^${name}=//p" "${plan_output}" | head -n 1
}

assert_equals() {
  local expected="$1"
  local actual="$2"
  local message="$3"
  if [[ "${expected}" != "${actual}" ]]; then
    echo "Expected ${message} to be '${expected}', but was '${actual}'" >&2
    exit 1
  fi
}

assert_jq() {
  local filter="$1"
  local json="$2"
  local message="$3"
  if ! jq -e "${filter}" <<< "${json}" > /dev/null; then
    echo "Failed assertion: ${message}" >&2
    echo "${json}" >&2
    exit 1
  fi
}

assert_cve_consistent() {
  if [[ "$(output_value run_cve)" == "true" ]]; then
    assert_jq '.include | length > 0' "$(output_value cve_matrix)" "CVE matrix is non-empty when CVE runs"
  fi
}

run_plan spark41 spark/v4.1/spark/src/main/java/Example.java
assert_equals false "$(output_value full_matrix)" "Spark 4.1 full matrix"
assert_equals true "$(output_value run_spark)" "Spark 4.1 Spark run"
assert_equals false "$(output_value run_flink)" "Spark 4.1 Flink run"
assert_jq '.jvm == [17]' "$(output_value jvm_matrix)" "Spark 4.1 uses primary JVM"
assert_jq '.include | map(.spark) | unique == ["4.1"]' "$(output_value spark_matrix)" "Spark 4.1 matrix"
assert_cve_consistent

run_plan flink20 flink/v2.0/flink/src/main/java/Example.java
assert_equals false "$(output_value full_matrix)" "Flink 2.0 full matrix"
assert_equals true "$(output_value run_flink)" "Flink 2.0 Flink run"
assert_equals false "$(output_value run_spark)" "Flink 2.0 Spark run"
assert_jq '.jvm == [17]' "$(output_value jvm_matrix)" "Flink 2.0 uses primary JVM"
assert_jq '.include | map(.flink) | unique == ["2.0"]' "$(output_value flink_matrix)" "Flink 2.0 matrix"
assert_cve_consistent

run_plan core core/src/main/java/org/apache/iceberg/Example.java
assert_equals false "$(output_value full_matrix)" "Core full matrix"
assert_equals true "$(output_value run_java)" "Core Java run"
assert_equals true "$(output_value run_spark)" "Core Spark canary"
assert_equals true "$(output_value run_flink)" "Core Flink canary"
assert_equals false "$(output_value run_hive)" "Core Hive run"
assert_jq '.include | map(.spark) | unique == ["4.1"]' "$(output_value spark_matrix)" "Core Spark canary version"
assert_jq '.include | map(.flink) | unique == ["2.1"]' "$(output_value flink_matrix)" "Core Flink canary version"

run_plan format format/spec.md
assert_equals false "$(output_value full_matrix)" "Format full matrix"
assert_equals true "$(output_value run_java)" "Format Java run"
assert_equals true "$(output_value run_spark)" "Format Spark canary"
assert_equals true "$(output_value run_flink)" "Format Flink canary"

run_plan global gradle/libs.versions.toml
assert_equals true "$(output_value full_matrix)" "Global full matrix"
assert_equals true "$(output_value run_java)" "Global Java run"
assert_equals true "$(output_value run_spark)" "Global Spark run"
assert_equals true "$(output_value run_flink)" "Global Flink run"
assert_equals true "$(output_value run_hive)" "Global Hive run"
assert_equals true "$(output_value run_kafka)" "Global Kafka run"
assert_equals true "$(output_value run_delta)" "Global Delta run"
assert_equals true "$(output_value run_cve)" "Global CVE run"
assert_jq '.jvm == [17,21]' "$(output_value jvm_matrix)" "Global JVM matrix"
assert_cve_consistent

run_plan docs docs/README.md
assert_equals false "$(output_value full_matrix)" "Docs full matrix"
assert_equals false "$(output_value run_java)" "Docs Java run"
assert_equals false "$(output_value run_spark)" "Docs Spark run"
assert_equals false "$(output_value run_flink)" "Docs Flink run"
assert_equals false "$(output_value run_cve)" "Docs CVE run"

run_plan unknown new-module/src/main/java/Example.java
assert_equals true "$(output_value full_matrix)" "Unknown path full matrix"
assert_equals true "$(output_value run_java)" "Unknown path Java run"
assert_equals true "$(output_value run_spark)" "Unknown path Spark run"
assert_equals true "$(output_value run_flink)" "Unknown path Flink run"
assert_cve_consistent

run_plan old_spark spark/v3.4/spark-runtime/src/main/java/Example.java
assert_equals true "$(output_value full_matrix)" "Unknown Spark version full matrix"
assert_equals true "$(output_value run_cve)" "Unknown Spark version CVE run"
assert_cve_consistent

echo "plan-pr-ci tests passed"
