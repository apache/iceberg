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

source .github/scripts/ci-plan-common.sh

known_spark_versions="$(gradle_property knownSparkVersions)"
IFS=',' read -r -a all_spark_versions <<< "${known_spark_versions}"
known_scala_versions="$(gradle_property knownScalaVersions)"
IFS=',' read -r -a all_scala_versions <<< "${known_scala_versions}"
unsupported_spark_scala_versions="$(gradle_property unsupportedSparkScalaVersions)"

if [[ ${#all_spark_versions[@]} -eq 0 || -z "${all_spark_versions[0]}" ]]; then
  echo "knownSparkVersions must not be empty" >&2
  exit 1
fi

if [[ ${#all_scala_versions[@]} -eq 0 || -z "${all_scala_versions[0]}" ]]; then
  echo "knownScalaVersions must not be empty" >&2
  exit 1
fi

add_selected_spark_version() {
  local value="$1"
  if ! value_in_list "${value}" "${selected_spark_versions[@]:-}"; then
    selected_spark_versions+=("${value}")
  fi
}

select_spark_versions() {
  local file
  local spark_version
  full_spark_matrix=false
  selected_spark_versions=()

  if [[ "${GITHUB_EVENT_NAME:-}" != "pull_request" ]]; then
    full_spark_matrix=true
    selected_spark_versions=("${all_spark_versions[@]}")
    return
  fi

  read_changed_files
  if [[ ${#changed_files[@]} -eq 0 ]]; then
    full_spark_matrix=true
    selected_spark_versions=("${all_spark_versions[@]}")
    return
  fi

  for file in "${changed_files[@]}"; do
    if [[ "${file}" =~ ^spark/v([^/]+)/ ]]; then
      spark_version="${BASH_REMATCH[1]}"
      if value_in_list "${spark_version}" "${all_spark_versions[@]}"; then
        add_selected_spark_version "${spark_version}"
      else
        full_spark_matrix=true
        selected_spark_versions=("${all_spark_versions[@]}")
        return
      fi
    else
      full_spark_matrix=true
      selected_spark_versions=("${all_spark_versions[@]}")
      return
    fi
  done
}

spark_scala_excludes() {
  local entry
  local scala_version
  local scala_versions
  local spark_version

  if [[ -z "${unsupported_spark_scala_versions}" ]]; then
    return
  fi

  IFS=';' read -r -a entries <<< "${unsupported_spark_scala_versions}"
  for entry in "${entries[@]}"; do
    spark_version="${entry%%:*}"
    scala_versions="${entry#*:}"
    if [[ -z "${spark_version}" || -z "${scala_versions}" || "${spark_version}" == "${scala_versions}" ]]; then
      echo "Invalid unsupportedSparkScalaVersions entry: ${entry}" >&2
      exit 1
    fi

    if ! value_in_list "${spark_version}" "${all_spark_versions[@]}"; then
      echo "Unknown Spark version in unsupportedSparkScalaVersions: ${spark_version}" >&2
      exit 1
    fi

    IFS=',' read -r -a entry_scala_versions <<< "${scala_versions}"
    for scala_version in "${entry_scala_versions[@]}"; do
      if ! value_in_list "${scala_version}" "${all_scala_versions[@]}"; then
        echo "Unknown Scala version in unsupportedSparkScalaVersions: ${scala_version}" >&2
        exit 1
      fi

      if value_in_list "${spark_version}" "${selected_spark_versions[@]}"; then
        printf '%s|%s\n' "${spark_version}" "${scala_version}"
      fi
    done
  done
}

select_spark_versions

if [[ ${#selected_spark_versions[@]} -eq 0 ]]; then
  echo "Spark CI planner selected no Spark versions" >&2
  exit 1
fi

spark_versions_json="$({
  for spark_version in "${selected_spark_versions[@]}"; do
    printf '%s\n' "${spark_version}"
  done
} | jq -R . | jq -s -c .
)"

scala_versions_json="$({
  for scala_version in "${all_scala_versions[@]}"; do
    printf '%s\n' "${scala_version}"
  done
} | jq -R . | jq -s -c .
)"

matrix_excludes_json="$(
  spark_scala_excludes \
    | jq -R 'split("|") | {spark: .[0], scala: .[1]}' \
    | jq -s -c '. + [{"event_name":"pull_request","jvm":21}]'
)"

{
  echo "spark_versions=${spark_versions_json}"
  echo "scala_versions=${scala_versions_json}"
  echo "matrix_excludes=${matrix_excludes_json}"
} >> "${GITHUB_OUTPUT}"
