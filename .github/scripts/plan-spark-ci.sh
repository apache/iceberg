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

add_selected_spark_version() {
  local value="$1"
  if ! value_in_list "${value}" "${selected_spark_versions[@]:-}"; then
    selected_spark_versions+=("${value}")
  fi
}

spark_scala_versions() {
  local spark_version="$1"
  if [[ "${spark_version}" == "3.5" ]]; then
    printf '%s\n' "2.12" "2.13"
  else
    printf '%s\n' "2.13"
  fi
}

select_spark_versions() {
  local file
  local spark_version
  full_spark_matrix=false
  selected_spark_versions=()

  if [[ "${GITHUB_EVENT_NAME:-}" != "pull_request" || "${FULL_CI_LABEL:-false}" == "true" ]]; then
    full_spark_matrix=true
    selected_spark_versions=("${all_spark_versions[@]}")
    return
  fi

  read_pull_request_files
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

jvm_versions() {
  jq -r '.jvm[]' <<< "${JVM_MATRIX:?JVM_MATRIX must be set by the workflow}"
}

spark_matrix() {
  local spark_version
  local scala_version
  local test_group
  local jvm
  local -a rows=()

  select_spark_versions

  while IFS= read -r jvm; do
    for spark_version in "${selected_spark_versions[@]}"; do
      while IFS= read -r scala_version; do
        for test_group in core extensions; do
          rows+=("${jvm}|${spark_version}|${scala_version}|${test_group}")
        done
      done < <(spark_scala_versions "${spark_version}")
    done
  done < <(jvm_versions)

  printf '%s\n' "${rows[@]}" \
    | jq -R 'split("|") | {jvm: .[0], spark: .[1], scala: .[2], tests: .[3]}' \
    | jq -s -c '{include: .}'
}

echo "spark_matrix=$(spark_matrix)" >> "${GITHUB_OUTPUT}"
