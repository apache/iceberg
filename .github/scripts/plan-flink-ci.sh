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

known_flink_versions="$(gradle_property knownFlinkVersions)"
IFS=',' read -r -a all_flink_versions <<< "${known_flink_versions}"

add_selected_flink_version() {
  local value="$1"
  if ! value_in_list "${value}" "${selected_flink_versions[@]:-}"; then
    selected_flink_versions+=("${value}")
  fi
}

select_flink_versions() {
  local file
  local flink_version
  full_flink_matrix=false
  selected_flink_versions=()

  if [[ "${GITHUB_EVENT_NAME:-}" != "pull_request" ]]; then
    full_flink_matrix=true
    selected_flink_versions=("${all_flink_versions[@]}")
    return
  fi

  read_pull_request_files
  if [[ ${#changed_files[@]} -eq 0 ]]; then
    full_flink_matrix=true
    selected_flink_versions=("${all_flink_versions[@]}")
    return
  fi

  for file in "${changed_files[@]}"; do
    if [[ "${file}" =~ ^flink/v([^/]+)/ ]]; then
      flink_version="${BASH_REMATCH[1]}"
      if value_in_list "${flink_version}" "${all_flink_versions[@]}"; then
        add_selected_flink_version "${flink_version}"
      else
        full_flink_matrix=true
        selected_flink_versions=("${all_flink_versions[@]}")
        return
      fi
    else
      full_flink_matrix=true
      selected_flink_versions=("${all_flink_versions[@]}")
      return
    fi
  done
}

jvm_versions() {
  if [[ "${GITHUB_EVENT_NAME:-}" == "pull_request" ]]; then
    printf '%s\n' 17
  else
    printf '%s\n' 17 21
  fi
}

flink_matrix() {
  local flink_version
  local jvm
  local -a rows=()

  select_flink_versions

  while IFS= read -r jvm; do
    for flink_version in "${selected_flink_versions[@]}"; do
      rows+=("${jvm}|${flink_version}")
    done
  done < <(jvm_versions)

  printf '%s\n' "${rows[@]}" \
    | jq -R 'split("|") | {jvm: .[0], flink: .[1]}' \
    | jq -s -c '{include: .}'
}

echo "flink_matrix=$(flink_matrix)" >> "${GITHUB_OUTPUT}"
