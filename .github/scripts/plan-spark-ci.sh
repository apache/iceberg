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

# Spark version directories are matched against systemProp.knownSparkVersions in gradle.properties.
known_spark_versions="$(
  awk -F= '$1 == "systemProp.knownSparkVersions" {print substr($0, index($0, "=") + 1); exit}' \
    gradle.properties
)"
IFS=',' read -r -a all_spark_versions <<< "${known_spark_versions}"

if [[ ${#all_spark_versions[@]} -eq 0 || -z "${all_spark_versions[0]}" ]]; then
  echo "knownSparkVersions must not be empty" >&2
  exit 1
fi

for spark_version in "${all_spark_versions[@]}"; do
  if [[ ! "${spark_version}" =~ ^[0-9]+(\.[0-9]+)*$ ]]; then
    echo "Invalid Spark version in knownSparkVersions: ${spark_version}" >&2
    exit 1
  fi
done

json_array() {
  printf '%s\n' "$@" | jq -R . | jq -s -c .
}

known_spark_version() {
  local value="$1"
  local spark_version

  for spark_version in "${all_spark_versions[@]}"; do
    if [[ "${spark_version}" == "${value}" ]]; then
      return 0
    fi
  done

  return 1
}

select_changed_spark_version() {
  local file
  local selected_spark_version=""
  local spark_version
  local found_changed_file=false

  while IFS= read -r file; do
    if [[ -z "${file}" ]]; then
      continue
    fi

    found_changed_file=true
    if [[ ! "${file}" =~ ^spark/v([^/]+)/ ]]; then
      return 1
    fi

    spark_version="${BASH_REMATCH[1]}"
    if ! known_spark_version "${spark_version}"; then
      return 1
    fi

    if [[ -z "${selected_spark_version}" ]]; then
      selected_spark_version="${spark_version}"
    elif [[ "${selected_spark_version}" != "${spark_version}" ]]; then
      return 1
    fi
  done

  if [[ "${found_changed_file}" != "true" ]]; then
    return 1
  fi

  printf '%s\n' "${selected_spark_version}"
}

selected_spark_versions=("${all_spark_versions[@]}")
if [[ "${GITHUB_EVENT_NAME:-}" == "pull_request" && -n "${BASE_SHA:-}" ]] &&
  git fetch --quiet --no-tags --depth=1 origin "${BASE_SHA}"; then
  changed_files="$(git diff --name-only "${BASE_SHA}" HEAD)"

  if changed_spark_version="$(select_changed_spark_version <<< "${changed_files}")"; then
    selected_spark_versions=("${changed_spark_version}")
  fi
fi

spark_versions_json="$(json_array "${selected_spark_versions[@]}")"

echo "spark_versions=${spark_versions_json}" >> "${GITHUB_OUTPUT}"
