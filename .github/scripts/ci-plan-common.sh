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

gradle_property() {
  local name="$1"
  sed -n "s/^systemProp\.${name}=//p" gradle.properties | head -n 1
}

changed_files=()
read_changed_files() {
  local file
  changed_files=()

  if [[ -n "${CHANGED_FILES_FILE:-}" ]]; then
    while IFS= read -r file; do
      if [[ -n "${file}" ]]; then
        changed_files+=("${file}")
      fi
    done < "${CHANGED_FILES_FILE}"
  elif [[ -n "${BASE_SHA:-}" ]]; then
    git fetch --no-tags --depth=1 origin "${BASE_SHA}"
    while IFS= read -r file; do
      if [[ -n "${file}" ]]; then
        changed_files+=("${file}")
      fi
    done < <(git diff --name-only "${BASE_SHA}" HEAD)
  fi
}

value_in_list() {
  local value="$1"
  shift
  local candidate
  for candidate in "$@"; do
    if [[ "${candidate}" == "${value}" ]]; then
      return 0
    fi
  done
  return 1
}
