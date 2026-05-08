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
  _reset_version_vars
  source "${LIBS_DIR}/_version.sh"
}

# ---- validate_and_extract_version ----

@test "validate_and_extract_version: accepts 1.10.0" {
  run validate_and_extract_version "1.10.0"
  [ "$status" -eq 0 ]
}

@test "validate_and_extract_version: extracts major.minor.patch" {
  validate_and_extract_version "1.10.0"
  [ "$major" = "1" ]
  [ "$minor" = "10" ]
  [ "$patch" = "0" ]
  [ "$version_without_rc" = "1.10.0" ]
}

@test "validate_and_extract_version: accepts 2.0.10" {
  validate_and_extract_version "2.0.10"
  [ "$major" = "2" ]
  [ "$minor" = "0" ]
  [ "$patch" = "10" ]
}

@test "validate_and_extract_version: rejects missing patch" {
  run validate_and_extract_version "1.10"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_version: rejects empty string" {
  run validate_and_extract_version ""
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_version: rejects alpha characters" {
  run validate_and_extract_version "1.10.0-beta"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_version: rejects SNAPSHOT suffix" {
  run validate_and_extract_version "1.10.0-SNAPSHOT"
  [ "$status" -eq 1 ]
}

# ---- validate_and_extract_git_tag_version ----

@test "validate_and_extract_git_tag_version: parses apache-iceberg-1.10.0-rc0" {
  validate_and_extract_git_tag_version "apache-iceberg-1.10.0-rc0"
  [ "$major" = "1" ]
  [ "$minor" = "10" ]
  [ "$patch" = "0" ]
  [ "$rc_number" = "0" ]
  [ "$version_without_rc" = "1.10.0" ]
}

@test "validate_and_extract_git_tag_version: parses apache-iceberg-2.1.3-rc12" {
  validate_and_extract_git_tag_version "apache-iceberg-2.1.3-rc12"
  [ "$major" = "2" ]
  [ "$minor" = "1" ]
  [ "$patch" = "3" ]
  [ "$rc_number" = "12" ]
}

@test "validate_and_extract_git_tag_version: rejects tag without rc suffix" {
  run validate_and_extract_git_tag_version "apache-iceberg-1.10.0"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_git_tag_version: rejects wrong prefix" {
  run validate_and_extract_git_tag_version "apache-parquet-1.10.0-rc0"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_git_tag_version: rejects bare version" {
  run validate_and_extract_git_tag_version "1.10.0-rc0"
  [ "$status" -eq 1 ]
}

# ---- validate_and_extract_branch_version ----

@test "validate_and_extract_branch_version: parses 1.10.x" {
  validate_and_extract_branch_version "1.10.x"
  [ "$major" = "1" ]
  [ "$minor" = "10" ]
}

@test "validate_and_extract_branch_version: parses 0.14.x" {
  validate_and_extract_branch_version "0.14.x"
  [ "$major" = "0" ]
  [ "$minor" = "14" ]
}

@test "validate_and_extract_branch_version: parses 2.0.x" {
  validate_and_extract_branch_version "2.0.x"
  [ "$major" = "2" ]
  [ "$minor" = "0" ]
}

@test "validate_and_extract_branch_version: rejects iceberg- prefix" {
  run validate_and_extract_branch_version "iceberg-1.10.x"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_branch_version: rejects release/ prefix" {
  run validate_and_extract_branch_version "release/1.10.x"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_branch_version: rejects full version" {
  run validate_and_extract_branch_version "1.10.0"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_branch_version: rejects main" {
  run validate_and_extract_branch_version "main"
  [ "$status" -eq 1 ]
}

# ---- find_next_rc_number ----

@test "find_next_rc_number: returns 0 when no tags exist" {
  git() { echo ""; }
  export -f git
  find_next_rc_number "1.10.0"
  [ "$rc_number" = "0" ]
}

@test "find_next_rc_number: returns 1 after rc0" {
  git() {
    if [[ "$1" == "tag" && "$2" == "-l" ]]; then
      echo "apache-iceberg-1.10.0-rc0"
    fi
  }
  export -f git
  find_next_rc_number "1.10.0"
  [ "$rc_number" = "1" ]
}

@test "find_next_rc_number: returns 3 after rc0, rc1, rc2" {
  git() {
    if [[ "$1" == "tag" && "$2" == "-l" ]]; then
      printf "apache-iceberg-1.10.0-rc0\napache-iceberg-1.10.0-rc1\napache-iceberg-1.10.0-rc2\n"
    fi
  }
  export -f git
  find_next_rc_number "1.10.0"
  [ "$rc_number" = "3" ]
}

@test "find_next_rc_number: handles gap in rc numbers" {
  git() {
    if [[ "$1" == "tag" && "$2" == "-l" ]]; then
      printf "apache-iceberg-1.10.0-rc0\napache-iceberg-1.10.0-rc5\n"
    fi
  }
  export -f git
  find_next_rc_number "1.10.0"
  [ "$rc_number" = "6" ]
}

@test "find_next_rc_number: ignores tags for other versions" {
  git() {
    if [[ "$1" == "tag" && "$2" == "-l" ]]; then
      echo ""
    fi
  }
  export -f git
  find_next_rc_number "1.11.0"
  [ "$rc_number" = "0" ]
}

# ---- find_latest_rc_number ----

@test "find_latest_rc_number: returns highest RC number" {
  cd "$(mktemp -d)"
  git init -q
  git commit --allow-empty -m "init" -q
  git tag "apache-iceberg-1.10.0-rc0"
  git tag "apache-iceberg-1.10.0-rc1"
  git tag "apache-iceberg-1.10.0-rc2"
  find_latest_rc_number "1.10.0"
  [ "$latest_rc_number" = "2" ]
}

@test "find_latest_rc_number: returns 0 when only rc0 exists" {
  cd "$(mktemp -d)"
  git init -q
  git commit --allow-empty -m "init" -q
  git tag "apache-iceberg-2.0.0-rc0"
  find_latest_rc_number "2.0.0"
  [ "$latest_rc_number" = "0" ]
}

@test "find_latest_rc_number: fails when no RC tags exist" {
  cd "$(mktemp -d)"
  git init -q
  git commit --allow-empty -m "init" -q
  run find_latest_rc_number "9.9.9"
  [ "$status" -eq 1 ]
  [[ "$output" == *"No RC tags found"* ]]
}

# ---- expected_branch_for_version ----

@test "expected_branch_for_version: returns X.Y.x form" {
  result=$(expected_branch_for_version "1" "10")
  [ "$result" = "1.10.x" ]
}

@test "expected_branch_for_version: handles two-digit minor" {
  result=$(expected_branch_for_version "1" "10")
  [ "$result" = "1.10.x" ]
}

@test "expected_branch_for_version: handles 0.x line" {
  result=$(expected_branch_for_version "0" "14")
  [ "$result" = "0.14.x" ]
}
