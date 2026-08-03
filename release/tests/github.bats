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
  source "${LIBS_DIR}/_github.sh"
}

@test "check_github_checks_passed: fails when GITHUB_TOKEN not set" {
  unset GITHUB_TOKEN
  DRY_RUN=0
  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"GITHUB_TOKEN is required"* ]]
}

@test "check_github_checks_passed: skips in dry-run even without GITHUB_TOKEN" {
  unset GITHUB_TOKEN
  DRY_RUN=1
  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"DRY_RUN"* ]]
}

@test "check_github_checks_passed: skips in dry-run mode" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=1
  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"DRY_RUN"* ]]
}

@test "check_github_checks_passed: succeeds when all checks completed and passed" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    echo "0"
    return 0
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"All GitHub checks passed"* ]]
}

@test "check_github_checks_passed: fails when checks are still running" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    if [[ "$*" == *"status"* && "$*" == *"length"* ]]; then
      echo "1"
    elif [[ "$*" == *"status"* ]]; then
      echo "  - CI: in_progress"
    else
      echo "0"
    fi
    return 0
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"still-running"* ]]
}

@test "check_github_checks_passed: fails when checks have failed conclusions" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    if [[ "$*" == *"status"* && "$*" == *"length"* ]]; then
      echo "0"
    elif [[ "$*" == *"conclusion"* && "$*" == *"length"* ]]; then
      echo "2"
    elif [[ "$*" == *"conclusion"* ]]; then
      echo "  - CI: failure"
    else
      echo "0"
    fi
    return 0
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"failed GitHub checks"* ]]
}

@test "check_github_checks_passed: fails when gh api errors" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() { return 1; }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Failed to fetch"* ]]
}
