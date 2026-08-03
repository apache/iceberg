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
  unset _LOG_LOADED
  source "${LIBS_DIR}/_log.sh"
  TEST_TMPDIR=$(mktemp -d)
}

teardown() {
  rm -rf "${TEST_TMPDIR}"
}

@test "print_error: writes to stderr" {
  run print_error "test error"
  [ "$status" -eq 0 ]
  [[ "$output" == *"ERROR: test error"* ]]
}

@test "print_warning: writes to stderr" {
  run print_warning "test warning"
  [ "$status" -eq 0 ]
  [[ "$output" == *"WARNING: test warning"* ]]
}

@test "print_info: writes to stderr" {
  run print_info "test info"
  [ "$status" -eq 0 ]
  [[ "$output" == *"INFO: test info"* ]]
}

@test "step_summary: writes to stdout when GITHUB_STEP_SUMMARY unset" {
  unset GITHUB_STEP_SUMMARY
  run step_summary "hello"
  [ "$status" -eq 0 ]
  [ "$output" = "hello" ]
}

@test "step_summary: writes to file when GITHUB_STEP_SUMMARY is set" {
  export GITHUB_STEP_SUMMARY="${TEST_TMPDIR}/summary.md"
  step_summary "line one"
  step_summary "line two"
  [ -f "${GITHUB_STEP_SUMMARY}" ]
  [[ "$(cat "${GITHUB_STEP_SUMMARY}")" == *"line one"* ]]
  [[ "$(cat "${GITHUB_STEP_SUMMARY}")" == *"line two"* ]]
}

@test "step_summary: appends to existing GITHUB_STEP_SUMMARY file" {
  export GITHUB_STEP_SUMMARY="${TEST_TMPDIR}/summary.md"
  echo "existing" > "${GITHUB_STEP_SUMMARY}"
  step_summary "new line"
  [[ "$(cat "${GITHUB_STEP_SUMMARY}")" == *"existing"* ]]
  [[ "$(cat "${GITHUB_STEP_SUMMARY}")" == *"new line"* ]]
}

@test "NO_COLOR suppresses colors" {
  export NO_COLOR=1
  [ -z "${RED}" ] || [ "${RED}" = "" ]
  [ -z "${GREEN}" ] || [ "${GREEN}" = "" ]
}
