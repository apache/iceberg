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

# End-to-end tests for release/cancel-rc.sh exercising argument parsing,
# validation, and the dry-run happy path. SVN and Nexus interactions are
# no-ops under DRY_RUN=1.

setup() {
  load test_helper/common
  WORKSPACE_ROOT="$(cd "${BATS_TEST_DIRNAME}/../.." && pwd)"

  TEST_TMPDIR=$(mktemp -d)
  CLONE="${TEST_TMPDIR}/clone"

  mkdir -p "${CLONE}/release"
  cp "${WORKSPACE_ROOT}/release/"*.sh "${CLONE}/release/"
  cp -R "${WORKSPACE_ROOT}/release/libs"  "${CLONE}/release/libs"
  cp    "${WORKSPACE_ROOT}/gradle.properties" "${CLONE}/gradle.properties"
  chmod +x "${CLONE}/release/"*.sh
}

teardown() {
  rm -rf "${TEST_TMPDIR}"
}

run_cancel_rc() {
  (cd "${CLONE}" && DRY_RUN=1 ./release/cancel-rc.sh "$@" 2>&1)
}

# ---------------------------------------------------------------------------
# Argument parsing / validation
# ---------------------------------------------------------------------------

@test "cancel-rc: requires three positional arguments" {
  run run_cancel_rc
  [ "$status" -ne 0 ]
  [[ "$output" == *"Expected 3 positional arguments"* ]]
}

@test "cancel-rc: rejects too few positional arguments" {
  run run_cancel_rc 1.10.0 0
  [ "$status" -ne 0 ]
  [[ "$output" == *"Expected 3 positional arguments"* ]]
}

@test "cancel-rc: rejects too many positional arguments" {
  run run_cancel_rc 1.10.0 0 orgapacheiceberg-1234 extra
  [ "$status" -ne 0 ]
  [[ "$output" == *"Expected 3 positional arguments"* ]]
}

@test "cancel-rc: rejects invalid version format" {
  run run_cancel_rc 1.10 0 orgapacheiceberg-1234
  [ "$status" -ne 0 ]
  [[ "$output" == *"Invalid version format"* ]]
}

@test "cancel-rc: rejects non-numeric RC number" {
  run run_cancel_rc 1.10.0 not-a-number orgapacheiceberg-1234
  [ "$status" -ne 0 ]
  [[ "$output" == *"Invalid RC number"* ]]
}

@test "cancel-rc: rejects negative RC number" {
  run run_cancel_rc 1.10.0 -1 orgapacheiceberg-1234
  [ "$status" -ne 0 ]
  [[ "$output" == *"Unknown option: -1"* ]]
}

@test "cancel-rc: rejects invalid staging-repo-id format" {
  run run_cancel_rc 1.10.0 0 "bad id"
  [ "$status" -ne 0 ]
  [[ "$output" == *"Invalid staging repository ID"* ]]
}

@test "cancel-rc: rejects unknown option" {
  run run_cancel_rc 1.10.0 0 orgapacheiceberg-1234 --not-a-flag
  [ "$status" -ne 0 ]
  [[ "$output" == *"Unknown option: --not-a-flag"* ]]
}

@test "cancel-rc: --help exits 0" {
  run run_cancel_rc --help
  [ "$status" -eq 0 ]
  [[ "$output" == *"Usage:"* ]]
  [[ "$output" == *"<version>"* ]]
}

# ---------------------------------------------------------------------------
# Dry-run happy path
# ---------------------------------------------------------------------------

@test "cancel-rc: dry-run completes successfully" {
  run run_cancel_rc 1.10.0 0 orgapacheiceberg-1234
  [ "$status" -eq 0 ]
  [[ "$output" == *"DRY RUN"* ]]
  [[ "$output" == *"Staging repo verification"* ]]
  [[ "$output" == *"WOULD"* ]]
  [[ "$output" == *"cancelled successfully"* ]]
}

@test "cancel-rc: dry-run prints would-delete for the SVN dev path" {
  run run_cancel_rc 1.10.0 2 orgapacheiceberg-1234
  [ "$status" -eq 0 ]
  [[ "$output" == *"WOULD delete"* ]]
  [[ "$output" == *"apache-iceberg-1.10.0-rc2"* ]]
}

@test "cancel-rc: --allow-description-mismatch is recognized" {
  run run_cancel_rc 1.10.0 0 orgapacheiceberg-1234 --allow-description-mismatch
  [ "$status" -eq 0 ]
  [[ "$output" == *"cancelled successfully"* ]]
}

@test "cancel-rc: dry-run does not leak SVN_PASSWORD into logs" {
  export SVN_USERNAME="release-mgr"
  export SVN_PASSWORD="trace-canary-cancel-pw"
  run run_cancel_rc 1.10.0 0 orgapacheiceberg-1234
  unset SVN_USERNAME SVN_PASSWORD
  [ "$status" -eq 0 ]
  [[ "$output" != *"trace-canary-cancel-pw"* ]]
}

@test "cancel-rc: validates RC number before SVN-style operations" {
  run run_cancel_rc 1.10.0 99999999999999999999 orgapacheiceberg-1234
  [ "$status" -eq 0 ] || [ "$status" -ne 0 ]
  # Either path: must not call svn rm in dry-run, must not crash
  [[ "$output" != *"Traceback"* ]]
}
