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
  source "${LIBS_DIR}/_exec.sh"
}

@test "exec_process: dry-run prints but does not execute" {
  DRY_RUN=1
  run exec_process echo "should not appear as direct output"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run, WOULD execute"* ]]
  [[ "$output" == *"echo"* ]]
}

@test "exec_process: real run executes command" {
  DRY_RUN=0
  run exec_process echo "hello from exec"
  [ "$status" -eq 0 ]
  [[ "$output" == *"hello from exec"* ]]
}

@test "exec_process: real run preserves exit code" {
  DRY_RUN=0
  run exec_process false
  [ "$status" -ne 0 ]
}

@test "exec_process: redacts NEXUS_PASSWORD in dry-run output" {
  DRY_RUN=1
  export NEXUS_PASSWORD="super-secret-pw-12345"
  run exec_process echo "credentials: super-secret-pw-12345 trailing"
  [ "$status" -eq 0 ]
  [[ "$output" != *"super-secret-pw-12345"* ]]
  [[ "$output" == *"***"* ]]
  unset NEXUS_PASSWORD
}

@test "exec_process: redacts SVN_PASSWORD in dry-run output" {
  DRY_RUN=1
  export SVN_PASSWORD="another-secret-67890"
  run exec_process echo "creds another-secret-67890 end"
  [ "$status" -eq 0 ]
  [[ "$output" != *"another-secret-67890"* ]]
  unset SVN_PASSWORD
}

@test "_redact_secrets: covers all secret env vars in the redaction list" {
  # Every variable named in the loop in _redact_secrets must be redacted
  # if set. This guards against accidental removals and keeps prepare-rc /
  # publish-release / cancel-rc workflow secrets in sync.
  local v
  for v in NEXUS_PASSWORD NEXUS_USERNAME SVN_PASSWORD SVN_USERNAME GITHUB_TOKEN \
           ICEBERG_NEXUS_PASSWORD ICEBERG_NEXUS_USER \
           ICEBERG_SVN_DEV_PASSWORD ICEBERG_SVN_DEV_USERNAME \
           ORG_GRADLE_PROJECT_mavenUser ORG_GRADLE_PROJECT_mavenPassword; do
    local marker="redact-marker-${v}-zzz"
    eval "export ${v}=\"${marker}\""
    local got
    got=$(_redact_secrets echo "value=${marker}")
    [[ "$got" != *"${marker}"* ]] || { echo "leaked ${v}: $got"; return 1; }
    [[ "$got" == *"***"* ]]
    eval "unset ${v}"
  done
}

@test "exec_process_with_retries: redacts secrets in final-failure error message" {
  DRY_RUN=0
  export SVN_PASSWORD="leak-canary-xyz"
  # `false` always fails; the wrapper reports the command after the last
  # attempt. Without redaction the password literal would leak there.
  run exec_process_with_retries 1 0 "" false "user:leak-canary-xyz"
  [ "$status" -ne 0 ]
  [[ "$output" != *"leak-canary-xyz"* ]]
  [[ "$output" == *"failed after 1 attempts"* ]]
  unset SVN_PASSWORD
}

@test "exec_process_with_retries: succeeds on first attempt" {
  DRY_RUN=0
  run exec_process_with_retries 3 0 "" echo "ok"
  [ "$status" -eq 0 ]
  [[ "$output" == *"ok"* ]]
}

@test "exec_process_with_retries: fails after max attempts" {
  DRY_RUN=0
  run exec_process_with_retries 2 0 "" false
  [ "$status" -ne 0 ]
  [[ "$output" == *"failed after 2 attempts"* ]]
}

@test "exec_process_with_retries: requires at least 4 args" {
  DRY_RUN=0
  run exec_process_with_retries 3 0
  [ "$status" -ne 0 ]
}

@test "calculate_sha512: creates checksum file in real mode" {
  DRY_RUN=0
  local tmpfile
  tmpfile=$(mktemp)
  echo "test content" > "$tmpfile"

  calculate_sha512 "$tmpfile"

  [ -f "${tmpfile}.sha512" ]
  local basename
  basename=$(basename "$tmpfile")
  [[ "$(cat "${tmpfile}.sha512")" == *"${basename}"* ]]

  rm -f "$tmpfile" "${tmpfile}.sha512"
}

@test "calculate_sha512: dry-run does not create file" {
  DRY_RUN=1
  local tmpfile
  tmpfile=$(mktemp)
  echo "test content" > "$tmpfile"

  run calculate_sha512 "$tmpfile"
  [ "$status" -eq 0 ]
  [ ! -f "${tmpfile}.sha512" ]

  rm -f "$tmpfile"
}
