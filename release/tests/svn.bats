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
  source "${LIBS_DIR}/_svn.sh"
  TEST_TMPDIR=$(mktemp -d)
  ARGV_FILE="${TEST_TMPDIR}/argv"
  STDIN_FILE="${TEST_TMPDIR}/stdin"
  EXIT_FILE="${TEST_TMPDIR}/exit"
  echo "0" > "${EXIT_FILE}"
  export ARGV_FILE STDIN_FILE EXIT_FILE
}

teardown() {
  rm -rf "${TEST_TMPDIR}"
  unset SVN_USERNAME SVN_PASSWORD ARGV_FILE STDIN_FILE EXIT_FILE
}

# Stub svn that records its argv and stdin to disk so tests can assert on
# what was actually invoked. Exit code is read from EXIT_FILE so tests can
# simulate failures.
_stub_svn() {
  svn() {
    printf '%s\n' "$@" > "${ARGV_FILE}"
    cat > "${STDIN_FILE}"
    return "$(cat "${EXIT_FILE}")"
  }
  export -f svn
}

# ---- svn_run ----

@test "svn_run: real mode passes password via stdin, never on argv" {
  _stub_svn
  DRY_RUN=0
  export SVN_USERNAME="release-mgr"
  export SVN_PASSWORD="hunter2-very-secret"

  run svn_run co --depth=empty https://example/repo /tmp/wc

  [ "$status" -eq 0 ]
  ! grep -q -- "hunter2-very-secret" "${ARGV_FILE}" \
    || { echo "password leaked to argv"; cat "${ARGV_FILE}"; return 1; }
  grep -q -- "--password-from-stdin" "${ARGV_FILE}"
  grep -q -- "--non-interactive" "${ARGV_FILE}"
  grep -q -- "--no-auth-cache" "${ARGV_FILE}"
  grep -q -- "--username" "${ARGV_FILE}"
  grep -q -- "release-mgr" "${ARGV_FILE}"
  grep -q -- "co" "${ARGV_FILE}"
  grep -q -- "https://example/repo" "${ARGV_FILE}"
  [ "$(cat "${STDIN_FILE}")" = "hunter2-very-secret" ]
}

@test "svn_run: real mode redacts password in the printed log line" {
  _stub_svn
  DRY_RUN=0
  export SVN_USERNAME="release-mgr"
  export SVN_PASSWORD="hunter2-very-secret"

  run svn_run mv https://example/dev/r1 https://example/release/r1 -m "promote"

  [ "$status" -eq 0 ]
  [[ "$output" != *"hunter2-very-secret"* ]] \
    || { echo "password leaked to log"; printf '%s\n' "$output"; return 1; }
}

@test "svn_run: dry-run prints the command but does not invoke svn" {
  _stub_svn
  DRY_RUN=1

  run svn_run co --depth=empty https://example/repo /tmp/wc

  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run, WOULD execute"* ]]
  [[ "$output" == *"--password-from-stdin"* ]]
  [ ! -f "${ARGV_FILE}" ] || [ ! -s "${ARGV_FILE}" ]
}

@test "svn_run: real mode requires SVN_USERNAME" {
  _stub_svn
  DRY_RUN=0
  unset SVN_USERNAME
  export SVN_PASSWORD="some-pw"

  run svn_run co https://example/repo /tmp/wc
  [ "$status" -ne 0 ]
  [[ "$output" == *"SVN_USERNAME must be set"* ]]
}

@test "svn_run: real mode requires SVN_PASSWORD" {
  _stub_svn
  DRY_RUN=0
  export SVN_USERNAME="release-mgr"
  unset SVN_PASSWORD

  run svn_run co https://example/repo /tmp/wc
  [ "$status" -ne 0 ]
  [[ "$output" == *"SVN_PASSWORD must be set"* ]]
}

@test "svn_run: dry-run does not require SVN_USERNAME or SVN_PASSWORD" {
  _stub_svn
  DRY_RUN=1
  unset SVN_USERNAME SVN_PASSWORD

  run svn_run co https://example/repo /tmp/wc
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run"* ]]
}

# ---- svn_path_exists ----

@test "svn_path_exists: returns 0 when svn ls exits 0" {
  _stub_svn
  DRY_RUN=0
  export SVN_USERNAME="u"
  export SVN_PASSWORD="p"
  echo "0" > "${EXIT_FILE}"

  run svn_path_exists https://example/repo/foo
  [ "$status" -eq 0 ]
}

@test "svn_path_exists: returns nonzero when svn ls exits nonzero" {
  _stub_svn
  DRY_RUN=0
  export SVN_USERNAME="u"
  export SVN_PASSWORD="p"
  echo "1" > "${EXIT_FILE}"

  run svn_path_exists https://example/repo/missing
  [ "$status" -ne 0 ]
}

@test "svn_path_exists: dry-run returns success without calling svn" {
  _stub_svn
  DRY_RUN=1
  run svn_path_exists https://example/repo/foo
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run, WOULD probe SVN path"* ]]
  [ ! -f "${ARGV_FILE}" ] || [ ! -s "${ARGV_FILE}" ]
}

# ---- svn_list ----

@test "svn_list: pipes password via stdin and captures stdout" {
  svn() {
    printf '%s\n' "$@" > "${ARGV_FILE}"
    cat > "${STDIN_FILE}"
    echo "apache-iceberg-1.9.0/"
    echo "apache-iceberg-1.10.0/"
  }
  export -f svn
  export SVN_USERNAME="u"
  export SVN_PASSWORD="leak-canary-789"

  run svn_list https://example/release
  [ "$status" -eq 0 ]
  [[ "$output" == *"apache-iceberg-1.9.0/"* ]]
  [[ "$output" == *"apache-iceberg-1.10.0/"* ]]
  ! grep -q -- "leak-canary-789" "${ARGV_FILE}"
  [ "$(cat "${STDIN_FILE}")" = "leak-canary-789" ]
}

# ---- svn_run_with_retries ----

@test "svn_run_with_retries: succeeds on first attempt without sleeping" {
  _stub_svn
  DRY_RUN=0
  export SVN_USERNAME="u"
  export SVN_PASSWORD="p"
  echo "0" > "${EXIT_FILE}"

  run svn_run_with_retries 3 0 "${TEST_TMPDIR}/wc" co https://example/repo /tmp/wc
  [ "$status" -eq 0 ]
}

@test "svn_run_with_retries: retries on failure and removes cleanup_path between attempts" {
  # Make svn fail the first 2 calls, then succeed.
  cnt=0
  cnt_file="${TEST_TMPDIR}/cnt"
  echo 0 > "${cnt_file}"
  svn() {
    local n
    n=$(cat "${cnt_file}")
    n=$((n + 1))
    echo "${n}" > "${cnt_file}"
    cat > /dev/null
    if [[ "${n}" -lt 3 ]]; then
      mkdir -p "${TEST_TMPDIR}/cleanup_target"
      return 1
    fi
    return 0
  }
  export -f svn
  export SVN_USERNAME="u"
  export SVN_PASSWORD="p"
  DRY_RUN=0

  run svn_run_with_retries 3 0 "${TEST_TMPDIR}/cleanup_target" co https://example/repo /tmp/wc
  [ "$status" -eq 0 ]
  [ "$(cat "${cnt_file}")" -eq 3 ]
  [ ! -e "${TEST_TMPDIR}/cleanup_target" ] \
    || { echo "cleanup_path was not removed between attempts"; return 1; }
}

@test "svn_run_with_retries: gives up after max_attempts and exits nonzero" {
  svn() {
    cat > /dev/null
    return 1
  }
  export -f svn
  export SVN_USERNAME="u"
  export SVN_PASSWORD="p"
  DRY_RUN=0

  run svn_run_with_retries 2 0 "" co https://example/repo /tmp/wc
  [ "$status" -ne 0 ]
  [[ "$output" == *"failed after 2 attempts"* ]]
}

# ---- filter_superseded_patch_releases ----

# Mimics what `svn list https://dist.apache.org/repos/dist/release/iceberg/`
# returns: directory entries always carry a trailing slash, files (KEYS,
# the index) do not. Tests below mirror that shape.
_listing_fixture() {
  cat <<'LIST'
KEYS
apache-iceberg-1.8.0/
apache-iceberg-1.8.1/
apache-iceberg-1.9.0/
apache-iceberg-1.9.1/
apache-iceberg-1.9.2/
apache-iceberg-1.10.0/
apache-iceberg-2.0.0/
LIST
}

@test "filter_superseded_patch_releases: 1.9.2 removes prior 1.9.x patches only" {
  listing="$(_listing_fixture)"
  run filter_superseded_patch_releases "${listing}" "1.9.2"
  [ "$status" -eq 0 ]
  # Sort to make the assertion order-independent of grep output.
  sorted=$(echo "$output" | sort | tr '\n' ',')
  [ "${sorted}" = "apache-iceberg-1.9.0,apache-iceberg-1.9.1," ]
}

@test "filter_superseded_patch_releases: 1.10.0 with no prior 1.10.x emits nothing" {
  listing="$(_listing_fixture)"
  run filter_superseded_patch_releases "${listing}" "1.10.0"
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "filter_superseded_patch_releases: 2.0.0 with no prior 2.0.x emits nothing" {
  listing="$(_listing_fixture)"
  run filter_superseded_patch_releases "${listing}" "2.0.0"
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "filter_superseded_patch_releases: ignores other minor lines" {
  listing="$(_listing_fixture)"
  run filter_superseded_patch_releases "${listing}" "1.9.2"
  [ "$status" -eq 0 ]
  # 1.8.x, 1.10.x, 2.0.x must all survive.
  [[ "$output" != *"1.8.0"* ]]
  [[ "$output" != *"1.8.1"* ]]
  [[ "$output" != *"1.10.0"* ]]
  [[ "$output" != *"2.0.0"* ]]
}

@test "filter_superseded_patch_releases: ignores non-release entries" {
  listing="$(_listing_fixture)"
  run filter_superseded_patch_releases "${listing}" "1.9.2"
  [ "$status" -eq 0 ]
  [[ "$output" != *"KEYS"* ]]
}

@test "filter_superseded_patch_releases: never includes the new version itself" {
  # Listing already contains the new version (svn mv has run).
  listing="$(_listing_fixture)"
  run filter_superseded_patch_releases "${listing}" "1.9.2"
  [ "$status" -eq 0 ]
  while IFS= read -r line; do
    [[ "$line" != "apache-iceberg-1.9.2" ]] \
      || { echo "new version was included in cleanup"; return 1; }
  done <<< "$output"
}

@test "filter_superseded_patch_releases: tolerates entries without trailing slash" {
  listing=$'apache-iceberg-1.9.0\napache-iceberg-1.9.1\napache-iceberg-1.9.2'
  run filter_superseded_patch_releases "${listing}" "1.9.2"
  [ "$status" -eq 0 ]
  sorted=$(echo "$output" | sort | tr '\n' ',')
  [ "${sorted}" = "apache-iceberg-1.9.0,apache-iceberg-1.9.1," ]
}

@test "filter_superseded_patch_releases: empty listing emits nothing and succeeds" {
  run filter_superseded_patch_releases "" "1.9.2"
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "filter_superseded_patch_releases: rejects MAJOR.MINOR cross-matches via boundary anchoring" {
  # Adversarial entries that share a numeric prefix with the new version's
  # MAJOR.MINOR: 1.90.0 must not be matched when releasing 1.9.0 (and
  # vice versa: 1.9.x is not matched when releasing 1.90.0).
  listing=$'apache-iceberg-1.9.0/\napache-iceberg-1.9.1/\napache-iceberg-1.90.0/\napache-iceberg-1.90.1/'

  run filter_superseded_patch_releases "${listing}" "1.9.2"
  [ "$status" -eq 0 ]
  [[ "$output" == *"apache-iceberg-1.9.0"* ]]
  [[ "$output" == *"apache-iceberg-1.9.1"* ]]
  [[ "$output" != *"1.90.0"* ]]
  [[ "$output" != *"1.90.1"* ]]

  run filter_superseded_patch_releases "${listing}" "1.90.2"
  [ "$status" -eq 0 ]
  [[ "$output" == *"apache-iceberg-1.90.0"* ]]
  [[ "$output" == *"apache-iceberg-1.90.1"* ]]
  [[ "$output" != *"apache-iceberg-1.9.0"* ]]
  [[ "$output" != *"apache-iceberg-1.9.1"* ]]
}
