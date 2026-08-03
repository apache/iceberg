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

# End-to-end tests for release/publish-release.sh exercising argument
# parsing, validation guards, and the dry-run happy path against a
# hermetic git fixture. SVN, Nexus, and GitHub interactions are no-ops
# under DRY_RUN=1.

setup() {
  load test_helper/common
  WORKSPACE_ROOT="$(cd "${BATS_TEST_DIRNAME}/../.." && pwd)"

  TEST_TMPDIR=$(mktemp -d)
  CLONE="${TEST_TMPDIR}/clone"
  UPSTREAM="${TEST_TMPDIR}/upstream.git"

  git init --bare --initial-branch=main "${UPSTREAM}" >/dev/null
  git -c init.defaultBranch=main clone --quiet "${UPSTREAM}" "${CLONE}"
  pushd "${CLONE}" >/dev/null
    git config user.email release-tests@example.com
    git config user.name "Release Tests"
    git config commit.gpgsign false
    git config tag.gpgsign false

    mkdir -p release dev
    cp "${WORKSPACE_ROOT}/release/"*.sh release/
    cp -R "${WORKSPACE_ROOT}/release/libs"  release/libs
    cp    "${WORKSPACE_ROOT}/dev/stage-binaries.sh" dev/stage-binaries.sh
    cp    "${WORKSPACE_ROOT}/gradle.properties" gradle.properties
    chmod +x release/*.sh dev/stage-binaries.sh

    echo first > seed.txt
    git add -A
    git commit --quiet -m "initial"
    git push --quiet origin main

    git tag -a "apache-iceberg-1.10.0-rc0" -m "Apache Iceberg 1.10.0 RC0"
  popd >/dev/null
}

teardown() {
  rm -rf "${TEST_TMPDIR}"
  unset GITHUB_TOKEN
}

run_publish_release() {
  (cd "${CLONE}" && DRY_RUN=1 GITHUB_TOKEN="" \
    ./release/publish-release.sh "$@" 2>&1)
}

# ---------------------------------------------------------------------------
# Argument parsing / validation
# ---------------------------------------------------------------------------

@test "publish-release: requires version and staging-repo-id" {
  run run_publish_release
  [ "$status" -ne 0 ]
  [[ "$output" == *"Expected 2 positional arguments"* ]]
}

@test "publish-release: requires staging-repo-id" {
  run run_publish_release 1.10.0
  [ "$status" -ne 0 ]
  [[ "$output" == *"Expected 2 positional arguments"* ]]
}

@test "publish-release: rejects invalid version format" {
  run run_publish_release 1.10 orgapacheiceberg-1234
  [ "$status" -ne 0 ]
  [[ "$output" == *"Invalid version format"* ]]
}

@test "publish-release: rejects invalid staging-repo-id format" {
  run run_publish_release 1.10.0 "bad id with spaces"
  [ "$status" -ne 0 ]
  [[ "$output" == *"Invalid staging repository ID"* ]]
}

@test "publish-release: rejects non-numeric --rc value" {
  run run_publish_release 1.10.0 orgapacheiceberg-1234 --rc abc
  [ "$status" -ne 0 ]
  [[ "$output" == *"Invalid RC number"* ]]
}

@test "publish-release: rejects unknown option" {
  run run_publish_release 1.10.0 orgapacheiceberg-1234 --bogus-flag
  [ "$status" -ne 0 ]
  [[ "$output" == *"Unknown option: --bogus-flag"* ]]
}

# ---------------------------------------------------------------------------
# Pre-publish guards
# ---------------------------------------------------------------------------

@test "publish-release: fails when RC tag does not exist locally" {
  run run_publish_release 1.99.0 orgapacheiceberg-1234 --rc 0
  [ "$status" -ne 0 ]
  [[ "$output" == *"RC tag apache-iceberg-1.99.0-rc0 does not exist locally"* ]]
}

@test "publish-release: fails when final tag already exists" {
  pushd "${CLONE}" >/dev/null
    git tag -a "apache-iceberg-1.10.0" -m "already-released"
  popd >/dev/null

  run run_publish_release 1.10.0 orgapacheiceberg-1234 --rc 0
  [ "$status" -ne 0 ]
  [[ "$output" == *"Final release tag apache-iceberg-1.10.0 already exists"* ]]
}

@test "publish-release: fails when HEAD does not match RC tag commit" {
  pushd "${CLONE}" >/dev/null
    echo drift > drift.txt
    git add drift.txt
    git commit --quiet -m "drift HEAD past the RC commit"
  popd >/dev/null

  run run_publish_release 1.10.0 orgapacheiceberg-1234 --rc 0
  [ "$status" -ne 0 ]
  [[ "$output" == *"HEAD"* && "$output" == *"does not match RC tag"* ]]
  [[ "$output" == *"git checkout apache-iceberg-1.10.0-rc0"* ]]
}

@test "publish-release: rejects publishing an older RC when a newer one exists" {
  pushd "${CLONE}" >/dev/null
    git tag -a "apache-iceberg-1.10.0-rc1" -m "Apache Iceberg 1.10.0 RC1"
  popd >/dev/null

  run run_publish_release 1.10.0 orgapacheiceberg-1234 --rc 0
  [ "$status" -ne 0 ]
  [[ "$output" == *"is not the latest RC"* ]]
  [[ "$output" == *"Latest is rc1"* ]]
}

# ---------------------------------------------------------------------------
# Dry-run happy path
# ---------------------------------------------------------------------------

@test "publish-release: dry-run with explicit --rc completes successfully" {
  run run_publish_release 1.10.0 orgapacheiceberg-1234 --rc 0
  [ "$status" -eq 0 ]
  [[ "$output" == *"DRY RUN"* ]]
  [[ "$output" == *"Staging repo verification"* ]]
  [[ "$output" == *"Dry-run, WOULD execute"* ]]
  [[ "$output" == *"published successfully"* ]]
}

@test "publish-release: dry-run auto-detects latest RC when --rc omitted" {
  pushd "${CLONE}" >/dev/null
    # Force a higher RC to be the latest.
    git tag -a "apache-iceberg-1.10.0-rc2" -m "Apache Iceberg 1.10.0 RC2"
    # Reset HEAD to the rc2 commit (same as rc0 here).
  popd >/dev/null

  run run_publish_release 1.10.0 orgapacheiceberg-1234
  [ "$status" -eq 0 ]
  [[ "$output" == *"Auto-detected latest RC: rc2"* ]]
}

@test "publish-release: --keep-old-releases skips dist/release cleanup messaging" {
  run run_publish_release 1.10.0 orgapacheiceberg-1234 --rc 0 --keep-old-releases
  [ "$status" -eq 0 ]
  [[ "$output" == *"Skipping dist/release cleanup"* ]] \
    || { echo "expected --keep-old-releases skip message"; printf '%s\n' "$output"; return 1; }
}

@test "publish-release: dry-run cleanup wording is scoped to the MAJOR.MINOR.x line" {
  # No --keep-old-releases this time; the script must announce that it
  # would only touch the 1.10.x line, not every release in dist/release.
  run run_publish_release 1.10.0 orgapacheiceberg-1234 --rc 0
  [ "$status" -eq 0 ]
  [[ "$output" == *"superseded 1.10.x patch releases"* ]] \
    || { echo "expected MAJOR.MINOR.x-scoped dry-run wording"; printf '%s\n' "$output"; return 1; }
}

@test "publish-release: dry-run does not leak SVN_PASSWORD into logs" {
  export SVN_USERNAME="release-mgr"
  export SVN_PASSWORD="trace-canary-svn-pw"
  run run_publish_release 1.10.0 orgapacheiceberg-1234 --rc 0
  unset SVN_USERNAME SVN_PASSWORD
  [ "$status" -eq 0 ]
  [[ "$output" != *"trace-canary-svn-pw"* ]] \
    || { echo "SVN password leaked into dry-run output"; printf '%s\n' "$output"; return 1; }
}
