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

# These tests cover prepare-rc.sh's branch-selection rules: X.Y.0 tags
# from main; X.Y.Z (Z >= 1) requires the X.Y.x branch to already exist
# on the remote. We build a hermetic git fixture so we can vary which
# remote refs exist, and run the script in dry-run.

setup() {
  load test_helper/common
  WORKSPACE_ROOT="$(cd "${BATS_TEST_DIRNAME}/../.." && pwd)"

  TEST_TMPDIR=$(mktemp -d)
  UPSTREAM="${TEST_TMPDIR}/upstream.git"
  CLONE="${TEST_TMPDIR}/clone"

  git init --bare --initial-branch=main "${UPSTREAM}" >/dev/null

  git -c init.defaultBranch=main clone --quiet "${UPSTREAM}" "${CLONE}"
  pushd "${CLONE}" >/dev/null
    git config user.email release-tests@example.com
    git config user.name "Release Tests"
    git config commit.gpgsign false
    git config tag.gpgsign false

    # Stage the release scripts and a stub gradlew/dev/source-release.sh so
    # the early prerequisite checks pass.
    mkdir -p release dev
    cp "${WORKSPACE_ROOT}/release/"*.sh release/
    cp -R "${WORKSPACE_ROOT}/release/libs"  release/libs
    cp    "${WORKSPACE_ROOT}/dev/stage-binaries.sh" dev/stage-binaries.sh
    cp    "${WORKSPACE_ROOT}/gradle.properties" gradle.properties
    chmod +x release/*.sh dev/stage-binaries.sh
    cat >gradlew <<'EOF'
#!/bin/bash
echo "gradlew stub: $*"
EOF
    chmod +x gradlew

    git add -A
    git commit --quiet -m "initial"
    git push --quiet origin main
  popd >/dev/null
}

teardown() {
  rm -rf "${TEST_TMPDIR}"
}

# Run prepare-rc.sh inside the clone with --skip-ci-check so we don't need
# a real GitHub backend, and capture stdout+stderr while preserving the
# script's exit status (the trailing popd would otherwise mask it).
run_prepare_rc() {
  local version="$1"
  shift
  (cd "${CLONE}" && DRY_RUN=1 GITHUB_TOKEN="" \
    ./release/prepare-rc.sh "${version}" --skip-ci-check "$@" 2>&1)
}

# ---------------------------------------------------------------------------
# X.Y.0: tag from main, no branch needed
# ---------------------------------------------------------------------------

@test "prepare-rc X.Y.0: tags from origin/main without creating a branch" {
  run run_prepare_rc 1.99.0
  [ "$status" -eq 0 ]
  [[ "$output" == *"X.Y.0 release: tagging"* ]]
  [[ "$output" == *"origin/main"* ]]
  [[ "$output" != *"git checkout -b 1.99.x"* ]] \
    || { echo "should not create 1.99.x branch for X.Y.0"; printf '%s\n' "$output"; return 1; }
  [[ "$output" != *"git push origin 1.99.x"* ]] \
    || { echo "should not push 1.99.x branch for X.Y.0"; printf '%s\n' "$output"; return 1; }
}

# ---------------------------------------------------------------------------
# X.Y.Z (Z >= 1): require X.Y.x to exist on the remote, never auto-create
# ---------------------------------------------------------------------------

@test "prepare-rc X.Y.Z: fails with helpful message when X.Y.x does not exist on remote" {
  run run_prepare_rc 1.94.1
  [ "$status" -ne 0 ]
  [[ "$output" == *"Release branch 'origin/1.94.x' not found on remote"* ]]
  [[ "$output" == *"apache-iceberg-1.94.0"* ]] \
    || { echo "expected pointer to apache-iceberg-1.94.0 final tag"; printf '%s\n' "$output"; return 1; }
  [[ "$output" == *"Iceberg's convention"* ]]
}

@test "prepare-rc X.Y.Z: tags from existing X.Y.x branch when present on remote" {
  pushd "${CLONE}" >/dev/null
    git checkout -b 1.93.x main
    echo patch > patch.txt
    git add patch.txt
    git commit --quiet -m "patch backport on 1.93.x"
    git push --quiet origin 1.93.x
    git checkout main
    git branch -D 1.93.x
  popd >/dev/null

  run run_prepare_rc 1.93.1
  [ "$status" -eq 0 ]
  [[ "$output" == *"Patch release: tagging from origin/1.93.x"* ]]
  [[ "$output" != *"git checkout -b 1.93.x origin/main"* ]] \
    || { echo "patch path must not branch from main"; printf '%s\n' "$output"; return 1; }
  [[ "$output" != *"git push origin 1.93.x --set-upstream"* ]] \
    || { echo "should not push --set-upstream when branch already exists"; printf '%s\n' "$output"; return 1; }
}

# ---------------------------------------------------------------------------
# Removed flags: --source-branch and --skip-branch-creation are no longer
# recognized. Both should be rejected as unknown options.
# ---------------------------------------------------------------------------

@test "prepare-rc: --source-branch is no longer a recognized flag" {
  run run_prepare_rc 1.91.0 --source-branch main
  [ "$status" -ne 0 ]
  [[ "$output" == *"Unknown option: --source-branch"* ]]
}

@test "prepare-rc: --skip-branch-creation is no longer a recognized flag" {
  run run_prepare_rc 1.91.0 --skip-branch-creation
  [ "$status" -ne 0 ]
  [[ "$output" == *"Unknown option: --skip-branch-creation"* ]]
}

# ---------------------------------------------------------------------------
# Default remote selection: prefer 'apache' when configured, fall back to 'origin'
# ---------------------------------------------------------------------------

@test "prepare-rc: defaults to 'apache' remote when configured" {
  pushd "${CLONE}" >/dev/null
    git remote add apache "${UPSTREAM}"
    git fetch --quiet apache
  popd >/dev/null

  run run_prepare_rc 1.92.0
  [ "$status" -eq 0 ]
  [[ "$output" == *"apache/main"* ]] \
    || { echo "expected default remote to be 'apache' when configured"; printf '%s\n' "$output"; return 1; }
}

@test "prepare-rc: falls back to 'origin' when 'apache' is not configured" {
  run run_prepare_rc 1.92.0
  [ "$status" -eq 0 ]
  [[ "$output" == *"origin/main"* ]]
}

# ---------------------------------------------------------------------------
# Workspace alignment: HEAD is moved to release_hash before tarball + binary
# build so both come from the same commit (parity with manual flow).
# ---------------------------------------------------------------------------

@test "prepare-rc X.Y.0: aligns workspace to release_hash when HEAD lags origin/main" {
  pushd "${CLONE}" >/dev/null
    # Move local HEAD behind origin/main so the alignment check fires.
    echo lagged > lagged.txt
    git add lagged.txt
    git commit --quiet -m "local-only commit, leaves origin/main behind"
    # Force HEAD onto a different commit than origin/main by resetting to
    # the previous commit; origin/main has advanced via a fresh push.
    pushd "${UPSTREAM}" >/dev/null
      # Append a remote-only commit so origin/main is ahead of local HEAD.
    popd >/dev/null
    # Add a commit on origin/main only, by pushing from a separate worktree.
    git checkout --quiet -b advance HEAD
    echo advance > advance.txt
    git add advance.txt
    git commit --quiet -m "advance origin/main"
    git push --quiet origin advance:main
    git checkout --quiet main
    git reset --hard --quiet HEAD~1
    git branch -D advance >/dev/null 2>&1 || true
  popd >/dev/null

  run run_prepare_rc 1.95.0
  [ "$status" -eq 0 ]
  [[ "$output" == *"WOULD checkout"* ]] \
    || { echo "expected workspace alignment message"; printf '%s\n' "$output"; return 1; }
}
