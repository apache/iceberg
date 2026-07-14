#!/bin/bash
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

[[ -n "${_GITHUB_LOADED:-}" ]] && return 0 2>/dev/null || true
_GITHUB_LOADED=1

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=release/libs/_log.sh
source "${LIBS_DIR}/_log.sh"
# shellcheck source=release/libs/_constants.sh
source "${LIBS_DIR}/_constants.sh"
# shellcheck source=release/libs/_exec.sh
source "${LIBS_DIR}/_exec.sh"

# Returns 0 only when every check-run on the given commit has completed
# (status == "completed") and concluded with success or skipped. Cutting an
# RC from a commit with failing or in-progress CI is a clear bug, so this is
# wired up as a hard gate in prepare-rc.sh. In dry-run mode the check is
# skipped so the script can be exercised against any commit.
function check_github_checks_passed() {
  local commit_sha="$1"

  print_info "Checking GitHub CI status for commit ${commit_sha}..."

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_info "DRY_RUN is enabled, skipping GitHub check verification"
    return 0
  fi

  if [[ -z "${GITHUB_TOKEN:-}" ]]; then
    print_error "GITHUB_TOKEN is required to verify CI checks"
    return 1
  fi

  local repo_info="${GITHUB_REPO}"

  local num_incomplete
  if ! num_incomplete=$(gh api "repos/${repo_info}/commits/${commit_sha}/check-runs" \
    --jq '[.check_runs[] | select(.status != "completed")] | length'); then
    print_error "Failed to fetch GitHub check runs for commit ${commit_sha}"
    return 1
  fi

  if [[ ${num_incomplete} -ne 0 ]]; then
    print_error "Found ${num_incomplete} still-running GitHub checks for commit ${commit_sha}"
    gh api "repos/${repo_info}/commits/${commit_sha}/check-runs" \
      --jq '.check_runs[] | select(.status != "completed") | "  - \(.name): \(.status)"' >&2
    return 1
  fi

  local num_failed
  if ! num_failed=$(gh api "repos/${repo_info}/commits/${commit_sha}/check-runs" \
    --jq '[.check_runs[] | select(.conclusion != "success" and .conclusion != "skipped")] | length'); then
    print_error "Failed to fetch GitHub check runs for commit ${commit_sha}"
    return 1
  fi

  if [[ ${num_failed} -ne 0 ]]; then
    print_error "Found ${num_failed} failed GitHub checks for commit ${commit_sha}"
    gh api "repos/${repo_info}/commits/${commit_sha}/check-runs" \
      --jq '.check_runs[] | select(.conclusion != "success" and .conclusion != "skipped") | "  - \(.name): \(.conclusion)"' >&2
    return 1
  fi

  print_info "All GitHub checks passed for commit ${commit_sha}"
  return 0
}
