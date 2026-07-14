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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIBS_DIR="${SCRIPT_DIR}/libs"

# shellcheck source=release/libs/_constants.sh
source "${LIBS_DIR}/_constants.sh"
# shellcheck source=release/libs/_log.sh
source "${LIBS_DIR}/_log.sh"
# shellcheck source=release/libs/_exec.sh
source "${LIBS_DIR}/_exec.sh"
# shellcheck source=release/libs/_version.sh
source "${LIBS_DIR}/_version.sh"
# shellcheck source=release/libs/_nexus.sh
source "${LIBS_DIR}/_nexus.sh"
# shellcheck source=release/libs/_svn.sh
source "${LIBS_DIR}/_svn.sh"

function usage {
  cat <<EOF
Usage: $0 <version> <rc-num> <staging-repo-id> [--allow-description-mismatch]

Cancel an Apache Iceberg release candidate after a failed vote.

This drops the Nexus staging repo and removes the RC artifacts from the
SVN dist/dev tree. The RC git tag is intentionally left in place: Apache
release policy is to leave failed RC tags as a permanent record, and the
next prepare-rc invocation will pick the next RC number automatically.

Arguments:
  version                          Release version (e.g., 1.10.0)
  rc-num                           RC number to cancel (e.g., 0)
  staging-repo-id                  Nexus staging repository ID (e.g., orgapacheiceberg-1234)

Options:
  --allow-description-mismatch     Continue even if the staging repo description does not match
                                   "Apache Iceberg <version> RC<rc-num>". Useful only if the
                                   description was edited via the Nexus UI during recovery.
  --help, -h                       Show this help

Environment variables:
  DRY_RUN               Set to 0 for real execution (default: 1)
  NEXUS_USERNAME        Apache Nexus username
  NEXUS_PASSWORD        Apache Nexus password
  SVN_USERNAME          SVN username for dist.apache.org
  SVN_PASSWORD          SVN password

Example:
  DRY_RUN=1 $0 1.10.0 0 orgapacheiceberg-1234
EOF
  exit "${1:-0}"
}

allow_description_mismatch=0
positional=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --allow-description-mismatch)
      allow_description_mismatch=1
      shift
      ;;
    --help|-h)
      usage 0
      ;;
    -*)
      print_error "Unknown option: $1"
      usage 1
      ;;
    *)
      positional+=("$1")
      shift
      ;;
  esac
done

if [[ ${#positional[@]} -ne 3 ]]; then
  print_error "Expected 3 positional arguments (version, rc-num, staging-repo-id), got ${#positional[@]}"
  usage 1
fi

version="${positional[0]}"
rc_num="${positional[1]}"
staging_repo_id="${positional[2]}"

# ---------------------------------------------------------------------------
# Validate inputs
# ---------------------------------------------------------------------------
step_summary "## Release Candidate Cancellation"
step_summary ""

if [[ ${DRY_RUN:-1} -eq 1 ]]; then
  step_summary "> **DRY RUN** -- no changes will be made"
  step_summary ""
fi

if ! validate_and_extract_version "${version}"; then
  print_error "Invalid version format: '${version}'"
  exit 1
fi

if ! [[ "${rc_num}" =~ ^[0-9]+$ ]]; then
  print_error "Invalid RC number: '${rc_num}'. Expected a non-negative integer."
  exit 1
fi

if ! [[ "${staging_repo_id}" =~ ^[a-zA-Z][a-zA-Z0-9._-]*$ ]]; then
  print_error "Invalid staging repository ID: '${staging_repo_id}'. Expected alphanumeric with dots/hyphens (e.g., orgapacheiceberg-1234)."
  exit 1
fi

rc_tag="${TAG_PREFIX}${version}-rc${rc_num}"

step_summary "| Parameter | Value |"
step_summary "| --- | --- |"
step_summary "| Version | \`${version}\` |"
step_summary "| RC tag | \`${rc_tag}\` |"
step_summary "| Staging repo | \`${staging_repo_id}\` |"

# ---------------------------------------------------------------------------
# Step 1: Verify the Nexus staging repository matches this RC
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Staging Repository Verification"

if ! nexus_verify_staging_repo "${staging_repo_id}" "${version}" "${rc_num}"; then
  step_summary "Staging repo verification: **FAILED**"
  exit 1
fi
step_summary "Staging repo verification: **PASSED**"

# ---------------------------------------------------------------------------
# Step 2: Drop the Nexus staging repo
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Nexus Cleanup"

nexus_drop_staging_repo "${staging_repo_id}" "Cancel Apache Iceberg ${version} RC${rc_num}"

step_summary "Dropped staging repository \`${staging_repo_id}\`"

# ---------------------------------------------------------------------------
# Step 3: Remove the RC artifacts from SVN dist/dev
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### SVN Cleanup"

dev_url="${APACHE_DIST_URL}${APACHE_DIST_DEV_PATH}/${rc_tag}"

if [[ ${DRY_RUN:-1} -ne 1 ]]; then
  if svn_path_exists "${dev_url}"; then
    svn_run rm "${dev_url}" -m "Cancel Apache Iceberg ${version} RC${rc_num}"
    step_summary "Deleted \`${dev_url}\`"
  else
    print_warning "SVN directory not found: ${dev_url}"
    step_summary "Directory not found at \`${dev_url}\` (may already be deleted)"
  fi
else
  print_command "Dry-run, WOULD delete ${dev_url}"
  step_summary "Would delete \`${dev_url}\` (dry-run)"
fi

# ---------------------------------------------------------------------------
# Step 4: Generate the vote-failure email template
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Vote Failure Email"
step_summary ""
step_summary '```'
step_summary "To: dev@iceberg.apache.org"
step_summary "Subject: [RESULT][VOTE] Release Apache Iceberg ${version} RC${rc_num}"
step_summary ""
step_summary "Hello everyone,"
step_summary ""
step_summary "Thanks to all who participated in the vote for Release Apache Iceberg ${version} RC${rc_num}."
step_summary ""
step_summary "The vote result is:"
step_summary ""
step_summary "+1: a (binding), b (non-binding)"
step_summary "+0: c (binding), d (non-binding)"
step_summary "-1: e (binding), f (non-binding)"
step_summary ""
step_summary "The vote failed due to [REASON - TO BE FILLED BY RELEASE MANAGER]."
step_summary ""
step_summary "A new release candidate will be proposed soon once the issues are addressed."
step_summary ""
step_summary "Thanks,"
step_summary '```'

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
step_summary ""
step_summary "---"
step_summary "### Summary"
step_summary ""
step_summary "| Step | Status |"
step_summary "| --- | --- |"
step_summary "| Nexus staging repo | dropped |"
step_summary "| SVN dist/dev | deleted |"
step_summary "| Failure email | generated |"

print_success "Release candidate ${rc_tag} cancelled successfully."
