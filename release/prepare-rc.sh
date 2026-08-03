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
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck source=release/libs/_constants.sh
source "${LIBS_DIR}/_constants.sh"
# shellcheck source=release/libs/_log.sh
source "${LIBS_DIR}/_log.sh"
# shellcheck source=release/libs/_exec.sh
source "${LIBS_DIR}/_exec.sh"
# shellcheck source=release/libs/_version.sh
source "${LIBS_DIR}/_version.sh"
# shellcheck source=release/libs/_github.sh
source "${LIBS_DIR}/_github.sh"
# shellcheck source=release/libs/_nexus.sh
source "${LIBS_DIR}/_nexus.sh"
# shellcheck source=release/libs/_svn.sh
source "${LIBS_DIR}/_svn.sh"
# shellcheck source=release/libs/_gradle.sh
source "${LIBS_DIR}/_gradle.sh"

function usage {
  cat <<EOF
Usage: $0 <version> [OPTIONS]

Prepare a release candidate for Apache Iceberg.

This script automates the steps in dev/source-release.sh and
dev/stage-binaries.sh, plus Nexus close and GitHub pre-release creation.

Branching follows Iceberg convention:
  * X.Y.0   RCs are tagged from main; the X.Y.x release branch does
            not need to exist yet.
  * X.Y.Z   patch RCs (Z >= 1) require the X.Y.x branch to already
            exist on the remote. Create it from the apache-iceberg-X.Y.0
            tag commit after the X.Y.0 final ships.

Arguments:
  version               Release version (e.g., 1.10.0)

Options:
  --rc <num>            Override RC number (default: auto-detect from tags)
  --key <keyid>         GPG key ID to sign the source tarball (default: gpg's default key)
  --remote <name>       Git remote to push tags to (default: 'apache' if configured, else 'origin')
  --skip-ci-check       Skip the GitHub CI status check (use with care)
  --help                Show this help

Environment variables:
  DRY_RUN               Set to 0 for real execution (default: 1)
  NEXUS_USERNAME        Apache Nexus username
  NEXUS_PASSWORD        Apache Nexus password
  SVN_USERNAME          SVN username for dist.apache.org
  SVN_PASSWORD          SVN password
  GITHUB_TOKEN          GitHub token for CI checks and pre-release creation

Examples:
  DRY_RUN=1 $0 1.10.0
  DRY_RUN=0 $0 1.10.0 --rc 2 --key ABCD1234
  DRY_RUN=0 $0 1.10.1 --key ABCD1234     # requires apache/1.10.x to exist
EOF
  exit "${1:-0}"
}

version=""
rc_override=""
keyid=""
remote=""
skip_ci_check=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rc)
      if [[ -z "${2:-}" ]]; then
        print_error "--rc requires a value"
        usage 1
      fi
      rc_override="$2"
      if [[ ! "${rc_override}" =~ ^[0-9]+$ ]]; then
        print_error "--rc value must be a non-negative integer, got: '${rc_override}'"
        exit 1
      fi
      shift 2
      ;;
    --key)
      if [[ -z "${2:-}" ]]; then
        print_error "--key requires a value"
        usage 1
      fi
      keyid="$2"
      shift 2
      ;;
    --remote)
      if [[ -z "${2:-}" ]]; then
        print_error "--remote requires a value"
        usage 1
      fi
      remote="$2"
      shift 2
      ;;
    --skip-ci-check)
      skip_ci_check=true
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
      if [[ -z "${version}" ]]; then
        version="$1"
      else
        print_error "Unexpected argument: $1"
        usage 1
      fi
      shift
      ;;
  esac
done

if [[ -z "${version}" ]]; then
  print_error "Version is required"
  usage 1
fi

if [[ -z "${remote}" ]]; then
  if git remote get-url apache >/dev/null 2>&1; then
    remote="apache"
  else
    remote="origin"
  fi
fi

# ---------------------------------------------------------------------------
# Step 0: Validate inputs and prerequisites
# ---------------------------------------------------------------------------
step_summary "## Release Candidate Preparation"
step_summary ""

if [[ ${DRY_RUN:-1} -eq 1 ]]; then
  step_summary "> **DRY RUN** -- no changes will be made"
  step_summary ""
fi

if ! validate_and_extract_version "${version}"; then
  print_error "Invalid version format: '${version}'. Expected: X.Y.Z"
  exit 1
fi

step_summary "| Parameter | Value |"
step_summary "| --- | --- |"
step_summary "| Version | \`${version}\` |"

if ! command -v gpg &>/dev/null; then
  print_warning "gpg not found -- GPG signing will fail"
fi
if ! command -v svn &>/dev/null; then
  print_warning "svn not found -- SVN staging will fail"
fi
if [[ ! -x "${PROJECT_DIR}/gradlew" ]]; then
  print_error "gradlew not found at ${PROJECT_DIR}/gradlew. Run from the iceberg repo root."
  exit 1
fi

if [[ ${DRY_RUN:-1} -ne 1 ]]; then
  if ! verify_jdk_17; then
    exit 1
  fi
fi

# Verify the configured git remote is reachable.
if ! git ls-remote --exit-code "${remote}" >/dev/null 2>&1; then
  print_error "Git remote '${remote}' is not configured or unreachable. Pass --remote to override."
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 1: Pick the ref to tag from
# ---------------------------------------------------------------------------
#
#   X.Y.0  tags from ${remote}/main; the X.Y.x branch is not required.
#   X.Y.Z  (Z >= 1) requires ${remote}/X.Y.x to exist on the remote.
exec_process git fetch "${remote}" --tags

release_branch="$(expected_branch_for_version "${major}" "${minor}")"
step_summary ""
step_summary "### Release Source"

if [[ "${patch}" -eq 0 ]]; then
  if ! git show-ref --verify --quiet "refs/remotes/${remote}/main"; then
    print_error "Branch '${remote}/main' not found on remote; cannot tag X.Y.0."
    exit 1
  fi
  release_hash="$(git rev-parse "${remote}/main")"
  print_info "X.Y.0 release: tagging RC from ${remote}/main at ${release_hash}"
  step_summary "| Tagging from | \`${remote}/main\` |"
  step_summary "| Release branch | not yet created -- Iceberg convention is to branch from \`apache-iceberg-${version}\` after the final ships |"
else
  if ! git show-ref --verify --quiet "refs/remotes/${remote}/${release_branch}"; then
    print_error "Release branch '${remote}/${release_branch}' not found on remote."
    print_error "Iceberg's convention is to create the ${release_branch} branch from the"
    print_error "apache-iceberg-${major}.${minor}.0 final tag commit after that release ships."
    print_error "To create it now:"
    print_error "  git push ${remote} apache-iceberg-${major}.${minor}.0:refs/heads/${release_branch}"
    print_error "Then re-run prepare-rc."
    exit 1
  fi
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    if [[ "$(git branch --show-current 2>/dev/null || echo '')" != "${release_branch}" ]]; then
      if git show-ref --verify --quiet "refs/heads/${release_branch}"; then
        exec_process git checkout "${release_branch}"
      else
        exec_process git checkout -b "${release_branch}" --track "${remote}/${release_branch}"
      fi
    fi
    exec_process git pull --ff-only "${remote}" "${release_branch}"
  else
    print_command "Dry-run, WOULD checkout ${release_branch} (tracking ${remote}/${release_branch}) and fast-forward"
  fi
  release_hash="$(git rev-parse "${remote}/${release_branch}")"
  print_info "Patch release: tagging from ${remote}/${release_branch} at ${release_hash}"
  step_summary "| Tagging from | \`${remote}/${release_branch}\` |"
  step_summary "| Release branch | \`${release_branch}\` |"
fi

# ---------------------------------------------------------------------------
# Step 2: Auto-detect RC number
# ---------------------------------------------------------------------------
if [[ -n "${rc_override}" ]]; then
  rc_number="${rc_override}"
  print_info "Using RC override: rc${rc_number}"
else
  find_next_rc_number "${version}"
  print_info "Auto-detected next RC: rc${rc_number}"
fi

rc_tag="${TAG_PREFIX}${version}-rc${rc_number}"
final_tag="${TAG_PREFIX}${version}"
step_summary "| RC number | \`${rc_number}\` |"
step_summary "| RC tag | \`${rc_tag}\` |"

if git rev-parse "${rc_tag}" >/dev/null 2>&1; then
  print_error "Tag ${rc_tag} already exists. Use --rc to specify a different RC number."
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 3: Verify CI checks on the release commit
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### CI Verification"
step_summary "| Commit | \`${release_hash}\` |"

if [[ "${skip_ci_check}" == "true" ]]; then
  print_warning "Skipping CI verification (--skip-ci-check)"
  step_summary "CI checks: **SKIPPED**"
elif ! check_github_checks_passed "${release_hash}"; then
  print_error "CI checks are not passing. Fix CI before creating an RC."
  step_summary "CI checks: **FAILED**"
  exit 1
else
  step_summary "CI checks: **PASSED**"
fi

# ---------------------------------------------------------------------------
# Step 4: Create and push the RC tag
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Tag and Push"

exec_process git tag -a "${rc_tag}" -m "Apache Iceberg ${version} RC${rc_number}" "${release_hash}"
exec_process git push "${remote}" "${rc_tag}"

step_summary "Created tag \`${rc_tag}\` at \`${release_hash}\` and pushed to \`${remote}\`"

# Align the workspace with release_hash so both the source tarball
# (which runs `./gradlew generateGitProperties` and `git archive` from
# the working tree) and the convenience binaries (which run gradle
# publish tasks against the working tree) build from the same commit.
# For the patch path we are already at release_hash via the earlier
# checkout / pull --ff-only, so this is a no-op.
if [[ "$(git rev-parse HEAD)" != "${release_hash}" ]]; then
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    exec_process git checkout --detach "${release_hash}"
  else
    print_command "Dry-run, WOULD checkout ${release_hash} (detached) so binaries match the tarball"
  fi
fi

# ---------------------------------------------------------------------------
# Step 5: Build source tarball
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Source Tarball"

tarball_name="${final_tag}.tar.gz"
build_source_tarball "${PROJECT_DIR}" "${final_tag}" "${release_hash}" "${version}" "${keyid}"

step_summary "Built \`${tarball_name}\` from \`${release_hash}\`"
step_summary "Wrote \`${tarball_name}.asc\` (GPG signature) and \`${tarball_name}.sha512\` (SHA-512)"

# ---------------------------------------------------------------------------
# Step 6: Stage source artifacts to SVN dist/dev
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### SVN Staging"

svn_dir="${APACHE_DIST_URL}${APACHE_DIST_DEV_PATH}"
rc_svn_dir="${rc_tag}"

if [[ ${DRY_RUN:-1} -ne 1 ]]; then
  : "${SVN_USERNAME:?SVN_USERNAME must be set for real (non-dry-run) execution}"
  : "${SVN_PASSWORD:?SVN_PASSWORD must be set for real (non-dry-run) execution}"

  tmpdir="${PROJECT_DIR}/tmp-svn-stage"
  if [[ -d "${tmpdir}" ]]; then
    rm -rf "${tmpdir}"
  fi

  svn_run_with_retries 5 60 "${tmpdir}" \
    co --depth=empty "${svn_dir}" "${tmpdir}"

  mkdir -p "${tmpdir}/${rc_svn_dir}"
  cp "${PROJECT_DIR}/${tarball_name}" \
     "${PROJECT_DIR}/${tarball_name}.asc" \
     "${PROJECT_DIR}/${tarball_name}.sha512" \
     "${tmpdir}/${rc_svn_dir}/"

  (cd "${tmpdir}" && exec_process svn add "${rc_svn_dir}")
  (cd "${tmpdir}" && svn_run ci -m "Apache Iceberg ${version} RC${rc_number}")

  rm -rf "${tmpdir}"
else
  print_command "Dry-run, WOULD stage to ${svn_dir}/${rc_svn_dir}"
fi

step_summary "Staged source tarball to \`${svn_dir}/${rc_svn_dir}\`"

# ---------------------------------------------------------------------------
# Step 7: Stage convenience binaries to Nexus
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Nexus Deployment"

(cd "${PROJECT_DIR}" && stage_convenience_binaries)

step_summary "Published Iceberg artifacts to Apache Nexus staging across the configured Scala/Spark/Flink/Kafka matrix"

# ---------------------------------------------------------------------------
# Step 8: Find and close the Nexus staging repo
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Nexus Staging Close"

nexus_find_open_staging_repo "${NEXUS_STAGING_PROFILE}"
nexus_close_staging_repo "${staging_repo_id}" "Apache Iceberg ${version} RC${rc_number}"

step_summary "Closed staging repository: \`${staging_repo_id}\`"

# ---------------------------------------------------------------------------
# Step 9: Create GitHub pre-release
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### GitHub Pre-Release"

if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  exec_process gh release create "${rc_tag}" \
    --title "Apache Iceberg ${version} RC${rc_number}" \
    --prerelease \
    --generate-notes \
    --target "${release_hash}" \
    --repo "${GITHUB_REPO}"
  step_summary "Created GitHub pre-release for \`${rc_tag}\`"
else
  print_warning "GITHUB_TOKEN not set, skipping GitHub pre-release creation"
  step_summary "Skipped GitHub pre-release (no GITHUB_TOKEN)"
fi

# ---------------------------------------------------------------------------
# Step 10: Generate vote email template
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Vote Email"
step_summary ""
step_summary '```'
step_summary "To: dev@iceberg.apache.org"
step_summary "Subject: [VOTE] Release Apache Iceberg ${version} RC${rc_number}"
step_summary ""
step_summary "Hi everyone,"
step_summary ""
step_summary "I propose that we release the following RC as the official Apache Iceberg ${version} release."
step_summary ""
step_summary "The commit ID is ${release_hash}"
step_summary "* This corresponds to the tag: ${rc_tag}"
step_summary "* https://github.com/${GITHUB_REPO}/commits/${rc_tag}"
step_summary "* https://github.com/${GITHUB_REPO}/tree/${release_hash}"
step_summary ""
step_summary "The release tarball, signature, and checksums are here:"
step_summary "* ${APACHE_DIST_URL}${APACHE_DIST_DEV_PATH}/${rc_tag}"
step_summary ""
step_summary "You can find the KEYS file here:"
step_summary "* https://downloads.apache.org/iceberg/KEYS"
step_summary ""
step_summary "Convenience binary artifacts are staged on Nexus. The Maven repository URL is:"
step_summary "* https://repository.apache.org/content/repositories/${staging_repo_id}/"
step_summary ""
step_summary "Please download, verify, and test."
step_summary ""
step_summary "Instructions for verifying a release can be found here:"
step_summary "* https://iceberg.apache.org/how-to-release/#how-to-verify-a-release"
step_summary ""
step_summary "Please vote in the next 72 hours. (Weekends excluded)"
step_summary ""
step_summary "[ ] +1 Release this as Apache Iceberg ${version}"
step_summary "[ ] +0"
step_summary "[ ] -1 Do not release this because..."
step_summary ""
step_summary "Only PMC members have binding votes, but other community members are encouraged to cast"
step_summary "non-binding votes. This vote will pass if there are 3 binding +1 votes and more binding"
step_summary "+1 votes than -1 votes."
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
if [[ "${patch}" -eq 0 ]]; then
  step_summary "| Tagged from | \`${remote}/main\` |"
else
  step_summary "| Tagged from | \`${remote}/${release_branch}\` |"
fi
step_summary "| RC tag | \`${rc_tag}\` |"
step_summary "| Source tarball | \`${tarball_name}\` |"
step_summary "| SVN dist/dev | staged |"
step_summary "| Nexus staging repo | \`${staging_repo_id:-UNKNOWN}\` (closed) |"
step_summary "| GitHub pre-release | created |"
step_summary "| Vote email | generated |"

print_success "Release candidate ${rc_tag} prepared successfully!"
