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
Usage: $0 <version> <staging-repo-id> [--rc <num>]

Publish an Apache Iceberg release after a successful vote.

Iceberg derives its development version from git tags via the gitVersion
plugin, so this script does not commit a "next development iteration"
version bump.

Arguments:
  version               Release version (e.g., 1.10.0)
  staging-repo-id       Nexus staging repository ID (e.g., orgapacheiceberg-1234)

Options:
  --rc <num>                       RC number that passed the vote (default: auto-detect latest)
  --remote <name>                  Git remote to push the final tag to (default: origin)
  --keep-old-releases              Skip removal of superseded patch releases for this MAJOR.MINOR line
  --allow-description-mismatch     Continue even if the staging repo description does not match
                                   "Apache Iceberg <version> RC<num>". Useful only if the
                                   description was edited via the Nexus UI during recovery.
  --help                           Show this help

Environment variables:
  DRY_RUN               Set to 0 for real execution (default: 1)
  NEXUS_USERNAME        Apache Nexus username
  NEXUS_PASSWORD        Apache Nexus password
  SVN_USERNAME          SVN username for dist.apache.org
  SVN_PASSWORD          SVN password
  GITHUB_TOKEN          GitHub token for release creation

Examples:
  DRY_RUN=1 $0 1.10.0 orgapacheiceberg-1234
  DRY_RUN=0 $0 1.10.0 orgapacheiceberg-1234 --rc 2
EOF
  exit "${1:-0}"
}

version=""
staging_repo_id=""
rc_num=""
remote="origin"
keep_old_releases=false
allow_description_mismatch=0
positional=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rc)
      if [[ -z "${2:-}" ]]; then
        print_error "--rc requires a value"
        usage 1
      fi
      rc_num="$2"
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
    --keep-old-releases)
      keep_old_releases=true
      shift
      ;;
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

if [[ ${#positional[@]} -lt 2 ]]; then
  print_error "Expected 2 positional arguments (version, staging-repo-id), got ${#positional[@]}"
  usage 1
fi

version="${positional[0]}"
staging_repo_id="${positional[1]}"

# ---------------------------------------------------------------------------
# Validate inputs
# ---------------------------------------------------------------------------
step_summary "## Release Publication"
step_summary ""

if [[ ${DRY_RUN:-1} -eq 1 ]]; then
  step_summary "> **DRY RUN** -- no changes will be made"
  step_summary ""
fi

if ! validate_and_extract_version "${version}"; then
  print_error "Invalid version format: '${version}'"
  exit 1
fi

if ! [[ "${staging_repo_id}" =~ ^[a-zA-Z][a-zA-Z0-9._-]*$ ]]; then
  print_error "Invalid staging repository ID: '${staging_repo_id}'. Expected alphanumeric with dots/hyphens (e.g., orgapacheiceberg-1234)."
  exit 1
fi

if [[ -z "${rc_num}" ]]; then
  print_info "No RC number specified, auto-detecting latest RC for ${version}..."
  if ! find_latest_rc_number "${version}"; then
    exit 1
  fi
  rc_num="${latest_rc_number}"
  print_info "Auto-detected latest RC: rc${rc_num}"
else
  if ! [[ "${rc_num}" =~ ^[0-9]+$ ]]; then
    print_error "Invalid RC number: '${rc_num}'. Expected a non-negative integer."
    exit 1
  fi

  if find_latest_rc_number "${version}" 2>/dev/null; then
    if [[ "${rc_num}" -ne "${latest_rc_number}" ]]; then
      print_error "RC${rc_num} is not the latest RC for ${version}. Latest is rc${latest_rc_number}."
      print_error "Publishing an older RC is likely a mistake. If intentional, delete the newer RC tags first."
      exit 1
    fi
  fi
fi

rc_tag="${TAG_PREFIX}${version}-rc${rc_num}"
final_tag="${TAG_PREFIX}${version}"

if ! git rev-parse "${rc_tag}" >/dev/null 2>&1; then
  print_error "RC tag ${rc_tag} does not exist locally; fetch it from the remote and retry"
  exit 1
fi

rc_commit=$(git rev-list -1 "${rc_tag}")

if git rev-parse "${final_tag}" >/dev/null 2>&1; then
  print_error "Final release tag ${final_tag} already exists"
  exit 1
fi

# HEAD must match the RC tag commit exactly.
head_commit=$(git rev-parse HEAD)
if [[ "${head_commit}" != "${rc_commit}" ]]; then
  print_error "HEAD (${head_commit}) does not match RC tag ${rc_tag} (${rc_commit})."
  print_error "Check out the RC tag exactly (e.g. 'git checkout ${rc_tag}') and re-run."
  exit 1
fi

step_summary "| Parameter | Value |"
step_summary "| --- | --- |"
step_summary "| Version | \`${version}\` |"
step_summary "| RC tag | \`${rc_tag}\` |"
step_summary "| Final tag | \`${final_tag}\` |"
step_summary "| Staging repo | \`${staging_repo_id}\` |"
step_summary "| Commit | \`${rc_commit}\` |"

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
# Step 2: Promote SVN artifacts from dist/dev to dist/release
# ---------------------------------------------------------------------------
# Requires PMC privileges on dist.apache.org.
step_summary ""
step_summary "### SVN Promotion"

dev_url="${APACHE_DIST_URL}${APACHE_DIST_DEV_PATH}/${rc_tag}"
release_url="${APACHE_DIST_URL}${APACHE_DIST_RELEASE_PATH}/${final_tag}"

svn_run mv "${dev_url}" "${release_url}" -m "Iceberg: Add release ${version}"

step_summary "Moved \`${dev_url}\` -> \`${release_url}\`"

# ---------------------------------------------------------------------------
# Step 3: Clean up superseded patch releases from dist/release
# ---------------------------------------------------------------------------
#
# When promoting a patch release we remove earlier patches from the same
# MAJOR.MINOR line (e.g. publishing 1.9.2 removes 1.9.0 and 1.9.1).
# Older minor lines are left in place; the standard ASF archival flow
# moves them to archive.apache.org separately.
step_summary ""
step_summary "### Old Release Cleanup"

if [[ "${keep_old_releases}" == "true" ]]; then
  print_info "Skipping dist/release cleanup (--keep-old-releases)"
  step_summary "Skipped (--keep-old-releases)"
elif [[ ${DRY_RUN:-1} -ne 1 ]]; then
  release_base_url="${APACHE_DIST_URL}${APACHE_DIST_RELEASE_PATH}"
  svn_listing=""
  if ! svn_listing=$(svn_list "${release_base_url}"); then
    print_error "Failed to list SVN releases at ${release_base_url}: ${svn_listing}"
    exit 1
  fi

  old_versions=$(filter_superseded_patch_releases "${svn_listing}" "${version}")

  if [[ -n "${old_versions}" ]]; then
    step_summary "Removing superseded patch releases for ${major}.${minor}.x:"
    while IFS= read -r old_dir; do
      [[ -z "${old_dir}" ]] && continue
      svn_run rm "${release_base_url}/${old_dir}" \
        -m "Remove old release ${old_dir} (superseded by ${version})"
      step_summary "- Removed \`${old_dir}\`"
    done <<< "${old_versions}"
  else
    step_summary "No superseded patch releases to remove for ${major}.${minor}.x"
  fi
else
  print_command "Dry-run, WOULD remove superseded ${major}.${minor}.x patch releases from ${APACHE_DIST_RELEASE_PATH}"
  step_summary "Would remove superseded ${major}.${minor}.x patch releases (dry-run)"
fi

# ---------------------------------------------------------------------------
# Step 4: Create and push the final release tag
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Release Tag"

exec_process git tag -a "${final_tag}" "${rc_commit}" -m "Release Apache Iceberg ${version}"
exec_process git push "${remote}" "${final_tag}"

step_summary "Created tag \`${final_tag}\` at \`${rc_commit}\` and pushed to \`${remote}\`"

# ---------------------------------------------------------------------------
# Step 5: Release the Nexus staging repo to Maven Central
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Nexus Release"

nexus_release_staging_repo "${staging_repo_id}" "Apache Iceberg ${version}"

step_summary "Released staging repository \`${staging_repo_id}\` to Maven Central"

# ---------------------------------------------------------------------------
# Step 6: Create the GitHub Release
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### GitHub Release"

if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  exec_process gh release create "${final_tag}" \
    --title "Apache Iceberg ${version}" \
    --generate-notes \
    --latest \
    --target "${rc_commit}" \
    --repo "${GITHUB_REPO}"

  step_summary "Created GitHub release for \`${final_tag}\`"
else
  print_warning "GITHUB_TOKEN not set, skipping GitHub release creation"
  step_summary "Skipped GitHub release (no GITHUB_TOKEN)"
fi

# ---------------------------------------------------------------------------
# Step 7: Generate the announce email template
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Announce Email"
step_summary ""
step_summary '```'
step_summary "To: dev@iceberg.apache.org, announce@apache.org"
step_summary "Subject: [ANNOUNCE] Apache Iceberg release ${version}"
step_summary ""
step_summary "I'm pleased to announce the release of Apache Iceberg ${version}!"
step_summary ""
step_summary "Apache Iceberg is an open table format for huge analytic datasets. Iceberg"
step_summary "delivers high query performance for tables with tens of petabytes of data,"
step_summary "along with atomic commits, concurrent writes, and SQL-compatible table"
step_summary "evolution."
step_summary ""
step_summary "This release can be downloaded from: https://www.apache.org/dyn/closer.cgi/iceberg/${final_tag}/${final_tag}.tar.gz"
step_summary ""
step_summary "Release notes: https://iceberg.apache.org/releases/#${version//./}-release"
step_summary ""
step_summary "Java artifacts are available from Maven Central."
step_summary ""
step_summary "Thanks to everyone for contributing!"
step_summary '```'

# ---------------------------------------------------------------------------
# Step 8: Manual follow-up reminders
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Manual Follow-ups"
step_summary ""
step_summary "The following steps are still manual and must be completed by the release manager:"
step_summary ""
step_summary "1. **Send the announce email** above to dev@iceberg.apache.org and announce@apache.org"
step_summary "   (only after Maven Central has mirrored the artifacts; allow up to 24 hours)."
step_summary "2. **Update revapi**: open a PR to enable revapi against the new release"
step_summary "   ([example](https://github.com/apache/iceberg/pull/6275))."
step_summary "3. **Update GitHub issue template**: open a PR to add \`${version}\` to the version dropdown"
step_summary "   ([example](https://github.com/apache/iceberg/pull/6287))."
step_summary "4. **Update doap.rdf**: open a PR adding a new \`<release/>\` block for \`${version}\`"
step_summary "   in [doap.rdf](https://github.com/${GITHUB_REPO}/blob/main/doap.rdf)."
step_summary "5. **Generate versioned docs**: copy \`docs/\` from the release tag onto the \`docs\` branch"
step_summary "   under \`${version}/\` and update \`docs/latest\` -> \`docs/${version}\` in the copied"
step_summary "   \`mkdocs.yml\` (see [example](https://github.com/${GITHUB_REPO}/pull/12411))."
step_summary "6. **Generate versioned Javadoc** (with JDK 17+): \`./gradlew refreshJavadoc\` in an"
step_summary "   extracted source tarball, copy \`site/docs/javadoc/${version}\` onto the \`javadoc\`"
step_summary "   branch (see [example](https://github.com/${GITHUB_REPO}/pull/12412))."
step_summary "7. **Site update**: open a PR updating the Iceberg version, doc links, and release notes"
step_summary "   on the website (see [example](https://github.com/${GITHUB_REPO}/pull/12242))."

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
step_summary ""
step_summary "---"
step_summary "### Summary"
step_summary ""
step_summary "| Step | Status |"
step_summary "| --- | --- |"
step_summary "| SVN promotion | done |"
step_summary "| Old release cleanup | done |"
step_summary "| Final release tag | \`${final_tag}\` |"
step_summary "| Nexus release | \`${staging_repo_id}\` released |"
step_summary "| GitHub release | created |"
step_summary "| Announce email | generated |"
step_summary "| Manual follow-ups | see above |"

print_success "Release ${version} published successfully!"
