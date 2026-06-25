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

[[ -n "${_NEXUS_LOADED:-}" ]] && return 0 2>/dev/null || true
_NEXUS_LOADED=1

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=release/libs/_constants.sh
source "${LIBS_DIR}/_constants.sh"
# shellcheck source=release/libs/_exec.sh
source "${LIBS_DIR}/_exec.sh"

# nexus_find_open_staging_repo populates the caller-visible global
# staging_repo_id; shellcheck flags it as unused inside this file.
# shellcheck disable=SC2034

# Apache's Nexus 2 staging API exposes "close", "promote" and "drop" as
# bulk operations that take a list of repo IDs plus an optional
# description. The auth credentials are passed via curl's `-K` flag (config
# file on stdin) so they never appear on the command line, even in a
# debug-traced run.
function _nexus_bulk_action {
  local action="$1"
  local repo_id="$2"
  local description="$3"

  local url="${NEXUS_BASE_URL}/staging/bulk/${action}"
  local payload
  payload=$(jq -n --arg id "${repo_id}" --arg desc "${description}" \
    '{"data": {"stagedRepositoryIds": [$id], "description": $desc}}')

  print_info "Nexus ${action}: repo_id=${repo_id}"

  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    : "${NEXUS_USERNAME:?NEXUS_USERNAME must be set for real (non-dry-run) execution}"
    : "${NEXUS_PASSWORD:?NEXUS_PASSWORD must be set for real (non-dry-run) execution}"
    print_command "Executing 'curl --fail -X POST ${url}' (credentials via stdin)"
    curl --fail --silent --show-error \
      -K <(printf 'user = "%s:%s"\n' "${NEXUS_USERNAME}" "${NEXUS_PASSWORD}") \
      -H "Content-Type: application/json" \
      -d "${payload}" \
      "${url}"
  else
    print_command "Dry-run, WOULD POST to ${url} with payload for repo ${repo_id}"
  fi
}

function nexus_close_staging_repo {
  local repo_id="$1"
  local description="${2:-Closing staging repository}"
  _nexus_bulk_action "close" "${repo_id}" "${description}"
}

function nexus_release_staging_repo {
  local repo_id="$1"
  local description="${2:-Releasing staging repository}"
  _nexus_bulk_action "promote" "${repo_id}" "${description}"
}

function nexus_drop_staging_repo {
  local repo_id="$1"
  local description="${2:-Dropping staging repository}"
  _nexus_bulk_action "drop" "${repo_id}" "${description}"
}

# Locates the single open staging repository for the configured profile
# (default: org.apache.iceberg) and exports its ID into staging_repo_id.
# Fails if zero or more than one open repo is found.
function nexus_find_open_staging_repo {
  local profile_name="${1:-${NEXUS_STAGING_PROFILE}}"

  print_info "Searching for open staging repository for ${profile_name}..."

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD search Nexus for open staging repo"
    staging_repo_id="DRY-RUN-REPO-ID"
    return 0
  fi

  : "${NEXUS_USERNAME:?NEXUS_USERNAME must be set for real (non-dry-run) execution}"
  : "${NEXUS_PASSWORD:?NEXUS_PASSWORD must be set for real (non-dry-run) execution}"

  local response
  if ! response=$(curl --fail --silent --show-error \
    -K <(printf 'user = "%s:%s"\n' "${NEXUS_USERNAME}" "${NEXUS_PASSWORD}") \
    "${NEXUS_BASE_URL}/staging/profile_repositories"); then
    print_error "Failed to query Nexus staging repositories"
    return 1
  fi

  local matches
  matches=$(echo "${response}" | \
    NEXUS_PROFILE_NAME="${profile_name}" python3 -c "
import sys, os, xml.etree.ElementTree as ET
profile = os.environ['NEXUS_PROFILE_NAME']
tree = ET.parse(sys.stdin)
for repo in tree.findall('.//stagingProfileRepository'):
    repo_type = repo.find('type')
    repo_id = repo.find('repositoryId')
    if repo_type is not None and repo_type.text == 'open' and repo_id is not None:
        if profile.replace('.', '') in (repo_id.text or ''):
            print(repo_id.text)
" 2>/dev/null)

  local match_count
  match_count=$(echo -n "${matches}" | grep -c . || true)

  if [[ ${match_count} -eq 0 ]]; then
    print_error "No open staging repository found for ${profile_name}"
    return 1
  fi

  if [[ ${match_count} -gt 1 ]]; then
    print_error "Found ${match_count} open staging repositories for ${profile_name}; expected exactly one"
    print_error "Open repos: $(echo "${matches}" | tr '\n' ' ')"
    print_error "Multiple staging repositories typically indicate that gradle parallelism was enabled or"
    print_error "that the publish ran from a network with floating outbound IPs. Drop the extras manually"
    print_error "in the Nexus UI and re-run."
    return 1
  fi

  staging_repo_id="${matches}"
  print_info "Found staging repository: ${staging_repo_id}"
  return 0
}

# Fetches `<staging-repo>/repository/${repo_id}` from the Nexus 2 API and
# extracts profileName, type (state), and description. Sets these globals
# for callers to consume:
#   _nexus_repo_profile, _nexus_repo_state, _nexus_repo_description
#
# Returns non-zero if the repository does not exist or the API call fails.
# In dry-run mode this is a no-op and returns the placeholder values so
# downstream verification logic can be exercised end-to-end.
# shellcheck disable=SC2034
function nexus_get_staging_repo_metadata {
  local repo_id="$1"

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD fetch Nexus metadata for ${repo_id}"
    _nexus_repo_profile="${NEXUS_STAGING_PROFILE}"
    _nexus_repo_state="closed"
    _nexus_repo_description="DRY-RUN"
    return 0
  fi

  : "${NEXUS_USERNAME:?NEXUS_USERNAME must be set for real (non-dry-run) execution}"
  : "${NEXUS_PASSWORD:?NEXUS_PASSWORD must be set for real (non-dry-run) execution}"

  local response
  if ! response=$(curl --fail --silent --show-error \
    -K <(printf 'user = "%s:%s"\n' "${NEXUS_USERNAME}" "${NEXUS_PASSWORD}") \
    "${NEXUS_BASE_URL}/staging/repository/${repo_id}"); then
    print_error "Could not fetch Nexus staging repository ${repo_id} (does it exist?)"
    return 1
  fi

  local parsed
  # One field per line. Bash command substitution strips NUL bytes, so
  # NUL separators do not survive; embedded newlines in description
  # are squashed to spaces.
  if ! parsed=$(echo "${response}" | python3 -c "
import sys, xml.etree.ElementTree as ET
tree = ET.parse(sys.stdin)
def clean(s):
    return (s or '').strip().replace('\n', ' ').replace('\r', ' ')
print(clean(tree.findtext('.//profileName')))
print(clean(tree.findtext('.//type')))
print(clean(tree.findtext('.//description')))
" 2>/dev/null); then
    print_error "Could not parse Nexus staging repository metadata for ${repo_id}"
    return 1
  fi

  {
    IFS= read -r _nexus_repo_profile
    IFS= read -r _nexus_repo_state
    IFS= read -r _nexus_repo_description
  } <<< "${parsed}"
  return 0
}

# Returns 0 if iceberg-core-${version}.pom exists in the staging
# repository's content tree, non-zero otherwise.
function nexus_check_iceberg_artifact_exists {
  local repo_id="$1"
  local version="$2"

  # The content base lives at ${NEXUS_BASE_URL%/service/local}/content/repositories/${repo_id}
  local content_base="${NEXUS_BASE_URL%/service/local}/content/repositories/${repo_id}"
  local pom_path="org/apache/iceberg/iceberg-core/${version}/iceberg-core-${version}.pom"
  local url="${content_base}/${pom_path}"

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD HEAD ${url}"
    return 0
  fi

  : "${NEXUS_USERNAME:?NEXUS_USERNAME must be set for real (non-dry-run) execution}"
  : "${NEXUS_PASSWORD:?NEXUS_PASSWORD must be set for real (non-dry-run) execution}"

  local http_status
  if ! http_status=$(curl --silent --output /dev/null --write-out '%{http_code}' \
    -I -K <(printf 'user = "%s:%s"\n' "${NEXUS_USERNAME}" "${NEXUS_PASSWORD}") \
    "${url}"); then
    print_error "HEAD ${url} failed (network error)"
    return 1
  fi

  if [[ "${http_status}" != "200" ]]; then
    print_error "Expected iceberg-core ${version} POM at ${url}, got HTTP ${http_status}"
    return 1
  fi
  return 0
}

# Verifies that ${repo_id} is the right staging repo for this RC.
#
#   1. profile  == NEXUS_STAGING_PROFILE                   (hard fail)
#   2. state    == "closed"                                (hard fail)
#   3. iceberg-core-${version}.pom is present              (hard fail)
#   4. description contains "Apache Iceberg ${version} RC${rc}"
#                                                          (warn; bypass
#                                                           with caller-set
#                                                           allow_description_mismatch=1)
#
# Args:
#   $1: repo_id
#   $2: expected version (e.g. 1.10.0)
#   $3: expected RC number (optional - description check is skipped if empty)
function nexus_verify_staging_repo {
  local repo_id="$1"
  local expected_version="$2"
  local expected_rc="${3:-}"

  print_info "Verifying staging repository ${repo_id} matches Iceberg ${expected_version}${expected_rc:+ RC${expected_rc}}..."

  if ! nexus_get_staging_repo_metadata "${repo_id}"; then
    return 1
  fi

  if [[ "${_nexus_repo_profile}" != "${NEXUS_STAGING_PROFILE}" ]]; then
    print_error "Staging repo ${repo_id} belongs to profile '${_nexus_repo_profile}', expected '${NEXUS_STAGING_PROFILE}'"
    print_error "This usually means an ID for a different project was pasted into the workflow input."
    return 1
  fi

  if [[ "${_nexus_repo_state}" != "closed" ]]; then
    print_error "Staging repo ${repo_id} is in state '${_nexus_repo_state}', expected 'closed'"
    case "${_nexus_repo_state}" in
      open)
        print_error "The repo is still open; prepare-rc.sh did not finish closing it. Re-run prepare-rc."
        ;;
      released)
        print_error "The repo has already been released; promoting twice is not safe."
        ;;
      *)
        print_error "Unexpected state - check the Nexus UI."
        ;;
    esac
    return 1
  fi

  if ! nexus_check_iceberg_artifact_exists "${repo_id}" "${expected_version}"; then
    print_error "Staging repo ${repo_id} does not contain iceberg-core ${expected_version} artifacts."
    print_error "Either the repository is for a different version or the publish step failed silently."
    return 1
  fi

  # Skip the description check in dry-run; the placeholder never matches.
  if [[ ${DRY_RUN:-1} -ne 1 && -n "${expected_rc}" ]]; then
    local expected_desc="Apache Iceberg ${expected_version} RC${expected_rc}"
    if [[ "${_nexus_repo_description}" != *"${expected_desc}"* ]]; then
      if [[ "${allow_description_mismatch:-0}" == "1" ]]; then
        print_warning "Staging repo ${repo_id} description '${_nexus_repo_description}' does not contain '${expected_desc}'"
        print_warning "Continuing anyway because --allow-description-mismatch was passed."
      else
        print_error "Staging repo ${repo_id} description '${_nexus_repo_description}' does not contain '${expected_desc}'"
        print_error "This usually means an ID from a different RC of the same version was pasted."
        print_error "If you're certain the repo is correct (e.g. description was edited), pass --allow-description-mismatch."
        return 1
      fi
    fi
  fi

  print_success "Staging repo ${repo_id} verified: profile=${_nexus_repo_profile}, state=${_nexus_repo_state}"
  return 0
}
