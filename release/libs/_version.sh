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

[[ -n "${_VERSION_LOADED:-}" ]] && return 0 2>/dev/null || true
_VERSION_LOADED=1

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=release/libs/_constants.sh
source "$LIBS_DIR/_constants.sh"
# shellcheck source=release/libs/_exec.sh
source "$LIBS_DIR/_exec.sh"

# Functions in this file populate caller-visible globals via dynamic scope
# (e.g. major/minor/patch/rc_number/latest_rc_number/version_without_rc).
# Tests cover this contract; shellcheck flags it as unused inside this file.
# shellcheck disable=SC2034

# Validates an X.Y.Z version string and exports the components.
# Iceberg releases use plain semver without -SNAPSHOT or -beta suffixes
# because the gitVersion plugin computes the development version
# automatically from the latest tag.
function validate_and_extract_version {
  local version="$1"
  if [[ ! ${version} =~ ^${VERSION_REGEX}$ ]]; then
    return 1
  fi
  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  version_without_rc="${major}.${minor}.${patch}"
  return 0
}

# Validates an "apache-iceberg-X.Y.Z-rcN" RC tag and exports its components.
function validate_and_extract_git_tag_version {
  local tag="$1"
  if [[ ! ${tag} =~ ${VERSION_REGEX_GIT_TAG} ]]; then
    return 1
  fi
  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  rc_number="${BASH_REMATCH[4]}"
  version_without_rc="${major}.${minor}.${patch}"
  return 0
}

# Validates a "X.Y.x" release branch name (e.g. "1.10.x") and exports the
# major/minor components.
function validate_and_extract_branch_version {
  local branch="$1"
  if [[ ! ${branch} =~ ${BRANCH_VERSION_REGEX} ]]; then
    return 1
  fi
  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  return 0
}

# Lists all RC tags for the given X.Y.Z version, suppressing matches that
# happen to start with the prefix but contain extra characters.
function _filter_rc_tags {
  local version_without_rc="$1"
  local exact_pattern="^${TAG_PREFIX}${version_without_rc}-rc[0-9]+$"
  git tag -l "${TAG_PREFIX}${version_without_rc}-rc*" | grep -E "${exact_pattern}"
}

# Sets rc_number to the next unused RC index for the given version. Returns
# 0 even when no prior RC exists (in that case rc_number=0).
function find_next_rc_number {
  local version_without_rc="$1"
  local existing_tags
  existing_tags=$(_filter_rc_tags "${version_without_rc}" || true)

  if [[ -z "${existing_tags}" ]]; then
    # shellcheck disable=SC2034 # consumed by callers
    rc_number=0
  else
    local highest_rc
    highest_rc=$(echo "${existing_tags}" | sed "s/${TAG_PREFIX}${version_without_rc}-rc//" | sort -n | tail -1)
    # shellcheck disable=SC2034 # consumed by callers
    rc_number=$((highest_rc + 1))
  fi
  return 0
}

# Sets latest_rc_number to the highest existing RC index for the given
# version. Fails if no RC tag exists, since "latest" is undefined in that
# case.
function find_latest_rc_number {
  local version_without_rc="$1"
  local existing_tags
  existing_tags=$(_filter_rc_tags "${version_without_rc}" || true)

  if [[ -z "${existing_tags}" ]]; then
    print_error "No RC tags found for version ${version_without_rc}"
    return 1
  fi

  # shellcheck disable=SC2034 # consumed by callers
  latest_rc_number=$(echo "${existing_tags}" | sed "s/${TAG_PREFIX}${version_without_rc}-rc//" | sort -n | tail -1)
  return 0
}

# Suggests the expected branch name for a given version. Used purely for
# advisory warnings: the release manager is responsible for being on the
# correct branch (Iceberg's existing dev/source-release.sh tags HEAD without
# any branch enforcement, and that behavior is preserved here).
function expected_branch_for_version {
  local major_arg="$1"
  local minor_arg="$2"
  echo "${major_arg}.${minor_arg}${BRANCH_SUFFIX}"
}
