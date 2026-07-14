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

[[ -n "${_SVN_LOADED:-}" ]] && return 0 2>/dev/null || true
_SVN_LOADED=1

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=release/libs/_constants.sh
source "${LIBS_DIR}/_constants.sh"
# shellcheck source=release/libs/_log.sh
source "${LIBS_DIR}/_log.sh"
# shellcheck source=release/libs/_exec.sh
source "${LIBS_DIR}/_exec.sh"

# Pipes SVN_PASSWORD on stdin so it never appears in argv (visible via
# /proc/<pid>/cmdline). All svn calls in the release scripts go through
# this helper.
#
# Args are passed straight through to `svn`; standard flags
# (--username, --password-from-stdin, --non-interactive, --no-auth-cache)
# are appended automatically. SVN_USERNAME and SVN_PASSWORD must be set
# in real (non-dry-run) mode.
function svn_run {
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    : "${SVN_USERNAME:?SVN_USERNAME must be set for real (non-dry-run) execution}"
    : "${SVN_PASSWORD:?SVN_PASSWORD must be set for real (non-dry-run) execution}"
  fi

  local cmd=(svn "$@"
    --username "${SVN_USERNAME:-}"
    --password-from-stdin
    --non-interactive
    --no-auth-cache)
  local redacted
  redacted=$(_redact_secrets "${cmd[@]}")

  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    print_command "Executing '${redacted}'"
    printf '%s\n' "${SVN_PASSWORD}" | "${cmd[@]}"
  else
    print_command "Dry-run, WOULD execute '${redacted}'"
  fi
}

# Same as svn_run but with retry semantics matching exec_process_with_retries:
# on failure, removes cleanup_path and retries up to max_attempts times.
# Apache SVN occasionally returns transient connection failures during
# checkout/commit; this helper handles those.
function svn_run_with_retries {
  if [[ $# -lt 4 ]]; then
    echo "ERROR: svn_run_with_retries requires: max_attempts sleep_duration cleanup_path svn-args..."
    exit 1
  fi

  local max_attempts="${1}"
  local sleep_duration="${2}"
  local cleanup_path="${3}"
  shift 3

  local attempt=1
  while true; do
    if svn_run "$@"; then
      break
    fi
    if [[ $attempt -ge $max_attempts ]]; then
      local redacted
      redacted=$(_redact_secrets "svn $*")
      echo "ERROR: SVN command failed after ${max_attempts} attempts: ${redacted}"
      exit 1
    fi
    echo "WARNING: SVN command failed (attempt ${attempt}/${max_attempts}), retrying in ${sleep_duration} seconds..."
    if [[ -n "${cleanup_path}" && -e "${cleanup_path}" ]]; then
      rm -rf "${cleanup_path}"
    fi
    sleep "${sleep_duration}"
    ((attempt++))
  done
}

# Returns success (0) if the given URL exists in SVN, failure otherwise.
# Suppresses output (svn ls is the standard existence probe). In dry-run
# mode this prints what it would check and returns success.
function svn_path_exists {
  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD probe SVN path: $1"
    return 0
  fi
  : "${SVN_USERNAME:?SVN_USERNAME must be set for real (non-dry-run) execution}"
  : "${SVN_PASSWORD:?SVN_PASSWORD must be set for real (non-dry-run) execution}"

  printf '%s\n' "${SVN_PASSWORD}" | svn ls \
    --username "${SVN_USERNAME}" \
    --password-from-stdin \
    --non-interactive \
    --no-auth-cache \
    "$1" >/dev/null 2>&1
}

# Captures the output of an `svn list`-style command. Used by the old-release
# cleanup flow which needs to enumerate directories before deleting them.
# The password is piped via stdin so it never appears in argv. Output goes
# to stdout (caller can capture via $(svn_list ...)). Real-mode only;
# callers must guard with their own DRY_RUN check.
function svn_list {
  : "${SVN_USERNAME:?SVN_USERNAME must be set for real (non-dry-run) execution}"
  : "${SVN_PASSWORD:?SVN_PASSWORD must be set for real (non-dry-run) execution}"

  printf '%s\n' "${SVN_PASSWORD}" | svn list \
    --username "${SVN_USERNAME}" \
    --password-from-stdin \
    --non-interactive \
    --no-auth-cache \
    "$@" 2>&1
}

# Filters an `svn list` of dist/release/iceberg/ to only the patch-release
# directories that should be deleted as superseded by <new_version>. A
# directory is selected when:
#   * its name matches apache-iceberg-<MAJOR>.<MINOR>.<patch>, AND
#   * <MAJOR>.<MINOR> equals the major.minor of <new_version>, AND
#   * the directory is not <new_version> itself.
#
# Promoting 1.9.2 therefore removes 1.9.0 and 1.9.1 but leaves 1.10.x,
# 2.0.0, and any KEYS/non-release entries alone. Older minor lines are
# served from archive.apache.org via the standard ASF release archival.
#
# Outputs one bare directory name per line. Returns 0 even when nothing
# matches.
function filter_superseded_patch_releases {
  local listing="$1"
  local new_version="$2"

  local major minor rest
  major="${new_version%%.*}"
  rest="${new_version#*.}"
  minor="${rest%%.*}"

  local final_tag="${TAG_PREFIX}${new_version}"

  echo "${listing}" \
    | grep -E "^${TAG_PREFIX}${major}\.${minor}\.[0-9]+/?$" \
    | sed 's|/$||' \
    | grep -v "^${final_tag}$" \
    || true
}
