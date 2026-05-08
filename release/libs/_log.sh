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

[[ -n "${_LOG_LOADED:-}" ]] && return 0 2>/dev/null || true
_LOG_LOADED=1

if [[ -t 2 ]] &&
  [[ "${NO_COLOR:-}" != "1" ]] &&
  [[ "${TERM:-}" != "dumb" ]] &&
  command -v tput >/dev/null; then
  RED=${RED:-$(tput setaf 1)}
  GREEN=${GREEN:-$(tput setaf 2)}
  YELLOW=${YELLOW:-$(tput bold; tput setaf 3)}
  BLUE=${BLUE:-$(tput setaf 4)}
  RESET=${RESET:-$(tput sgr0)}
else
  RED=${RED:-''}
  GREEN=${GREEN:-''}
  YELLOW=${YELLOW:-''}
  BLUE=${BLUE:-''}
  RESET=${RESET:-''}
fi

function print_error() {
  echo -e "${RED}ERROR: $*${RESET}" >&2
}

function print_warning() {
  echo -e "${YELLOW}WARNING: $*${RESET}" >&2
}

function print_info() {
  echo "INFO: $*" >&2
}

function print_success() {
  echo -e "${GREEN}SUCCESS: $*${RESET}" >&2
}

function print_command() {
  echo -e "${BLUE}DEBUG: $*${RESET}" >&2
}

# step_summary writes a line both to stdout (for local terminal visibility)
# and, when running inside GitHub Actions, to $GITHUB_STEP_SUMMARY so the
# workflow run page shows a structured recap.
function step_summary() {
  local msg="$1"
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    echo "$msg" >> "$GITHUB_STEP_SUMMARY"
  fi
  echo "$msg"
}
