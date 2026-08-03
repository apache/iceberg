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

LIBS_DIR="${BATS_TEST_DIRNAME}/../libs"

# Reset the include guards so each test re-sources the libraries cleanly.
unset _CONSTANTS_LOADED _LOG_LOADED _EXEC_LOADED _VERSION_LOADED
unset _GITHUB_LOADED _NEXUS_LOADED _GRADLE_LOADED

# Reset globals that library functions populate so leakage between tests
# can't mask real bugs.
_reset_version_vars() {
  unset major minor patch rc_number latest_rc_number version_without_rc staging_repo_id
}

export NO_COLOR=1
export DRY_RUN=1
