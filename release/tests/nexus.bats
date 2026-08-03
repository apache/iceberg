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

setup() {
  load test_helper/common
  source "${LIBS_DIR}/_nexus.sh"
  export NEXUS_USERNAME="testuser"
  export NEXUS_PASSWORD="testpass"
}

@test "nexus_close_staging_repo: dry-run does not call curl" {
  DRY_RUN=1
  run nexus_close_staging_repo "orgapacheiceberg-1234" "test close"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run"* ]]
  [[ "$output" == *"close"* ]]
}

@test "nexus_close_staging_repo: constructs correct URL in dry-run" {
  DRY_RUN=1
  run nexus_close_staging_repo "orgapacheiceberg-1234"
  [[ "$output" == *"staging/bulk/close"* ]]
}

@test "nexus_release_staging_repo: dry-run includes promote URL" {
  DRY_RUN=1
  run nexus_release_staging_repo "orgapacheiceberg-1234"
  [[ "$output" == *"staging/bulk/promote"* ]]
}

@test "nexus_drop_staging_repo: dry-run includes drop URL" {
  DRY_RUN=1
  run nexus_drop_staging_repo "orgapacheiceberg-1234"
  [[ "$output" == *"staging/bulk/drop"* ]]
}

@test "_nexus_bulk_action: includes repo ID in payload" {
  DRY_RUN=1
  run _nexus_bulk_action "close" "orgapacheiceberg-5678" "test"
  [[ "$output" == *"orgapacheiceberg-5678"* ]]
}

@test "_nexus_bulk_action: uses correct base URL" {
  DRY_RUN=1
  NEXUS_BASE_URL="https://example.com/nexus"
  run _nexus_bulk_action "drop" "repo-123" "test"
  [[ "$output" == *"example.com/nexus/staging/bulk/drop"* ]]
}

@test "nexus_find_open_staging_repo: dry-run returns placeholder" {
  DRY_RUN=1
  nexus_find_open_staging_repo "org.apache.iceberg"
  [ "$staging_repo_id" = "DRY-RUN-REPO-ID" ]
}

@test "nexus_close_staging_repo: real mode calls curl with correct args" {
  DRY_RUN=0
  curl() {
    echo "CURL_ARGS: $*" >&2
    return 0
  }
  export -f curl
  run nexus_close_staging_repo "orgapacheiceberg-9999" "test desc"
  [ "$status" -eq 0 ]
  [[ "$output" == *"staging/bulk/close"* ]]
  [[ "$output" == *"orgapacheiceberg-9999"* ]]
}

@test "nexus_drop_staging_repo: real mode calls curl with auth" {
  DRY_RUN=0
  curl() {
    echo "CURL_ARGS: $*" >&2
    return 0
  }
  export -f curl
  run nexus_drop_staging_repo "orgapacheiceberg-1111"
  [ "$status" -eq 0 ]
  [[ "$output" == *"staging/bulk/drop"* ]]
}

@test "nexus_find_open_staging_repo: fails when no open repo found" {
  DRY_RUN=0
  curl() {
    cat <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<stagingProfileRepositories><data /></stagingProfileRepositories>
EOF
    return 0
  }
  export -f curl
  run nexus_find_open_staging_repo "org.apache.iceberg"
  [ "$status" -eq 1 ]
  [[ "$output" == *"No open staging repository"* ]]
}

@test "nexus_find_open_staging_repo: finds single open repo" {
  DRY_RUN=0
  curl() {
    cat <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<stagingProfileRepositories>
  <data>
    <stagingProfileRepository>
      <type>open</type>
      <repositoryId>orgapacheiceberg-1234</repositoryId>
    </stagingProfileRepository>
  </data>
</stagingProfileRepositories>
EOF
    return 0
  }
  export -f curl
  nexus_find_open_staging_repo "org.apache.iceberg"
  [ "$staging_repo_id" = "orgapacheiceberg-1234" ]
}

@test "nexus_find_open_staging_repo: fails when multiple open repos found" {
  DRY_RUN=0
  curl() {
    cat <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<stagingProfileRepositories>
  <data>
    <stagingProfileRepository>
      <type>open</type>
      <repositoryId>orgapacheiceberg-1234</repositoryId>
    </stagingProfileRepository>
    <stagingProfileRepository>
      <type>open</type>
      <repositoryId>orgapacheiceberg-5678</repositoryId>
    </stagingProfileRepository>
  </data>
</stagingProfileRepositories>
EOF
    return 0
  }
  export -f curl
  run nexus_find_open_staging_repo "org.apache.iceberg"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Found 2 open"* ]]
}

@test "nexus_find_open_staging_repo: fails gracefully when curl returns non-zero" {
  DRY_RUN=0
  curl() {
    return 22
  }
  export -f curl
  run nexus_find_open_staging_repo "org.apache.iceberg"
  [ "$status" -ne 0 ]
  [[ "$output" == *"Failed to query Nexus staging repositories"* ]]
}

@test "nexus_find_open_staging_repo: error hint does not mention --staging-repo-id flag" {
  # The script that wraps this function does not expose --staging-repo-id;
  # an earlier message suggested it and was misleading.
  DRY_RUN=0
  curl() {
    cat <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<stagingProfileRepositories>
  <data>
    <stagingProfileRepository>
      <type>open</type>
      <repositoryId>orgapacheiceberg-1234</repositoryId>
    </stagingProfileRepository>
    <stagingProfileRepository>
      <type>open</type>
      <repositoryId>orgapacheiceberg-5678</repositoryId>
    </stagingProfileRepository>
  </data>
</stagingProfileRepositories>
EOF
    return 0
  }
  export -f curl
  run nexus_find_open_staging_repo "org.apache.iceberg"
  [[ "$output" != *"--staging-repo-id"* ]]
}

@test "_nexus_bulk_action: fails fast in real mode without NEXUS_USERNAME" {
  DRY_RUN=0
  unset NEXUS_USERNAME
  unset NEXUS_PASSWORD
  run _nexus_bulk_action "drop" "orgapacheiceberg-1" "test"
  [ "$status" -ne 0 ]
  [[ "$output" == *"NEXUS_USERNAME must be set"* ]]
}

# ---------------------------------------------------------------------------
# Staging repository verification (publish/cancel safety net)
# ---------------------------------------------------------------------------

# Helper: writes a curl stub that returns a metadata XML response with the
# given profile/state/description, and returns 200 for HEAD requests
# against the iceberg-core POM for the given version (default 1.10.0).
# HEADs for any other version return 404. State is exported via globals
# rather than closed-over locals because bash re-parses function bodies
# at call time and locals are out of scope by then.
function _stub_nexus_curl {
  export _STUB_NEXUS_PROFILE="$1"
  export _STUB_NEXUS_STATE="$2"
  export _STUB_NEXUS_DESCRIPTION="$3"
  export _STUB_NEXUS_VERSION="${4:-1.10.0}"
  curl() {
    local args=("$@")
    local is_head=0
    local i
    for ((i = 0; i < ${#args[@]}; i++)); do
      if [[ "${args[i]}" == "-I" ]]; then
        is_head=1
      fi
    done
    if [[ ${is_head} -eq 1 ]]; then
      local url="${args[$((${#args[@]} - 1))]}"
      if [[ "${url}" == *"iceberg-core/${_STUB_NEXUS_VERSION}/iceberg-core-${_STUB_NEXUS_VERSION}.pom" ]]; then
        echo -n "200"
      else
        echo -n "404"
      fi
      return 0
    fi
    cat <<XML
<?xml version="1.0" encoding="UTF-8"?>
<stagingProfileRepository>
  <profileName>${_STUB_NEXUS_PROFILE}</profileName>
  <type>${_STUB_NEXUS_STATE}</type>
  <repositoryId>orgapacheiceberg-1234</repositoryId>
  <description>${_STUB_NEXUS_DESCRIPTION}</description>
</stagingProfileRepository>
XML
    return 0
  }
  export -f curl
}

@test "nexus_get_staging_repo_metadata: dry-run fills placeholder values" {
  DRY_RUN=1
  nexus_get_staging_repo_metadata "orgapacheiceberg-1234"
  [ "${_nexus_repo_profile}" = "${NEXUS_STAGING_PROFILE}" ]
  [ "${_nexus_repo_state}" = "closed" ]
  [ "${_nexus_repo_description}" = "DRY-RUN" ]
}

@test "nexus_get_staging_repo_metadata: parses real-mode XML" {
  DRY_RUN=0
  _stub_nexus_curl "org.apache.iceberg" "closed" "Apache Iceberg 1.10.0 RC2"
  nexus_get_staging_repo_metadata "orgapacheiceberg-1234"
  [ "${_nexus_repo_profile}" = "org.apache.iceberg" ]
  [ "${_nexus_repo_state}" = "closed" ]
  [ "${_nexus_repo_description}" = "Apache Iceberg 1.10.0 RC2" ]
}

@test "nexus_get_staging_repo_metadata: surfaces curl failure" {
  DRY_RUN=0
  curl() { return 22; }
  export -f curl
  run nexus_get_staging_repo_metadata "orgapacheiceberg-9999"
  [ "$status" -ne 0 ]
  [[ "$output" == *"Could not fetch Nexus staging repository"* ]]
}

@test "nexus_check_iceberg_artifact_exists: 200 returns success" {
  DRY_RUN=0
  _stub_nexus_curl "org.apache.iceberg" "closed" "" "1.10.0"
  run nexus_check_iceberg_artifact_exists "orgapacheiceberg-1234" "1.10.0"
  [ "$status" -eq 0 ]
}

@test "nexus_check_iceberg_artifact_exists: 404 fails with helpful message" {
  DRY_RUN=0
  _stub_nexus_curl "org.apache.iceberg" "closed" "" "9.9.9"
  run nexus_check_iceberg_artifact_exists "orgapacheiceberg-1234" "1.10.0"
  [ "$status" -ne 0 ]
  [[ "$output" == *"got HTTP 404"* ]]
}

@test "nexus_check_iceberg_artifact_exists: dry-run is a no-op" {
  DRY_RUN=1
  run nexus_check_iceberg_artifact_exists "orgapacheiceberg-1234" "1.10.0"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run"* ]]
}

@test "nexus_verify_staging_repo: passes when profile, state, artifact, description all match" {
  DRY_RUN=0
  _stub_nexus_curl "org.apache.iceberg" "closed" "Apache Iceberg 1.10.0 RC2" "1.10.0"
  run nexus_verify_staging_repo "orgapacheiceberg-1234" "1.10.0" "2"
  [ "$status" -eq 0 ]
  [[ "$output" == *"verified"* ]]
}

@test "nexus_verify_staging_repo: fails on profile mismatch" {
  DRY_RUN=0
  _stub_nexus_curl "org.apache.parquet" "closed" "Apache Iceberg 1.10.0 RC2" "1.10.0"
  run nexus_verify_staging_repo "orgapacheiceberg-1234" "1.10.0" "2"
  [ "$status" -ne 0 ]
  [[ "$output" == *"belongs to profile 'org.apache.parquet'"* ]]
}

@test "nexus_verify_staging_repo: fails when repo is still open" {
  DRY_RUN=0
  _stub_nexus_curl "org.apache.iceberg" "open" "Apache Iceberg 1.10.0 RC2" "1.10.0"
  run nexus_verify_staging_repo "orgapacheiceberg-1234" "1.10.0" "2"
  [ "$status" -ne 0 ]
  [[ "$output" == *"in state 'open'"* ]]
  [[ "$output" == *"Re-run prepare-rc"* ]]
}

@test "nexus_verify_staging_repo: fails when repo already released" {
  DRY_RUN=0
  _stub_nexus_curl "org.apache.iceberg" "released" "Apache Iceberg 1.10.0 RC2" "1.10.0"
  run nexus_verify_staging_repo "orgapacheiceberg-1234" "1.10.0" "2"
  [ "$status" -ne 0 ]
  [[ "$output" == *"in state 'released'"* ]]
  [[ "$output" == *"already been released"* ]]
}

@test "nexus_verify_staging_repo: fails when version artifact missing" {
  DRY_RUN=0
  _stub_nexus_curl "org.apache.iceberg" "closed" "Apache Iceberg 1.10.0 RC2" "9.9.9"
  run nexus_verify_staging_repo "orgapacheiceberg-1234" "1.10.0" "2"
  [ "$status" -ne 0 ]
  [[ "$output" == *"does not contain iceberg-core 1.10.0"* ]]
}

@test "nexus_verify_staging_repo: fails on description mismatch (different RC) by default" {
  DRY_RUN=0
  _stub_nexus_curl "org.apache.iceberg" "closed" "Apache Iceberg 1.10.0 RC1" "1.10.0"
  run nexus_verify_staging_repo "orgapacheiceberg-1234" "1.10.0" "2"
  [ "$status" -ne 0 ]
  [[ "$output" == *"description"* ]]
  [[ "$output" == *"Apache Iceberg 1.10.0 RC2"* ]]
}

@test "nexus_verify_staging_repo: allow_description_mismatch=1 turns description mismatch into a warning" {
  DRY_RUN=0
  allow_description_mismatch=1
  _stub_nexus_curl "org.apache.iceberg" "closed" "Apache Iceberg 1.10.0 RC1" "1.10.0"
  run nexus_verify_staging_repo "orgapacheiceberg-1234" "1.10.0" "2"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Continuing anyway"* ]]
}

@test "nexus_verify_staging_repo: skips description check when expected_rc is empty" {
  DRY_RUN=0
  _stub_nexus_curl "org.apache.iceberg" "closed" "" "1.10.0"
  run nexus_verify_staging_repo "orgapacheiceberg-1234" "1.10.0" ""
  [ "$status" -eq 0 ]
}

@test "nexus_verify_staging_repo: dry-run passes without hitting network" {
  DRY_RUN=1
  curl() { echo "FAIL: curl should not be invoked in dry-run" >&2; return 1; }
  export -f curl
  run nexus_verify_staging_repo "orgapacheiceberg-1234" "1.10.0" "2"
  [ "$status" -eq 0 ]
  [[ "$output" != *"FAIL: curl should not be invoked"* ]]
}
