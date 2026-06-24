#!/usr/bin/env bash
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

primary_jvm="${PRIMARY_JVM:-17}"

gradle_property() {
  local name="$1"
  sed -n "s/^systemProp\\.${name}=//p" gradle.properties | head -n 1
}

known_spark_versions="$(gradle_property knownSparkVersions)"
known_flink_versions="$(gradle_property knownFlinkVersions)"
default_spark_version="$(gradle_property defaultSparkVersions)"
default_flink_version="$(gradle_property defaultFlinkVersions)"

IFS=',' read -r -a all_spark_versions <<< "${known_spark_versions}"
IFS=',' read -r -a all_flink_versions <<< "${known_flink_versions}"

changed_files=()
read_changed_files() {
  local file
  while IFS= read -r file; do
    if [[ -n "${file}" ]]; then
      changed_files+=("${file}")
    fi
  done
}

if [[ -n "${CHANGED_FILES_FILE:-}" ]]; then
  read_changed_files < "${CHANGED_FILES_FILE}"
elif [[ "${GITHUB_EVENT_NAME:-}" == "pull_request" ]]; then
  base_sha="${BASE_SHA:-}"
  if [[ -n "${base_sha}" ]]; then
    git fetch --no-tags --depth=1 origin "${base_sha}"
    read_changed_files < <(git diff --name-only "${base_sha}" HEAD)
  else
    read_changed_files < <(git diff --name-only origin/main HEAD)
  fi
fi

contains_file() {
  local pattern="$1"
  local file
  if [[ ${#changed_files[@]} -eq 0 ]]; then
    return 1
  fi

  for file in "${changed_files[@]}"; do
    if [[ "${file}" =~ ${pattern} ]]; then
      return 0
    fi
  done
  return 1
}

add_unique() {
  local name="$1"
  local value="$2"
  local existing
  eval "existing=(\"\${${name}[@]:-}\")"
  for existing in "${existing[@]}"; do
    if [[ "${existing}" == "${value}" ]]; then
      return
    fi
  done
  eval "${name}+=(\"\${value}\")"
}

version_in_list() {
  local version="$1"
  shift
  local known
  for known in "$@"; do
    if [[ "${known}" == "${version}" ]]; then
      return 0
    fi
  done
  return 1
}

docs_only_file() {
  local file="$1"
  case "${file}" in
    .gitattributes|CONTRIBUTING.md|doap.rdf|README.md|LICENSE|NOTICE|docs/*|site/*|*/README.md|*/LICENSE|*/NOTICE)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

global_build_file() {
  local file="$1"
  case "${file}" in
    .github/actions/*|.github/scripts/*|.github/workflows/*|build.gradle|settings.gradle|gradle.properties|baseline.gradle|deploy.gradle|jmh.gradle|runtime-deps.gradle|tasks.gradle|gradle/*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

known_selective_file() {
  local file="$1"
  case "${file}" in
    aliyun/*|api/*|arrow/*|aws/*|aws-bundle/*|azure/*|azure-bundle/*|bigquery/*|bom/*|bundled-guava/*|common/*|core/*|data/*|dell/*|delta-lake/*|flink/*|flink-runtime/*|format/*|gcp/*|gcp-bundle/*|hive-metastore/*|hive-runtime/*|hive3/*|hive3-orc-bundle/*|kafka-connect/*|mr/*|nessie/*|open-api/*|orc/*|parquet/*|pig/*|snowflake/*|spark/*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

selected_spark_path_versions=()
selected_flink_path_versions=()
unknown_spark_version=false
unknown_flink_version=false
unknown_changed_files=()
has_code_changes=false
has_global_build_change=false

for file in "${changed_files[@]:-}"; do
  if docs_only_file "${file}"; then
    continue
  fi

  has_code_changes=true

  if global_build_file "${file}"; then
    has_global_build_change=true
    continue
  fi

  if [[ "${file}" =~ ^spark/v([^/]+)/ ]]; then
    spark_version="${BASH_REMATCH[1]}"
    if version_in_list "${spark_version}" "${all_spark_versions[@]}"; then
      add_unique selected_spark_path_versions "${spark_version}"
    else
      unknown_spark_version=true
    fi
  fi

  if [[ "${file}" =~ ^flink/v([^/]+)/ ]]; then
    flink_version="${BASH_REMATCH[1]}"
    if version_in_list "${flink_version}" "${all_flink_versions[@]}"; then
      add_unique selected_flink_path_versions "${flink_version}"
    else
      unknown_flink_version=true
    fi
  fi

  if ! known_selective_file "${file}"; then
    unknown_changed_files+=("${file}")
  fi
done

has_full_ci_label=false
if [[ "${FULL_CI_LABEL:-false}" == "true" ]]; then
  has_full_ci_label=true
fi

full_matrix=false
full_matrix_reason=""
if [[ "${GITHUB_EVENT_NAME:-}" != "pull_request" ]]; then
  full_matrix=true
  full_matrix_reason="non-PR event"
elif [[ "${has_full_ci_label}" == "true" ]]; then
  full_matrix=true
  full_matrix_reason="full-ci label"
elif [[ ${#changed_files[@]} -eq 0 ]]; then
  full_matrix=true
  full_matrix_reason="no changed files detected"
elif [[ "${has_global_build_change}" == "true" ]]; then
  full_matrix=true
  full_matrix_reason="global build or workflow change"
elif [[ "${unknown_spark_version}" == "true" || "${unknown_flink_version}" == "true" ]]; then
  full_matrix=true
  full_matrix_reason="unknown Spark or Flink version path"
elif [[ ${#unknown_changed_files[@]} -gt 0 ]]; then
  full_matrix=true
  full_matrix_reason="unknown changed path"
fi

is_upstream_canary_change=false
if contains_file '^(api|common|core|data|parquet|orc|arrow|bundled-guava|format)/'; then
  is_upstream_canary_change=true
fi

run_java=false
run_spark=false
run_flink=false
run_hive=false
run_kafka=false
run_delta=false
run_cve=false

if [[ "${full_matrix}" == "true" ]]; then
  run_java=true
  run_spark=true
  run_flink=true
  run_hive=true
  run_kafka=true
  run_delta=true
  run_cve=true
elif [[ "${has_code_changes}" == "true" ]]; then
  if [[ "${is_upstream_canary_change}" == "true" ]]; then
    run_java=true
    run_spark=true
    run_flink=true
  fi

  if contains_file '^(aliyun|api|arrow|aws|aws-bundle|azure|azure-bundle|bigquery|bom|bundled-guava|common|core|data|dell|format|gcp|gcp-bundle|hive-metastore|hive-runtime|hive3|hive3-orc-bundle|nessie|orc|parquet|pig|snowflake)/'; then
    run_java=true
  fi

  if contains_file '^spark/'; then
    run_spark=true
  fi

  if contains_file '^(flink|flink-runtime)/'; then
    run_flink=true
  fi

  if contains_file '^(mr|hive-metastore|hive-runtime|hive3|hive3-orc-bundle)/'; then
    run_hive=true
  fi

  if contains_file '^kafka-connect/'; then
    run_kafka=true
  fi

  if contains_file '^delta-lake/'; then
    run_delta=true
  fi

  if contains_file '^(aws|aws-bundle|azure|azure-bundle|gcp|gcp-bundle|kafka-connect|open-api|spark|flink|flink-runtime)/'; then
    run_cve=true
  fi
fi

jvm_matrix() {
  if [[ "${full_matrix}" == "true" ]]; then
    jq -cn '{jvm: [17, 21]}'
  else
    jq -cn --argjson jvm "${primary_jvm}" '{jvm: [$jvm]}'
  fi
}

spark_scala_versions() {
  local spark_version="$1"
  if [[ "${spark_version}" == "3.5" ]]; then
    printf '%s\n' "2.12" "2.13"
  else
    printf '%s\n' "2.13"
  fi
}

spark_cve_scala_version() {
  local spark_version="$1"
  if [[ "${spark_version}" == "3.5" ]]; then
    printf '%s\n' "2.12"
  else
    printf '%s\n' "2.13"
  fi
}

add_spark_rows() {
  local spark_version="$1"
  local scala_version
  local test_group
  local jvm
  local -a jvms
  if [[ "${full_matrix}" == "true" ]]; then
    jvms=(17 21)
  else
    jvms=("${primary_jvm}")
  fi

  for jvm in "${jvms[@]}"; do
    while IFS= read -r scala_version; do
      for test_group in core extensions; do
        spark_rows+=("${jvm}|${spark_version}|${scala_version}|${test_group}")
      done
    done < <(spark_scala_versions "${spark_version}")
  done
}

spark_matrix() {
  local spark_version
  local -a selected_spark_versions=()
  spark_rows=()

  if [[ "${run_spark}" != "true" ]]; then
    jq -cn '{include: []}'
    return
  fi

  if [[ "${full_matrix}" == "true" ]]; then
    selected_spark_versions=("${all_spark_versions[@]}")
  elif [[ "${is_upstream_canary_change}" == "true" ]]; then
    selected_spark_versions=("${default_spark_version}")
  elif [[ ${#selected_spark_path_versions[@]} -gt 0 ]]; then
    selected_spark_versions=("${selected_spark_path_versions[@]}")
  else
    selected_spark_versions=("${all_spark_versions[@]}")
  fi

  for spark_version in "${selected_spark_versions[@]}"; do
    add_spark_rows "${spark_version}"
  done

  printf '%s\n' "${spark_rows[@]:-}" \
    | jq -R 'select(length > 0) | split("|") | {jvm: .[0], spark: .[1], scala: .[2], tests: .[3]}' \
    | jq -s -c '{include: .}'
}

flink_matrix() {
  local flink_version
  local jvm
  local -a selected_flink_versions=()
  local -a jvms=()
  local -a flink_rows=()

  if [[ "${run_flink}" != "true" ]]; then
    jq -cn '{include: []}'
    return
  fi

  if [[ "${full_matrix}" == "true" ]]; then
    selected_flink_versions=("${all_flink_versions[@]}")
    jvms=(17 21)
  elif [[ "${is_upstream_canary_change}" == "true" ]]; then
    selected_flink_versions=("${default_flink_version}")
    jvms=("${primary_jvm}")
  elif [[ ${#selected_flink_path_versions[@]} -gt 0 ]]; then
    selected_flink_versions=("${selected_flink_path_versions[@]}")
    jvms=("${primary_jvm}")
  else
    selected_flink_versions=("${all_flink_versions[@]}")
    jvms=("${primary_jvm}")
  fi

  for jvm in "${jvms[@]}"; do
    for flink_version in "${selected_flink_versions[@]}"; do
      flink_rows+=("${jvm}|${flink_version}")
    done
  done

  printf '%s\n' "${flink_rows[@]:-}" \
    | jq -R 'select(length > 0) | split("|") | {jvm: .[0], flink: .[1]}' \
    | jq -s -c '{include: .}'
}

add_cve_distribution() {
  add_unique cve_distributions "$1"
}

select_cve_distributions() {
  local spark_version
  local flink_version
  cve_distributions=()

  if [[ "${run_cve}" != "true" ]]; then
    return
  fi

  add_cve_distribution kafka-connect-runtime
  add_cve_distribution aws-bundle
  add_cve_distribution azure-bundle
  add_cve_distribution gcp-bundle

  for spark_version in "${all_spark_versions[@]}"; do
    add_cve_distribution "spark-runtime-${spark_version}_$(spark_cve_scala_version "${spark_version}")"
  done

  for flink_version in "${all_flink_versions[@]}"; do
    add_cve_distribution "flink-runtime-${flink_version}"
  done

  add_cve_distribution open-api-test-fixtures-runtime
}

cve_row() {
  local distribution="$1"
  local spark_version
  local scala_version
  local flink_version

  case "${distribution}" in
    kafka-connect-runtime)
      printf '%s|%s|%s|%s\n' "${distribution}" "-DkafkaVersions=3 :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip" "kafka-connect/kafka-connect-runtime/build/distributions" "true"
      ;;
    aws-bundle)
      printf '%s|%s|%s|%s\n' "${distribution}" ":iceberg-aws-bundle:shadowJar" "aws-bundle/build/libs" "false"
      ;;
    azure-bundle)
      printf '%s|%s|%s|%s\n' "${distribution}" ":iceberg-azure-bundle:shadowJar" "azure-bundle/build/libs" "false"
      ;;
    gcp-bundle)
      printf '%s|%s|%s|%s\n' "${distribution}" ":iceberg-gcp-bundle:shadowJar" "gcp-bundle/build/libs" "false"
      ;;
    spark-runtime-*)
      spark_version="${distribution#spark-runtime-}"
      scala_version="${spark_version#*_}"
      spark_version="${spark_version%_*}"
      printf '%s|%s|%s|%s\n' "${distribution}" "-DsparkVersions=${spark_version} :iceberg-spark:iceberg-spark-runtime-${spark_version}_${scala_version}:shadowJar" "spark/v${spark_version}/spark-runtime/build/libs" "false"
      ;;
    flink-runtime-*)
      flink_version="${distribution#flink-runtime-}"
      printf '%s|%s|%s|%s\n' "${distribution}" "-DflinkVersions=${flink_version} :iceberg-flink:iceberg-flink-runtime-${flink_version}:shadowJar" "flink/v${flink_version}/flink-runtime/build/libs" "false"
      ;;
    open-api-test-fixtures-runtime)
      printf '%s|%s|%s|%s\n' "${distribution}" ":iceberg-open-api:shadowJar" "open-api/build/libs" "false"
      ;;
    *)
      echo "Unsupported CVE distribution: ${distribution}" >&2
      return 1
      ;;
  esac
}

cve_matrix() {
  local distribution
  select_cve_distributions
  if [[ ${#cve_distributions[@]} -eq 0 ]]; then
    jq -cn '{include: []}'
    return
  fi

  for distribution in "${cve_distributions[@]}"; do
    cve_row "${distribution}"
  done \
    | jq -R 'select(length > 0) | split("|") | {distribution: .[0], "build-task": .[1], "scan-path": .[2], unpack: (.[3] == "true")}' \
    | jq -s -c '{include: .}'
}

write_output() {
  local name="$1"
  local value="$2"
  echo "${name}=${value}" >> "${GITHUB_OUTPUT}"
}

jvm_matrix_json="$(jvm_matrix)"
spark_matrix_json="$(spark_matrix)"
flink_matrix_json="$(flink_matrix)"
cve_matrix_json="$(cve_matrix)"

write_output full_matrix "${full_matrix}"
write_output run_java "${run_java}"
write_output run_spark "${run_spark}"
write_output run_flink "${run_flink}"
write_output run_hive "${run_hive}"
write_output run_kafka "${run_kafka}"
write_output run_delta "${run_delta}"
write_output run_cve "${run_cve}"
write_output jvm_matrix "${jvm_matrix_json}"
write_output spark_matrix "${spark_matrix_json}"
write_output flink_matrix "${flink_matrix_json}"
write_output cve_matrix "${cve_matrix_json}"

{
  echo "full_matrix=${full_matrix}"
  if [[ -n "${full_matrix_reason}" ]]; then
    echo "full_matrix_reason=${full_matrix_reason}"
  fi
  echo "run_java=${run_java}"
  echo "run_spark=${run_spark}"
  echo "run_flink=${run_flink}"
  echo "run_hive=${run_hive}"
  echo "run_kafka=${run_kafka}"
  echo "run_delta=${run_delta}"
  echo "run_cve=${run_cve}"
  echo "changed_files=${#changed_files[@]}"
  if [[ ${#changed_files[@]} -gt 0 ]]; then
    printf 'changed_file=%s\n' "${changed_files[@]}"
  fi
  if [[ ${#unknown_changed_files[@]} -gt 0 ]]; then
    printf 'unknown_changed_file=%s\n' "${unknown_changed_files[@]}"
  fi
} >&2

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "### PR CI plan"
    echo
    echo "| Item | Value |"
    echo "| --- | --- |"
    echo "| Full matrix | ${full_matrix} |"
    echo "| Full matrix reason | ${full_matrix_reason:-n/a} |"
    echo "| Java | ${run_java} |"
    echo "| Spark | ${run_spark} |"
    echo "| Flink | ${run_flink} |"
    echo "| Hive | ${run_hive} |"
    echo "| Kafka | ${run_kafka} |"
    echo "| Delta | ${run_delta} |"
    echo "| CVE | ${run_cve} |"
    echo "| Changed files | ${#changed_files[@]} |"
  } >> "${GITHUB_STEP_SUMMARY}"
fi
