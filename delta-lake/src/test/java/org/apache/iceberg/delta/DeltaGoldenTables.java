/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.delta;

import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

final class DeltaGoldenTables {
  static final Set<String> SKIPPED_GOLDEN_TABLES =
      ImmutableSet.of(
          // Unsupported columnMapping feature
          "data-reader-timestamp_ntz-id-mode",
          "data-reader-timestamp_ntz-name-mode",
          "data-skipping-basic-stats-all-types-columnmapping-id",
          "data-skipping-basic-stats-all-types-columnmapping-name",
          "dv-with-columnmapping",
          "table-with-columnmapping-mode-id",
          "table-with-columnmapping-mode-name",
          "table-with-icebegCompatV2Enabled",
          // Unsupported metadata evolution across Delta versions
          "data-skipping-change-stats-collected-across-versions",
          "log-replay-latest-metadata-protocol",
          "time-travel-partition-changes-b",
          "time-travel-schema-changes-b",
          "type-widening",
          "type-widening-nested",
          "v2-checkpoint-json",
          "v2-checkpoint-parquet",
          // Unsupported timestamp/timestamptz partition value conversion
          "data-reader-partition-values",
          "data-reader-timestamp_ntz",
          "kernel-timestamp-INT96",
          "kernel-timestamp-PST",
          "kernel-timestamp-TIMESTAMP_MICROS",
          "kernel-timestamp-TIMESTAMP_MILLIS",
          "kernel-timestamp-partition-col-ISO8601",
          // Negative, corrupted, or synthetic log-only Delta fixtures
          "canonicalized-paths-normal-a",
          "canonicalized-paths-normal-b",
          "canonicalized-paths-special-a",
          "canonicalized-paths-special-b",
          "checkpoint",
          "corrupted-last-checkpoint",
          "data-reader-absolute-paths-escaped-chars",
          "delete-re-add-same-file-different-transactions",
          "deltalog-commit-info",
          "deltalog-invalid-protocol-version",
          "deltalog-state-reconstruction-from-checkpoint-missing-metadata",
          "deltalog-state-reconstruction-from-checkpoint-missing-protocol",
          "deltalog-state-reconstruction-without-metadata",
          "deltalog-state-reconstruction-without-protocol",
          "log-replay-special-characters-b",
          "update-deleted-directory",
          "versions-not-contiguous");

  private DeltaGoldenTables() {}
}
