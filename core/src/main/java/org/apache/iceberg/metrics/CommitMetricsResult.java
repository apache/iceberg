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
package org.apache.iceberg.metrics;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.immutables.value.Value;

/** A serializable version of {@link CommitMetrics} that carries its results. */
@Value.Immutable
public interface CommitMetricsResult {
  String ADDED_DATA_FILES = "added-data-files";
  String REMOVED_DATA_FILES = "removed-data-files";
  String TOTAL_DATA_FILES = "total-data-files";
  String ADDED_DELETE_FILES = "added-delete-files";
  String ADDED_EQ_DELETE_FILES = "added-equality-delete-files";
  String ADDED_POS_DELETE_FILES = "added-positional-delete-files";
  String REMOVED_POS_DELETE_FILES = "removed-positional-delete-files";
  String REMOVED_EQ_DELETE_FILES = "removed-equality-delete-files";
  String REMOVED_DELETE_FILES = "removed-delete-files";
  String TOTAL_DELETE_FILES = "total-delete-files";
  String ADDED_RECORDS = "added-records";
  String REMOVED_RECORDS = "removed-records";
  String TOTAL_RECORDS = "total-records";
  String ADDED_FILE_SIZE_BYTES = "added-files-size-bytes";
  String REMOVED_FILE_SIZE_BYTES = "removed-files-size-bytes";
  String TOTAL_FILE_SIZE_BYTES = "total-files-size-bytes";
  String ADDED_POS_DELETES = "added-positional-deletes";
  String REMOVED_POS_DELETES = "removed-positional-deletes";
  String TOTAL_POS_DELETES = "total-positional-deletes";
  String ADDED_EQ_DELETES = "added-equality-deletes";
  String REMOVED_EQ_DELETES = "removed-equality-deletes";
  String TOTAL_EQ_DELETES = "total-equality-deletes";
  String TOTAL_DATA_MANIFEST_FILES = "total-data-manifest-files";
  String TOTAL_DELETE_MANIFEST_FILES = "total-delete-manifest-files";

  @Nullable
  TimerResult totalDuration();

  @Nullable
  CounterResult attempts();

  @Nullable
  CounterResult addedDataFiles();

  @Nullable
  CounterResult removedDataFiles();

  @Nullable
  CounterResult totalDataFiles();

  @Nullable
  CounterResult addedDeleteFiles();

  @Nullable
  CounterResult addedEqualityDeleteFiles();

  @Nullable
  CounterResult addedPositionalDeleteFiles();

  @Nullable
  CounterResult removedDeleteFiles();

  @Nullable
  CounterResult removedEqualityDeleteFiles();

  @Nullable
  CounterResult removedPositionalDeleteFiles();

  @Nullable
  CounterResult totalDeleteFiles();

  @Nullable
  CounterResult addedRecords();

  @Nullable
  CounterResult removedRecords();

  @Nullable
  CounterResult totalRecords();

  @Nullable
  CounterResult addedFilesSizeInBytes();

  @Nullable
  CounterResult removedFilesSizeInBytes();

  @Nullable
  CounterResult totalFilesSizeInBytes();

  @Nullable
  CounterResult addedPositionalDeletes();

  @Nullable
  CounterResult removedPositionalDeletes();

  @Nullable
  CounterResult totalPositionalDeletes();

  @Nullable
  CounterResult addedEqualityDeletes();

  @Nullable
  CounterResult removedEqualityDeletes();

  @Nullable
  CounterResult totalEqualityDeletes();

  @Nullable
  CounterResult totalDataManifestFiles();

  @Nullable
  CounterResult totalDeleteManifestFiles();

  static CommitMetricsResult from(
      CommitMetrics commitMetrics, Map<String, String> snapshotSummary) {
    Preconditions.checkArgument(null != commitMetrics, "Invalid commit metrics: null");
    Preconditions.checkArgument(null != snapshotSummary, "Invalid snapshot summary: null");
    return ImmutableCommitMetricsResult.builder()
        .attempts(CounterResult.fromCounter(commitMetrics.attempts()))
        .totalDuration(TimerResult.fromTimer(commitMetrics.totalDuration()))
        .addedDataFiles(counterFrom(snapshotSummary, SnapshotSummary.ADDED_FILES_PROP))
        .removedDataFiles(counterFrom(snapshotSummary, SnapshotSummary.DELETED_FILES_PROP))
        .totalDataFiles(counterFrom(snapshotSummary, SnapshotSummary.TOTAL_DATA_FILES_PROP))
        .addedDeleteFiles(counterFrom(snapshotSummary, SnapshotSummary.ADDED_DELETE_FILES_PROP))
        .addedPositionalDeleteFiles(
            counterFrom(snapshotSummary, SnapshotSummary.ADD_POS_DELETE_FILES_PROP))
        .addedEqualityDeleteFiles(
            counterFrom(snapshotSummary, SnapshotSummary.ADD_EQ_DELETE_FILES_PROP))
        .removedDeleteFiles(counterFrom(snapshotSummary, SnapshotSummary.REMOVED_DELETE_FILES_PROP))
        .removedEqualityDeleteFiles(
            counterFrom(snapshotSummary, SnapshotSummary.REMOVED_EQ_DELETE_FILES_PROP))
        .removedPositionalDeleteFiles(
            counterFrom(snapshotSummary, SnapshotSummary.REMOVED_POS_DELETE_FILES_PROP))
        .totalDeleteFiles(counterFrom(snapshotSummary, SnapshotSummary.TOTAL_DELETE_FILES_PROP))
        .addedRecords(counterFrom(snapshotSummary, SnapshotSummary.ADDED_RECORDS_PROP))
        .removedRecords(counterFrom(snapshotSummary, SnapshotSummary.DELETED_RECORDS_PROP))
        .totalRecords(counterFrom(snapshotSummary, SnapshotSummary.TOTAL_RECORDS_PROP))
        .addedFilesSizeInBytes(
            counterFrom(snapshotSummary, SnapshotSummary.ADDED_FILE_SIZE_PROP, Unit.BYTES))
        .removedFilesSizeInBytes(
            counterFrom(snapshotSummary, SnapshotSummary.REMOVED_FILE_SIZE_PROP, Unit.BYTES))
        .totalFilesSizeInBytes(
            counterFrom(snapshotSummary, SnapshotSummary.TOTAL_FILE_SIZE_PROP, Unit.BYTES))
        .addedPositionalDeletes(
            counterFrom(snapshotSummary, SnapshotSummary.ADDED_POS_DELETES_PROP))
        .removedPositionalDeletes(
            counterFrom(snapshotSummary, SnapshotSummary.REMOVED_POS_DELETES_PROP))
        .totalPositionalDeletes(
            counterFrom(snapshotSummary, SnapshotSummary.TOTAL_POS_DELETES_PROP))
        .addedEqualityDeletes(counterFrom(snapshotSummary, SnapshotSummary.ADDED_EQ_DELETES_PROP))
        .removedEqualityDeletes(
            counterFrom(snapshotSummary, SnapshotSummary.REMOVED_EQ_DELETES_PROP))
        .totalEqualityDeletes(counterFrom(snapshotSummary, SnapshotSummary.TOTAL_EQ_DELETES_PROP))
        .totalDataManifestFiles(
            counterFrom(snapshotSummary, SnapshotSummary.TOTAL_DATA_MANIFEST_FILES))
        .totalDeleteManifestFiles(
            counterFrom(snapshotSummary, SnapshotSummary.TOTAL_DELETE_MANIFEST_FILES))
        .build();
  }

  static CounterResult counterFrom(Map<String, String> snapshotSummary, String metricName) {
    return counterFrom(snapshotSummary, metricName, Unit.COUNT);
  }

  static CounterResult counterFrom(
      Map<String, String> snapshotSummary, String metricName, Unit unit) {
    if (!snapshotSummary.containsKey(metricName)) {
      return null;
    }

    try {
      return CounterResult.of(unit, Long.parseLong(snapshotSummary.get(metricName)));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
