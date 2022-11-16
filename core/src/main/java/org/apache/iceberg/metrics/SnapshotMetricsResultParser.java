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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class SnapshotMetricsResultParser {
  private SnapshotMetricsResultParser() {}

  static String toJson(SnapshotMetricsResult metrics) {
    return toJson(metrics, false);
  }

  static String toJson(SnapshotMetricsResult metrics, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(metrics, gen), pretty);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  static void toJson(SnapshotMetricsResult metrics, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != metrics, "Invalid snapshot metrics: null");

    gen.writeStartObject();

    if (null != metrics.totalDuration()) {
      gen.writeFieldName(SnapshotMetrics.TOTAL_DURATION);
      TimerResultParser.toJson(metrics.totalDuration(), gen);
    }

    if (null != metrics.attempts()) {
      gen.writeFieldName(SnapshotMetrics.ATTEMPTS);
      CounterResultParser.toJson(metrics.attempts(), gen);
    }

    if (null != metrics.addedDataFiles()) {
      gen.writeFieldName(SnapshotMetricsResult.ADDED_DATA_FILES);
      CounterResultParser.toJson(metrics.addedDataFiles(), gen);
    }

    if (null != metrics.removedDataFiles()) {
      gen.writeFieldName(SnapshotMetricsResult.REMOVED_DATA_FILES);
      CounterResultParser.toJson(metrics.removedDataFiles(), gen);
    }

    if (null != metrics.totalDataFiles()) {
      gen.writeFieldName(SnapshotMetricsResult.TOTAL_DATA_FILES);
      CounterResultParser.toJson(metrics.totalDataFiles(), gen);
    }

    if (null != metrics.addedDeleteFiles()) {
      gen.writeFieldName(SnapshotMetricsResult.ADDED_DELETE_FILES);
      CounterResultParser.toJson(metrics.addedDeleteFiles(), gen);
    }

    if (null != metrics.addedEqualityDeleteFiles()) {
      gen.writeFieldName(SnapshotMetricsResult.ADDED_EQ_DELETE_FILES);
      CounterResultParser.toJson(metrics.addedEqualityDeleteFiles(), gen);
    }

    if (null != metrics.addedPositionalDeleteFiles()) {
      gen.writeFieldName(SnapshotMetricsResult.ADDED_POS_DELETE_FILES);
      CounterResultParser.toJson(metrics.addedPositionalDeleteFiles(), gen);
    }

    if (null != metrics.removedDeleteFiles()) {
      gen.writeFieldName(SnapshotMetricsResult.REMOVED_DELETE_FILES);
      CounterResultParser.toJson(metrics.removedDeleteFiles(), gen);
    }

    if (null != metrics.removedPositionalDeleteFiles()) {
      gen.writeFieldName(SnapshotMetricsResult.REMOVED_POS_DELETE_FILES);
      CounterResultParser.toJson(metrics.removedPositionalDeleteFiles(), gen);
    }

    if (null != metrics.removedEqualityDeleteFiles()) {
      gen.writeFieldName(SnapshotMetricsResult.REMOVED_EQ_DELETE_FILES);
      CounterResultParser.toJson(metrics.removedEqualityDeleteFiles(), gen);
    }

    if (null != metrics.totalDeleteFiles()) {
      gen.writeFieldName(SnapshotMetricsResult.TOTAL_DELETE_FILES);
      CounterResultParser.toJson(metrics.totalDeleteFiles(), gen);
    }

    if (null != metrics.addedRecords()) {
      gen.writeFieldName(SnapshotMetricsResult.ADDED_RECORDS);
      CounterResultParser.toJson(metrics.addedRecords(), gen);
    }

    if (null != metrics.removedRecords()) {
      gen.writeFieldName(SnapshotMetricsResult.REMOVED_RECORDS);
      CounterResultParser.toJson(metrics.removedRecords(), gen);
    }

    if (null != metrics.totalRecords()) {
      gen.writeFieldName(SnapshotMetricsResult.TOTAL_RECORDS);
      CounterResultParser.toJson(metrics.totalRecords(), gen);
    }

    if (null != metrics.addedFilesSizeInBytes()) {
      gen.writeFieldName(SnapshotMetricsResult.ADDED_FILE_SIZE_BYTES);
      CounterResultParser.toJson(metrics.addedFilesSizeInBytes(), gen);
    }

    if (null != metrics.removedFilesSizeInBytes()) {
      gen.writeFieldName(SnapshotMetricsResult.REMOVED_FILE_SIZE_BYTES);
      CounterResultParser.toJson(metrics.removedFilesSizeInBytes(), gen);
    }

    if (null != metrics.totalFilesSizeInBytes()) {
      gen.writeFieldName(SnapshotMetricsResult.TOTAL_FILE_SIZE_BYTES);
      CounterResultParser.toJson(metrics.totalFilesSizeInBytes(), gen);
    }

    if (null != metrics.addedPositionalDeletes()) {
      gen.writeFieldName(SnapshotMetricsResult.ADDED_POS_DELETES);
      CounterResultParser.toJson(metrics.addedPositionalDeletes(), gen);
    }

    if (null != metrics.removedPositionalDeletes()) {
      gen.writeFieldName(SnapshotMetricsResult.REMOVED_POS_DELETES);
      CounterResultParser.toJson(metrics.removedPositionalDeletes(), gen);
    }

    if (null != metrics.totalPositionalDeletes()) {
      gen.writeFieldName(SnapshotMetricsResult.TOTAL_POS_DELETES);
      CounterResultParser.toJson(metrics.totalPositionalDeletes(), gen);
    }

    if (null != metrics.addedEqualityDeletes()) {
      gen.writeFieldName(SnapshotMetricsResult.ADDED_EQ_DELETES);
      CounterResultParser.toJson(metrics.addedEqualityDeletes(), gen);
    }

    if (null != metrics.removedEqualityDeletes()) {
      gen.writeFieldName(SnapshotMetricsResult.REMOVED_EQ_DELETES);
      CounterResultParser.toJson(metrics.removedEqualityDeletes(), gen);
    }

    if (null != metrics.totalEqualityDeletes()) {
      gen.writeFieldName(SnapshotMetricsResult.TOTAL_EQ_DELETES);
      CounterResultParser.toJson(metrics.totalEqualityDeletes(), gen);
    }

    gen.writeEndObject();
  }

  static SnapshotMetricsResult fromJson(String json) {
    return JsonUtil.parse(json, SnapshotMetricsResultParser::fromJson);
  }

  static SnapshotMetricsResult fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse snapshot metrics from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse snapshot metrics from non-object: %s", json);

    return ImmutableSnapshotMetricsResult.builder()
        .attempts(CounterResultParser.fromJson(SnapshotMetrics.ATTEMPTS, json))
        .totalDuration(TimerResultParser.fromJson(SnapshotMetrics.TOTAL_DURATION, json))
        .addedDataFiles(CounterResultParser.fromJson(SnapshotMetricsResult.ADDED_DATA_FILES, json))
        .removedDataFiles(
            CounterResultParser.fromJson(SnapshotMetricsResult.REMOVED_DATA_FILES, json))
        .totalDataFiles(CounterResultParser.fromJson(SnapshotMetricsResult.TOTAL_DATA_FILES, json))
        .addedDeleteFiles(
            CounterResultParser.fromJson(SnapshotMetricsResult.ADDED_DELETE_FILES, json))
        .addedEqualityDeleteFiles(
            CounterResultParser.fromJson(SnapshotMetricsResult.ADDED_EQ_DELETE_FILES, json))
        .addedPositionalDeleteFiles(
            CounterResultParser.fromJson(SnapshotMetricsResult.ADDED_POS_DELETE_FILES, json))
        .removedEqualityDeleteFiles(
            CounterResultParser.fromJson(SnapshotMetricsResult.REMOVED_EQ_DELETE_FILES, json))
        .removedPositionalDeleteFiles(
            CounterResultParser.fromJson(SnapshotMetricsResult.REMOVED_POS_DELETE_FILES, json))
        .removedDeleteFiles(
            CounterResultParser.fromJson(SnapshotMetricsResult.REMOVED_DELETE_FILES, json))
        .totalDeleteFiles(
            CounterResultParser.fromJson(SnapshotMetricsResult.TOTAL_DELETE_FILES, json))
        .addedRecords(CounterResultParser.fromJson(SnapshotMetricsResult.ADDED_RECORDS, json))
        .removedRecords(CounterResultParser.fromJson(SnapshotMetricsResult.REMOVED_RECORDS, json))
        .totalRecords(CounterResultParser.fromJson(SnapshotMetricsResult.TOTAL_RECORDS, json))
        .addedFilesSizeInBytes(
            CounterResultParser.fromJson(SnapshotMetricsResult.ADDED_FILE_SIZE_BYTES, json))
        .removedFilesSizeInBytes(
            CounterResultParser.fromJson(SnapshotMetricsResult.REMOVED_FILE_SIZE_BYTES, json))
        .totalFilesSizeInBytes(
            CounterResultParser.fromJson(SnapshotMetricsResult.TOTAL_FILE_SIZE_BYTES, json))
        .addedPositionalDeletes(
            CounterResultParser.fromJson(SnapshotMetricsResult.ADDED_POS_DELETES, json))
        .removedPositionalDeletes(
            CounterResultParser.fromJson(SnapshotMetricsResult.REMOVED_POS_DELETES, json))
        .totalPositionalDeletes(
            CounterResultParser.fromJson(SnapshotMetricsResult.TOTAL_POS_DELETES, json))
        .addedEqualityDeletes(
            CounterResultParser.fromJson(SnapshotMetricsResult.ADDED_EQ_DELETES, json))
        .removedEqualityDeletes(
            CounterResultParser.fromJson(SnapshotMetricsResult.REMOVED_EQ_DELETES, json))
        .totalEqualityDeletes(
            CounterResultParser.fromJson(SnapshotMetricsResult.TOTAL_EQ_DELETES, json))
        .build();
  }
}
