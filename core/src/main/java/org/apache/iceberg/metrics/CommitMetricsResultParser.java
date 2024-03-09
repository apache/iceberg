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

class CommitMetricsResultParser {
  private CommitMetricsResultParser() {}

  static String toJson(CommitMetricsResult metrics) {
    return toJson(metrics, false);
  }

  static String toJson(CommitMetricsResult metrics, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(metrics, gen), pretty);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  static void toJson(CommitMetricsResult metrics, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != metrics, "Invalid commit metrics: null");

    gen.writeStartObject();

    if (null != metrics.totalDuration()) {
      gen.writeFieldName(CommitMetrics.TOTAL_DURATION);
      TimerResultParser.toJson(metrics.totalDuration(), gen);
    }

    if (null != metrics.attempts()) {
      gen.writeFieldName(CommitMetrics.ATTEMPTS);
      CounterResultParser.toJson(metrics.attempts(), gen);
    }

    if (null != metrics.addedDataFiles()) {
      gen.writeFieldName(CommitMetricsResult.ADDED_DATA_FILES);
      CounterResultParser.toJson(metrics.addedDataFiles(), gen);
    }

    if (null != metrics.removedDataFiles()) {
      gen.writeFieldName(CommitMetricsResult.REMOVED_DATA_FILES);
      CounterResultParser.toJson(metrics.removedDataFiles(), gen);
    }

    if (null != metrics.totalDataFiles()) {
      gen.writeFieldName(CommitMetricsResult.TOTAL_DATA_FILES);
      CounterResultParser.toJson(metrics.totalDataFiles(), gen);
    }

    if (null != metrics.addedDeleteFiles()) {
      gen.writeFieldName(CommitMetricsResult.ADDED_DELETE_FILES);
      CounterResultParser.toJson(metrics.addedDeleteFiles(), gen);
    }

    if (null != metrics.addedEqualityDeleteFiles()) {
      gen.writeFieldName(CommitMetricsResult.ADDED_EQ_DELETE_FILES);
      CounterResultParser.toJson(metrics.addedEqualityDeleteFiles(), gen);
    }

    if (null != metrics.addedPositionalDeleteFiles()) {
      gen.writeFieldName(CommitMetricsResult.ADDED_POS_DELETE_FILES);
      CounterResultParser.toJson(metrics.addedPositionalDeleteFiles(), gen);
    }

    if (null != metrics.removedDeleteFiles()) {
      gen.writeFieldName(CommitMetricsResult.REMOVED_DELETE_FILES);
      CounterResultParser.toJson(metrics.removedDeleteFiles(), gen);
    }

    if (null != metrics.removedPositionalDeleteFiles()) {
      gen.writeFieldName(CommitMetricsResult.REMOVED_POS_DELETE_FILES);
      CounterResultParser.toJson(metrics.removedPositionalDeleteFiles(), gen);
    }

    if (null != metrics.removedEqualityDeleteFiles()) {
      gen.writeFieldName(CommitMetricsResult.REMOVED_EQ_DELETE_FILES);
      CounterResultParser.toJson(metrics.removedEqualityDeleteFiles(), gen);
    }

    if (null != metrics.totalDeleteFiles()) {
      gen.writeFieldName(CommitMetricsResult.TOTAL_DELETE_FILES);
      CounterResultParser.toJson(metrics.totalDeleteFiles(), gen);
    }

    if (null != metrics.addedRecords()) {
      gen.writeFieldName(CommitMetricsResult.ADDED_RECORDS);
      CounterResultParser.toJson(metrics.addedRecords(), gen);
    }

    if (null != metrics.removedRecords()) {
      gen.writeFieldName(CommitMetricsResult.REMOVED_RECORDS);
      CounterResultParser.toJson(metrics.removedRecords(), gen);
    }

    if (null != metrics.totalRecords()) {
      gen.writeFieldName(CommitMetricsResult.TOTAL_RECORDS);
      CounterResultParser.toJson(metrics.totalRecords(), gen);
    }

    if (null != metrics.addedFilesSizeInBytes()) {
      gen.writeFieldName(CommitMetricsResult.ADDED_FILE_SIZE_BYTES);
      CounterResultParser.toJson(metrics.addedFilesSizeInBytes(), gen);
    }

    if (null != metrics.removedFilesSizeInBytes()) {
      gen.writeFieldName(CommitMetricsResult.REMOVED_FILE_SIZE_BYTES);
      CounterResultParser.toJson(metrics.removedFilesSizeInBytes(), gen);
    }

    if (null != metrics.totalFilesSizeInBytes()) {
      gen.writeFieldName(CommitMetricsResult.TOTAL_FILE_SIZE_BYTES);
      CounterResultParser.toJson(metrics.totalFilesSizeInBytes(), gen);
    }

    if (null != metrics.addedPositionalDeletes()) {
      gen.writeFieldName(CommitMetricsResult.ADDED_POS_DELETES);
      CounterResultParser.toJson(metrics.addedPositionalDeletes(), gen);
    }

    if (null != metrics.removedPositionalDeletes()) {
      gen.writeFieldName(CommitMetricsResult.REMOVED_POS_DELETES);
      CounterResultParser.toJson(metrics.removedPositionalDeletes(), gen);
    }

    if (null != metrics.totalPositionalDeletes()) {
      gen.writeFieldName(CommitMetricsResult.TOTAL_POS_DELETES);
      CounterResultParser.toJson(metrics.totalPositionalDeletes(), gen);
    }

    if (null != metrics.addedEqualityDeletes()) {
      gen.writeFieldName(CommitMetricsResult.ADDED_EQ_DELETES);
      CounterResultParser.toJson(metrics.addedEqualityDeletes(), gen);
    }

    if (null != metrics.removedEqualityDeletes()) {
      gen.writeFieldName(CommitMetricsResult.REMOVED_EQ_DELETES);
      CounterResultParser.toJson(metrics.removedEqualityDeletes(), gen);
    }

    if (null != metrics.totalEqualityDeletes()) {
      gen.writeFieldName(CommitMetricsResult.TOTAL_EQ_DELETES);
      CounterResultParser.toJson(metrics.totalEqualityDeletes(), gen);
    }

    gen.writeEndObject();
  }

  static CommitMetricsResult fromJson(String json) {
    return JsonUtil.parse(json, CommitMetricsResultParser::fromJson);
  }

  static CommitMetricsResult fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse commit metrics from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse commit metrics from non-object: %s", json);

    return ImmutableCommitMetricsResult.builder()
        .attempts(CounterResultParser.fromJson(CommitMetrics.ATTEMPTS, json))
        .totalDuration(TimerResultParser.fromJson(CommitMetrics.TOTAL_DURATION, json))
        .addedDataFiles(CounterResultParser.fromJson(CommitMetricsResult.ADDED_DATA_FILES, json))
        .removedDataFiles(
            CounterResultParser.fromJson(CommitMetricsResult.REMOVED_DATA_FILES, json))
        .totalDataFiles(CounterResultParser.fromJson(CommitMetricsResult.TOTAL_DATA_FILES, json))
        .addedDeleteFiles(
            CounterResultParser.fromJson(CommitMetricsResult.ADDED_DELETE_FILES, json))
        .addedEqualityDeleteFiles(
            CounterResultParser.fromJson(CommitMetricsResult.ADDED_EQ_DELETE_FILES, json))
        .addedPositionalDeleteFiles(
            CounterResultParser.fromJson(CommitMetricsResult.ADDED_POS_DELETE_FILES, json))
        .removedEqualityDeleteFiles(
            CounterResultParser.fromJson(CommitMetricsResult.REMOVED_EQ_DELETE_FILES, json))
        .removedPositionalDeleteFiles(
            CounterResultParser.fromJson(CommitMetricsResult.REMOVED_POS_DELETE_FILES, json))
        .removedDeleteFiles(
            CounterResultParser.fromJson(CommitMetricsResult.REMOVED_DELETE_FILES, json))
        .totalDeleteFiles(
            CounterResultParser.fromJson(CommitMetricsResult.TOTAL_DELETE_FILES, json))
        .addedRecords(CounterResultParser.fromJson(CommitMetricsResult.ADDED_RECORDS, json))
        .removedRecords(CounterResultParser.fromJson(CommitMetricsResult.REMOVED_RECORDS, json))
        .totalRecords(CounterResultParser.fromJson(CommitMetricsResult.TOTAL_RECORDS, json))
        .addedFilesSizeInBytes(
            CounterResultParser.fromJson(CommitMetricsResult.ADDED_FILE_SIZE_BYTES, json))
        .removedFilesSizeInBytes(
            CounterResultParser.fromJson(CommitMetricsResult.REMOVED_FILE_SIZE_BYTES, json))
        .totalFilesSizeInBytes(
            CounterResultParser.fromJson(CommitMetricsResult.TOTAL_FILE_SIZE_BYTES, json))
        .addedPositionalDeletes(
            CounterResultParser.fromJson(CommitMetricsResult.ADDED_POS_DELETES, json))
        .removedPositionalDeletes(
            CounterResultParser.fromJson(CommitMetricsResult.REMOVED_POS_DELETES, json))
        .totalPositionalDeletes(
            CounterResultParser.fromJson(CommitMetricsResult.TOTAL_POS_DELETES, json))
        .addedEqualityDeletes(
            CounterResultParser.fromJson(CommitMetricsResult.ADDED_EQ_DELETES, json))
        .removedEqualityDeletes(
            CounterResultParser.fromJson(CommitMetricsResult.REMOVED_EQ_DELETES, json))
        .totalEqualityDeletes(
            CounterResultParser.fromJson(CommitMetricsResult.TOTAL_EQ_DELETES, json))
        .build();
  }
}
