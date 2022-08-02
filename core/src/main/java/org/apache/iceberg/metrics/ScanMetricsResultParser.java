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
import org.apache.iceberg.metrics.ScanReport.CounterResult;
import org.apache.iceberg.metrics.ScanReport.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport.TimerResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class ScanMetricsResultParser {
  private static final String TOTAL_PLANNING_DURATION = "total-planning-duration";
  private static final String RESULT_DATA_FILES = "result-data-files";
  private static final String RESULT_DELETE_FILES = "result-delete-files";
  private static final String TOTAL_DATA_MANIFESTS = "total-data-manifests";
  private static final String TOTAL_DELETE_MANIFESTS = "total-delete-manifests";
  private static final String SCANNED_DATA_MANIFESTS = "scanned-data-manifests";
  private static final String SKIPPED_DATA_MANIFESTS = "skipped-data-manifests";
  private static final String TOTAL_FILE_SIZE_BYTES = "total-file-size-bytes";
  private static final String TOTAL_DELETE_FILE_SIZE_BYTES = "total-delete-file-size-bytes";

  private ScanMetricsResultParser() {}

  static String toJson(ScanMetricsResult metrics) {
    return toJson(metrics, false);
  }

  static String toJson(ScanMetricsResult metrics, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(metrics, gen), pretty);
  }

  static void toJson(ScanMetricsResult metrics, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != metrics, "Invalid scan metrics: null");
    gen.writeStartObject();

    gen.writeFieldName(TOTAL_PLANNING_DURATION);
    TimerResultParser.toJson(metrics.totalPlanningDuration(), gen);

    gen.writeFieldName(RESULT_DATA_FILES);
    CounterResultParser.toJson(metrics.resultDataFiles(), gen);

    gen.writeFieldName(RESULT_DELETE_FILES);
    CounterResultParser.toJson(metrics.resultDeleteFiles(), gen);

    gen.writeFieldName(TOTAL_DATA_MANIFESTS);
    CounterResultParser.toJson(metrics.totalDataManifests(), gen);

    gen.writeFieldName(TOTAL_DELETE_MANIFESTS);
    CounterResultParser.toJson(metrics.totalDeleteManifests(), gen);

    gen.writeFieldName(SCANNED_DATA_MANIFESTS);
    CounterResultParser.toJson(metrics.scannedDataManifests(), gen);

    gen.writeFieldName(SKIPPED_DATA_MANIFESTS);
    CounterResultParser.toJson(metrics.skippedDataManifests(), gen);

    gen.writeFieldName(TOTAL_FILE_SIZE_BYTES);
    CounterResultParser.toJson(metrics.totalFileSizeInBytes(), gen);

    gen.writeFieldName(TOTAL_DELETE_FILE_SIZE_BYTES);
    CounterResultParser.toJson(metrics.totalDeleteFileSizeInBytes(), gen);

    gen.writeEndObject();
  }

  static ScanMetricsResult fromJson(String json) {
    return JsonUtil.parse(json, ScanMetricsResultParser::fromJson);
  }

  static ScanMetricsResult fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse scan metrics from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse scan metrics from non-object: '%s'", json);

    TimerResult totalPlanningDuration = TimerResultParser.fromJson(TOTAL_PLANNING_DURATION, json);
    CounterResult<Integer> resultDataFiles =
        CounterResultParser.intCounterFromJson(RESULT_DATA_FILES, json);
    CounterResult<Integer> resultDeleteFiles =
        CounterResultParser.intCounterFromJson(RESULT_DELETE_FILES, json);
    CounterResult<Integer> totalDataManifests =
        CounterResultParser.intCounterFromJson(TOTAL_DATA_MANIFESTS, json);
    CounterResult<Integer> totalDeleteManifests =
        CounterResultParser.intCounterFromJson(TOTAL_DELETE_MANIFESTS, json);
    CounterResult<Integer> scannedDataManifests =
        CounterResultParser.intCounterFromJson(SCANNED_DATA_MANIFESTS, json);
    CounterResult<Integer> skippedDataManifests =
        CounterResultParser.intCounterFromJson(SKIPPED_DATA_MANIFESTS, json);
    CounterResult<Long> totalFileSize =
        CounterResultParser.longCounterFromJson(TOTAL_FILE_SIZE_BYTES, json);
    CounterResult<Long> totalDeleteFileSize =
        CounterResultParser.longCounterFromJson(TOTAL_DELETE_FILE_SIZE_BYTES, json);
    return new ScanMetricsResult(
        totalPlanningDuration,
        resultDataFiles,
        resultDeleteFiles,
        totalDataManifests,
        totalDeleteManifests,
        scannedDataManifests,
        skippedDataManifests,
        totalFileSize,
        totalDeleteFileSize);
  }
}
