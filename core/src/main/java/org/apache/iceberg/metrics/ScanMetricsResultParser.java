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
import org.apache.iceberg.metrics.ScanReport.ScanMetrics;
import org.apache.iceberg.metrics.ScanReport.ScanMetricsResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class ScanMetricsResultParser {
  private ScanMetricsResultParser() {}

  static String toJson(ScanMetricsResult metrics) {
    return toJson(metrics, false);
  }

  static String toJson(ScanMetricsResult metrics, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(metrics, gen), pretty);
  }

  static void toJson(ScanMetricsResult metrics, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != metrics, "Invalid scan metrics: null");
    boolean includeMetricName = false;

    gen.writeStartObject();

    gen.writeFieldName(metrics.totalPlanningDuration().name());
    TimerResultParser.toJson(metrics.totalPlanningDuration(), gen, includeMetricName);

    gen.writeFieldName(metrics.resultDataFiles().name());
    CounterResultParser.toJson(metrics.resultDataFiles(), gen, includeMetricName);

    gen.writeFieldName(metrics.resultDeleteFiles().name());
    CounterResultParser.toJson(metrics.resultDeleteFiles(), gen, includeMetricName);

    gen.writeFieldName(metrics.totalDataManifests().name());
    CounterResultParser.toJson(metrics.totalDataManifests(), gen, includeMetricName);

    gen.writeFieldName(metrics.totalDeleteManifests().name());
    CounterResultParser.toJson(metrics.totalDeleteManifests(), gen, includeMetricName);

    gen.writeFieldName(metrics.scannedDataManifests().name());
    CounterResultParser.toJson(metrics.scannedDataManifests(), gen, includeMetricName);

    gen.writeFieldName(metrics.skippedDataManifests().name());
    CounterResultParser.toJson(metrics.skippedDataManifests(), gen, includeMetricName);

    gen.writeFieldName(metrics.totalFileSizeInBytes().name());
    CounterResultParser.toJson(metrics.totalFileSizeInBytes(), gen, includeMetricName);

    gen.writeFieldName(metrics.totalDeleteFileSizeInBytes().name());
    CounterResultParser.toJson(metrics.totalDeleteFileSizeInBytes(), gen, includeMetricName);

    gen.writeEndObject();
  }

  static ScanMetricsResult fromJson(String json) {
    return JsonUtil.parse(json, ScanMetricsResultParser::fromJson);
  }

  static ScanMetricsResult fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse scan metrics from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse scan metrics from non-object: %s", json);

    return new ScanMetricsResult(
        TimerResultParser.fromJson(ScanMetrics.TOTAL_PLANNING_DURATION, json),
        CounterResultParser.fromJson(ScanMetrics.RESULT_DATA_FILES, json),
        CounterResultParser.fromJson(ScanMetrics.RESULT_DELETE_FILES, json),
        CounterResultParser.fromJson(ScanMetrics.TOTAL_DATA_MANIFESTS, json),
        CounterResultParser.fromJson(ScanMetrics.TOTAL_DELETE_MANIFESTS, json),
        CounterResultParser.fromJson(ScanMetrics.SCANNED_DATA_MANIFESTS, json),
        CounterResultParser.fromJson(ScanMetrics.SKIPPED_DATA_MANIFESTS, json),
        CounterResultParser.fromJson(ScanMetrics.TOTAL_FILE_SIZE_IN_BYTES, json),
        CounterResultParser.fromJson(ScanMetrics.TOTAL_DELETE_FILE_SIZE_IN_BYTES, json));
  }
}
