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
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.metrics.ScanReport.ScanMetricsResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class ScanReportParser {
  private static final String TABLE_NAME = "table-name";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String FILTER = "filter";
  private static final String PROJECTION = "projection";
  private static final String METRICS = "metrics";

  private ScanReportParser() {}

  public static String toJson(ScanReport scanReport) {
    return toJson(scanReport, false);
  }

  public static String toJson(ScanReport scanReport, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(scanReport, gen), pretty);
  }

  public static void toJson(ScanReport scanReport, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != scanReport, "Invalid scan report: null");

    gen.writeStartObject();

    gen.writeStringField(TABLE_NAME, scanReport.tableName());
    gen.writeNumberField(SNAPSHOT_ID, scanReport.snapshotId());

    gen.writeFieldName(PROJECTION);
    SchemaParser.toJson(scanReport.projection(), gen);

    gen.writeFieldName(METRICS);
    ScanMetricsResultParser.toJson(scanReport.scanMetrics(), gen);

    gen.writeEndObject();
  }

  public static ScanReport fromJson(String json) {
    return JsonUtil.parse(json, ScanReportParser::fromJson);
  }

  public static ScanReport fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse scan report from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse scan report from non-object: %s", json);

    String tableName = JsonUtil.getString(TABLE_NAME, json);
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, json);
    Schema projection = SchemaParser.fromJson(JsonUtil.get(PROJECTION, json));
    ScanMetricsResult scanMetricsResult =
        ScanMetricsResultParser.fromJson(JsonUtil.get(METRICS, json));
    return ScanReport.builder()
        .withTableName(tableName)
        .withSnapshotId(snapshotId)
        .withProjection(projection)
        .fromScanMetricsResult(scanMetricsResult)
        .build();
  }
}
