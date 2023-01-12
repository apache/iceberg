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
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class IncrementalScanReportParser {
  private static final String TABLE_NAME = "table-name";
  private static final String FROM_SNAPSHOT_ID = "from-snapshot-id";
  private static final String TO_SNAPSHOT_ID = "to-snapshot-id";
  private static final String FILTER = "filter";
  private static final String SCHEMA_ID = "schema-id";
  private static final String PROJECTED_FIELD_IDS = "projected-field-ids";
  private static final String PROJECTED_FIELD_NAMES = "projected-field-names";
  private static final String METRICS = "metrics";
  private static final String METADATA = "metadata";

  private IncrementalScanReportParser() {}

  public static String toJson(IncrementalScanReport scanReport) {
    return toJson(scanReport, false);
  }

  public static String toJson(IncrementalScanReport scanReport, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(scanReport, gen), pretty);
  }

  public static void toJson(IncrementalScanReport scanReport, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != scanReport, "Invalid scan report: null");

    gen.writeStartObject();
    toJsonWithoutStartEnd(scanReport, gen);
    gen.writeEndObject();
  }

  /**
   * This serializes the {@link IncrementalScanReport} without writing a start/end object and is
   * mainly used by {@link org.apache.iceberg.rest.requests.ReportMetricsRequestParser}.
   *
   * @param scanReport The {@link IncrementalScanReport} to serialize
   * @param gen The {@link JsonGenerator} to use
   * @throws IOException If an error occurs while serializing
   */
  public static void toJsonWithoutStartEnd(IncrementalScanReport scanReport, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != scanReport, "Invalid scan report: null");

    gen.writeStringField(TABLE_NAME, scanReport.tableName());
    gen.writeNumberField(FROM_SNAPSHOT_ID, scanReport.fromSnapshotId());
    gen.writeNumberField(TO_SNAPSHOT_ID, scanReport.toSnapshotId());

    gen.writeFieldName(FILTER);
    ExpressionParser.toJson(scanReport.filter(), gen);

    gen.writeNumberField(SCHEMA_ID, scanReport.schemaId());

    JsonUtil.writeIntegerArray(PROJECTED_FIELD_IDS, scanReport.projectedFieldIds(), gen);
    JsonUtil.writeStringArray(PROJECTED_FIELD_NAMES, scanReport.projectedFieldNames(), gen);

    gen.writeFieldName(METRICS);
    ScanMetricsResultParser.toJson(scanReport.scanMetrics(), gen);

    if (!scanReport.metadata().isEmpty()) {
      JsonUtil.writeStringMap(METADATA, scanReport.metadata(), gen);
    }
  }

  public static IncrementalScanReport fromJson(String json) {
    return JsonUtil.parse(json, IncrementalScanReportParser::fromJson);
  }

  public static IncrementalScanReport fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse scan report from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse scan report from non-object: %s", json);

    ImmutableIncrementalScanReport.Builder builder =
        ImmutableIncrementalScanReport.builder()
            .tableName(JsonUtil.getString(TABLE_NAME, json))
            .fromSnapshotId(JsonUtil.getLong(FROM_SNAPSHOT_ID, json))
            .toSnapshotId(JsonUtil.getLong(TO_SNAPSHOT_ID, json))
            .filter(ExpressionParser.fromJson(JsonUtil.get(FILTER, json)))
            .schemaId(JsonUtil.getInt(SCHEMA_ID, json))
            .projectedFieldIds(JsonUtil.getIntegerList(PROJECTED_FIELD_IDS, json))
            .projectedFieldNames(JsonUtil.getStringList(PROJECTED_FIELD_NAMES, json))
            .scanMetrics(ScanMetricsResultParser.fromJson(JsonUtil.get(METRICS, json)));

    if (json.has(METADATA)) {
      builder.metadata(JsonUtil.getStringMap(METADATA, json));
    }

    return builder.build();
  }
}
