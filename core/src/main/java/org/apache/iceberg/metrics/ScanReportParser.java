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
import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class ScanReportParser {
  private static final String TABLE_NAME = "table-name";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String FILTER = "filter";
  private static final String SCHEMA_ID = "schema-id";
  private static final String PROJECTED_FIELD_IDS = "projected-field-ids";
  private static final String PROJECTED_FIELD_NAMES = "projected-field-names";
  private static final String METRICS = "metrics";
  private static final String METADATA = "metadata";

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
    toJsonWithoutStartEnd(scanReport, gen);
    gen.writeEndObject();
  }

  /**
   * This serializes the {@link ScanReport} without writing a start/end object and is mainly used by
   * {@link org.apache.iceberg.rest.requests.ReportMetricsRequestParser}.
   *
   * @param scanReport The {@link ScanReport} to serialize
   * @param gen The {@link JsonGenerator} to use
   * @throws IOException If an error occurs while serializing
   */
  public static void toJsonWithoutStartEnd(ScanReport scanReport, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != scanReport, "Invalid scan report: null");

    gen.writeStringField(TABLE_NAME, scanReport.tableName());
    gen.writeNumberField(SNAPSHOT_ID, scanReport.snapshotId());

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

  public static ScanReport fromJson(String json) {
    return JsonUtil.parse(json, ScanReportParser::fromJson);
  }

  public static ScanReport fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse scan report from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse scan report from non-object: %s", json);

    String tableName = JsonUtil.getString(TABLE_NAME, json);
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, json);
    Expression filter = ExpressionParser.fromJson(JsonUtil.get(FILTER, json));
    int schemaId = JsonUtil.getInt(SCHEMA_ID, json);
    List<Integer> projectedFieldIds = JsonUtil.getIntegerList(PROJECTED_FIELD_IDS, json);
    List<String> projectedFieldNames = JsonUtil.getStringList(PROJECTED_FIELD_NAMES, json);
    ScanMetricsResult scanMetricsResult =
        ScanMetricsResultParser.fromJson(JsonUtil.get(METRICS, json));
    ImmutableScanReport.Builder builder =
        ImmutableScanReport.builder()
            .tableName(tableName)
            .snapshotId(snapshotId)
            .schemaId(schemaId)
            .projectedFieldIds(projectedFieldIds)
            .projectedFieldNames(projectedFieldNames)
            .filter(filter)
            .scanMetrics(scanMetricsResult);

    if (json.has(METADATA)) {
      builder.metadata(JsonUtil.getStringMap(METADATA, json));
    }

    return builder.build();
  }
}
