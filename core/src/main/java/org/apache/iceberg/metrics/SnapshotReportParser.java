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

public class SnapshotReportParser {
  private static final String TABLE_NAME = "table-name";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String SEQUENCE_NUMBER = "sequence-number";
  private static final String OPERATION = "operation";
  private static final String METRICS = "metrics";
  private static final String METADATA = "metadata";

  private SnapshotReportParser() {}

  public static String toJson(SnapshotReport snapshotReport) {
    return toJson(snapshotReport, false);
  }

  public static String toJson(SnapshotReport snapshotReport, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(snapshotReport, gen), pretty);
  }

  public static void toJson(SnapshotReport snapshotReport, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != snapshotReport, "Invalid snapshot report: null");

    gen.writeStartObject();
    toJsonWithoutStartEnd(snapshotReport, gen);
    gen.writeEndObject();
  }

  /**
   * This serializes the {@link SnapshotReport} without writing a start/end object and is mainly
   * used by {@link org.apache.iceberg.rest.requests.ReportMetricsRequestParser}.
   *
   * @param snapshotReport The {@link SnapshotReport} to serialize
   * @param gen The {@link JsonGenerator} to use
   * @throws IOException If an error occurs while serializing
   */
  public static void toJsonWithoutStartEnd(SnapshotReport snapshotReport, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != snapshotReport, "Invalid snapshot report: null");

    gen.writeStringField(TABLE_NAME, snapshotReport.tableName());
    gen.writeNumberField(SNAPSHOT_ID, snapshotReport.snapshotId());
    gen.writeNumberField(SEQUENCE_NUMBER, snapshotReport.sequenceNumber());
    gen.writeStringField(OPERATION, snapshotReport.operation());

    gen.writeFieldName(METRICS);
    SnapshotMetricsResultParser.toJson(snapshotReport.snapshotMetrics(), gen);

    if (!snapshotReport.metadata().isEmpty()) {
      JsonUtil.writeStringMap(METADATA, snapshotReport.metadata(), gen);
    }
  }

  public static SnapshotReport fromJson(String json) {
    return JsonUtil.parse(json, SnapshotReportParser::fromJson);
  }

  public static SnapshotReport fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse snapshot report from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse snapshot report from non-object: %s", json);

    ImmutableSnapshotReport.Builder builder =
        ImmutableSnapshotReport.builder()
            .tableName(JsonUtil.getString(TABLE_NAME, json))
            .snapshotId(JsonUtil.getLong(SNAPSHOT_ID, json))
            .sequenceNumber(JsonUtil.getLong(SEQUENCE_NUMBER, json))
            .operation(JsonUtil.getString(OPERATION, json))
            .snapshotMetrics(SnapshotMetricsResultParser.fromJson(JsonUtil.get(METRICS, json)));

    if (json.has(METADATA)) {
      builder.metadata(JsonUtil.getStringMap(METADATA, json));
    }

    return builder.build();
  }
}
