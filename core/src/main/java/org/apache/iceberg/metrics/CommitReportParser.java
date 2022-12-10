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

public class CommitReportParser {
  private static final String TABLE_NAME = "table-name";
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String SEQUENCE_NUMBER = "sequence-number";
  private static final String OPERATION = "operation";
  private static final String METRICS = "metrics";
  private static final String METADATA = "metadata";

  private CommitReportParser() {}

  public static String toJson(CommitReport commitReport) {
    return toJson(commitReport, false);
  }

  public static String toJson(CommitReport commitReport, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(commitReport, gen), pretty);
  }

  public static void toJson(CommitReport commitReport, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != commitReport, "Invalid commit report: null");

    gen.writeStartObject();
    toJsonWithoutStartEnd(commitReport, gen);
    gen.writeEndObject();
  }

  /**
   * This serializes the {@link CommitReport} without writing a start/end object and is mainly used
   * by {@link org.apache.iceberg.rest.requests.ReportMetricsRequestParser}.
   *
   * @param commitReport The {@link CommitReport} to serialize
   * @param gen The {@link JsonGenerator} to use
   * @throws IOException If an error occurs while serializing
   */
  public static void toJsonWithoutStartEnd(CommitReport commitReport, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != commitReport, "Invalid commit report: null");

    gen.writeStringField(TABLE_NAME, commitReport.tableName());
    gen.writeNumberField(SNAPSHOT_ID, commitReport.snapshotId());
    gen.writeNumberField(SEQUENCE_NUMBER, commitReport.sequenceNumber());
    gen.writeStringField(OPERATION, commitReport.operation());

    gen.writeFieldName(METRICS);
    CommitMetricsResultParser.toJson(commitReport.commitMetrics(), gen);

    if (!commitReport.metadata().isEmpty()) {
      JsonUtil.writeStringMap(METADATA, commitReport.metadata(), gen);
    }
  }

  public static CommitReport fromJson(String json) {
    return JsonUtil.parse(json, CommitReportParser::fromJson);
  }

  public static CommitReport fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse commit report from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse commit report from non-object: %s", json);

    ImmutableCommitReport.Builder builder =
        ImmutableCommitReport.builder()
            .tableName(JsonUtil.getString(TABLE_NAME, json))
            .snapshotId(JsonUtil.getLong(SNAPSHOT_ID, json))
            .sequenceNumber(JsonUtil.getLong(SEQUENCE_NUMBER, json))
            .operation(JsonUtil.getString(OPERATION, json))
            .commitMetrics(CommitMetricsResultParser.fromJson(JsonUtil.get(METRICS, json)));

    if (json.has(METADATA)) {
      builder.metadata(JsonUtil.getStringMap(METADATA, json));
    }

    return builder.build();
  }
}
