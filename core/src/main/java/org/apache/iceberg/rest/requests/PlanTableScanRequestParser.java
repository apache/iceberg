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
package org.apache.iceberg.rest.requests;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class PlanTableScanRequestParser {
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String SELECT = "select";
  private static final String FILTER = "filter";
  private static final String CASE_SENSITIVE = "case-sensitive";
  private static final String USE_SNAPSHOT_SCHEMA = "use-snapshot-schema";
  private static final String START_SNAPSHOT_ID = "start-snapshot-id";
  private static final String END_SNAPSHOT_ID = "end-snapshot-id";
  private static final String STATS_FIELDS = "stats-fields";

  private PlanTableScanRequestParser() {}

  public static String toJson(PlanTableScanRequest request) {
    return toJson(request, false);
  }

  public static String toJson(PlanTableScanRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static void toJson(PlanTableScanRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid request: planTableScanRequest null");

    if (request.snapshotId() != null
        || request.startSnapshotId() != null
        || request.endSnapshotId() != null) {
      Preconditions.checkArgument(
          request.snapshotId() != null
              ^ (request.startSnapshotId() != null && request.endSnapshotId() != null),
          "Either snapshotId must be provided or both startSnapshotId and endSnapshotId must be provided");
    }

    gen.writeStartObject();
    if (request.snapshotId() != null) {
      gen.writeNumberField(SNAPSHOT_ID, request.snapshotId());
    }

    if (request.startSnapshotId() != null) {
      gen.writeNumberField(START_SNAPSHOT_ID, request.startSnapshotId());
    }

    if (request.endSnapshotId() != null) {
      gen.writeNumberField(END_SNAPSHOT_ID, request.endSnapshotId());
    }

    if (request.select() != null && !request.select().isEmpty()) {
      JsonUtil.writeStringArray(SELECT, request.select(), gen);
    }

    if (request.filter() != null) {
      gen.writeStringField(FILTER, ExpressionParser.toJson(request.filter()));
    }

    gen.writeBooleanField(CASE_SENSITIVE, request.caseSensitive());
    gen.writeBooleanField(USE_SNAPSHOT_SCHEMA, request.useSnapshotSchema());

    if (request.statsFields() != null && !request.statsFields().isEmpty()) {
      JsonUtil.writeStringArray(STATS_FIELDS, request.statsFields(), gen);
    }

    gen.writeEndObject();
  }

  public static PlanTableScanRequest fromJson(String json) {
    return JsonUtil.parse(json, PlanTableScanRequestParser::fromJson);
  }

  public static PlanTableScanRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Invalid request: planTableScanRequest null");

    Long snapshotId = JsonUtil.getLongOrNull(SNAPSHOT_ID, json);
    List<String> select = JsonUtil.getStringListOrNull(SELECT, json);

    Expression filter = null;
    if (json.has(FILTER)) {
      // TODO without text value it adds another " "
      filter = ExpressionParser.fromJson(json.get(FILTER).textValue());
    }

    Boolean caseSensitive = true;
    if (json.has(CASE_SENSITIVE)) {
      caseSensitive = JsonUtil.getBool(CASE_SENSITIVE, json);
    }

    Boolean useSnapshotSchema = false;
    if (json.has(USE_SNAPSHOT_SCHEMA)) {
      useSnapshotSchema = JsonUtil.getBool(USE_SNAPSHOT_SCHEMA, json);
    }

    Long startSnapshotId = JsonUtil.getLongOrNull(START_SNAPSHOT_ID, json);
    Long endSnapshotId = JsonUtil.getLongOrNull(END_SNAPSHOT_ID, json);
    List<String> statsFields = JsonUtil.getStringListOrNull(STATS_FIELDS, json);

    return new PlanTableScanRequest.Builder()
        .withSnapshotId(snapshotId)
        .withSelect(select)
        .withFilter(filter)
        .withCaseSensitive(caseSensitive)
        .withUseSnapshotSchema(useSnapshotSchema)
        .withStartSnapshotId(startSnapshotId)
        .withEndSnapshotId(endSnapshotId)
        .withStatsFields(statsFields)
        .build();
  }
}
