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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

public class BatchLoadTablesRequestParser {

  private static final String TABLES = "tables";

  private BatchLoadTablesRequestParser() {}

  public static String toJson(BatchLoadTablesRequest request) {
    return toJson(request, false);
  }

  public static String toJson(BatchLoadTablesRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(BatchLoadTablesRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid batch load tables request: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(TABLES);
    for (BatchLoadRequestedTable table : request.tables()) {
      BatchLoadRequestedTableParser.toJson(table, gen);
    }

    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static BatchLoadTablesRequest fromJson(String json) {
    return JsonUtil.parse(json, BatchLoadTablesRequestParser::fromJson);
  }

  public static BatchLoadTablesRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch load tables request from null object");
    Preconditions.checkArgument(json.hasNonNull(TABLES), "Cannot parse missing field: %s", TABLES);

    ImmutableList.Builder<BatchLoadRequestedTable> tables = ImmutableList.builder();
    JsonNode tablesNode = JsonUtil.get(TABLES, json);
    Preconditions.checkArgument(
        tablesNode.isArray(), "Cannot parse %s from non-array: %s", TABLES, tablesNode);

    for (JsonNode tableNode : tablesNode) {
      tables.add(BatchLoadRequestedTableParser.fromJson(tableNode));
    }

    return ImmutableBatchLoadTablesRequest.builder().tables(tables.build()).build();
  }
}
