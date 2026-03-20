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
package org.apache.iceberg.rest.responses;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.requests.BatchLoadRequestedTable;
import org.apache.iceberg.rest.requests.BatchLoadRequestedTableParser;
import org.apache.iceberg.util.JsonUtil;

public class BatchLoadTablesResponseParser {

  private static final String RESULTS = "results";
  private static final String UNPROCESSED_TABLES = "unprocessed-tables";

  private BatchLoadTablesResponseParser() {}

  public static String toJson(BatchLoadTablesResponse response) {
    return toJson(response, false);
  }

  public static String toJson(BatchLoadTablesResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(BatchLoadTablesResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != response, "Invalid batch load tables response: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(RESULTS);
    for (BatchLoadTablesResultItem item : response.results()) {
      BatchLoadTablesResultItemParser.toJson(item, gen);
    }

    gen.writeEndArray();

    List<BatchLoadRequestedTable> unprocessed = response.unprocessedTables();
    if (null != unprocessed && !unprocessed.isEmpty()) {
      gen.writeArrayFieldStart(UNPROCESSED_TABLES);
      for (BatchLoadRequestedTable table : unprocessed) {
        BatchLoadRequestedTableParser.toJson(table, gen);
      }

      gen.writeEndArray();
    }

    gen.writeEndObject();
  }

  public static BatchLoadTablesResponse fromJson(String json) {
    return JsonUtil.parse(json, BatchLoadTablesResponseParser::fromJson);
  }

  public static BatchLoadTablesResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch load tables response from null object");
    Preconditions.checkArgument(
        json.hasNonNull(RESULTS), "Cannot parse missing field: %s", RESULTS);

    ImmutableList.Builder<BatchLoadTablesResultItem> results = ImmutableList.builder();
    JsonNode resultsNode = JsonUtil.get(RESULTS, json);
    Preconditions.checkArgument(
        resultsNode.isArray(), "Cannot parse %s from non-array: %s", RESULTS, resultsNode);

    for (JsonNode itemNode : resultsNode) {
      results.add(BatchLoadTablesResultItemParser.fromJson(itemNode));
    }

    ImmutableBatchLoadTablesResponse.Builder builder =
        ImmutableBatchLoadTablesResponse.builder().results(results.build());

    if (json.hasNonNull(UNPROCESSED_TABLES)) {
      JsonNode unprocessedNode = JsonUtil.get(UNPROCESSED_TABLES, json);
      Preconditions.checkArgument(
          unprocessedNode.isArray(),
          "Cannot parse %s from non-array: %s",
          UNPROCESSED_TABLES,
          unprocessedNode);

      ImmutableList.Builder<BatchLoadRequestedTable> unprocessed = ImmutableList.builder();
      for (JsonNode tableNode : unprocessedNode) {
        unprocessed.add(BatchLoadRequestedTableParser.fromJson(tableNode));
      }

      builder.unprocessedTables(unprocessed.build());
    }

    return builder.build();
  }
}
