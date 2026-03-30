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
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class BatchLoadRelationsResponseParser {

  private static final String RESULTS = "results";
  private static final String UNPROCESSED_IDENTIFIERS = "unprocessed-identifiers";

  private BatchLoadRelationsResponseParser() {}

  public static String toJson(BatchLoadRelationsResponse response) {
    return toJson(response, false);
  }

  public static String toJson(BatchLoadRelationsResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(BatchLoadRelationsResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != response, "Invalid batch load relations response: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(RESULTS);
    for (BatchLoadRelationResultItem item : response.results()) {
      BatchLoadRelationResultItemParser.toJson(item, gen);
    }

    gen.writeEndArray();

    if (!response.unprocessedIdentifiers().isEmpty()) {
      gen.writeArrayFieldStart(UNPROCESSED_IDENTIFIERS);
      for (org.apache.iceberg.catalog.TableIdentifier ident : response.unprocessedIdentifiers()) {
        TableIdentifierParser.toJson(ident, gen);
      }

      gen.writeEndArray();
    }

    gen.writeEndObject();
  }

  public static BatchLoadRelationsResponse fromJson(String json) {
    return JsonUtil.parse(json, BatchLoadRelationsResponseParser::fromJson);
  }

  public static BatchLoadRelationsResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch load relations response from null object");

    BatchLoadRelationsResponse.Builder builder = BatchLoadRelationsResponse.builder();

    JsonNode resultsNode = JsonUtil.get(RESULTS, json);
    Preconditions.checkArgument(
        resultsNode.isArray(), "Cannot parse results from non-array: %s", resultsNode);

    for (JsonNode itemNode : resultsNode) {
      builder.addResult(BatchLoadRelationResultItemParser.fromJson(itemNode));
    }

    if (json.hasNonNull(UNPROCESSED_IDENTIFIERS)) {
      JsonNode unprocessedNode = json.get(UNPROCESSED_IDENTIFIERS);
      Preconditions.checkArgument(
          unprocessedNode.isArray(),
          "Cannot parse unprocessed-identifiers from non-array: %s",
          unprocessedNode);

      for (JsonNode identNode : unprocessedNode) {
        builder.addUnprocessedIdentifier(TableIdentifierParser.fromJson(identNode));
      }
    }

    return builder.build();
  }
}
