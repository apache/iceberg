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
import org.apache.iceberg.catalog.IndexIdentifier;
import org.apache.iceberg.catalog.IndexIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class ListIndexesResponseParser {

  private static final String IDENTIFIERS = "identifiers";
  private static final String NEXT_PAGE_TOKEN = "next-page-token";

  private ListIndexesResponseParser() {}

  public static String toJson(ListIndexesResponse response) {
    return toJson(response, false);
  }

  public static String toJson(ListIndexesResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(ListIndexesResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid list indexes response: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(IDENTIFIERS);
    for (IndexIdentifier identifier : response.identifiers()) {
      IndexIdentifierParser.toJson(identifier, gen);
    }
    gen.writeEndArray();

    if (response.nextPageToken() != null) {
      gen.writeStringField(NEXT_PAGE_TOKEN, response.nextPageToken());
    } else {
      gen.writeNullField(NEXT_PAGE_TOKEN);
    }

    gen.writeEndObject();
  }

  public static ListIndexesResponse fromJson(String json) {
    return JsonUtil.parse(json, ListIndexesResponseParser::fromJson);
  }

  public static ListIndexesResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse list indexes response from null object");

    ListIndexesResponse.Builder builder = ListIndexesResponse.builder();

    if (json.hasNonNull(IDENTIFIERS)) {
      JsonNode identifiersNode = json.get(IDENTIFIERS);
      Preconditions.checkArgument(
          identifiersNode.isArray(),
          "Cannot parse identifiers from non-array: %s",
          identifiersNode);

      for (JsonNode identifierNode : identifiersNode) {
        builder.add(IndexIdentifierParser.fromJson(identifierNode));
      }
    }

    if (json.hasNonNull(NEXT_PAGE_TOKEN)) {
      builder.nextPageToken(JsonUtil.getString(NEXT_PAGE_TOKEN, json));
    }

    return builder.build();
  }
}
