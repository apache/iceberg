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
import org.apache.iceberg.util.JsonUtil;

public class BatchLoadRelationsRequestParser {

  private static final String IDENTIFIERS = "identifiers";

  private BatchLoadRelationsRequestParser() {}

  public static String toJson(BatchLoadRelationsRequest request) {
    return toJson(request, false);
  }

  public static String toJson(BatchLoadRelationsRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(BatchLoadRelationsRequest request, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != request, "Invalid batch load relations request: null");

    gen.writeStartObject();

    gen.writeArrayFieldStart(IDENTIFIERS);
    for (BatchLoadRelationRequestItem item : request.identifiers()) {
      BatchLoadRelationRequestItemParser.toJson(item, gen);
    }

    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static BatchLoadRelationsRequest fromJson(String json) {
    return JsonUtil.parse(json, BatchLoadRelationsRequestParser::fromJson);
  }

  public static BatchLoadRelationsRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch load relations request from null object");

    BatchLoadRelationsRequest.Builder builder = BatchLoadRelationsRequest.builder();

    JsonNode identifiersNode = JsonUtil.get(IDENTIFIERS, json);
    Preconditions.checkArgument(
        identifiersNode.isArray(), "Cannot parse identifiers from non-array: %s", identifiersNode);

    for (JsonNode itemNode : identifiersNode) {
      builder.addIdentifier(BatchLoadRelationRequestItemParser.fromJson(itemNode));
    }

    return builder.build();
  }
}
