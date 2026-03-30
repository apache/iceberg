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

public class BatchLoadRelationsForbiddenResponseParser {

  private static final String MESSAGE = "message";
  private static final String TYPE = "type";
  private static final String CODE = "code";
  private static final String FORBIDDEN_IDENTIFIERS = "forbidden-identifiers";

  private BatchLoadRelationsForbiddenResponseParser() {}

  public static String toJson(BatchLoadRelationsForbiddenResponse response) {
    return toJson(response, false);
  }

  public static String toJson(BatchLoadRelationsForbiddenResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(BatchLoadRelationsForbiddenResponse response, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(
        null != response, "Invalid batch load relations forbidden response: null");

    gen.writeStartObject();

    gen.writeStringField(MESSAGE, response.message());
    gen.writeStringField(TYPE, response.type());
    gen.writeNumberField(CODE, response.code());

    gen.writeArrayFieldStart(FORBIDDEN_IDENTIFIERS);
    for (org.apache.iceberg.catalog.TableIdentifier ident : response.forbiddenIdentifiers()) {
      TableIdentifierParser.toJson(ident, gen);
    }

    gen.writeEndArray();

    gen.writeEndObject();
  }

  public static BatchLoadRelationsForbiddenResponse fromJson(String json) {
    return JsonUtil.parse(json, BatchLoadRelationsForbiddenResponseParser::fromJson);
  }

  public static BatchLoadRelationsForbiddenResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse batch load relations forbidden response from null object");

    BatchLoadRelationsForbiddenResponse.Builder builder =
        BatchLoadRelationsForbiddenResponse.builder()
            .withMessage(JsonUtil.getStringOrNull(MESSAGE, json))
            .withType(JsonUtil.getStringOrNull(TYPE, json))
            .withCode(JsonUtil.getInt(CODE, json));

    JsonNode forbiddenNode = JsonUtil.get(FORBIDDEN_IDENTIFIERS, json);
    Preconditions.checkArgument(
        forbiddenNode.isArray(),
        "Cannot parse forbidden-identifiers from non-array: %s",
        forbiddenNode);

    for (JsonNode identNode : forbiddenNode) {
      builder.addForbiddenIdentifier(TableIdentifierParser.fromJson(identNode));
    }

    return builder.build();
  }
}
