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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.CatalogObjectType;
import org.apache.iceberg.util.JsonUtil;

public class LoadRelationResponseParser {

  private static final String OBJECT_TYPE = "object-type";
  private static final String TABLE = "table";
  private static final String VIEW = "view";

  private LoadRelationResponseParser() {}

  public static String toJson(LoadRelationResponse response) {
    return toJson(response, false);
  }

  public static String toJson(LoadRelationResponse response, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(response, gen), pretty);
  }

  public static void toJson(LoadRelationResponse response, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != response, "Invalid load relation response: null");

    gen.writeStartObject();

    gen.writeStringField(OBJECT_TYPE, response.objectType().value());

    switch (response.objectType()) {
      case TABLE:
        gen.writeFieldName(TABLE);
        LoadTableResponseParser.toJson(response.tableResponse(), gen);
        break;
      case VIEW:
        gen.writeFieldName(VIEW);
        LoadViewResponseParser.toJson(response.viewResponse(), gen);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported object-type: %s", response.objectType()));
    }

    gen.writeEndObject();
  }

  public static LoadRelationResponse fromJson(String json) {
    return JsonUtil.parse(json, LoadRelationResponseParser::fromJson);
  }

  public static LoadRelationResponse fromJson(JsonNode json) {
    Preconditions.checkArgument(
        null != json, "Cannot parse load relation response from null object");

    CatalogObjectType objectType =
        CatalogObjectType.fromString(JsonUtil.getString(OBJECT_TYPE, json));

    LoadRelationResponse.Builder builder =
        LoadRelationResponse.builder().withObjectType(objectType);

    switch (objectType) {
      case TABLE:
        builder.withTableResponse(LoadTableResponseParser.fromJson(JsonUtil.get(TABLE, json)));
        break;
      case VIEW:
        builder.withViewResponse(LoadViewResponseParser.fromJson(JsonUtil.get(VIEW, json)));
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported object-type: %s", objectType));
    }

    return builder.build();
  }
}
