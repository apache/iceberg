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
import org.apache.iceberg.index.IndexType;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class CreateIndexRequestParser {

  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String INDEX_COLUMN_IDS = "index-column-ids";
  private static final String OPTIMIZED_COLUMN_IDS = "optimized-column-ids";
  private static final String LOCATION = "location";
  private static final String PROPERTIES = "properties";

  private CreateIndexRequestParser() {}

  public static String toJson(CreateIndexRequest request) {
    return toJson(request, false);
  }

  public static String toJson(CreateIndexRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(CreateIndexRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid create index request: null");

    gen.writeStartObject();

    gen.writeStringField(NAME, request.name());
    gen.writeStringField(TYPE, request.type().typeName());
    JsonUtil.writeIntegerArray(INDEX_COLUMN_IDS, request.indexColumnIds(), gen);

    if (!request.optimizedColumnIds().isEmpty()) {
      JsonUtil.writeIntegerArray(OPTIMIZED_COLUMN_IDS, request.optimizedColumnIds(), gen);
    }

    if (request.location() != null) {
      gen.writeStringField(LOCATION, request.location());
    }

    if (!request.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, request.properties(), gen);
    }

    gen.writeEndObject();
  }

  public static CreateIndexRequest fromJson(String json) {
    return JsonUtil.parse(json, CreateIndexRequestParser::fromJson);
  }

  public static CreateIndexRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse create index request from null object");

    String name = JsonUtil.getString(NAME, json);
    IndexType type = IndexType.fromString(JsonUtil.getString(TYPE, json));

    CreateIndexRequest.Builder builder =
        CreateIndexRequest.builder()
            .withName(name)
            .withType(type)
            .withIndexColumnIds(JsonUtil.getIntegerList(INDEX_COLUMN_IDS, json));

    if (json.hasNonNull(OPTIMIZED_COLUMN_IDS)) {
      builder.withOptimizedColumnIds(JsonUtil.getIntegerList(OPTIMIZED_COLUMN_IDS, json));
    }

    if (json.hasNonNull(LOCATION)) {
      builder.withLocation(JsonUtil.getString(LOCATION, json));
    }

    if (json.hasNonNull(PROPERTIES)) {
      builder.setProperties(JsonUtil.getStringMap(PROPERTIES, json));
    }

    return builder.build();
  }
}
