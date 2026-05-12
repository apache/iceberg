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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

public class QueryEventsRequestParser {

  private static final String CONTINUATION_TOKEN = "continuation-token";
  private static final String PAGE_SIZE = "page-size";
  private static final String AFTER_TIMESTAMP_MS = "after-timestamp-ms";
  private static final String OPERATION_TYPES = "operation-types";
  private static final String CATALOG_OBJECTS_BY_NAME = "catalog-objects-by-name";
  private static final String CATALOG_OBJECTS_BY_ID = "catalog-objects-by-id";
  private static final String OBJECT_TYPES = "object-types";
  private static final String CUSTOM_FILTERS = "custom-filters";
  private static final String UUID_FIELD = "uuid";
  private static final String TYPE_FIELD = "type";

  private QueryEventsRequestParser() {}

  public static String toJson(QueryEventsRequest request) {
    return toJson(request, false);
  }

  public static String toJson(QueryEventsRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(QueryEventsRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != request, "Invalid query events request: null");

    gen.writeStartObject();

    if (request.continuationToken() != null) {
      gen.writeStringField(CONTINUATION_TOKEN, request.continuationToken());
    }

    if (request.pageSize() != null) {
      gen.writeNumberField(PAGE_SIZE, request.pageSize());
    }

    if (request.afterTimestampMs() != null) {
      gen.writeNumberField(AFTER_TIMESTAMP_MS, request.afterTimestampMs());
    }

    if (request.operationTypes() != null) {
      JsonUtil.writeStringArray(OPERATION_TYPES, request.operationTypes(), gen);
    }

    if (request.catalogObjectsByName() != null) {
      gen.writeArrayFieldStart(CATALOG_OBJECTS_BY_NAME);
      for (List<String> name : request.catalogObjectsByName()) {
        gen.writeStartArray();
        for (String part : name) {
          gen.writeString(part);
        }
        gen.writeEndArray();
      }
      gen.writeEndArray();
    }

    if (request.catalogObjectsById() != null) {
      gen.writeArrayFieldStart(CATALOG_OBJECTS_BY_ID);
      for (QueryEventsRequest.CatalogObjectUuid obj : request.catalogObjectsById()) {
        gen.writeStartObject();
        gen.writeStringField(UUID_FIELD, obj.uuid());
        gen.writeStringField(TYPE_FIELD, obj.type());
        gen.writeEndObject();
      }
      gen.writeEndArray();
    }

    if (request.objectTypes() != null) {
      JsonUtil.writeStringArray(OBJECT_TYPES, request.objectTypes(), gen);
    }

    if (request.customFilters() != null) {
      gen.writeObjectFieldStart(CUSTOM_FILTERS);
      for (Map.Entry<String, Object> entry : request.customFilters().entrySet()) {
        gen.writeFieldName(entry.getKey());
        gen.writeObject(entry.getValue());
      }
      gen.writeEndObject();
    }

    gen.writeEndObject();
  }

  public static QueryEventsRequest fromJson(String json) {
    return JsonUtil.parse(json, QueryEventsRequestParser::fromJson);
  }

  public static QueryEventsRequest fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse query events request from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse query events request from non-object: %s", json);

    ImmutableQueryEventsRequest.Builder builder = ImmutableQueryEventsRequest.builder();

    if (json.has(CONTINUATION_TOKEN)) {
      builder.continuationToken(JsonUtil.getString(CONTINUATION_TOKEN, json));
    }

    if (json.has(PAGE_SIZE)) {
      builder.pageSize(JsonUtil.getInt(PAGE_SIZE, json));
    }

    if (json.has(AFTER_TIMESTAMP_MS)) {
      builder.afterTimestampMs(JsonUtil.getLong(AFTER_TIMESTAMP_MS, json));
    }

    if (json.has(OPERATION_TYPES)) {
      builder.operationTypes(ImmutableList.copyOf(JsonUtil.getStringList(OPERATION_TYPES, json)));
    }

    if (json.has(CATALOG_OBJECTS_BY_NAME)) {
      ImmutableList.Builder<List<String>> names = ImmutableList.builder();
      for (JsonNode nameNode : json.get(CATALOG_OBJECTS_BY_NAME)) {
        names.add(ImmutableList.copyOf(JsonUtil.getStringArray(nameNode)));
      }
      builder.catalogObjectsByName(names.build());
    }

    if (json.has(CATALOG_OBJECTS_BY_ID)) {
      ImmutableList.Builder<QueryEventsRequest.CatalogObjectUuid> objects = ImmutableList.builder();
      for (JsonNode objNode : json.get(CATALOG_OBJECTS_BY_ID)) {
        objects.add(
            ImmutableCatalogObjectUuid.builder()
                .uuid(JsonUtil.getString(UUID_FIELD, objNode))
                .type(JsonUtil.getString(TYPE_FIELD, objNode))
                .build());
      }
      builder.catalogObjectsById(objects.build());
    }

    if (json.has(OBJECT_TYPES)) {
      builder.objectTypes(ImmutableList.copyOf(JsonUtil.getStringList(OBJECT_TYPES, json)));
    }

    if (json.has(CUSTOM_FILTERS)) {
      ImmutableMap.Builder<String, Object> filters = ImmutableMap.builder();
      json.get(CUSTOM_FILTERS)
          .properties()
          .forEach(
              entry -> {
                JsonNode value = entry.getValue();
                if (value.isTextual()) {
                  filters.put(entry.getKey(), value.asText());
                } else if (value.isNumber()) {
                  filters.put(entry.getKey(), value.numberValue());
                } else if (value.isBoolean()) {
                  filters.put(entry.getKey(), value.asBoolean());
                } else {
                  filters.put(entry.getKey(), value.toString());
                }
              });
      builder.customFilters(filters.build());
    }

    return builder.build();
  }
}
