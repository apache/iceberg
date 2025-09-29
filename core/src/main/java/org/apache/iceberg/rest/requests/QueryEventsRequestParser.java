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
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.CatalogObject;
import org.apache.iceberg.catalog.CatalogObjectType;
import org.apache.iceberg.catalog.CatalogObjectUuid;
import org.apache.iceberg.catalog.CatalogObjectUuidParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.events.operations.OperationType;
import org.apache.iceberg.util.JsonUtil;

public class QueryEventsRequestParser {
  private static final String PAGE_TOKEN = "page-token";
  private static final String PAGE_SIZE = "page-size";
  private static final String AFTER_TIMESTAMP_MS = "after-timestamp-ms";
  private static final String OPERATION_TYPES = "operation-types";
  private static final String CATALOG_OBJECTS_BY_NAME = "catalog-objects-by-name";
  private static final String CATALOG_OBJECTS_BY_ID = "catalog-objects-by-id";
  private static final String OBJECT_TYPES = "object-types";
  private static final String CUSTOM_FILTERS = "custom-filters";

  private QueryEventsRequestParser() {}

  public static String toJson(QueryEventsRequest request) {
    return toJson(request, false);
  }

  public static String toJsonPretty(QueryEventsRequest request) {
    return toJson(request, true);
  }

  private static String toJson(QueryEventsRequest request, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(request, gen), pretty);
  }

  public static void toJson(QueryEventsRequest request, JsonGenerator gen) throws IOException {
    Preconditions.checkNotNull(request, "Invalid query events request: null");

    gen.writeStartObject();

    if (request.pageToken() != null) gen.writeStringField(PAGE_TOKEN, request.pageToken());

    if (request.pageSize() != null)
      gen.writeNumberField(PAGE_SIZE, request.pageSize());

    if(request.afterTimestampMs() != null)
      gen.writeNumberField(AFTER_TIMESTAMP_MS, request.afterTimestampMs());

    if(!request.operationTypes().isEmpty()){
      gen.writeArrayFieldStart(OPERATION_TYPES);

      for (OperationType operationType : request.operationTypes()){
        gen.writeString(operationType.type());
      }

      gen.writeEndArray();
    }

    if(!request.catalogObjectsByName().isEmpty()){
      gen.writeArrayFieldStart(CATALOG_OBJECTS_BY_NAME);

      for (CatalogObject catalogObject : request.catalogObjectsByName()){
        gen.writeString(catalogObject.toString());
      }

      gen.writeEndArray();
    }

    if(!request.catalogObjectsById().isEmpty()){
      gen.writeArrayFieldStart(CATALOG_OBJECTS_BY_ID);

      for (CatalogObjectUuid catalogObjectUuid : request.catalogObjectsById()){
        CatalogObjectUuidParser.toJson(catalogObjectUuid, gen);
      }

      gen.writeEndArray();
    }

    if(!request.objectTypes().isEmpty()){
      gen.writeArrayFieldStart(OBJECT_TYPES);

      for (CatalogObjectType objectType : request.objectTypes()){
        gen.writeString(objectType.type());
      }

      gen.writeEndArray();
    }

    if(!request.customFilters().isEmpty()){
      JsonUtil.writeStringMap(CUSTOM_FILTERS, request.customFilters(), gen);
    }

    gen.writeEndObject();
  }

  public static QueryEventsRequest fromJson(String json){
    return JsonUtil.parse(json, QueryEventsRequestParser::fromJson);
  }

  public static QueryEventsRequest fromJson(JsonNode json){
    Preconditions.checkNotNull(json, "Cannot parse query events request from null object");

    ImmutableQueryEventsRequest.Builder builder = ImmutableQueryEventsRequest.builder();

    if(json.has(PAGE_TOKEN))
      builder.pageToken(JsonUtil.getString(PAGE_TOKEN, json));

    if(json.has(PAGE_SIZE))
      builder.pageSize(JsonUtil.getInt(PAGE_SIZE, json));

    if(json.has(AFTER_TIMESTAMP_MS))
      builder.afterTimestampMs(JsonUtil.getLong(AFTER_TIMESTAMP_MS, json));

    if(json.has(OPERATION_TYPES)) {
      builder.operationTypes(
          JsonUtil.getStringList(OPERATION_TYPES, json)
              .stream()
              .map(OperationType::fromType)
              .collect(Collectors.toList()));
    }

    if(json.has(CATALOG_OBJECTS_BY_NAME)){
      builder.catalogObjectsByName(JsonUtil.getStringList(CATALOG_OBJECTS_BY_NAME, json)
          .stream()
          .map(CatalogObject::of)
          .collect(Collectors.toList()));
    }

    if(json.has(CATALOG_OBJECTS_BY_ID)) {
      builder.catalogObjectsById(
          JsonUtil.getObjectList(CATALOG_OBJECTS_BY_ID, json, CatalogObjectUuidParser::fromJson));
    }

    if(json.has(OBJECT_TYPES)) {
      builder.objectTypes(JsonUtil.getStringList(OBJECT_TYPES, json)
          .stream()
          .map(CatalogObjectType::fromType)
          .collect(Collectors.toList()));
    }

    if(json.has(CUSTOM_FILTERS)){
      builder.customFilters(JsonUtil.getStringMap(CUSTOM_FILTERS, json));
    }

    return builder.build();
  }
}
