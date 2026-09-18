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
import org.apache.iceberg.catalog.CatalogObjectIdentifier;
import org.apache.iceberg.catalog.CatalogObjectIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.events.CatalogObjectType;
import org.apache.iceberg.util.JsonUtil;

public class QueryEventsRequestParser {
  static final String CONTINUATION_TOKEN = "continuation-token";
  static final String PAGE_SIZE = "page-size";
  static final String SINCE_TIMESTAMP_MS = "since-timestamp-ms";
  static final String OPERATION_TYPES = "operation-types";
  static final String CATALOG_OBJECTS_BY_NAME = "catalog-objects-by-name";
  static final String CATALOG_OBJECTS_BY_UUID = "catalog-objects-by-uuid";
  static final String OBJECT_TYPES = "object-types";
  static final String CUSTOM_FILTERS = "custom-filters";

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
    JsonUtil.writeStringFieldIfPresent(CONTINUATION_TOKEN, request.continuationToken(), gen);
    JsonUtil.writeIntegerFieldIfPresent(PAGE_SIZE, request.pageSize(), gen);
    JsonUtil.writeLongFieldIfPresent(SINCE_TIMESTAMP_MS, request.sinceTimestampMs(), gen);

    if (request.operationTypes() != null) {
      JsonUtil.writeStringArray(OPERATION_TYPES, request.operationTypes(), gen);
    }

    if (request.catalogObjectsByName() != null) {
      gen.writeArrayFieldStart(CATALOG_OBJECTS_BY_NAME);
      for (CatalogObjectIdentifier identifier : request.catalogObjectsByName()) {
        CatalogObjectIdentifierParser.toJson(identifier, gen);
      }

      gen.writeEndArray();
    }

    if (request.catalogObjectsByUuid() != null) {
      JsonUtil.writeStringArray(CATALOG_OBJECTS_BY_UUID, request.catalogObjectsByUuid(), gen);
    }

    if (request.objectTypes() != null) {
      List<String> objectTypes = Lists.newArrayList();
      for (CatalogObjectType type : request.objectTypes()) {
        objectTypes.add(type.toString());
      }

      JsonUtil.writeStringArray(OBJECT_TYPES, objectTypes, gen);
    }

    if (request.customFilters() != null) {
      JsonUtil.writeStringMap(CUSTOM_FILTERS, request.customFilters(), gen);
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
    builder.continuationToken(JsonUtil.getStringOrNull(CONTINUATION_TOKEN, json));
    builder.pageSize(JsonUtil.getIntOrNull(PAGE_SIZE, json));
    builder.sinceTimestampMs(JsonUtil.getLongOrNull(SINCE_TIMESTAMP_MS, json));

    List<String> operationTypes = JsonUtil.getStringListOrNull(OPERATION_TYPES, json);
    if (operationTypes != null) {
      builder.operationTypes(operationTypes);
    }

    if (json.hasNonNull(CATALOG_OBJECTS_BY_NAME)) {
      JsonNode identifiers = JsonUtil.get(CATALOG_OBJECTS_BY_NAME, json);
      Preconditions.checkArgument(
          identifiers.isArray(),
          "Cannot parse catalog-objects-by-name from non-array: %s",
          identifiers);
      List<CatalogObjectIdentifier> names = Lists.newArrayList();
      for (JsonNode identifier : identifiers) {
        names.add(CatalogObjectIdentifierParser.fromJson(identifier));
      }

      builder.catalogObjectsByName(names);
    }

    List<String> catalogObjectsByUuid = JsonUtil.getStringListOrNull(CATALOG_OBJECTS_BY_UUID, json);
    if (catalogObjectsByUuid != null) {
      builder.catalogObjectsByUuid(catalogObjectsByUuid);
    }

    List<String> objectTypeNames = JsonUtil.getStringListOrNull(OBJECT_TYPES, json);
    if (objectTypeNames != null) {
      List<CatalogObjectType> objectTypes = Lists.newArrayList();
      for (String type : objectTypeNames) {
        objectTypes.add(CatalogObjectType.fromName(type));
      }

      builder.objectTypes(objectTypes);
    }

    if (json.hasNonNull(CUSTOM_FILTERS)) {
      builder.customFilters(JsonUtil.getStringMap(CUSTOM_FILTERS, json));
    }

    return builder.build();
  }
}
