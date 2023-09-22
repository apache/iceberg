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
package org.apache.iceberg;

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.NullOrder.NULLS_LAST;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class SortOrderParser {
  private static final String ORDER_ID = "order-id";
  private static final String FIELDS = "fields";
  private static final String DIRECTION = "direction";
  private static final String NULL_ORDER = "null-order";
  private static final String TRANSFORM = "transform";
  private static final String SOURCE_ID = "source-id";
  private static final String SOURCE_IDS = "source-ids";

  private SortOrderParser() {}

  public static void toJson(SortOrder sortOrder, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(ORDER_ID, sortOrder.orderId());
    generator.writeFieldName(FIELDS);
    toJsonFields(sortOrder, generator);
    generator.writeEndObject();
  }

  public static String toJson(SortOrder sortOrder) {
    return toJson(sortOrder, false);
  }

  public static String toJson(SortOrder sortOrder, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(sortOrder, gen), pretty);
  }

  private static String toJson(SortDirection direction) {
    return direction.toString().toLowerCase(Locale.ENGLISH);
  }

  private static String toJson(NullOrder nullOrder) {
    return nullOrder == NULLS_FIRST ? "nulls-first" : "nulls-last";
  }

  private static void toJsonFields(SortOrder sortOrder, JsonGenerator generator)
      throws IOException {
    generator.writeStartArray();
    for (SortField field : sortOrder.fields()) {
      generator.writeStartObject();
      generator.writeStringField(TRANSFORM, field.transform().toString());
      generator.writeNumberField(SOURCE_ID, field.sourceId());
      if (field.sourceIds().length > 1) {
        // fieldName: SOURCE_IDS, array: sourceIds.
        generator.writeFieldName(SOURCE_IDS);
        generator.writeStartArray();
        for (int i : field.sourceIds()) {
          generator.writeNumber(i);
        }
        generator.writeEndArray();
      }
      generator.writeStringField(DIRECTION, toJson(field.direction()));
      generator.writeStringField(NULL_ORDER, toJson(field.nullOrder()));
      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  public static void toJson(UnboundSortOrder sortOrder, JsonGenerator generator)
      throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(ORDER_ID, sortOrder.orderId());
    generator.writeFieldName(FIELDS);
    toJsonFields(sortOrder, generator);
    generator.writeEndObject();
  }

  public static String toJson(UnboundSortOrder sortOrder) {
    return toJson(sortOrder, false);
  }

  public static String toJson(UnboundSortOrder sortOrder, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(sortOrder, gen), pretty);
  }

  private static void toJsonFields(UnboundSortOrder sortOrder, JsonGenerator generator)
      throws IOException {
    generator.writeStartArray();
    for (UnboundSortOrder.UnboundSortField field : sortOrder.fields()) {
      generator.writeStartObject();
      generator.writeStringField(TRANSFORM, field.transformAsString());
      generator.writeNumberField(SOURCE_ID, field.sourceId());
      generator.writeStringField(DIRECTION, toJson(field.direction()));
      generator.writeStringField(NULL_ORDER, toJson(field.nullOrder()));
      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  public static SortOrder fromJson(Schema schema, String json) {
    return fromJson(json).bind(schema);
  }

  public static SortOrder fromJson(Schema schema, JsonNode json, int defaultSortOrderId) {
    UnboundSortOrder unboundSortOrder = fromJson(json);

    if (unboundSortOrder.orderId() == defaultSortOrderId) {
      return unboundSortOrder.bind(schema);
    } else {
      return unboundSortOrder.bindUnchecked(schema);
    }
  }

  public static SortOrder fromJson(Schema schema, JsonNode json) {
    return fromJson(json).bind(schema);
  }

  public static UnboundSortOrder fromJson(String json) {
    return JsonUtil.parse(json, SortOrderParser::fromJson);
  }

  public static UnboundSortOrder fromJson(JsonNode json) {
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse sort order from non-object: %s", json);
    int orderId = JsonUtil.getInt(ORDER_ID, json);
    UnboundSortOrder.Builder builder = UnboundSortOrder.builder().withOrderId(orderId);
    buildFromJsonFields(builder, JsonUtil.get(FIELDS, json));
    return builder.build();
  }

  private static void buildFromJsonFields(UnboundSortOrder.Builder builder, JsonNode json) {
    Preconditions.checkArgument(json != null, "Cannot parse null sort order fields");
    Preconditions.checkArgument(
        json.isArray(), "Cannot parse sort order fields, not an array: %s", json);

    Iterator<JsonNode> elements = json.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Preconditions.checkArgument(
          element.isObject(), "Cannot parse sort field, not an object: %s", element);

      String transform = JsonUtil.getString(TRANSFORM, element);
      int sourceId = JsonUtil.getInt(SOURCE_ID, element);
      int[] sourceIds = JsonUtil.getIntArrayOrNull(SOURCE_IDS, element);
      sourceIds = sourceIds == null ? new int[] {sourceId} : sourceIds;

      String directionAsString = JsonUtil.getString(DIRECTION, element);
      SortDirection direction = SortDirection.fromString(directionAsString);

      String nullOrderingAsString = JsonUtil.getString(NULL_ORDER, element);
      NullOrder nullOrder = toNullOrder(nullOrderingAsString);

      builder.addSortField(transform, sourceId, sourceIds, direction, nullOrder);
    }
  }

  private static NullOrder toNullOrder(String nullOrderingAsString) {
    switch (nullOrderingAsString.toLowerCase(Locale.ROOT)) {
      case "nulls-first":
        return NULLS_FIRST;
      case "nulls-last":
        return NULLS_LAST;
      default:
        throw new IllegalArgumentException("Unexpected null order: " + nullOrderingAsString);
    }
  }
}
