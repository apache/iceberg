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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import org.apache.iceberg.SortField.Direction;
import org.apache.iceberg.SortField.NullOrder;
import org.apache.iceberg.SortTransforms.SortTransform;
import org.apache.iceberg.util.JsonUtil;

public class SortOrderParser {

  private static final String ORDER_ID = "order-id";
  private static final String FIELDS = "fields";
  private static final String NAME = "name";
  private static final String DIRECTION = "direction";
  private static final String NULL_ORDERING = "null-order";
  private static final String TRANSFORM = "transform";
  private static final String SOURCE_IDS = "source-ids";

  private SortOrderParser() {}

  public static void toJson(SortOrder sortOrder, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(ORDER_ID, sortOrder.orderId());
    generator.writeFieldName(FIELDS);
    toJsonFields(sortOrder, generator);
    generator.writeEndObject();
  }

  private static void toJsonFields(SortOrder sortOrder, JsonGenerator generator) throws IOException {
    generator.writeStartArray();
    for (SortField field : sortOrder.fields()) {
      generator.writeStartObject();
      generator.writeStringField(NAME, field.name());
      generator.writeStringField(DIRECTION, field.direction().toString().toLowerCase(Locale.ROOT));
      generator.writeStringField(NULL_ORDERING, field.nullOrder().toString().toLowerCase(Locale.ROOT));
      generator.writeStringField(TRANSFORM, field.transform().toString());
      generator.writeFieldName(SOURCE_IDS);
      generator.writeStartArray();
      for (int sourceId : field.sourceIds()) {
        generator.writeNumber(sourceId);
      }
      generator.writeEndArray();
      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  public static SortOrder fromJson(Schema schema, JsonNode json) {
    Preconditions.checkArgument(json.isObject(), "Cannot parse sort order from non-object: %s", json);
    int orderId = JsonUtil.getInt(ORDER_ID, json);
    SortOrder.Builder builder = SortOrder.builderFor(schema).withOrderId(orderId);
    buildFromJsonFields(builder, json.get(FIELDS));
    return builder.build();
  }

  private static void buildFromJsonFields(SortOrder.Builder builder, JsonNode json) {
    Preconditions.checkArgument(json.isArray(), "Cannot parse partition order fields, not an array: %s", json);
    Iterator<JsonNode> elements = json.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Preconditions.checkArgument(element.isObject(), "Cannot parse sort field, not an object: %s", element);

      String name = JsonUtil.getString(NAME, element);

      String directionAsString = JsonUtil.getString(DIRECTION, element);
      Direction direction = Direction.valueOf(directionAsString.toUpperCase(Locale.ROOT));

      String nullOrderingAsString = JsonUtil.getString(NULL_ORDERING, element);
      NullOrder nullOrder = NullOrder.valueOf(nullOrderingAsString.toUpperCase(Locale.ROOT));

      String transformAsString = JsonUtil.getString(TRANSFORM, element);
      SortTransform transform = SortTransforms.fromString(transformAsString);

      JsonNode sourceIdNode = element.get(SOURCE_IDS);
      int size = sourceIdNode.size();
      int[] sourceIds = new int[size];
      Iterator<JsonNode> sourceIdElements = sourceIdNode.elements();
      for (int i = 0; i < size; i++) {
        sourceIds[i] = sourceIdElements.next().asInt();
      }

      builder.add(name, direction, nullOrder, transform, sourceIds);
    }
  }
}
