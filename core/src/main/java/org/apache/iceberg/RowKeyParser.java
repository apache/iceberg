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
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Iterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class RowKeyParser {
  private static final String FIELDS = "identifier-fields";
  private static final String SOURCE_ID = "source-id";

  private RowKeyParser() {
  }

  public static void toJson(RowKey rowKey, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeFieldName(FIELDS);
    toJsonFields(rowKey, generator);
    generator.writeEndObject();
  }

  public static String toJson(RowKey rowKey) {
    return toJson(rowKey, false);
  }

  public static String toJson(RowKey rowKey, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(rowKey, generator);
      generator.flush();
      return writer.toString();

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void toJsonFields(RowKey rowKey, JsonGenerator generator) throws IOException {
    generator.writeStartArray();
    for (RowKeyIdentifierField field : rowKey.identifierFields()) {
      generator.writeStartObject();
      generator.writeNumberField(SOURCE_ID, field.sourceId());
      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  public static RowKey fromJson(Schema schema, String json) {
    try {
      return fromJson(schema, JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static RowKey fromJson(Schema schema, JsonNode json) {
    Preconditions.checkArgument(json.isObject(), "Cannot parse row key from non-object: %s", json);
    RowKey.Builder builder = RowKey.builderFor(schema);
    buildFromJsonFields(builder, json.get(FIELDS));
    return builder.build();
  }

  private static void buildFromJsonFields(RowKey.Builder builder, JsonNode json) {
    Preconditions.checkArgument(json != null, "Cannot parse null row key fields");
    Preconditions.checkArgument(json.isArray(), "Cannot parse row key fields, not an array: %s", json);

    Iterator<JsonNode> elements = json.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Preconditions.checkArgument(element.isObject(),
          "Cannot parse row key field, not an object: %s", element);
      builder.addField(JsonUtil.getInt(SOURCE_ID, element));
    }
  }
}
