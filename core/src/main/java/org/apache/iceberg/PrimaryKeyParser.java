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
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class PrimaryKeyParser {
  private static final String PRIMARY_KEY_ID = "key-id";
  private static final String ENFORCE_UNIQUENESS = "enforce-uniqueness";
  private static final String FIELDS = "fields";
  private static final String SOURCE_ID = "source-id";

  private PrimaryKeyParser() {
  }

  public static void toJson(PrimaryKey primaryKey, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(PRIMARY_KEY_ID, primaryKey.keyId());
    generator.writeBooleanField(ENFORCE_UNIQUENESS, primaryKey.enforceUniqueness());
    generator.writeFieldName(FIELDS);
    toJsonFields(primaryKey, generator);
    generator.writeEndObject();
  }

  public static String toJson(PrimaryKey primaryKey) {
    return toJson(primaryKey, false);
  }

  public static String toJson(PrimaryKey primaryKey, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(primaryKey, generator);
      generator.flush();
      return writer.toString();

    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static void toJsonFields(PrimaryKey primaryKey, JsonGenerator generator) throws IOException {
    generator.writeStartArray();
    for (Integer sourceId : primaryKey.sourceIds()) {
      generator.writeStartObject();
      generator.writeNumberField(SOURCE_ID, sourceId);
      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  public static PrimaryKey fromJson(Schema schema, String json) {
    try {
      return fromJson(schema, JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static PrimaryKey fromJson(Schema schema, JsonNode json) {
    Preconditions.checkArgument(json.isObject(), "Cannot parse primary key from non-object: %s", json);
    int primaryKeyId = JsonUtil.getInt(PRIMARY_KEY_ID, json);
    boolean enforceUniqueness = JsonUtil.getBool(ENFORCE_UNIQUENESS, json);
    PrimaryKey.Builder builder = PrimaryKey.builderFor(schema)
        .withKeyId(primaryKeyId)
        .withEnforceUniqueness(enforceUniqueness);
    buildFromJsonFields(builder, json.get(FIELDS));
    return builder.build();
  }

  private static void buildFromJsonFields(PrimaryKey.Builder builder, JsonNode json) {
    Preconditions.checkArgument(json != null, "Cannot parse null primary key fields.");
    Preconditions.checkArgument(json.isArray(), "Cannot parse primary key fields, not an array: %s", json);

    Iterator<JsonNode> elements = json.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Preconditions.checkArgument(element.isObject(), "Cannot parse primary key field, not an object: %s", element);

      int fieldId = JsonUtil.getInt(SOURCE_ID, element);

      builder.addField(fieldId);
    }
  }
}
