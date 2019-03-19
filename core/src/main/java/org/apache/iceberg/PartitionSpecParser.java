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

package com.netflix.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.util.JsonUtil;
import com.netflix.iceberg.util.Pair;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

public class PartitionSpecParser {
  private PartitionSpecParser() {
  }

  private static final String SPEC_ID = "spec-id";
  private static final String FIELDS = "fields";
  private static final String SOURCE_ID = "source-id";
  private static final String TRANSFORM = "transform";
  private static final String NAME = "name";

  public static void toJson(PartitionSpec spec, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeNumberField(SPEC_ID, spec.specId());
    generator.writeFieldName(FIELDS);
    toJsonFields(spec, generator);
    generator.writeEndObject();
  }

  public static String toJson(PartitionSpec spec) {
    return toJson(spec, false);
  }

  public static String toJson(PartitionSpec spec, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(spec, generator);
      generator.flush();
      return writer.toString();

    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public static PartitionSpec fromJson(Schema schema, JsonNode json) {
    Preconditions.checkArgument(json.isObject(), "Cannot parse spec from non-object: %s", json);
    int specId = JsonUtil.getInt(SPEC_ID, json);
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema).withSpecId(specId);
    buildFromJsonFields(builder, json.get(FIELDS));
    return builder.build();
  }

  private static Cache<Pair<Types.StructType, String>, PartitionSpec> SPEC_CACHE = CacheBuilder
      .newBuilder()
      .weakValues()
      .build();

  public static PartitionSpec fromJson(Schema schema, String json) {
    try {
      return SPEC_CACHE.get(Pair.of(schema.asStruct(), json),
          () -> fromJson(schema, JsonUtil.mapper().readValue(json, JsonNode.class)));

    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw new RuntimeIOException(
            (IOException) e.getCause(), "Failed to parse partition spec: %s", json);
      } else {
        throw new RuntimeException("Failed to parse partition spec: " + json, e.getCause());
      }
    }
  }

  static void toJsonFields(PartitionSpec spec, JsonGenerator generator) throws IOException {
    generator.writeStartArray();
    for (PartitionField field : spec.fields()) {
      generator.writeStartObject();
      generator.writeStringField(NAME, field.name());
      generator.writeStringField(TRANSFORM, field.transform().toString());
      generator.writeNumberField(SOURCE_ID, field.sourceId());
      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  static String toJsonFields(PartitionSpec spec) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      toJsonFields(spec, generator);
      generator.flush();
      return writer.toString();

    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  static PartitionSpec fromJsonFields(Schema schema, int specId, JsonNode json) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema).withSpecId(specId);
    buildFromJsonFields(builder, json);
    return builder.build();
  }

  static PartitionSpec fromJsonFields(Schema schema, int specId, String json) {
    try {
      return fromJsonFields(schema, specId, JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to parse partition spec fields: " + json);
    }
  }

  private static void buildFromJsonFields(PartitionSpec.Builder builder, JsonNode json) {
    Preconditions.checkArgument(json.isArray(),
        "Cannot parse partition spec fields, not an array: %s", json);

    Iterator<JsonNode> elements = json.elements();
    while (elements.hasNext()) {
      JsonNode element = elements.next();
      Preconditions.checkArgument(element.isObject(),
          "Cannot parse partition field, not an object: %s", element);

      String name = JsonUtil.getString(NAME, element);
      String transform = JsonUtil.getString(TRANSFORM, element);
      int sourceId = JsonUtil.getInt(SOURCE_ID, element);

      builder.add(sourceId, name, transform);
    }
  }
}
