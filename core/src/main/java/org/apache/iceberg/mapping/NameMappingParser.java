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

package org.apache.iceberg.mapping;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.JsonUtil;

/**
 * Parses external name mappings from a JSON representation.
 * <pre>
 * [ { "field-id": 1, "names": ["id", "record_id"] },
 *   { "field-id": 2, "names": ["data"] },
 *   { "field-id": 3, "names": ["location"], "fields": [
 *       { "field-id": 4, "names": ["latitude", "lat"] },
 *       { "field-id": 5, "names": ["longitude", "long"] }
 *     ] } ]
 * </pre>
 */
public class NameMappingParser {

  private NameMappingParser() {
  }

  private static final String FIELD_ID = "field-id";
  private static final String NAMES = "names";
  private static final String FIELDS = "fields";

  public static String toJson(NameMapping mapping) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      generator.useDefaultPrettyPrinter();
      toJson(mapping, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json for: %s", mapping);
    }
  }

  static void toJson(NameMapping nameMapping, JsonGenerator generator) throws IOException {
    toJson(nameMapping.asMappedFields(), generator);
  }

  private static void toJson(MappedFields mapping, JsonGenerator generator) throws IOException {
    generator.writeStartArray();

    for (MappedField field : mapping.fields()) {
      toJson(field, generator);
    }

    generator.writeEndArray();
  }

  private static void toJson(MappedField field, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    generator.writeNumberField(FIELD_ID, field.id());

    generator.writeArrayFieldStart(NAMES);
    for (String name : field.names()) {
      generator.writeString(name);
    }
    generator.writeEndArray();

    MappedFields nested = field.nestedMapping();
    if (nested != null) {
      generator.writeFieldName(FIELDS);
      toJson(nested, generator);
    }

    generator.writeEndObject();
  }

  public static NameMapping fromJson(String json) {
    try {
      return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to convert version from json: %s", json);
    }
  }

  static NameMapping fromJson(JsonNode node) {
    return new NameMapping(fieldsFromJson(node));
  }

  private static MappedFields fieldsFromJson(JsonNode node) {
    Preconditions.checkArgument(node.isArray(), "Cannot parse non-array mapping fields: %s", node);

    List<MappedField> fields = Lists.newArrayList();
    node.elements().forEachRemaining(fieldNode -> fields.add(fieldFromJson(fieldNode)));

    return MappedFields.of(fields);
  }

  private static MappedField fieldFromJson(JsonNode node) {
    Preconditions.checkArgument(node != null && !node.isNull() && node.isObject(),
        "Cannot parse non-object mapping field: %s", node);

    Integer id = JsonUtil.getIntOrNull(FIELD_ID, node);

    Set<String> names;
    if (node.has(NAMES)) {
      names = ImmutableSet.copyOf(JsonUtil.getStringList(NAMES, node));
    } else {
      names = ImmutableSet.of();
    }

    MappedFields nested;
    if (node.has(FIELDS)) {
      nested = fieldsFromJson(node.get(FIELDS));
    } else {
      nested = null;
    }

    return MappedField.of(id, names, nested);
  }
}
