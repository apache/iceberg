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
package org.apache.iceberg.udf;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.JsonUtil;

/**
 * Utility for reading and writing UDF types. A type is either a primitive type string (e.g., "int",
 * "string", "variant") or a JSON object for nested types (struct, list, map).
 */
class UdfTypeUtil {

  private static final String TYPE = "type";
  private static final String LIST = "list";
  private static final String MAP = "map";
  private static final String STRUCT = "struct";
  private static final String ELEMENT = "element";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String FIELDS = "fields";
  private static final String NAME = "name";

  private UdfTypeUtil() {}

  /** Reads a UDF type from a JSON node. */
  static UdfType readType(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot read type from null node");

    if (node.isTextual()) {
      return UdfPrimitiveType.of(node.asText());
    } else if (node.isObject()) {
      String typeName = JsonUtil.getString(TYPE, node);
      switch (typeName) {
        case LIST:
          return UdfListType.of(readType(node.get(ELEMENT)));
        case MAP:
          return UdfMapType.of(readType(node.get(KEY)), readType(node.get(VALUE)));
        case STRUCT:
          return readStruct(node);
        default:
          throw new IllegalArgumentException(
              String.format("Cannot parse UDF type from object with type: %s", typeName));
      }
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot parse UDF type from node: %s", node));
    }
  }

  private static UdfStructType readStruct(JsonNode node) {
    JsonNode fieldsNode = node.get(FIELDS);
    Preconditions.checkArgument(
        fieldsNode != null && fieldsNode.isArray(),
        "Cannot parse struct type from non-array fields: %s",
        fieldsNode);

    ImmutableList.Builder<UdfFieldType> fields = ImmutableList.builder();
    for (JsonNode fieldNode : fieldsNode) {
      Preconditions.checkArgument(
          fieldNode.isObject(), "Cannot parse struct field from non-object: %s", fieldNode);
      fields.add(
          UdfFieldType.of(JsonUtil.getString(NAME, fieldNode), readType(fieldNode.get(TYPE))));
    }

    return UdfStructType.of(fields.build());
  }

  /** Writes a UDF type to a JSON generator under the given field name. */
  static void writeType(String fieldName, UdfType type, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(type != null, "Invalid type: null");
    generator.writeFieldName(fieldName);
    writeTypeValue(type, generator);
  }

  private static void writeTypeValue(UdfType type, JsonGenerator generator) throws IOException {
    switch (type.typeId()) {
      case PRIMITIVE:
        generator.writeString(type.asPrimitive().typeString());
        return;
      case LIST:
        generator.writeStartObject();
        generator.writeStringField(TYPE, LIST);
        writeType(ELEMENT, type.asListType().elementType(), generator);
        generator.writeEndObject();
        return;
      case MAP:
        UdfMapType mapType = type.asMapType();
        generator.writeStartObject();
        generator.writeStringField(TYPE, MAP);
        writeType(KEY, mapType.keyType(), generator);
        writeType(VALUE, mapType.valueType(), generator);
        generator.writeEndObject();
        return;
      case STRUCT:
        List<UdfFieldType> fields = type.asStructType().fields();
        generator.writeStartObject();
        generator.writeStringField(TYPE, STRUCT);
        generator.writeArrayFieldStart(FIELDS);
        for (UdfFieldType field : fields) {
          generator.writeStartObject();
          generator.writeStringField(NAME, field.name());
          writeType(TYPE, field.type(), generator);
          generator.writeEndObject();
        }
        generator.writeEndArray();
        generator.writeEndObject();
        return;
      default:
        throw new IllegalArgumentException("Unknown UDF type: " + type);
    }
  }
}
