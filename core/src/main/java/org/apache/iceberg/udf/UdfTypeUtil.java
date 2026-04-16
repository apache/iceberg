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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

/**
 * Utility for reading and writing UDF types. Types can be either a primitive type string (e.g.,
 * "int", "string", "variant") or a JSON object for nested types (struct, list, map).
 */
class UdfTypeUtil {

  private UdfTypeUtil() {}

  /**
   * Reads a UDF type from a JSON node. Returns a String for primitive types or a Map for nested
   * types.
   */
  static Object readType(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot read type from null node");

    if (node.isTextual()) {
      return node.asText();
    } else if (node.isObject()) {
      return JsonUtil.mapper().convertValue(node, java.util.Map.class);
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot parse UDF type from node: %s", node));
    }
  }

  /**
   * Writes a UDF type to a JSON generator under the given field name. The type can be a String
   * (primitive) or a Map (nested type).
   */
  @SuppressWarnings("unchecked")
  static void writeType(String fieldName, Object type, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(type != null, "Invalid type: null");

    if (type instanceof String) {
      generator.writeStringField(fieldName, (String) type);
    } else if (type instanceof java.util.Map) {
      generator.writeFieldName(fieldName);
      ObjectNode objectNode =
          JsonUtil.mapper().convertValue((java.util.Map<String, Object>) type, ObjectNode.class);
      generator.writeTree(objectNode);
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot serialize UDF type: %s (%s)", type, type.getClass().getName()));
    }
  }

  /**
   * Writes a UDF type value (without a field name) to a JSON generator. Used when writing array
   * elements.
   */
  @SuppressWarnings("unchecked")
  static void writeTypeValue(Object type, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(type != null, "Invalid type: null");

    if (type instanceof String) {
      generator.writeString((String) type);
    } else if (type instanceof java.util.Map) {
      ObjectNode objectNode =
          JsonUtil.mapper().convertValue((java.util.Map<String, Object>) type, ObjectNode.class);
      generator.writeTree(objectNode);
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot serialize UDF type: %s (%s)", type, type.getClass().getName()));
    }
  }
}
