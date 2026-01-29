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
package org.apache.iceberg.spark.udf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.udf.UdfSpecResolver;
import org.apache.spark.sql.types.DataType;

/** Parses engine-agnostic UDF JSON into a minimal {@link SqlFunctionSpec}. */
public class SqlFunctionSpecParser {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SqlFunctionSpecParser() {}

  public static SqlFunctionSpec parseScalar(String udfJson) {
    List<UdfSpecResolver.ResolvedSpec> resolved =
        UdfSpecResolver.resolveAll(udfJson, "spark"); // only keep spark representations
    if (resolved.isEmpty()) {
      throw new IllegalArgumentException("No Spark representation found in UDF metadata");
    }

    if (resolved.size() != 1) {
      throw new IllegalArgumentException(
          "Multiple overloads are not supported in Stage 1 (found " + resolved.size() + ")");
    }

    UdfSpecResolver.ResolvedSpec spec = resolved.get(0);
    if (!"udf".equalsIgnoreCase(spec.functionType())) {
      throw new IllegalArgumentException(
          "Only scalar UDFs are supported in Stage 1 (found type: " + spec.functionType() + ")");
    }

    List<SqlFunctionSpec.Parameter> params =
        spec.parameters().stream()
            .map(
                p -> {
                  if (p.defaultJson() != null) {
                    throw new IllegalArgumentException(
                        "Default parameter values are not supported in Stage 1");
                  }

                  return new SqlFunctionSpec.Parameter(
                      p.name(), p.icebergTypeJson(), null, p.doc());
                })
            .collect(Collectors.toList());

    return new SqlFunctionSpec(
        params, spec.returnTypeJson(), "spark", spec.body(), spec.deterministic(), spec.doc());
  }

  /**
   * Convert an Iceberg UDF type representation to Spark {@link DataType}.
   *
   * <p>The UDF spec allows types to be encoded either as a string (e.g. {@code "int"}, {@code
   * "list<int>"}, {@code "struct&lt;a:int,b:string&gt;"}) or as JSON objects for nested types.
   */
  public static DataType icebergTypeToSparkType(String icebergTypeJson) {
    if (icebergTypeJson == null) {
      throw new IllegalArgumentException("Null type is not supported");
    }

    Type icebergType = parseUdfType(icebergTypeJson);
    return SparkSchemaUtil.convert(icebergType);
  }

  private static Type parseUdfType(String icebergTypeJson) {
    String trimmed = icebergTypeJson.trim();
    // If it looks like a JSON object/array, parse it as JSON; otherwise treat as Iceberg type name.
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      try {
        JsonNode node = MAPPER.readTree(trimmed);
        return parseUdfTypeNode(node);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            "Failed to parse Iceberg type JSON: " + icebergTypeJson, e);
      }
    }

    // Strip surrounding quotes if present
    if ((trimmed.startsWith("\"") && trimmed.endsWith("\""))
        || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
      trimmed = trimmed.substring(1, trimmed.length() - 1);
    }

    return Types.fromTypeName(trimmed);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static Type parseUdfTypeNode(JsonNode node) {
    if (node == null || node.isNull()) {
      throw new IllegalArgumentException("Missing type");
    }

    if (node.isTextual()) {
      return Types.fromTypeName(node.asText());
    }

    if (!node.isObject()) {
      throw new IllegalArgumentException("Unsupported type JSON: " + node);
    }

    JsonNode typeNode = node.get("type");
    if (typeNode == null || !typeNode.isTextual()) {
      throw new IllegalArgumentException("Type object missing required field 'type'");
    }

    String type = typeNode.asText();
    if ("struct".equalsIgnoreCase(type)) {
      return parseStructType(node);
    } else if ("list".equalsIgnoreCase(type)) {
      return parseListType(node);
    } else if ("map".equalsIgnoreCase(type)) {
      return parseMapType(node);
    } else {
      throw new IllegalArgumentException("Unsupported complex type: " + type);
    }
  }

  private static Type parseStructType(JsonNode node) {
    JsonNode fields = node.get("fields");
    if (fields == null || !fields.isArray()) {
      throw new IllegalArgumentException("Struct type must have array field 'fields'");
    }

    List<Types.NestedField> icebergFields = new java.util.ArrayList<>(fields.size());
    int nextId = 1;
    for (JsonNode f : fields) {
      if (!f.isObject()) {
        throw new IllegalArgumentException("Invalid struct field: " + f);
      }

      String name = f.has("name") ? f.get("name").asText() : null;
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Struct field missing required field 'name'");
      }

      int id = f.has("id") ? f.get("id").asInt(nextId) : nextId;
      nextId = Math.max(nextId, id + 1);
      Type fieldType = parseUdfTypeNode(f.get("type"));
      // UDF spec doesn't currently encode required/optional; default to optional.
      icebergFields.add(Types.NestedField.optional(id, name, fieldType));
    }
    return Types.StructType.of(icebergFields);
  }

  private static Type parseListType(JsonNode node) {
    JsonNode element = node.get("element");
    if (element == null) {
      element = node.get("element-type");
    }

    if (element == null) {
      throw new IllegalArgumentException("List type must have field 'element' or 'element-type'");
    }
    Type elementType = parseUdfTypeNode(element);
    return Types.ListType.ofOptional(1, elementType);
  }

  private static Type parseMapType(JsonNode node) {
    JsonNode key = node.get("key");
    JsonNode value = node.get("value");
    if (key == null || value == null) {
      throw new IllegalArgumentException("Map type must have fields 'key' and 'value'");
    }

    Type keyType = parseUdfTypeNode(key);
    Type valueType = parseUdfTypeNode(value);
    return Types.MapType.ofOptional(1, 2, keyType, valueType);
  }
}
