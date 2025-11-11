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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.functions.UserSqlFunctions;
import org.apache.iceberg.udf.UdfSpecResolver.Param;
import org.apache.iceberg.udf.UdfSpecResolver.ResolvedSpec;
import org.apache.spark.sql.SparkSession;

/**
 * Registers SQL UDFs in a SparkSession from Iceberg UDF metadata JSON.
 *
 * <p>This utility parses a function specification (see Iceberg function spec) using {@link
 * org.apache.iceberg.udf.UdfSpecResolver}, filters to the requested dialect ("spark"), and
 * materializes one Spark 4.0 SQL UDF per matching overload by executing {@code CREATE OR REPLACE
 * TEMPORARY FUNCTION ... RETURN SQL} DDL.
 *
 * <p>Both scalar and table-valued functions (UDTF) are supported. Overloads are registered under
 * the same function name with parameter-type-specific signatures so Spark's analyzer can resolve
 * the appropriate implementation at call time.
 *
 * <p>Functions registered here are session-scoped. For discovery via Iceberg's FunctionCatalog, the
 * function name is also recorded in {@link org.apache.iceberg.spark.functions.UserSqlFunctions}.
 *
 * <p>Use {@link #registerFromJson(SparkSession, String, String)} to register from a JSON string or
 * {@link #registerFromJsonFile(SparkSession, String, Path)} to load from a file.
 */
public final class SparkUDFRegistrar {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SparkUDFRegistrar() {}

  public static void registerFromJson(
      SparkSession spark, String functionName, String udfMetadataJson) {
    try {
      // Validate that every definition's current version has a Spark representation
      preValidateAllDefinitionsSupportSpark(udfMetadataJson);

      List<ResolvedSpec> all =
          org.apache.iceberg.udf.UdfSpecResolver.resolveAll(udfMetadataJson, "spark");
      for (ResolvedSpec spec : all) {

        List<String> sparkParams = Lists.newArrayList();
        for (Param param : spec.parameters()) {
          String sparkType;
          try {
            sparkType = icebergTypeToSparkSql(parseTypeJson(param.icebergTypeJson()));
          } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "Unsupported parameter type for Spark in definition: " + param.name(), e);
          }
          sparkParams.add(param.name() + " " + sparkType);
        }

        String body = spec.body();

        String ddl;
        if (spec.functionType().equalsIgnoreCase("udtf")) {
          String tableColumns;
          try {
            tableColumns = structReturnToSparkColumns(parseTypeJson(spec.returnTypeJson()));
          } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "Unsupported UDTF return type for Spark (must be struct)", e);
          }
          ddl =
              String.format(
                  "CREATE OR REPLACE TEMPORARY FUNCTION %s(%s) RETURNS TABLE (%s) RETURN %s",
                  functionName, String.join(", ", sparkParams), tableColumns, body);
        } else {
          String returnType;
          try {
            returnType = icebergTypeToSparkSql(parseTypeJson(spec.returnTypeJson()));
          } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "Unsupported scalar return type for Spark", e);
          }
          ddl =
              String.format(
                  "CREATE OR REPLACE TEMPORARY FUNCTION %s(%s) RETURNS %s RETURN %s",
                  functionName, String.join(", ", sparkParams), returnType, body);
        }

        spark.sql(ddl);
      }

      UserSqlFunctions.register(functionName);
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed to parse UDF metadata JSON", e);
    }
  }

  private static void preValidateAllDefinitionsSupportSpark(String udfMetadataJson) {
    try {
      ObjectNode root = (ObjectNode) MAPPER.readTree(udfMetadataJson);
      if (root.has("secure") && root.get("secure").asBoolean()) {
        throw new IllegalArgumentException(
            "Spark does not support secure UDFs; 'secure': true is not allowed");
      }
      ArrayNode definitions = (ArrayNode) root.get("definitions");
      if (definitions == null || definitions.isEmpty()) {
        throw new IllegalArgumentException("UDF metadata must contain at least one definition");
      }

      // Validate that versions exist and current-version-id (if present) resolves to a version.
      for (JsonNode defNode : definitions) {
        ObjectNode definition = (ObjectNode) defNode;
        int currentVersionId =
            definition.has("current-version-id") ? definition.get("current-version-id").asInt() : -1;
        ArrayNode versions = (ArrayNode) definition.get("versions");
        if (versions == null || versions.isEmpty()) {
          throw new IllegalArgumentException("UDF definition must contain at least one version");
        }

        if (currentVersionId != -1) {
          boolean matched = false;
          for (JsonNode v : versions) {
            if (v.has("version-id") && v.get("version-id").asInt() == currentVersionId) {
              matched = true;
              break;
            }
          }
          if (!matched) {
            throw new IllegalArgumentException(
                "current-version-id does not match any version in definition: "
                    + getText(definition, "definition-id", "<unknown>"));
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse UDF metadata JSON", e);
    }
  }

  public static void registerFromJsonFile(SparkSession spark, String functionName, Path jsonPath) {
    try {
      String json = Files.readString(jsonPath, StandardCharsets.UTF_8);
      registerFromJson(spark, functionName, json);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read UDF metadata file: " + jsonPath, e);
    }
  }

  private static String getText(ObjectNode node, String field, String defaultVal) {
    return node.has(field) && !node.get(field).isNull() ? node.get(field).asText() : defaultVal;
  }

  private static JsonNode parseTypeJson(String jsonOrPrimitive) {
    if (jsonOrPrimitive == null) {
      return null;
    }
    if (!jsonOrPrimitive.startsWith("{") && !jsonOrPrimitive.startsWith("[")) {
      return MAPPER.getNodeFactory().textNode(jsonOrPrimitive);
    }
    try {
      return MAPPER.readTree(jsonOrPrimitive);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse type JSON: " + jsonOrPrimitive, e);
    }
  }

  private static String icebergTypeToSparkSql(JsonNode typeNode) {
    if (typeNode == null) {
      throw new IllegalArgumentException("Missing type");
    }

    if (typeNode.isTextual()) {
      String typeLower = typeNode.asText().toLowerCase(Locale.ROOT);
      switch (typeLower) {
        case "int":
          return "INT";
        case "long":
          return "BIGINT";
        case "float":
          return "FLOAT";
        case "double":
          return "DOUBLE";
        case "string":
          return "STRING";
        case "boolean":
          return "BOOLEAN";
        default:
          throw new IllegalArgumentException("Unsupported primitive type: " + typeLower);
      }
    }

    if (typeNode.isObject()) {
      ObjectNode obj = (ObjectNode) typeNode;
      String complexType = getText(obj, "type", null);
      if ("struct".equalsIgnoreCase(complexType)) {
        ArrayNode fields = (ArrayNode) obj.get("fields");
        if (fields == null) {
          throw new IllegalArgumentException("Struct type must have 'fields'");
        }

        List<String> parts = Lists.newArrayList();
        for (JsonNode fieldNode : fields) {
          String name = getText((ObjectNode) fieldNode, "name", null);
          String fieldType = icebergTypeToSparkSql(((ObjectNode) fieldNode).get("type"));
          parts.add(name + ":" + fieldType);
        }

        return "STRUCT<" + String.join(",", parts) + ">";
      }
    }

    throw new IllegalArgumentException("Unsupported type node: " + typeNode.toString());
  }

  private static String structReturnToSparkColumns(JsonNode returnTypeNode) {
    if (returnTypeNode == null || !returnTypeNode.isObject()) {
      throw new IllegalArgumentException("UDTF return-type must be a struct object");
    }

    ObjectNode obj = (ObjectNode) returnTypeNode;
    String complexType = getText(obj, "type", null);
    if (!"struct".equalsIgnoreCase(complexType)) {
      throw new IllegalArgumentException("UDTF return-type must be a struct");
    }

    ArrayNode fields = (ArrayNode) obj.get("fields");
    if (fields == null || fields.isEmpty()) {
      throw new IllegalArgumentException("UDTF return struct must have fields");
    }

    List<String> cols = Lists.newArrayList();
    Iterator<JsonNode> it = fields.elements();
    while (it.hasNext()) {
      ObjectNode fieldObj = (ObjectNode) it.next();
      String name = getText(fieldObj, "name", null);
      String typeSql = icebergTypeToSparkSql(fieldObj.get("type"));
      cols.add(name + " " + typeSql);
    }

    return String.join(", ", cols);
  }
}
