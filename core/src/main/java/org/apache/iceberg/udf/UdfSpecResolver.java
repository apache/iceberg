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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** Engine-agnostic resolver for Iceberg UDF spec JSON. */
public final class UdfSpecResolver {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private UdfSpecResolver() {}

  /**
   * Resolve all definitions' current versions for a dialect, returning one ResolvedSpec per
   * definition (overload).
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static List<ResolvedSpec> resolveAll(String udfMetadataJson, String dialect) {
    try {
      ObjectNode root = (ObjectNode) MAPPER.readTree(udfMetadataJson);
      ArrayNode definitions = (ArrayNode) root.get("definitions");
      if (definitions == null || definitions.isEmpty()) {
        throw new IllegalArgumentException("UDF metadata must contain at least one definition");
      }

      List<ResolvedSpec> results = Lists.newArrayList();
      for (JsonNode defNode : definitions) {
        ObjectNode definition = (ObjectNode) defNode;

        int currentVersionId =
            definition.has("current-version-id")
                ? definition.get("current-version-id").asInt()
                : -1;

        ArrayNode versions = (ArrayNode) definition.get("versions");
        if (versions == null || versions.isEmpty()) {
          throw new IllegalArgumentException("UDF definition must contain at least one version");
        }

        ObjectNode selectedVersion = null;
        for (JsonNode v : versions) {
          if (currentVersionId != -1
              && v.has("version-id")
              && v.get("version-id").asInt() == currentVersionId) {
            selectedVersion = (ObjectNode) v;
            break;
          }
        }

        if (selectedVersion == null) {
          selectedVersion = (ObjectNode) versions.get(0);
        }

        ArrayNode reps = (ArrayNode) selectedVersion.get("representations");
        if (reps == null || reps.isEmpty()) {
          throw new IllegalArgumentException(
              "UDF version must contain at least one representation");
        }

        ObjectNode chosen = null;
        for (JsonNode rep : reps) {
          String repDialect = text((ObjectNode) rep, "dialect", null);
          if (repDialect != null && repDialect.equalsIgnoreCase(dialect)) {
            chosen = (ObjectNode) rep;
            break;
          }
        }

        if (chosen == null) {
          // No representation for the requested dialect; skip this overload
          continue;
        }

        List<Param> params = Lists.newArrayList();
        ArrayNode paramsNode = (ArrayNode) chosen.get("parameters");
        if (paramsNode == null) {
          paramsNode = (ArrayNode) definition.get("parameters");
        }

        if (paramsNode != null) {
          Iterator<JsonNode> it = paramsNode.elements();
          while (it.hasNext()) {
            ObjectNode paramNode = (ObjectNode) it.next();
            String name = text(paramNode, "name", null);
            if (name == null) {
              throw new IllegalArgumentException("Parameter missing required field 'name'");
            }

            String typeJson = jsonString(paramNode.get("type"));
            params.add(new Param(name, typeJson));
          }
        }

        String returnTypeJson = jsonString(definition.get("return-type"));
        String functionType = text(definition, "function-type", "udf");
        String body = text(chosen, "body", null);
        if (body == null) {
          throw new IllegalArgumentException("Representation missing required field 'body'");
        }

        results.add(new ResolvedSpec(functionType, params, returnTypeJson, body));
      }

      return results;
    } catch (Exception e) {
      if (e instanceof IllegalArgumentException) {
        throw (IllegalArgumentException) e;
      }

      throw new IllegalArgumentException("Failed to parse UDF metadata JSON", e);
    }
  }

  private static String text(ObjectNode node, String field, String defaultVal) {
    return node != null && node.has(field) && !node.get(field).isNull()
        ? node.get(field).asText()
        : defaultVal;
  }

  private static String jsonString(JsonNode node) throws JsonProcessingException {
    if (node == null) {
      return null;
    }

    if (node.isTextual()) {
      return node.asText();
    }

    return MAPPER.writeValueAsString(node);
  }

  public static class Param {
    private final String name;
    private final String icebergTypeJson;

    public Param(String name, String icebergTypeJson) {
      this.name = name;
      this.icebergTypeJson = icebergTypeJson;
    }

    public String name() {
      return name;
    }

    public String icebergTypeJson() {
      return icebergTypeJson;
    }
  }

  public static class ResolvedSpec {
    private final String functionType;
    private final List<Param> parameters;
    private final String returnTypeJson;
    private final String body;

    public ResolvedSpec(
        String functionType, List<Param> parameters, String returnTypeJson, String body) {
      this.functionType = functionType;
      this.parameters = parameters;
      this.returnTypeJson = returnTypeJson;
      this.body = body;
    }

    public String functionType() {
      return functionType;
    }

    public List<Param> parameters() {
      return parameters;
    }

    public String returnTypeJson() {
      return returnTypeJson;
    }

    public String body() {
      return body;
    }
  }
}
