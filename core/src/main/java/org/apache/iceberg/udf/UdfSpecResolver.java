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

  private static final String FUNCTION_UUID = "function-uuid";
  private static final String FORMAT_VERSION = "format-version";
  private static final String PARAMETER_NAMES = "parameter-names";
  private static final String DEFINITIONS = "definitions";
  private static final String DEFINITION_LOG = "definition-log";
  private static final String SECURE = "secure";
  private static final String DOC = "doc";
  private static final String LOCATION = "location";
  private static final String PROPERTIES = "properties";

  private static final String DEFINITION_ID = "definition-id";
  private static final String PARAMETERS = "parameters";
  private static final String RETURN_TYPE = "return-type";
  private static final String FUNCTION_TYPE = "function-type";
  private static final String CURRENT_VERSION_ID = "current-version-id";
  private static final String VERSIONS = "versions";
  private static final String NULLABLE_RETURN = "nullable-return";

  private static final String VERSION_ID = "version-id";
  private static final String TIMESTAMP_MS = "timestamp-ms";
  private static final String REPRESENTATIONS = "representations";
  private static final String DETERMINISTIC = "deterministic";
  private static final String ON_NULL_INPUT = "on-null-input";
  private static final String DEFINITION_VERSIONS = "definition-versions";

  private static final String TYPE = "type";
  private static final String DEFAULT = "default";
  private static final String DIALECT = "dialect";
  private static final String BODY = "body";

  private static final class ParameterName {
    private final String name;
    private final String doc;

    private ParameterName(String name, String doc) {
      this.name = name;
      this.doc = doc;
    }
  }

  /**
   * Resolve all definitions' current versions for a dialect, returning one ResolvedSpec per
   * definition (overload).
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static List<ResolvedSpec> resolveAll(String udfMetadataJson, String dialect) {
    try {
      ObjectNode root = (ObjectNode) MAPPER.readTree(udfMetadataJson);
      validateRoot(root);

      boolean secure = root.has(SECURE) && root.get(SECURE).asBoolean(false);
      if (secure) {
        // Engines must not expose secure function bodies via inspection; Stage 1 implementations
        // do not support secure functions.
        throw new IllegalArgumentException("Secure UDFs are not supported");
      }

      ArrayNode definitions = (ArrayNode) root.get(DEFINITIONS);
      List<ParameterName> globalParameterNames = parseGlobalParameterNames(root);
      String functionDoc = text(root, DOC, null);

      List<ResolvedSpec> results = Lists.newArrayList();
      for (JsonNode defNode : definitions) {
        if (!defNode.isObject()) {
          throw new IllegalArgumentException("Invalid definition: expected object");
        }
        results.add(
            resolveDefinition((ObjectNode) defNode, globalParameterNames, functionDoc, dialect));
      }

      return results;
    } catch (Exception e) {
      if (e instanceof IllegalArgumentException) {
        throw (IllegalArgumentException) e;
      }

      throw new IllegalArgumentException("Failed to parse UDF metadata JSON", e);
    }
  }

  private static void validateRoot(ObjectNode root) {
    String uuid = text(root, FUNCTION_UUID, null);
    if (uuid == null || uuid.isEmpty()) {
      throw new IllegalArgumentException(
          "UDF metadata must contain required field 'function-uuid'");
    }

    if (!root.has(FORMAT_VERSION) || !root.get(FORMAT_VERSION).canConvertToInt()) {
      throw new IllegalArgumentException(
          "UDF metadata must contain required field 'format-version'");
    }

    int formatVersion = root.get(FORMAT_VERSION).asInt();
    if (formatVersion != 1) {
      throw new IllegalArgumentException(
          "Unsupported UDF metadata format-version: " + formatVersion);
    }

    if (!root.has(DEFINITION_LOG) || !root.get(DEFINITION_LOG).isArray()) {
      throw new IllegalArgumentException(
          "UDF metadata must contain required field 'definition-log'");
    }

    // Optional root fields: validate types when present
    validateOptionalBoolean(root, SECURE);
    validateOptionalString(root, DOC);
    validateOptionalString(root, LOCATION);
    validateOptionalStringMap(root, PROPERTIES);

    // validate definition-log structure and required fields
    validateDefinitionLog((ArrayNode) root.get(DEFINITION_LOG));

    ArrayNode definitions = (ArrayNode) root.get(DEFINITIONS);
    if (definitions == null || definitions.isEmpty()) {
      throw new IllegalArgumentException("UDF metadata must contain at least one definition");
    }
  }

  private static void validateDefinitionLog(ArrayNode definitionLog) {
    if (definitionLog.isEmpty()) {
      throw new IllegalArgumentException(
          "UDF metadata must contain at least one definition-log entry");
    }

    for (JsonNode entryNode : definitionLog) {
      if (!entryNode.isObject()) {
        throw new IllegalArgumentException("Invalid definition-log entry: expected object");
      }

      ObjectNode entryObj = (ObjectNode) entryNode;
      requiredLong(entryObj, TIMESTAMP_MS);
      ArrayNode defVersions = requiredArray(entryObj, DEFINITION_VERSIONS);
      if (defVersions.isEmpty()) {
        throw new IllegalArgumentException(
            "definition-log entry must contain at least one definition-versions entry");
      }

      for (JsonNode dvNode : defVersions) {
        if (!dvNode.isObject()) {
          throw new IllegalArgumentException("Invalid definition-versions entry: expected object");
        }

        ObjectNode dvObj = (ObjectNode) dvNode;
        requiredText(dvObj, DEFINITION_ID);
        requiredInt(dvObj, VERSION_ID);
      }
    }
  }

  private static ResolvedSpec resolveDefinition(
      ObjectNode definition,
      List<ParameterName> globalParameterNames,
      String functionDoc,
      String dialect)
      throws JsonProcessingException {
    requiredText(definition, DEFINITION_ID);
    requiredArray(definition, PARAMETERS);
    if (definition.get(RETURN_TYPE) == null || definition.get(RETURN_TYPE).isNull()) {
      throw new IllegalArgumentException("Missing required field '" + RETURN_TYPE + "'");
    }

    // Optional definition fields: validate types when present
    validateOptionalString(definition, DOC);
    validateOptionalString(definition, FUNCTION_TYPE);
    validateOptionalBoolean(definition, NULLABLE_RETURN);

    int currentVersionId = requiredInt(definition, CURRENT_VERSION_ID);
    ArrayNode versions = requiredArray(definition, VERSIONS);
    validateVersions(versions);

    ObjectNode selectedVersion = selectCurrentVersion(definition, versions, currentVersionId);
    ObjectNode chosenRepresentation =
        selectDialectRepresentation(definition, selectedVersion, dialect);

    String repType = text(chosenRepresentation, TYPE, null);
    if (!"sql".equalsIgnoreCase(repType)) {
      throw new IllegalArgumentException(
          "Unsupported representation type: " + repType + " (expected 'sql')");
    }

    List<Param> params = resolveParams(definition, globalParameterNames);
    String returnTypeJson = jsonString(definition.get(RETURN_TYPE));
    String functionType = text(definition, FUNCTION_TYPE, "udf");
    String body = text(chosenRepresentation, BODY, null);
    if (body == null) {
      throw new IllegalArgumentException("Representation missing required field 'body'");
    }

    boolean deterministic =
        selectedVersion.has(DETERMINISTIC) && selectedVersion.get(DETERMINISTIC).asBoolean();
    String onNullInput =
        selectedVersion.has(ON_NULL_INPUT) ? selectedVersion.get(ON_NULL_INPUT).asText() : null;

    String definitionDoc = text(definition, DOC, null);
    String resolvedDoc = definitionDoc != null ? definitionDoc : functionDoc;

    return new ResolvedSpec(
        functionType, params, returnTypeJson, body, deterministic, onNullInput, resolvedDoc);
  }

  private static List<Param> resolveParams(
      ObjectNode definition, List<ParameterName> globalParameterNames)
      throws JsonProcessingException {
    ArrayNode paramsNode = requiredArray(definition, PARAMETERS);
    int arity = paramsNode.size();
    if (arity > globalParameterNames.size()) {
      throw new IllegalArgumentException(
          "Definition has "
              + arity
              + " parameters but 'parameter-names' has only "
              + globalParameterNames.size());
    }

    List<Param> params = Lists.newArrayListWithExpectedSize(arity);
    for (int i = 0; i < arity; i++) {
      ObjectNode paramNode = (ObjectNode) paramsNode.get(i);
      ParameterName paramName = globalParameterNames.get(i);
      String typeJson = jsonString(paramNode.get(TYPE));
      String defaultJson = jsonString(paramNode.get(DEFAULT));
      params.add(new Param(paramName.name, typeJson, defaultJson, paramName.doc));
    }

    return params;
  }

  private static ObjectNode selectCurrentVersion(
      ObjectNode definition, ArrayNode versions, int currentVersionId) {
    for (JsonNode versionNode : versions) {
      ObjectNode versionObj = (ObjectNode) versionNode;
      if (versionObj.has(VERSION_ID) && versionObj.get(VERSION_ID).asInt() == currentVersionId) {
        return versionObj;
      }
    }

    String defId = text(definition, DEFINITION_ID, "<unknown>");
    throw new IllegalArgumentException(
        "current-version-id does not match any version in definition: " + defId);
  }

  private static ObjectNode selectDialectRepresentation(
      ObjectNode definition, ObjectNode selectedVersion, String dialect) {
    ArrayNode reps = requiredArray(selectedVersion, REPRESENTATIONS);
    for (JsonNode rep : reps) {
      ObjectNode repObj = (ObjectNode) rep;
      String repDialect = text(repObj, DIALECT, null);
      if (repDialect != null && repDialect.equalsIgnoreCase(dialect)) {
        return repObj;
      }
    }
    String defId = text(definition, DEFINITION_ID, "<unknown>");
    throw new IllegalArgumentException(
        "No representation found for dialect '" + dialect + "' in definition: " + defId);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static void validateVersions(ArrayNode versions) {
    for (JsonNode versionNode : versions) {
      if (!versionNode.isObject()) {
        throw new IllegalArgumentException("Invalid definition version: expected object");
      }
      ObjectNode versionObj = (ObjectNode) versionNode;
      requiredInt(versionObj, VERSION_ID);
      requiredLong(versionObj, TIMESTAMP_MS);

      JsonNode detNode = versionObj.get(DETERMINISTIC);
      if (detNode != null && !detNode.isNull() && !detNode.isBoolean()) {
        throw new IllegalArgumentException("Invalid '" + DETERMINISTIC + "': expected boolean");
      }

      JsonNode onNull = versionObj.get(ON_NULL_INPUT);
      if (onNull != null && !onNull.isNull() && !onNull.isTextual()) {
        throw new IllegalArgumentException("Invalid '" + ON_NULL_INPUT + "': expected string");
      }
      if (onNull != null && onNull.isTextual()) {
        String onNullValue = onNull.asText();
        if (!"returns_null_on_null_input".equals(onNullValue)
            && !"called_on_null_input".equals(onNullValue)) {
          throw new IllegalArgumentException(
              "Invalid '" + ON_NULL_INPUT + "': unsupported value '" + onNullValue + "'");
        }
      }

      validateRepresentations(requiredArray(versionObj, REPRESENTATIONS));
    }
  }

  private static void validateRepresentations(ArrayNode repsNode) {
    if (repsNode.isEmpty()) {
      throw new IllegalArgumentException("UDF version must contain at least one representation");
    }
    for (JsonNode repNode : repsNode) {
      if (!repNode.isObject()) {
        throw new IllegalArgumentException("Invalid representation: expected object");
      }
      ObjectNode repObj = (ObjectNode) repNode;
      requiredText(repObj, TYPE);
      requiredText(repObj, DIALECT);
      JsonNode bodyNode = repObj.get(BODY);
      if (bodyNode == null || bodyNode.isNull() || !bodyNode.isTextual()) {
        throw new IllegalArgumentException("Representation missing required field 'body'");
      }
    }
  }

  private static List<ParameterName> parseGlobalParameterNames(ObjectNode root) {
    ArrayNode namesNode = (ArrayNode) root.get(PARAMETER_NAMES);
    if (namesNode == null || namesNode.isEmpty()) {
      throw new IllegalArgumentException(
          "UDF metadata must contain required field 'parameter-names'");
    }

    List<ParameterName> names = Lists.newArrayListWithExpectedSize(namesNode.size());
    for (JsonNode n : namesNode) {
      if (!n.isObject()) {
        throw new IllegalArgumentException("Invalid parameter-names entry: expected object");
      }
      String name = text((ObjectNode) n, "name", null);
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("parameter-names entry missing required field 'name'");
      }
      ObjectNode paramNameObj = (ObjectNode) n;
      validateOptionalString(paramNameObj, DOC);
      String doc = text(paramNameObj, DOC, null);
      names.add(new ParameterName(name, doc));
    }
    return names;
  }

  private static ArrayNode requiredArray(ObjectNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || !value.isArray()) {
      throw new IllegalArgumentException("Missing required array field '" + field + "'");
    }

    return (ArrayNode) value;
  }

  private static int requiredInt(ObjectNode node, String field) {
    if (!node.has(field) || !node.get(field).canConvertToInt()) {
      throw new IllegalArgumentException("Missing required int field '" + field + "'");
    }
    return node.get(field).asInt();
  }

  private static long requiredLong(ObjectNode node, String field) {
    if (!node.has(field) || !node.get(field).canConvertToLong()) {
      throw new IllegalArgumentException("Missing required long field '" + field + "'");
    }

    return node.get(field).asLong();
  }

  private static String requiredText(ObjectNode node, String field) {
    String value = text(node, field, null);
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("Missing required field '" + field + "'");
    }

    return value;
  }

  private static void validateOptionalString(ObjectNode node, String field) {
    JsonNode value = node.get(field);
    if (value != null && !value.isNull() && !value.isTextual()) {
      throw new IllegalArgumentException("Invalid '" + field + "': expected string");
    }
  }

  private static void validateOptionalBoolean(ObjectNode node, String field) {
    JsonNode value = node.get(field);
    if (value != null && !value.isNull() && !value.isBoolean()) {
      throw new IllegalArgumentException("Invalid '" + field + "': expected boolean");
    }
  }

  private static void validateOptionalStringMap(ObjectNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return;
    }

    if (!value.isObject()) {
      throw new IllegalArgumentException("Invalid '" + field + "': expected object");
    }

    ObjectNode obj = (ObjectNode) value;
    Iterator<String> it = obj.fieldNames();
    while (it.hasNext()) {
      String key = it.next();
      JsonNode valueNode = obj.get(key);
      if (valueNode == null || valueNode.isNull() || !valueNode.isTextual()) {
        throw new IllegalArgumentException("Invalid '" + field + "': expected map<string,string>");
      }
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
    private final String defaultJson;
    private final String doc;

    public Param(String name, String icebergTypeJson, String defaultJson, String doc) {
      this.name = name;
      this.icebergTypeJson = icebergTypeJson;
      this.defaultJson = defaultJson;
      this.doc = doc;
    }

    public String name() {
      return name;
    }

    public String icebergTypeJson() {
      return icebergTypeJson;
    }

    public String defaultJson() {
      return defaultJson;
    }

    public String doc() {
      return doc;
    }
  }

  public static class ResolvedSpec {
    private final String functionType;
    private final List<Param> parameters;
    private final String returnTypeJson;
    private final String body;
    private final boolean deterministic;
    private final String onNullInput;
    private final String doc;

    public ResolvedSpec(
        String functionType,
        List<Param> parameters,
        String returnTypeJson,
        String body,
        boolean deterministic,
        String onNullInput,
        String doc) {
      this.functionType = functionType;
      this.parameters = parameters;
      this.returnTypeJson = returnTypeJson;
      this.body = body;
      this.deterministic = deterministic;
      this.onNullInput = onNullInput;
      this.doc = doc;
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

    public boolean deterministic() {
      return deterministic;
    }

    public String onNullInput() {
      return onNullInput;
    }

    public String doc() {
      return doc;
    }
  }
}
