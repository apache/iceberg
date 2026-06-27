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
package org.apache.iceberg.rest.events;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirementParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.TableIdentifierParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

/**
 * Parser for {@link CatalogOperation} objects, handling polymorphic serialization and
 * deserialization based on the operation-type discriminator.
 */
public class CatalogOperationParser {

  private static final String OPERATION_TYPE = "operation-type";
  private static final String NAMESPACE = "namespace";
  private static final String IDENTIFIER = "identifier";
  private static final String TABLE_UUID = "table-uuid";
  private static final String VIEW_UUID = "view-uuid";
  private static final String PURGE = "purge";
  private static final String PROPERTIES = "properties";
  private static final String UPDATES = "updates";
  private static final String REQUIREMENTS = "requirements";
  private static final String UPDATED = "updated";
  private static final String REMOVED = "removed";
  private static final String MISSING = "missing";
  private static final String SOURCE = "source";
  private static final String DESTINATION = "destination";
  private static final String CUSTOM_TYPE = "custom-type";
  private static final String ADDITIONAL_PROPERTIES = "additional-properties";

  // Operation type constants
  static final String CREATE_NAMESPACE = "create-namespace";
  static final String DROP_NAMESPACE = "drop-namespace";
  static final String UPDATE_NAMESPACE_PROPERTIES = "update-namespace-properties";
  static final String CREATE_TABLE = "create-table";
  static final String REGISTER_TABLE = "register-table";
  static final String DROP_TABLE = "drop-table";
  static final String UPDATE_TABLE = "update-table";
  static final String RENAME_TABLE = "rename-table";
  static final String CREATE_VIEW = "create-view";
  static final String DROP_VIEW = "drop-view";
  static final String UPDATE_VIEW = "update-view";
  static final String RENAME_VIEW = "rename-view";
  static final String CUSTOM = "custom";

  private CatalogOperationParser() {}

  public static void toJson(CatalogOperation operation, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != operation, "Invalid operation: null");

    gen.writeStartObject();
    gen.writeStringField(OPERATION_TYPE, operation.operationType());

    switch (operation.operationType()) {
      case CREATE_NAMESPACE:
        createNamespaceToJson((CreateNamespaceOperation) operation, gen);
        break;
      case DROP_NAMESPACE:
        dropNamespaceToJson((DropNamespaceOperation) operation, gen);
        break;
      case UPDATE_NAMESPACE_PROPERTIES:
        updateNamespacePropertiesToJson((UpdateNamespacePropertiesOperation) operation, gen);
        break;
      case CREATE_TABLE:
        createTableToJson((CreateTableOperation) operation, gen);
        break;
      case REGISTER_TABLE:
        registerTableToJson((RegisterTableOperation) operation, gen);
        break;
      case DROP_TABLE:
        dropTableToJson((DropTableOperation) operation, gen);
        break;
      case UPDATE_TABLE:
        updateTableToJson((UpdateTableOperation) operation, gen);
        break;
      case RENAME_TABLE:
        renameTableToJson((RenameTableOperation) operation, gen);
        break;
      case CREATE_VIEW:
        createViewToJson((CreateViewOperation) operation, gen);
        break;
      case DROP_VIEW:
        dropViewToJson((DropViewOperation) operation, gen);
        break;
      case UPDATE_VIEW:
        updateViewToJson((UpdateViewOperation) operation, gen);
        break;
      case RENAME_VIEW:
        renameViewToJson((RenameViewOperation) operation, gen);
        break;
      case CUSTOM:
        customToJson((CustomOperation) operation, gen);
        break;
      default:
        throw new UnsupportedOperationException(
            "Cannot serialize unknown operation type: " + operation.operationType());
    }

    gen.writeEndObject();
  }

  public static CatalogOperation fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse operation from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse operation from non-object: %s", json);

    String operationType = JsonUtil.getString(OPERATION_TYPE, json);

    switch (operationType) {
      case CREATE_NAMESPACE:
        return createNamespaceFromJson(json);
      case DROP_NAMESPACE:
        return dropNamespaceFromJson(json);
      case UPDATE_NAMESPACE_PROPERTIES:
        return updateNamespacePropertiesFromJson(json);
      case CREATE_TABLE:
        return createTableFromJson(json);
      case REGISTER_TABLE:
        return registerTableFromJson(json);
      case DROP_TABLE:
        return dropTableFromJson(json);
      case UPDATE_TABLE:
        return updateTableFromJson(json);
      case RENAME_TABLE:
        return renameTableFromJson(json);
      case CREATE_VIEW:
        return createViewFromJson(json);
      case DROP_VIEW:
        return dropViewFromJson(json);
      case UPDATE_VIEW:
        return updateViewFromJson(json);
      case RENAME_VIEW:
        return renameViewFromJson(json);
      case CUSTOM:
        return customFromJson(json);
      default:
        // Forward compatibility: return a custom operation for unknown types
        // so that clients can gracefully handle new operation types
        return ImmutableCustomOperation.builder()
            .operationType(operationType)
            .customType(operationType)
            .build();
    }
  }

  // --- Namespace operations ---

  private static void createNamespaceToJson(CreateNamespaceOperation op, JsonGenerator gen)
      throws IOException {
    writeNamespace(op.namespace(), gen);
    if (!op.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, op.properties(), gen);
    }
  }

  private static CatalogOperation createNamespaceFromJson(JsonNode json) {
    ImmutableCreateNamespaceOperation.Builder builder =
        ImmutableCreateNamespaceOperation.builder().namespace(namespaceFromJson(json));
    if (json.has(PROPERTIES)) {
      builder.properties(JsonUtil.getStringMap(PROPERTIES, json));
    }
    return builder.build();
  }

  private static void dropNamespaceToJson(DropNamespaceOperation op, JsonGenerator gen)
      throws IOException {
    writeNamespace(op.namespace(), gen);
  }

  private static CatalogOperation dropNamespaceFromJson(JsonNode json) {
    return ImmutableDropNamespaceOperation.builder().namespace(namespaceFromJson(json)).build();
  }

  private static void updateNamespacePropertiesToJson(
      UpdateNamespacePropertiesOperation op, JsonGenerator gen) throws IOException {
    writeNamespace(op.namespace(), gen);
    JsonUtil.writeStringArray(UPDATED, op.updated(), gen);
    JsonUtil.writeStringArray(REMOVED, op.removed(), gen);
    if (!op.missing().isEmpty()) {
      JsonUtil.writeStringArray(MISSING, op.missing(), gen);
    }
  }

  private static CatalogOperation updateNamespacePropertiesFromJson(JsonNode json) {
    ImmutableUpdateNamespacePropertiesOperation.Builder builder =
        ImmutableUpdateNamespacePropertiesOperation.builder()
            .namespace(namespaceFromJson(json))
            .updated(ImmutableList.copyOf(JsonUtil.getStringList(UPDATED, json)))
            .removed(ImmutableList.copyOf(JsonUtil.getStringList(REMOVED, json)));
    if (json.has(MISSING)) {
      builder.missing(ImmutableList.copyOf(JsonUtil.getStringList(MISSING, json)));
    }
    return builder.build();
  }

  // --- Table operations ---

  private static void createTableToJson(CreateTableOperation op, JsonGenerator gen)
      throws IOException {
    writeIdentifier(op.identifier(), gen);
    gen.writeStringField(TABLE_UUID, op.tableUuid());
    writeMetadataUpdates(op.updates(), gen);
  }

  private static CatalogOperation createTableFromJson(JsonNode json) {
    return ImmutableCreateTableOperation.builder()
        .identifier(identifierFromJson(json))
        .tableUuid(JsonUtil.getString(TABLE_UUID, json))
        .updates(metadataUpdatesFromJson(json))
        .build();
  }

  private static void registerTableToJson(RegisterTableOperation op, JsonGenerator gen)
      throws IOException {
    writeIdentifier(op.identifier(), gen);
    gen.writeStringField(TABLE_UUID, op.tableUuid());
    if (op.updates() != null) {
      writeMetadataUpdates(op.updates(), gen);
    }
  }

  private static CatalogOperation registerTableFromJson(JsonNode json) {
    ImmutableRegisterTableOperation.Builder builder =
        ImmutableRegisterTableOperation.builder()
            .identifier(identifierFromJson(json))
            .tableUuid(JsonUtil.getString(TABLE_UUID, json));
    if (json.has(UPDATES)) {
      builder.updates(metadataUpdatesFromJson(json));
    }
    return builder.build();
  }

  private static void dropTableToJson(DropTableOperation op, JsonGenerator gen) throws IOException {
    writeIdentifier(op.identifier(), gen);
    gen.writeStringField(TABLE_UUID, op.tableUuid());
    if (op.purge() != null) {
      gen.writeBooleanField(PURGE, op.purge());
    }
  }

  private static CatalogOperation dropTableFromJson(JsonNode json) {
    ImmutableDropTableOperation.Builder builder =
        ImmutableDropTableOperation.builder()
            .identifier(identifierFromJson(json))
            .tableUuid(JsonUtil.getString(TABLE_UUID, json));
    if (json.has(PURGE)) {
      builder.purge(json.get(PURGE).asBoolean());
    }
    return builder.build();
  }

  private static void updateTableToJson(UpdateTableOperation op, JsonGenerator gen)
      throws IOException {
    writeIdentifier(op.identifier(), gen);
    gen.writeStringField(TABLE_UUID, op.tableUuid());
    writeMetadataUpdates(op.updates(), gen);
    if (op.requirements() != null) {
      writeRequirements(op.requirements(), gen);
    }
  }

  private static CatalogOperation updateTableFromJson(JsonNode json) {
    ImmutableUpdateTableOperation.Builder builder =
        ImmutableUpdateTableOperation.builder()
            .identifier(identifierFromJson(json))
            .tableUuid(JsonUtil.getString(TABLE_UUID, json))
            .updates(metadataUpdatesFromJson(json));
    if (json.has(REQUIREMENTS)) {
      builder.requirements(requirementsFromJson(json));
    }
    return builder.build();
  }

  private static void renameTableToJson(RenameTableOperation op, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SOURCE);
    TableIdentifierParser.toJson(op.source(), gen);
    gen.writeFieldName(DESTINATION);
    TableIdentifierParser.toJson(op.destination(), gen);
    gen.writeStringField(TABLE_UUID, op.tableUuid());
  }

  private static CatalogOperation renameTableFromJson(JsonNode json) {
    return ImmutableRenameTableOperation.builder()
        .source(TableIdentifierParser.fromJson(JsonUtil.get(SOURCE, json)))
        .destination(TableIdentifierParser.fromJson(JsonUtil.get(DESTINATION, json)))
        .tableUuid(JsonUtil.getString(TABLE_UUID, json))
        .build();
  }

  // --- View operations ---

  private static void createViewToJson(CreateViewOperation op, JsonGenerator gen)
      throws IOException {
    writeIdentifier(op.identifier(), gen);
    gen.writeStringField(VIEW_UUID, op.viewUuid());
    writeMetadataUpdates(op.updates(), gen);
  }

  private static CatalogOperation createViewFromJson(JsonNode json) {
    return ImmutableCreateViewOperation.builder()
        .identifier(identifierFromJson(json))
        .viewUuid(JsonUtil.getString(VIEW_UUID, json))
        .updates(metadataUpdatesFromJson(json))
        .build();
  }

  private static void dropViewToJson(DropViewOperation op, JsonGenerator gen) throws IOException {
    writeIdentifier(op.identifier(), gen);
    gen.writeStringField(VIEW_UUID, op.viewUuid());
  }

  private static CatalogOperation dropViewFromJson(JsonNode json) {
    return ImmutableDropViewOperation.builder()
        .identifier(identifierFromJson(json))
        .viewUuid(JsonUtil.getString(VIEW_UUID, json))
        .build();
  }

  private static void updateViewToJson(UpdateViewOperation op, JsonGenerator gen)
      throws IOException {
    writeIdentifier(op.identifier(), gen);
    gen.writeStringField(VIEW_UUID, op.viewUuid());
    writeMetadataUpdates(op.updates(), gen);
    if (op.requirements() != null) {
      writeRequirements(op.requirements(), gen);
    }
  }

  private static CatalogOperation updateViewFromJson(JsonNode json) {
    ImmutableUpdateViewOperation.Builder builder =
        ImmutableUpdateViewOperation.builder()
            .identifier(identifierFromJson(json))
            .viewUuid(JsonUtil.getString(VIEW_UUID, json))
            .updates(metadataUpdatesFromJson(json));
    if (json.has(REQUIREMENTS)) {
      builder.requirements(requirementsFromJson(json));
    }
    return builder.build();
  }

  private static void renameViewToJson(RenameViewOperation op, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SOURCE);
    TableIdentifierParser.toJson(op.source(), gen);
    gen.writeFieldName(DESTINATION);
    TableIdentifierParser.toJson(op.destination(), gen);
    gen.writeStringField(VIEW_UUID, op.viewUuid());
  }

  private static CatalogOperation renameViewFromJson(JsonNode json) {
    return ImmutableRenameViewOperation.builder()
        .source(TableIdentifierParser.fromJson(JsonUtil.get(SOURCE, json)))
        .destination(TableIdentifierParser.fromJson(JsonUtil.get(DESTINATION, json)))
        .viewUuid(JsonUtil.getString(VIEW_UUID, json))
        .build();
  }

  // --- Custom operation ---

  private static void customToJson(CustomOperation op, JsonGenerator gen) throws IOException {
    gen.writeStringField(CUSTOM_TYPE, op.customType());
    if (op.identifier() != null) {
      writeIdentifier(op.identifier(), gen);
    }
    if (op.namespace() != null) {
      writeNamespace(op.namespace(), gen);
    }
    if (op.tableUuid() != null) {
      gen.writeStringField(TABLE_UUID, op.tableUuid());
    }
    if (op.viewUuid() != null) {
      gen.writeStringField(VIEW_UUID, op.viewUuid());
    }
    if (!op.additionalProperties().isEmpty()) {
      gen.writeObjectFieldStart(ADDITIONAL_PROPERTIES);
      for (Map.Entry<String, Object> entry : op.additionalProperties().entrySet()) {
        gen.writeFieldName(entry.getKey());
        gen.writeObject(entry.getValue());
      }
      gen.writeEndObject();
    }
  }

  private static CatalogOperation customFromJson(JsonNode json) {
    ImmutableCustomOperation.Builder builder =
        ImmutableCustomOperation.builder().customType(JsonUtil.getString(CUSTOM_TYPE, json));
    if (json.has(IDENTIFIER)) {
      builder.identifier(identifierFromJson(json));
    }
    if (json.has(NAMESPACE)) {
      builder.namespace(namespaceFromJson(json));
    }
    if (json.has(TABLE_UUID)) {
      builder.tableUuid(JsonUtil.getString(TABLE_UUID, json));
    }
    if (json.has(VIEW_UUID)) {
      builder.viewUuid(JsonUtil.getString(VIEW_UUID, json));
    }
    if (json.has(ADDITIONAL_PROPERTIES)) {
      ImmutableMap.Builder<String, Object> props = ImmutableMap.builder();
      json.get(ADDITIONAL_PROPERTIES)
          .properties()
          .forEach(
              entry -> {
                JsonNode value = entry.getValue();
                if (value.isTextual()) {
                  props.put(entry.getKey(), value.asText());
                } else if (value.isNumber()) {
                  props.put(entry.getKey(), value.numberValue());
                } else if (value.isBoolean()) {
                  props.put(entry.getKey(), value.asBoolean());
                } else {
                  props.put(entry.getKey(), value.toString());
                }
              });
      builder.additionalProperties(props.build());
    }
    return builder.build();
  }

  // --- Shared helpers ---

  private static void writeNamespace(Namespace namespace, JsonGenerator gen) throws IOException {
    gen.writeArrayFieldStart(NAMESPACE);
    for (String level : namespace.levels()) {
      gen.writeString(level);
    }
    gen.writeEndArray();
  }

  private static Namespace namespaceFromJson(JsonNode json) {
    String[] levels = JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, json));
    return Namespace.of(levels);
  }

  private static void writeIdentifier(TableIdentifier identifier, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(identifier, gen);
  }

  private static TableIdentifier identifierFromJson(JsonNode json) {
    return TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
  }

  private static void writeMetadataUpdates(List<MetadataUpdate> updates, JsonGenerator gen)
      throws IOException {
    gen.writeArrayFieldStart(UPDATES);
    for (MetadataUpdate update : updates) {
      MetadataUpdateParser.toJson(update, gen);
    }
    gen.writeEndArray();
  }

  private static List<MetadataUpdate> metadataUpdatesFromJson(JsonNode json) {
    ImmutableList.Builder<MetadataUpdate> updates = ImmutableList.builder();
    for (JsonNode node : json.get(UPDATES)) {
      updates.add(MetadataUpdateParser.fromJson(node));
    }
    return updates.build();
  }

  private static void writeRequirements(List<UpdateRequirement> requirements, JsonGenerator gen)
      throws IOException {
    gen.writeArrayFieldStart(REQUIREMENTS);
    for (UpdateRequirement requirement : requirements) {
      UpdateRequirementParser.toJson(requirement, gen);
    }
    gen.writeEndArray();
  }

  private static List<UpdateRequirement> requirementsFromJson(JsonNode json) {
    ImmutableList.Builder<UpdateRequirement> requirements = ImmutableList.builder();
    for (JsonNode node : json.get(REQUIREMENTS)) {
      requirements.add(UpdateRequirementParser.fromJson(node));
    }
    return requirements.build();
  }
}
