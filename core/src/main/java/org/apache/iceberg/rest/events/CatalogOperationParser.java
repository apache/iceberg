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
import java.util.Arrays;
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
import org.apache.iceberg.rest.events.operations.CatalogOperation;
import org.apache.iceberg.rest.events.operations.OperationType;
import org.apache.iceberg.util.JsonUtil;

public class CatalogOperationParser {

  private CatalogOperationParser() {}

  private static final String OPERATION_TYPE = "operation-type";

  // shared field names
  private static final String IDENTIFIER = "identifier";
  private static final String NAMESPACE = "namespace";
  private static final String TABLE_UUID = "table-uuid";
  private static final String VIEW_UUID = "view-uuid";
  private static final String UPDATES = "updates";
  private static final String REQUIREMENTS = "requirements";
  private static final String PROPERTIES = "properties";

  // DropTable
  private static final String PURGE = "purge";

  // Rename
  private static final String SOURCE_IDENTIFIER = "source";
  private static final String DEST_IDENTIFIER = "destination";

  // UpdateNamespaceProperties
  private static final String UPDATED = "updated";
  private static final String REMOVED = "removed";
  private static final String MISSING = "missing";

  // Custom
  private static final String CUSTOM_OPERATION_TYPE = "custom-type";

  public static String toJson(CatalogOperation operation) {
    return toJson(operation, false);
  }

  public static String toJson(CatalogOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(CatalogOperation operation, JsonGenerator gen) throws IOException {
    Preconditions.checkNotNull(operation, "Invalid operation: null");

    gen.writeStartObject();
    gen.writeStringField(OPERATION_TYPE, operation.operationType().type());

    switch (operation.operationType()) {
      case CREATE_NAMESPACE:
        writeCreateNamespace((CatalogOperation.CreateNamespace) operation, gen);
        break;
      case CREATE_TABLE:
        writeCreateTable((CatalogOperation.CreateTable) operation, gen);
        break;
      case CREATE_VIEW:
        writeCreateView((CatalogOperation.CreateView) operation, gen);
        break;
      case DROP_NAMESPACE:
        writeDropNamespace((CatalogOperation.DropNamespace) operation, gen);
        break;
      case DROP_TABLE:
        writeDropTable((CatalogOperation.DropTable) operation, gen);
        break;
      case DROP_VIEW:
        writeDropView((CatalogOperation.DropView) operation, gen);
        break;
      case REGISTER_TABLE:
        writeRegisterTable((CatalogOperation.RegisterTable) operation, gen);
        break;
      case RENAME_TABLE:
        writeRenameTable((CatalogOperation.RenameTable) operation, gen);
        break;
      case RENAME_VIEW:
        writeRenameView((CatalogOperation.RenameView) operation, gen);
        break;
      case UPDATE_NAMESPACE_PROPERTIES:
        writeUpdateNamespaceProperties((CatalogOperation.UpdateNamespaceProperties) operation, gen);
        break;
      case UPDATE_TABLE:
        writeUpdateTable((CatalogOperation.UpdateTable) operation, gen);
        break;
      case UPDATE_VIEW:
        writeUpdateView((CatalogOperation.UpdateView) operation, gen);
        break;
      case CUSTOM:
        writeCustom((CatalogOperation.Custom) operation, gen);
        break;
      default:
        throw new IllegalArgumentException("Unknown operation type: " + operation.operationType());
    }

    gen.writeEndObject();
  }

  public static CatalogOperation fromJson(String json) {
    return JsonUtil.parse(json, CatalogOperationParser::fromJson);
  }

  public static CatalogOperation fromJson(JsonNode json) {
    Preconditions.checkNotNull(json, "Cannot parse catalog operation from null object");

    OperationType operationType = OperationType.fromType(JsonUtil.getString(OPERATION_TYPE, json));
    switch (operationType) {
      case CREATE_NAMESPACE:
        return readCreateNamespace(json);
      case CREATE_TABLE:
        return readCreateTable(json);
      case CREATE_VIEW:
        return readCreateView(json);
      case DROP_NAMESPACE:
        return readDropNamespace(json);
      case DROP_TABLE:
        return readDropTable(json);
      case DROP_VIEW:
        return readDropView(json);
      case REGISTER_TABLE:
        return readRegisterTable(json);
      case RENAME_TABLE:
        return readRenameTable(json);
      case RENAME_VIEW:
        return readRenameView(json);
      case UPDATE_NAMESPACE_PROPERTIES:
        return readUpdateNamespaceProperties(json);
      case UPDATE_TABLE:
        return readUpdateTable(json);
      case UPDATE_VIEW:
        return readUpdateView(json);
      case CUSTOM:
        return readCustom(json);
      default:
        throw new IllegalArgumentException("Unknown operation type: " + operationType);
    }
  }

  private static void writeCreateNamespace(
      CatalogOperation.CreateNamespace operation, JsonGenerator gen) throws IOException {
    JsonUtil.writeStringArray(NAMESPACE, Arrays.asList(operation.namespace().levels()), gen);

    if (!operation.properties().isEmpty()) {
      gen.writeObjectField(PROPERTIES, operation.properties());
    }
  }

  private static CatalogOperation.CreateNamespace readCreateNamespace(JsonNode json) {
    Namespace namespace = Namespace.of(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, json)));
    Map<String, String> properties =
        json.has(PROPERTIES) ? JsonUtil.getStringMap(PROPERTIES, json) : ImmutableMap.of();
    return new CatalogOperation.CreateNamespace(namespace, properties);
  }

  private static void writeCreateTable(CatalogOperation.CreateTable operation, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(operation.identifier(), gen);
    gen.writeStringField(TABLE_UUID, operation.tableUuid());
    writeMetadataUpdates(operation.updates(), gen);
  }

  private static CatalogOperation.CreateTable readCreateTable(JsonNode json) {
    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    String tableUuid = JsonUtil.getString(TABLE_UUID, json);
    List<MetadataUpdate> updates =
        JsonUtil.getObjectList(UPDATES, json, MetadataUpdateParser::fromJson);
    return new CatalogOperation.CreateTable(identifier, tableUuid, updates);
  }

  private static void writeCreateView(CatalogOperation.CreateView operation, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(operation.identifier(), gen);
    gen.writeStringField(VIEW_UUID, operation.viewUuid());
    writeMetadataUpdates(operation.updates(), gen);
  }

  private static CatalogOperation.CreateView readCreateView(JsonNode json) {
    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    String viewUuid = JsonUtil.getString(VIEW_UUID, json);
    List<MetadataUpdate> updates =
        JsonUtil.getObjectList(UPDATES, json, MetadataUpdateParser::fromJson);
    return new CatalogOperation.CreateView(identifier, viewUuid, updates);
  }

  private static void writeDropNamespace(
      CatalogOperation.DropNamespace operation, JsonGenerator gen) throws IOException {
    JsonUtil.writeStringArray(NAMESPACE, Arrays.asList(operation.namespace().levels()), gen);
  }

  private static CatalogOperation.DropNamespace readDropNamespace(JsonNode json) {
    Namespace namespace = Namespace.of(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, json)));
    return new CatalogOperation.DropNamespace(namespace);
  }

  private static void writeDropTable(CatalogOperation.DropTable operation, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(operation.identifier(), gen);
    gen.writeStringField(TABLE_UUID, operation.tableUuid());

    if (operation.purge() != null) {
      gen.writeBooleanField(PURGE, operation.purge());
    }
  }

  private static CatalogOperation.DropTable readDropTable(JsonNode json) {
    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    String tableUuid = JsonUtil.getString(TABLE_UUID, json);
    Boolean purge = json.has(PURGE) ? JsonUtil.getBool(PURGE, json) : null;
    return new CatalogOperation.DropTable(identifier, tableUuid, purge);
  }

  private static void writeDropView(CatalogOperation.DropView operation, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(operation.identifier(), gen);
    gen.writeStringField(VIEW_UUID, operation.viewUuid());
  }

  private static CatalogOperation.DropView readDropView(JsonNode json) {
    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    String viewUuid = JsonUtil.getString(VIEW_UUID, json);
    return new CatalogOperation.DropView(identifier, viewUuid);
  }

  private static void writeRegisterTable(
      CatalogOperation.RegisterTable operation, JsonGenerator gen) throws IOException {
    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(operation.identifier(), gen);
    gen.writeStringField(TABLE_UUID, operation.tableUuid());

    if (!operation.updates().isEmpty()) {
      writeMetadataUpdates(operation.updates(), gen);
    }
  }

  private static CatalogOperation.RegisterTable readRegisterTable(JsonNode json) {
    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    String tableUuid = JsonUtil.getString(TABLE_UUID, json);
    List<MetadataUpdate> updates =
        json.has(UPDATES)
            ? JsonUtil.getObjectList(UPDATES, json, MetadataUpdateParser::fromJson)
            : ImmutableList.of();
    return new CatalogOperation.RegisterTable(identifier, tableUuid, updates);
  }

  private static void writeRenameTable(CatalogOperation.RenameTable operation, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(TABLE_UUID, operation.tableUuid());

    gen.writeFieldName(SOURCE_IDENTIFIER);
    TableIdentifierParser.toJson(operation.sourceIdentifier(), gen);

    gen.writeFieldName(DEST_IDENTIFIER);
    TableIdentifierParser.toJson(operation.destIdentifier(), gen);
  }

  private static CatalogOperation.RenameTable readRenameTable(JsonNode json) {
    String tableUuid = JsonUtil.getString(TABLE_UUID, json);
    TableIdentifier sourceIdentifier =
        TableIdentifierParser.fromJson(JsonUtil.get(SOURCE_IDENTIFIER, json));
    TableIdentifier destIdentifier =
        TableIdentifierParser.fromJson(JsonUtil.get(DEST_IDENTIFIER, json));
    return new CatalogOperation.RenameTable(sourceIdentifier, destIdentifier, tableUuid);
  }

  private static void writeRenameView(CatalogOperation.RenameView operation, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(VIEW_UUID, operation.viewUuid());

    gen.writeFieldName(SOURCE_IDENTIFIER);
    TableIdentifierParser.toJson(operation.sourceIdentifier(), gen);

    gen.writeFieldName(DEST_IDENTIFIER);
    TableIdentifierParser.toJson(operation.destIdentifier(), gen);
  }

  private static CatalogOperation.RenameView readRenameView(JsonNode json) {
    String viewUuid = JsonUtil.getString(VIEW_UUID, json);
    TableIdentifier sourceIdentifier =
        TableIdentifierParser.fromJson(JsonUtil.get(SOURCE_IDENTIFIER, json));
    TableIdentifier destIdentifier =
        TableIdentifierParser.fromJson(JsonUtil.get(DEST_IDENTIFIER, json));
    return new CatalogOperation.RenameView(sourceIdentifier, destIdentifier, viewUuid);
  }

  private static void writeUpdateNamespaceProperties(
      CatalogOperation.UpdateNamespaceProperties operation, JsonGenerator gen) throws IOException {
    JsonUtil.writeStringArray(NAMESPACE, Arrays.asList(operation.namespace().levels()), gen);
    JsonUtil.writeStringArray(UPDATED, operation.updated(), gen);
    JsonUtil.writeStringArray(REMOVED, operation.removed(), gen);

    if (!operation.missing().isEmpty()) {
      JsonUtil.writeStringArray(MISSING, operation.missing(), gen);
    }
  }

  private static CatalogOperation.UpdateNamespaceProperties readUpdateNamespaceProperties(
      JsonNode json) {
    Namespace namespace = Namespace.of(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, json)));
    List<String> updated = JsonUtil.getStringList(UPDATED, json);
    List<String> removed = JsonUtil.getStringList(REMOVED, json);
    List<String> missing =
        json.has(MISSING) ? JsonUtil.getStringList(MISSING, json) : ImmutableList.of();
    return new CatalogOperation.UpdateNamespaceProperties(namespace, removed, updated, missing);
  }

  private static void writeUpdateTable(CatalogOperation.UpdateTable operation, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(operation.identifier(), gen);
    gen.writeStringField(TABLE_UUID, operation.tableUuid());
    writeMetadataUpdates(operation.updates(), gen);

    if (!operation.requirements().isEmpty()) {
      writeUpdateRequirements(operation.requirements(), gen);
    }
  }

  private static CatalogOperation.UpdateTable readUpdateTable(JsonNode json) {
    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    String tableUuid = JsonUtil.getString(TABLE_UUID, json);
    List<MetadataUpdate> updates =
        JsonUtil.getObjectList(UPDATES, json, MetadataUpdateParser::fromJson);
    List<UpdateRequirement> requirements =
        json.has(REQUIREMENTS)
            ? JsonUtil.getObjectList(REQUIREMENTS, json, UpdateRequirementParser::fromJson)
            : null;
    return new CatalogOperation.UpdateTable(identifier, tableUuid, updates, requirements);
  }

  private static void writeUpdateView(CatalogOperation.UpdateView operation, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(operation.identifier(), gen);
    gen.writeStringField(VIEW_UUID, operation.viewUuid());
    writeMetadataUpdates(operation.updates(), gen);

    if (!operation.requirements().isEmpty()) {
      writeUpdateRequirements(operation.requirements(), gen);
    }
  }

  private static CatalogOperation.UpdateView readUpdateView(JsonNode json) {
    TableIdentifier identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    String viewUuid = JsonUtil.getString(VIEW_UUID, json);
    List<MetadataUpdate> updates =
        JsonUtil.getObjectList(UPDATES, json, MetadataUpdateParser::fromJson);
    List<UpdateRequirement> requirements =
        json.has(REQUIREMENTS)
            ? JsonUtil.getObjectList(REQUIREMENTS, json, UpdateRequirementParser::fromJson)
            : null;
    return new CatalogOperation.UpdateView(identifier, viewUuid, updates, requirements);
  }

  private static void writeCustom(CatalogOperation.Custom operation, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(CUSTOM_OPERATION_TYPE, operation.customOperationType().type());

    if (operation.identifier() != null) {
      gen.writeFieldName(IDENTIFIER);
      TableIdentifierParser.toJson(operation.identifier(), gen);
    }

    if (operation.namespace() != null) {
      JsonUtil.writeStringArray(NAMESPACE, Arrays.asList(operation.namespace().levels()), gen);
    }

    if (operation.tableUuid() != null) {
      gen.writeStringField(TABLE_UUID, operation.tableUuid());
    }

    if (operation.viewUuid() != null) {
      gen.writeStringField(VIEW_UUID, operation.viewUuid());
    }

    if (!operation.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, operation.properties(), gen);
    }
  }

  private static CatalogOperation.Custom readCustom(JsonNode json) {
    OperationType.CustomOperationType customOperationType =
        new OperationType.CustomOperationType(JsonUtil.getString(CUSTOM_OPERATION_TYPE, json));

    TableIdentifier identifier = null;
    if (json.has(IDENTIFIER)) {
      identifier = TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
    }

    Namespace namespace = null;
    if (json.has(NAMESPACE)) {
      namespace = Namespace.of(JsonUtil.getStringArray(JsonUtil.get(NAMESPACE, json)));
    }

    String tableUuid = json.has(TABLE_UUID) ? JsonUtil.getString(TABLE_UUID, json) : null;
    String viewUuid = json.has(VIEW_UUID) ? JsonUtil.getString(VIEW_UUID, json) : null;

    Map<String, String> properties =
        json.has(PROPERTIES) ? JsonUtil.getStringMap(PROPERTIES, json) : ImmutableMap.of();

    return new CatalogOperation.Custom(
        customOperationType, identifier, namespace, tableUuid, viewUuid, properties);
  }

  private static void writeMetadataUpdates(List<MetadataUpdate> updates, JsonGenerator gen)
      throws IOException {
    gen.writeArrayFieldStart(UPDATES);
    for (MetadataUpdate update : updates) {
      MetadataUpdateParser.toJson(update, gen);
    }
    gen.writeEndArray();
  }

  private static void writeUpdateRequirements(
      List<UpdateRequirement> requirements, JsonGenerator gen) throws IOException {
    gen.writeArrayFieldStart(REQUIREMENTS);
    for (UpdateRequirement req : requirements) {
      UpdateRequirementParser.toJson(req, gen);
    }
    gen.writeEndArray();
  }
}
