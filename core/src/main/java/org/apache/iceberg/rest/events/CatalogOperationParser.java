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
import org.apache.iceberg.util.JsonUtil;

public class CatalogOperationParser {
  static final String OPERATION_TYPE = "operation-type";
  static final String IDENTIFIER = "identifier";
  static final String TABLE_UUID = "table-uuid";
  static final String VIEW_UUID = "view-uuid";
  static final String UPDATES = "updates";
  static final String REQUIREMENTS = "requirements";
  static final String PURGE = "purge";
  static final String SOURCE = "source";
  static final String DESTINATION = "destination";
  static final String NAMESPACE = "namespace";
  static final String PROPERTIES = "properties";
  static final String UPDATED = "updated";
  static final String REMOVED = "removed";
  static final String MISSING = "missing";

  private CatalogOperationParser() {}

  public static String toJson(CatalogOperation operation) {
    return toJson(operation, false);
  }

  public static String toJson(CatalogOperation operation, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(operation, gen), pretty);
  }

  public static void toJson(CatalogOperation operation, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != operation, "Invalid catalog operation: null");

    gen.writeStartObject();
    gen.writeStringField(OPERATION_TYPE, operation.operationType());

    switch (operation.operationType()) {
      case OperationType.CREATE_TABLE ->
          writeCreateTable((CatalogOperation.CreateTable) operation, gen);
      case OperationType.REGISTER_TABLE ->
          writeRegisterTable((CatalogOperation.RegisterTable) operation, gen);
      case OperationType.DROP_TABLE -> writeDropTable((CatalogOperation.DropTable) operation, gen);
      case OperationType.UPDATE_TABLE ->
          writeUpdateTable((CatalogOperation.UpdateTable) operation, gen);
      case OperationType.RENAME_TABLE ->
          writeRenameTable((CatalogOperation.RenameTable) operation, gen);
      case OperationType.CREATE_VIEW ->
          writeCreateView((CatalogOperation.CreateView) operation, gen);
      case OperationType.DROP_VIEW -> writeDropView((CatalogOperation.DropView) operation, gen);
      case OperationType.UPDATE_VIEW ->
          writeUpdateView((CatalogOperation.UpdateView) operation, gen);
      case OperationType.RENAME_VIEW ->
          writeRenameView((CatalogOperation.RenameView) operation, gen);
      case OperationType.CREATE_NAMESPACE ->
          writeCreateNamespace((CatalogOperation.CreateNamespace) operation, gen);
      case OperationType.UPDATE_NAMESPACE_PROPERTIES ->
          writeUpdateNamespaceProperties(
              (CatalogOperation.UpdateNamespaceProperties) operation, gen);
      case OperationType.DROP_NAMESPACE ->
          writeDropNamespace((CatalogOperation.DropNamespace) operation, gen);
      default -> {
        // unknown operation types serialize as operation-type only
      }
    }

    gen.writeEndObject();
  }

  public static CatalogOperation fromJson(String json) {
    return JsonUtil.parse(json, CatalogOperationParser::fromJson);
  }

  public static CatalogOperation fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse catalog operation from null object");
    Preconditions.checkArgument(
        json.isObject(), "Cannot parse catalog operation from non-object: %s", json);

    String type = JsonUtil.getString(OPERATION_TYPE, json);
    return switch (type) {
      case OperationType.CREATE_TABLE -> parseCreateTable(json);
      case OperationType.REGISTER_TABLE -> parseRegisterTable(json);
      case OperationType.DROP_TABLE -> parseDropTable(json);
      case OperationType.UPDATE_TABLE -> parseUpdateTable(json);
      case OperationType.RENAME_TABLE -> parseRenameTable(json);
      case OperationType.CREATE_VIEW -> parseCreateView(json);
      case OperationType.DROP_VIEW -> parseDropView(json);
      case OperationType.UPDATE_VIEW -> parseUpdateView(json);
      case OperationType.RENAME_VIEW -> parseRenameView(json);
      case OperationType.CREATE_NAMESPACE -> parseCreateNamespace(json);
      case OperationType.UPDATE_NAMESPACE_PROPERTIES -> parseUpdateNamespaceProperties(json);
      case OperationType.DROP_NAMESPACE -> parseDropNamespace(json);
      default -> ImmutableCatalogOperation.Unknown.builder().operationType(type).build();
    };
  }

  private static void writeCreateTable(CatalogOperation.CreateTable operation, JsonGenerator gen)
      throws IOException {
    writeIdentifier(operation.identifier(), gen);
    gen.writeStringField(TABLE_UUID, operation.tableUuid());
    writeUpdates(operation.updates(), gen);
  }

  private static CatalogOperation parseCreateTable(JsonNode json) {
    return ImmutableCatalogOperation.CreateTable.builder()
        .identifier(identifier(json))
        .tableUuid(JsonUtil.getString(TABLE_UUID, json))
        .updates(requiredUpdates(json))
        .build();
  }

  private static void writeRegisterTable(
      CatalogOperation.RegisterTable operation, JsonGenerator gen) throws IOException {
    writeIdentifier(operation.identifier(), gen);
    gen.writeStringField(TABLE_UUID, operation.tableUuid());
    writeUpdatesIfPresent(operation.updates(), gen);
  }

  private static CatalogOperation parseRegisterTable(JsonNode json) {
    ImmutableCatalogOperation.RegisterTable.Builder builder =
        ImmutableCatalogOperation.RegisterTable.builder()
            .identifier(identifier(json))
            .tableUuid(JsonUtil.getString(TABLE_UUID, json));

    List<MetadataUpdate> updates = optionalUpdates(json);
    if (updates != null) {
      builder.updates(updates);
    }

    return builder.build();
  }

  private static void writeDropTable(CatalogOperation.DropTable operation, JsonGenerator gen)
      throws IOException {
    writeIdentifier(operation.identifier(), gen);
    gen.writeStringField(TABLE_UUID, operation.tableUuid());
    if (operation.purge() != null) {
      gen.writeBooleanField(PURGE, operation.purge());
    }
  }

  private static CatalogOperation parseDropTable(JsonNode json) {
    return ImmutableCatalogOperation.DropTable.builder()
        .identifier(identifier(json))
        .tableUuid(JsonUtil.getString(TABLE_UUID, json))
        .purge(JsonUtil.getBoolOrNull(PURGE, json))
        .build();
  }

  private static void writeUpdateTable(CatalogOperation.UpdateTable operation, JsonGenerator gen)
      throws IOException {
    writeIdentifier(operation.identifier(), gen);
    gen.writeStringField(TABLE_UUID, operation.tableUuid());
    writeUpdates(operation.updates(), gen);
    writeRequirementsIfPresent(operation.requirements(), gen);
  }

  private static CatalogOperation parseUpdateTable(JsonNode json) {
    ImmutableCatalogOperation.UpdateTable.Builder builder =
        ImmutableCatalogOperation.UpdateTable.builder()
            .identifier(identifier(json))
            .tableUuid(JsonUtil.getString(TABLE_UUID, json))
            .updates(requiredUpdates(json));

    List<UpdateRequirement> requirements = optionalRequirements(json);
    if (requirements != null) {
      builder.requirements(requirements);
    }

    return builder.build();
  }

  private static void writeRenameTable(CatalogOperation.RenameTable operation, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SOURCE);
    TableIdentifierParser.toJson(operation.source(), gen);
    gen.writeFieldName(DESTINATION);
    TableIdentifierParser.toJson(operation.destination(), gen);
    gen.writeStringField(TABLE_UUID, operation.tableUuid());
  }

  private static CatalogOperation parseRenameTable(JsonNode json) {
    return ImmutableCatalogOperation.RenameTable.builder()
        .source(TableIdentifierParser.fromJson(JsonUtil.get(SOURCE, json)))
        .destination(TableIdentifierParser.fromJson(JsonUtil.get(DESTINATION, json)))
        .tableUuid(JsonUtil.getString(TABLE_UUID, json))
        .build();
  }

  private static void writeCreateView(CatalogOperation.CreateView operation, JsonGenerator gen)
      throws IOException {
    writeIdentifier(operation.identifier(), gen);
    gen.writeStringField(VIEW_UUID, operation.viewUuid());
    writeUpdates(operation.updates(), gen);
  }

  private static CatalogOperation parseCreateView(JsonNode json) {
    return ImmutableCatalogOperation.CreateView.builder()
        .identifier(identifier(json))
        .viewUuid(JsonUtil.getString(VIEW_UUID, json))
        .updates(requiredUpdates(json))
        .build();
  }

  private static void writeDropView(CatalogOperation.DropView operation, JsonGenerator gen)
      throws IOException {
    writeIdentifier(operation.identifier(), gen);
    gen.writeStringField(VIEW_UUID, operation.viewUuid());
  }

  private static CatalogOperation parseDropView(JsonNode json) {
    return ImmutableCatalogOperation.DropView.builder()
        .identifier(identifier(json))
        .viewUuid(JsonUtil.getString(VIEW_UUID, json))
        .build();
  }

  private static void writeUpdateView(CatalogOperation.UpdateView operation, JsonGenerator gen)
      throws IOException {
    writeIdentifier(operation.identifier(), gen);
    gen.writeStringField(VIEW_UUID, operation.viewUuid());
    writeUpdates(operation.updates(), gen);
    writeRequirementsIfPresent(operation.requirements(), gen);
  }

  private static CatalogOperation parseUpdateView(JsonNode json) {
    ImmutableCatalogOperation.UpdateView.Builder builder =
        ImmutableCatalogOperation.UpdateView.builder()
            .identifier(identifier(json))
            .viewUuid(JsonUtil.getString(VIEW_UUID, json))
            .updates(requiredUpdates(json));

    List<UpdateRequirement> requirements = optionalRequirements(json);
    if (requirements != null) {
      builder.requirements(requirements);
    }

    return builder.build();
  }

  private static void writeRenameView(CatalogOperation.RenameView operation, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SOURCE);
    TableIdentifierParser.toJson(operation.source(), gen);
    gen.writeFieldName(DESTINATION);
    TableIdentifierParser.toJson(operation.destination(), gen);
    gen.writeStringField(VIEW_UUID, operation.viewUuid());
  }

  private static CatalogOperation parseRenameView(JsonNode json) {
    return ImmutableCatalogOperation.RenameView.builder()
        .source(TableIdentifierParser.fromJson(JsonUtil.get(SOURCE, json)))
        .destination(TableIdentifierParser.fromJson(JsonUtil.get(DESTINATION, json)))
        .viewUuid(JsonUtil.getString(VIEW_UUID, json))
        .build();
  }

  private static void writeCreateNamespace(
      CatalogOperation.CreateNamespace operation, JsonGenerator gen) throws IOException {
    writeNamespace(operation.namespace(), gen);
    if (!operation.properties().isEmpty()) {
      JsonUtil.writeStringMap(PROPERTIES, operation.properties(), gen);
    }
  }

  private static CatalogOperation parseCreateNamespace(JsonNode json) {
    ImmutableCatalogOperation.CreateNamespace.Builder builder =
        ImmutableCatalogOperation.CreateNamespace.builder().namespace(namespace(json));

    Map<String, String> properties = JsonUtil.getStringMapOrNull(PROPERTIES, json);
    if (properties != null) {
      builder.properties(properties);
    }

    return builder.build();
  }

  private static void writeUpdateNamespaceProperties(
      CatalogOperation.UpdateNamespaceProperties operation, JsonGenerator gen) throws IOException {
    writeNamespace(operation.namespace(), gen);
    JsonUtil.writeStringArray(UPDATED, operation.updated(), gen);
    JsonUtil.writeStringArray(REMOVED, operation.removed(), gen);
    if (operation.missing() != null) {
      JsonUtil.writeStringArray(MISSING, operation.missing(), gen);
    }
  }

  private static CatalogOperation parseUpdateNamespaceProperties(JsonNode json) {
    ImmutableCatalogOperation.UpdateNamespaceProperties.Builder builder =
        ImmutableCatalogOperation.UpdateNamespaceProperties.builder()
            .namespace(namespace(json))
            .updated(JsonUtil.getStringList(UPDATED, json))
            .removed(JsonUtil.getStringList(REMOVED, json));

    List<String> missing = JsonUtil.getStringListOrNull(MISSING, json);
    if (missing != null) {
      builder.missing(missing);
    }

    return builder.build();
  }

  private static void writeDropNamespace(
      CatalogOperation.DropNamespace operation, JsonGenerator gen) throws IOException {
    writeNamespace(operation.namespace(), gen);
  }

  private static CatalogOperation parseDropNamespace(JsonNode json) {
    return ImmutableCatalogOperation.DropNamespace.builder().namespace(namespace(json)).build();
  }

  private static void writeIdentifier(TableIdentifier identifier, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(IDENTIFIER);
    TableIdentifierParser.toJson(identifier, gen);
  }

  private static TableIdentifier identifier(JsonNode json) {
    return TableIdentifierParser.fromJson(JsonUtil.get(IDENTIFIER, json));
  }

  private static void writeNamespace(Namespace namespace, JsonGenerator gen) throws IOException {
    gen.writeFieldName(NAMESPACE);
    gen.writeArray(namespace.levels(), 0, namespace.length());
  }

  private static Namespace namespace(JsonNode json) {
    return Namespace.of(JsonUtil.getStringArray(NAMESPACE, json));
  }

  private static void writeUpdates(List<MetadataUpdate> updates, JsonGenerator gen)
      throws IOException {
    gen.writeArrayFieldStart(UPDATES);
    for (MetadataUpdate update : updates) {
      MetadataUpdateParser.toJson(update, gen);
    }

    gen.writeEndArray();
  }

  private static void writeUpdatesIfPresent(List<MetadataUpdate> updates, JsonGenerator gen)
      throws IOException {
    if (updates != null) {
      writeUpdates(updates, gen);
    }
  }

  private static List<MetadataUpdate> requiredUpdates(JsonNode json) {
    return JsonUtil.getObjectList(UPDATES, json, MetadataUpdateParser::fromJson);
  }

  private static List<MetadataUpdate> optionalUpdates(JsonNode json) {
    return JsonUtil.getObjectListOrNull(UPDATES, json, MetadataUpdateParser::fromJson);
  }

  private static void writeRequirementsIfPresent(
      List<UpdateRequirement> requirements, JsonGenerator gen) throws IOException {
    if (requirements == null) {
      return;
    }

    gen.writeArrayFieldStart(REQUIREMENTS);
    for (UpdateRequirement requirement : requirements) {
      UpdateRequirementParser.toJson(requirement, gen);
    }

    gen.writeEndArray();
  }

  private static List<UpdateRequirement> optionalRequirements(JsonNode json) {
    return JsonUtil.getObjectListOrNull(REQUIREMENTS, json, UpdateRequirementParser::fromJson);
  }
}
