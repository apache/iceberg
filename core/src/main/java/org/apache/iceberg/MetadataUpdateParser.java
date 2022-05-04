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

package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

public class MetadataUpdateParser {

  private MetadataUpdateParser() {
  }

  private static final String ACTION = "action";

  // action types - visible for testing
  static final String ASSIGN_UUID = "assign-uuid";
  static final String UPGRADE_FORMAT_VERSION = "upgrade-format-version";
  static final String ADD_SCHEMA = "add-schema";
  static final String SET_CURRENT_SCHEMA = "set-current-schema";
  static final String ADD_PARTITION_SPEC = "add-spec";
  static final String SET_DEFAULT_PARTITION_SPEC = "set-default-spec";
  static final String ADD_SORT_ORDER = "add-sort-order";
  static final String SET_DEFAULT_SORT_ORDER = "set-default-sort-order";
  static final String ADD_SNAPSHOT = "add-snapshot";
  static final String REMOVE_SNAPSHOTS = "remove-snapshots";
  static final String SET_SNAPSHOT_REF = "set-snapshot-ref";
  static final String SET_PROPERTIES = "set-properties";
  static final String REMOVE_PROPERTIES = "remove-properties";
  static final String SET_LOCATION = "set-location";

  // UpgradeFormatVersion
  private static final String FORMAT_VERSION = "format-version";

  // AddSchema
  private static final String SCHEMA = "schema";
  private static final String LAST_COLUMN_ID = "last-column-id";

  // SetCurrentSchema
  private static final String SCHEMA_ID = "schema-id";

  // AddPartitionSpec
  private static final String SPEC = "spec";

  // SetDefaultPartitionSpec
  private static final String SPEC_ID = "spec-id";

  // AddSortOrder
  private static final String SORT_ORDER = "sort-order";

  private static final Map<Class<? extends MetadataUpdate>, String> ACTIONS = ImmutableMap
      .<Class<? extends MetadataUpdate>, String>builder()
      .put(MetadataUpdate.AssignUUID.class, ASSIGN_UUID)
      .put(MetadataUpdate.UpgradeFormatVersion.class, UPGRADE_FORMAT_VERSION)
      .put(MetadataUpdate.AddSchema.class, ADD_SCHEMA)
      .put(MetadataUpdate.SetCurrentSchema.class, SET_CURRENT_SCHEMA)
      .put(MetadataUpdate.AddPartitionSpec.class, ADD_PARTITION_SPEC)
      .put(MetadataUpdate.SetDefaultPartitionSpec.class, SET_DEFAULT_PARTITION_SPEC)
      .put(MetadataUpdate.AddSortOrder.class, ADD_SORT_ORDER)
      .put(MetadataUpdate.SetDefaultSortOrder.class, SET_DEFAULT_SORT_ORDER)
      .put(MetadataUpdate.AddSnapshot.class, ADD_SNAPSHOT)
      .put(MetadataUpdate.RemoveSnapshot.class, REMOVE_SNAPSHOTS)
      .put(MetadataUpdate.SetSnapshotRef.class, SET_SNAPSHOT_REF)
      .put(MetadataUpdate.SetProperties.class, SET_PROPERTIES)
      .put(MetadataUpdate.RemoveProperties.class, REMOVE_PROPERTIES)
      .put(MetadataUpdate.SetLocation.class, SET_LOCATION)
      .build();

  public static String toJson(MetadataUpdate metadataUpdate) {
    return toJson(metadataUpdate, false);
  }

  public static String toJson(MetadataUpdate metadataUpdate, boolean pretty) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson(metadataUpdate, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write metadata update json for: %s", metadataUpdate), e);
    }
  }

  public static void toJson(MetadataUpdate metadataUpdate, JsonGenerator generator) throws IOException {
    String updateAction = ACTIONS.get(metadataUpdate.getClass());

    generator.writeStartObject();
    generator.writeStringField(ACTION, updateAction);

    switch (updateAction) {
      case ASSIGN_UUID:
        throw new UnsupportedOperationException("Not Implemented: MetadataUpdate#toJson for AssignUUID");
      case UPGRADE_FORMAT_VERSION:
        writeUpgradeFormatVersion((MetadataUpdate.UpgradeFormatVersion) metadataUpdate, generator);
        break;
      case ADD_SCHEMA:
        writeAddSchema((MetadataUpdate.AddSchema) metadataUpdate, generator);
        break;
      case SET_CURRENT_SCHEMA:
        writeSetCurrentSchema((MetadataUpdate.SetCurrentSchema) metadataUpdate, generator);
        break;
      case ADD_PARTITION_SPEC:
        writeAddPartitionSpec((MetadataUpdate.AddPartitionSpec) metadataUpdate, generator);
        break;
      case SET_DEFAULT_PARTITION_SPEC:
        writeSetDefaultPartitionSpec((MetadataUpdate.SetDefaultPartitionSpec) metadataUpdate, generator);
        break;
      case ADD_SORT_ORDER:
        writeAddSortOrder((MetadataUpdate.AddSortOrder) metadataUpdate, generator);
        break;
      case SET_DEFAULT_SORT_ORDER:
      case ADD_SNAPSHOT:
      case REMOVE_SNAPSHOTS:
      case SET_SNAPSHOT_REF:
      case SET_PROPERTIES:
      case REMOVE_PROPERTIES:
      case SET_LOCATION:
        throw new UnsupportedOperationException("Not Implemented: MetadataUpdate#toJson for " + updateAction);
      default:
        throw new IllegalArgumentException(
            String.format("Cannot convert metadata update to json. Unrecognized action: %s", updateAction));
    }

    generator.writeEndObject();
  }

  /**
   * Read MetadataUpdate from a JSON string.
   *
   * @param json a JSON string of a MetadataUpdate
   * @return a MetadataUpdate object
   */
  public static MetadataUpdate fromJson(String json) {
    try {
      return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read JSON string: " + json, e);
    }
  }

  public static MetadataUpdate fromJson(JsonNode jsonNode) {
    Preconditions.checkArgument(jsonNode != null && jsonNode.isObject(),
        "Cannot parse metadata update from non-object value: %s", jsonNode);
    Preconditions.checkArgument(jsonNode.hasNonNull(ACTION), "Cannot parse metadata update. Missing field: action");
    String action = JsonUtil.getString(ACTION, jsonNode).toLowerCase(Locale.ROOT);

    switch (action) {
      case ASSIGN_UUID:
        throw new UnsupportedOperationException("Not implemented: AssignUUID");
      case UPGRADE_FORMAT_VERSION:
        return readUpgradeFormatVersion(jsonNode);
      case ADD_SCHEMA:
        return readAddSchema(jsonNode);
      case SET_CURRENT_SCHEMA:
        return readSetCurrentSchema(jsonNode);
      case ADD_PARTITION_SPEC:
        return readAddPartitionSpec(jsonNode);
      case SET_DEFAULT_PARTITION_SPEC:
        return readSetDefaultPartitionSpec(jsonNode);
      case ADD_SORT_ORDER:
        return readAddSortOrder(jsonNode);
      case SET_DEFAULT_SORT_ORDER:
      case ADD_SNAPSHOT:
      case REMOVE_SNAPSHOTS:
      case SET_SNAPSHOT_REF:
      case SET_PROPERTIES:
      case REMOVE_PROPERTIES:
      case SET_LOCATION:
        throw new UnsupportedOperationException("Not Implemented: MetadataUpdatefromJson for " + action);
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot convert metadata update action to json: %s", action));
    }
  }

  private static void writeUpgradeFormatVersion(MetadataUpdate.UpgradeFormatVersion update, JsonGenerator gen)
      throws IOException {
    gen.writeNumberField(FORMAT_VERSION, update.formatVersion());
  }

  private static void writeAddSchema(MetadataUpdate.AddSchema update, JsonGenerator gen) throws IOException {
    gen.writeFieldName(SCHEMA);
    SchemaParser.toJson(update.schema(), gen);
    gen.writeNumberField(LAST_COLUMN_ID, update.lastColumnId());
  }

  private static void writeSetCurrentSchema(MetadataUpdate.SetCurrentSchema update, JsonGenerator gen)
      throws IOException {
    gen.writeNumberField(SCHEMA_ID, update.schemaId());
  }

  private static void writeAddPartitionSpec(MetadataUpdate.AddPartitionSpec update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SPEC);
    PartitionSpecParser.toJson(update.spec(), gen);
  }

  private static void writeSetDefaultPartitionSpec(MetadataUpdate.SetDefaultPartitionSpec update, JsonGenerator gen)
      throws IOException {
    gen.writeNumberField(SPEC_ID, update.specId());
  }

  private static void writeAddSortOrder(MetadataUpdate.AddSortOrder update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SORT_ORDER);
    SortOrderParser.toJson(update.sortOrder(), gen);
  }

  private static MetadataUpdate readUpgradeFormatVersion(JsonNode node) {
    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    return new MetadataUpdate.UpgradeFormatVersion(formatVersion);
  }

  private static MetadataUpdate readAddSchema(JsonNode node) {
    Preconditions.checkArgument(node.hasNonNull(SCHEMA),
        "Cannot parse missing field: schema");
    JsonNode schemaNode = node.get(SCHEMA);
    Schema schema = SchemaParser.fromJson(schemaNode);
    int lastColumnId = JsonUtil.getInt(LAST_COLUMN_ID, node);
    return new MetadataUpdate.AddSchema(schema, lastColumnId);
  }

  private static MetadataUpdate readSetCurrentSchema(JsonNode node) {
    int schemaId = JsonUtil.getInt(SCHEMA_ID, node);
    return new MetadataUpdate.SetCurrentSchema(schemaId);
  }

  private static MetadataUpdate readAddPartitionSpec(JsonNode node) {
    Preconditions.checkArgument(node.hasNonNull(SPEC), "Missing required field: spec");
    JsonNode specNode = node.get(SPEC);
    UnboundPartitionSpec spec = PartitionSpecParser.fromJson(specNode);
    return new MetadataUpdate.AddPartitionSpec(spec);
  }

  private static MetadataUpdate readSetDefaultPartitionSpec(JsonNode node) {
    int specId = JsonUtil.getInt(SPEC_ID, node);
    return new MetadataUpdate.SetDefaultPartitionSpec(specId);
  }

  private static MetadataUpdate readAddSortOrder(JsonNode node) {
    Preconditions.checkArgument(node.hasNonNull(SORT_ORDER), "Cannot parse missing field: sort-order");
    JsonNode sortOrderNode = node.get(SORT_ORDER);
    UnboundSortOrder sortOrder = SortOrderParser.fromJson(sortOrderNode);
    return new MetadataUpdate.AddSortOrder(sortOrder);
  }
}

