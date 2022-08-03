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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.JsonUtil;

public class MetadataUpdateParser {

  private MetadataUpdateParser() {}

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
  static final String REMOVE_SNAPSHOT_REF = "remove-snapshot-ref";
  static final String SET_SNAPSHOT_REF = "set-snapshot-ref";
  static final String SET_PROPERTIES = "set-properties";
  static final String REMOVE_PROPERTIES = "remove-properties";
  static final String SET_LOCATION = "set-location";

  // AssignUUID
  private static final String UUID = "uuid";

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

  // SetDefaultSortOrder
  private static final String SORT_ORDER_ID = "sort-order-id";

  // AddSnapshot
  private static final String SNAPSHOT = "snapshot";

  // RemoveSnapshot
  private static final String SNAPSHOT_IDS = "snapshot-ids";

  // SetSnapshotRef
  private static final String REF_NAME = "ref-name"; // Also used in RemoveSnapshotRef
  private static final String SNAPSHOT_ID = "snapshot-id";
  private static final String TYPE = "type";
  private static final String MIN_SNAPSHOTS_TO_KEEP = "min-snapshots-to-keep";
  private static final String MAX_SNAPSHOT_AGE_MS = "max-snapshot-age-ms";
  private static final String MAX_REF_AGE_MS = "max-ref-age-ms";

  // SetProperties
  private static final String UPDATED = "updated";

  // RemoveProperties
  private static final String REMOVED = "removed";

  // SetLocation
  private static final String LOCATION = "location";

  private static final Map<Class<? extends MetadataUpdate>, String> ACTIONS =
      ImmutableMap.<Class<? extends MetadataUpdate>, String>builder()
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
          .put(MetadataUpdate.RemoveSnapshotRef.class, REMOVE_SNAPSHOT_REF)
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
      throw new UncheckedIOException(
          String.format("Failed to write metadata update json for: %s", metadataUpdate), e);
    }
  }

  public static void toJson(MetadataUpdate metadataUpdate, JsonGenerator generator)
      throws IOException {
    String updateAction = ACTIONS.get(metadataUpdate.getClass());

    // Provide better exception message than the NPE thrown by writing null for the update action,
    // which is required
    Preconditions.checkArgument(
        updateAction != null,
        "Cannot convert metadata update to json. Unrecognized metadata update type: {}",
        metadataUpdate.getClass().getName());

    generator.writeStartObject();
    generator.writeStringField(ACTION, updateAction);

    switch (updateAction) {
      case ASSIGN_UUID:
        writeAssignUUID((MetadataUpdate.AssignUUID) metadataUpdate, generator);
        break;
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
        writeSetDefaultPartitionSpec(
            (MetadataUpdate.SetDefaultPartitionSpec) metadataUpdate, generator);
        break;
      case ADD_SORT_ORDER:
        writeAddSortOrder((MetadataUpdate.AddSortOrder) metadataUpdate, generator);
        break;
      case SET_DEFAULT_SORT_ORDER:
        writeSetDefaultSortOrder((MetadataUpdate.SetDefaultSortOrder) metadataUpdate, generator);
        break;
      case ADD_SNAPSHOT:
        writeAddSnapshot((MetadataUpdate.AddSnapshot) metadataUpdate, generator);
        break;
      case REMOVE_SNAPSHOTS:
        writeRemoveSnapshots((MetadataUpdate.RemoveSnapshot) metadataUpdate, generator);
        break;
      case REMOVE_SNAPSHOT_REF:
        writeRemoveSnapshotRef((MetadataUpdate.RemoveSnapshotRef) metadataUpdate, generator);
        break;
      case SET_SNAPSHOT_REF:
        writeSetSnapshotRef((MetadataUpdate.SetSnapshotRef) metadataUpdate, generator);
        break;
      case SET_PROPERTIES:
        writeSetProperties((MetadataUpdate.SetProperties) metadataUpdate, generator);
        break;
      case REMOVE_PROPERTIES:
        writeRemoveProperties((MetadataUpdate.RemoveProperties) metadataUpdate, generator);
        break;
      case SET_LOCATION:
        writeSetLocation((MetadataUpdate.SetLocation) metadataUpdate, generator);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert metadata update to json. Unrecognized action: %s", updateAction));
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
    Preconditions.checkArgument(
        jsonNode != null && jsonNode.isObject(),
        "Cannot parse metadata update from non-object value: %s",
        jsonNode);
    Preconditions.checkArgument(
        jsonNode.hasNonNull(ACTION), "Cannot parse metadata update. Missing field: action");
    String action = JsonUtil.getString(ACTION, jsonNode).toLowerCase(Locale.ROOT);

    switch (action) {
      case ASSIGN_UUID:
        return readAssignUUID(jsonNode);
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
        return readSetDefaultSortOrder(jsonNode);
      case ADD_SNAPSHOT:
        return readAddSnapshot(jsonNode);
      case REMOVE_SNAPSHOTS:
        return readRemoveSnapshots(jsonNode);
      case REMOVE_SNAPSHOT_REF:
        return readRemoveSnapshotRef(jsonNode);
      case SET_SNAPSHOT_REF:
        return readSetSnapshotRef(jsonNode);
      case SET_PROPERTIES:
        return readSetProperties(jsonNode);
      case REMOVE_PROPERTIES:
        return readRemoveProperties(jsonNode);
      case SET_LOCATION:
        return readSetLocation(jsonNode);
      default:
        throw new UnsupportedOperationException(
            String.format("Cannot convert metadata update action to json: %s", action));
    }
  }

  private static void writeAssignUUID(MetadataUpdate.AssignUUID update, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(UUID, update.uuid());
  }

  private static void writeUpgradeFormatVersion(
      MetadataUpdate.UpgradeFormatVersion update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(FORMAT_VERSION, update.formatVersion());
  }

  private static void writeAddSchema(MetadataUpdate.AddSchema update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SCHEMA);
    SchemaParser.toJson(update.schema(), gen);
    gen.writeNumberField(LAST_COLUMN_ID, update.lastColumnId());
  }

  private static void writeSetCurrentSchema(
      MetadataUpdate.SetCurrentSchema update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(SCHEMA_ID, update.schemaId());
  }

  private static void writeAddPartitionSpec(
      MetadataUpdate.AddPartitionSpec update, JsonGenerator gen) throws IOException {
    gen.writeFieldName(SPEC);
    PartitionSpecParser.toJson(update.spec(), gen);
  }

  private static void writeSetDefaultPartitionSpec(
      MetadataUpdate.SetDefaultPartitionSpec update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(SPEC_ID, update.specId());
  }

  private static void writeAddSortOrder(MetadataUpdate.AddSortOrder update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SORT_ORDER);
    SortOrderParser.toJson(update.sortOrder(), gen);
  }

  private static void writeSetDefaultSortOrder(
      MetadataUpdate.SetDefaultSortOrder update, JsonGenerator gen) throws IOException {
    gen.writeNumberField(SORT_ORDER_ID, update.sortOrderId());
  }

  private static void writeAddSnapshot(MetadataUpdate.AddSnapshot update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(SNAPSHOT);
    SnapshotParser.toJson(update.snapshot(), gen);
  }

  // TODO - Reconcile the spec's set-based removal with the current class implementation that only
  // handles one value.
  private static void writeRemoveSnapshots(MetadataUpdate.RemoveSnapshot update, JsonGenerator gen)
      throws IOException {
    gen.writeArrayFieldStart(SNAPSHOT_IDS);
    for (long snapshotId : ImmutableSet.of(update.snapshotId())) {
      gen.writeNumber(snapshotId);
    }
    gen.writeEndArray();
  }

  private static void writeSetSnapshotRef(MetadataUpdate.SetSnapshotRef update, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(REF_NAME, update.name());
    gen.writeNumberField(SNAPSHOT_ID, update.snapshotId());
    gen.writeStringField(TYPE, update.type());
    JsonUtil.writeIntegerFieldIf(
        update.minSnapshotsToKeep() != null,
        MIN_SNAPSHOTS_TO_KEEP,
        update.minSnapshotsToKeep(),
        gen);
    JsonUtil.writeLongFieldIf(
        update.maxSnapshotAgeMs() != null, MAX_SNAPSHOT_AGE_MS, update.maxSnapshotAgeMs(), gen);
    JsonUtil.writeLongFieldIf(
        update.maxRefAgeMs() != null, MAX_REF_AGE_MS, update.maxRefAgeMs(), gen);
  }

  private static void writeRemoveSnapshotRef(
      MetadataUpdate.RemoveSnapshotRef update, JsonGenerator gen) throws IOException {
    gen.writeStringField(REF_NAME, update.name());
  }

  private static void writeSetProperties(MetadataUpdate.SetProperties update, JsonGenerator gen)
      throws IOException {
    gen.writeFieldName(UPDATED);
    gen.writeObject(update.updated());
  }

  private static void writeRemoveProperties(
      MetadataUpdate.RemoveProperties update, JsonGenerator gen) throws IOException {
    gen.writeFieldName(REMOVED);
    gen.writeObject(update.removed());
  }

  private static void writeSetLocation(MetadataUpdate.SetLocation update, JsonGenerator gen)
      throws IOException {
    gen.writeStringField(LOCATION, update.location());
  }

  private static MetadataUpdate readAssignUUID(JsonNode node) {
    String uuid = JsonUtil.getString(UUID, node);
    return new MetadataUpdate.AssignUUID(uuid);
  }

  private static MetadataUpdate readUpgradeFormatVersion(JsonNode node) {
    int formatVersion = JsonUtil.getInt(FORMAT_VERSION, node);
    return new MetadataUpdate.UpgradeFormatVersion(formatVersion);
  }

  private static MetadataUpdate readAddSchema(JsonNode node) {
    Preconditions.checkArgument(node.hasNonNull(SCHEMA), "Cannot parse missing field: schema");
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
    Preconditions.checkArgument(
        node.hasNonNull(SORT_ORDER), "Cannot parse missing field: sort-order");
    JsonNode sortOrderNode = node.get(SORT_ORDER);
    UnboundSortOrder sortOrder = SortOrderParser.fromJson(sortOrderNode);
    return new MetadataUpdate.AddSortOrder(sortOrder);
  }

  private static MetadataUpdate readSetDefaultSortOrder(JsonNode node) {
    int sortOrderId = JsonUtil.getInt(SORT_ORDER_ID, node);
    return new MetadataUpdate.SetDefaultSortOrder(sortOrderId);
  }

  private static MetadataUpdate readAddSnapshot(JsonNode node) {
    Preconditions.checkArgument(node.has(SNAPSHOT), "Cannot parse missing field: snapshot");
    Snapshot snapshot = SnapshotParser.fromJson(null, node.get(SNAPSHOT));
    return new MetadataUpdate.AddSnapshot(snapshot);
  }

  private static MetadataUpdate readRemoveSnapshots(JsonNode node) {
    Set<Long> snapshotIds = JsonUtil.getLongSetOrNull(SNAPSHOT_IDS, node);
    Preconditions.checkArgument(
        snapshotIds != null && snapshotIds.size() == 1,
        "Invalid set of snapshot ids to remove. Expected one value but received: %s",
        snapshotIds);
    Long snapshotId = Iterables.getOnlyElement(snapshotIds);
    return new MetadataUpdate.RemoveSnapshot(snapshotId);
  }

  private static MetadataUpdate readSetSnapshotRef(JsonNode node) {
    String refName = JsonUtil.getString(REF_NAME, node);
    long snapshotId = JsonUtil.getLong(SNAPSHOT_ID, node);
    SnapshotRefType type =
        SnapshotRefType.valueOf(JsonUtil.getString(TYPE, node).toUpperCase(Locale.ENGLISH));
    Integer minSnapshotsToKeep = JsonUtil.getIntOrNull(MIN_SNAPSHOTS_TO_KEEP, node);
    Long maxSnapshotAgeMs = JsonUtil.getLongOrNull(MAX_SNAPSHOT_AGE_MS, node);
    Long maxRefAgeMs = JsonUtil.getLongOrNull(MAX_REF_AGE_MS, node);
    return new MetadataUpdate.SetSnapshotRef(
        refName, snapshotId, type, minSnapshotsToKeep, maxSnapshotAgeMs, maxRefAgeMs);
  }

  private static MetadataUpdate readRemoveSnapshotRef(JsonNode node) {
    String refName = JsonUtil.getString(REF_NAME, node);
    return new MetadataUpdate.RemoveSnapshotRef(refName);
  }

  private static MetadataUpdate readSetProperties(JsonNode node) {
    Map<String, String> updated = JsonUtil.getStringMap(UPDATED, node);
    return new MetadataUpdate.SetProperties(updated);
  }

  private static MetadataUpdate readRemoveProperties(JsonNode node) {
    Set<String> removed = JsonUtil.getStringSet(REMOVED, node);
    return new MetadataUpdate.RemoveProperties(removed);
  }

  private static MetadataUpdate readSetLocation(JsonNode node) {
    String location = JsonUtil.getString(LOCATION, node);
    return new MetadataUpdate.SetLocation(location);
  }
}
