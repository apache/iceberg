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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Metadata for a table.
 */
public class TableMetadata implements Serializable {
  static final long INITIAL_SEQUENCE_NUMBER = 0;
  static final long INVALID_SEQUENCE_NUMBER = -1;
  static final int DEFAULT_TABLE_FORMAT_VERSION = 1;
  static final int SUPPORTED_TABLE_FORMAT_VERSION = 2;
  static final int INITIAL_SPEC_ID = 0;
  static final int INITIAL_SORT_ORDER_ID = 1;
  static final int INITIAL_SCHEMA_ID = 0;

  private static final long ONE_MINUTE = TimeUnit.MINUTES.toMillis(1);

  public static TableMetadata newTableMetadata(Schema schema,
                                               PartitionSpec spec,
                                               SortOrder sortOrder,
                                               String location,
                                               Map<String, String> properties) {
    int formatVersion = PropertyUtil.propertyAsInt(properties, TableProperties.FORMAT_VERSION,
        DEFAULT_TABLE_FORMAT_VERSION);
    return newTableMetadata(schema, spec, sortOrder, location, unreservedProperties(properties), formatVersion);
  }

  public static TableMetadata newTableMetadata(Schema schema,
                                               PartitionSpec spec,
                                               String location,
                                               Map<String, String> properties) {
    SortOrder sortOrder = SortOrder.unsorted();
    int formatVersion = PropertyUtil.propertyAsInt(properties, TableProperties.FORMAT_VERSION,
        DEFAULT_TABLE_FORMAT_VERSION);
    return newTableMetadata(schema, spec, sortOrder, location, unreservedProperties(properties), formatVersion);
  }

  private static Map<String, String> unreservedProperties(Map<String, String> rawProperties) {
    return rawProperties.entrySet().stream()
        .filter(e -> !TableProperties.RESERVED_PROPERTIES.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static TableMetadata newTableMetadata(Schema schema,
                                        PartitionSpec spec,
                                        SortOrder sortOrder,
                                        String location,
                                        Map<String, String> properties,
                                        int formatVersion) {
    Preconditions.checkArgument(properties.keySet().stream().noneMatch(TableProperties.RESERVED_PROPERTIES::contains),
        "Table properties should not contain reserved properties, but got %s", properties);

    // reassign all column ids to ensure consistency
    AtomicInteger lastColumnId = new AtomicInteger(0);
    Schema freshSchema = TypeUtil.assignFreshIds(INITIAL_SCHEMA_ID, schema, lastColumnId::incrementAndGet);

    // rebuild the partition spec using the new column ids
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(freshSchema)
        .withSpecId(INITIAL_SPEC_ID);
    for (PartitionField field : spec.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = schema.findColumnName(field.sourceId());
      // reassign all partition fields with fresh partition field Ids to ensure consistency
      specBuilder.add(
          freshSchema.findField(sourceName).fieldId(),
          field.name(),
          field.transform().toString());
    }
    PartitionSpec freshSpec = specBuilder.build();

    // rebuild the sort order using the new column ids
    int freshSortOrderId = sortOrder.isUnsorted() ? sortOrder.orderId() : INITIAL_SORT_ORDER_ID;
    SortOrder freshSortOrder = freshSortOrder(freshSortOrderId, freshSchema, sortOrder);

    // Validate the metrics configuration. Note: we only do this on new tables to we don't
    // break existing tables.
    MetricsConfig.fromProperties(properties).validateReferencedColumns(schema);

    return new TableMetadata(null, formatVersion, UUID.randomUUID().toString(), location,
        INITIAL_SEQUENCE_NUMBER, System.currentTimeMillis(),
        lastColumnId.get(), freshSchema.schemaId(), ImmutableList.of(freshSchema),
        freshSpec.specId(), ImmutableList.of(freshSpec), freshSpec.lastAssignedFieldId(),
        freshSortOrderId, ImmutableList.of(freshSortOrder),
        ImmutableMap.copyOf(properties), -1, ImmutableList.of(),
        ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
  }

  public static class SnapshotLogEntry implements HistoryEntry {
    private final long timestampMillis;
    private final long snapshotId;

    SnapshotLogEntry(long timestampMillis, long snapshotId) {
      this.timestampMillis = timestampMillis;
      this.snapshotId = snapshotId;
    }

    @Override
    public long timestampMillis() {
      return timestampMillis;
    }

    @Override
    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (!(other instanceof SnapshotLogEntry)) {
        return false;
      }
      SnapshotLogEntry that = (SnapshotLogEntry) other;
      return timestampMillis == that.timestampMillis && snapshotId == that.snapshotId;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(timestampMillis, snapshotId);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("timestampMillis", timestampMillis)
          .add("snapshotId", snapshotId)
          .toString();
    }
  }

  public static class MetadataLogEntry {
    private final long timestampMillis;
    private final String file;

    MetadataLogEntry(long timestampMillis, String file) {
      this.timestampMillis = timestampMillis;
      this.file = file;
    }

    public long timestampMillis() {
      return timestampMillis;
    }

    public String file() {
      return file;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (!(other instanceof MetadataLogEntry)) {
        return false;
      }
      MetadataLogEntry that = (MetadataLogEntry) other;
      return timestampMillis == that.timestampMillis &&
              java.util.Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(timestampMillis, file);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("timestampMillis", timestampMillis)
          .add("file", file)
          .toString();
    }
  }

  // stored metadata
  private final String metadataFileLocation;
  private final int formatVersion;
  private final String uuid;
  private final String location;
  private final long lastSequenceNumber;
  private final long lastUpdatedMillis;
  private final int lastColumnId;
  private final int currentSchemaId;
  private final List<Schema> schemas;
  private final int defaultSpecId;
  private final List<PartitionSpec> specs;
  private final int lastAssignedPartitionId;
  private final int defaultSortOrderId;
  private final List<SortOrder> sortOrders;
  private final Map<String, String> properties;
  private final long currentSnapshotId;
  private final List<Snapshot> snapshots;
  private final Map<Long, Snapshot> snapshotsById;
  private final Map<Integer, Schema> schemasById;
  private final Map<Integer, PartitionSpec> specsById;
  private final Map<Integer, SortOrder> sortOrdersById;
  private final List<HistoryEntry> snapshotLog;
  private final List<MetadataLogEntry> previousFiles;
  private final List<MetadataUpdate> changes;

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  TableMetadata(String metadataFileLocation,
                int formatVersion,
                String uuid,
                String location,
                long lastSequenceNumber,
                long lastUpdatedMillis,
                int lastColumnId,
                int currentSchemaId,
                List<Schema> schemas,
                int defaultSpecId,
                List<PartitionSpec> specs,
                int lastAssignedPartitionId,
                int defaultSortOrderId,
                List<SortOrder> sortOrders,
                Map<String, String> properties,
                long currentSnapshotId,
                List<Snapshot> snapshots,
                List<HistoryEntry> snapshotLog,
                List<MetadataLogEntry> previousFiles,
                List<MetadataUpdate> changes) {
    Preconditions.checkArgument(specs != null && !specs.isEmpty(), "Partition specs cannot be null or empty");
    Preconditions.checkArgument(sortOrders != null && !sortOrders.isEmpty(), "Sort orders cannot be null or empty");
    Preconditions.checkArgument(formatVersion <= SUPPORTED_TABLE_FORMAT_VERSION,
        "Unsupported format version: v%s", formatVersion);
    Preconditions.checkArgument(formatVersion == 1 || uuid != null,
        "UUID is required in format v%s", formatVersion);
    Preconditions.checkArgument(formatVersion > 1 || lastSequenceNumber == 0,
        "Sequence number must be 0 in v1: %s", lastSequenceNumber);
    Preconditions.checkArgument(metadataFileLocation == null || changes.isEmpty(),
        "Cannot create TableMetadata with a metadata location and changes");

    this.metadataFileLocation = metadataFileLocation;
    this.formatVersion = formatVersion;
    this.uuid = uuid;
    this.location = location;
    this.lastSequenceNumber = lastSequenceNumber;
    this.lastUpdatedMillis = lastUpdatedMillis;
    this.lastColumnId = lastColumnId;
    this.currentSchemaId = currentSchemaId;
    this.schemas = schemas;
    this.specs = specs;
    this.defaultSpecId = defaultSpecId;
    this.lastAssignedPartitionId = lastAssignedPartitionId;
    this.defaultSortOrderId = defaultSortOrderId;
    this.sortOrders = sortOrders;
    this.properties = properties;
    this.currentSnapshotId = currentSnapshotId;
    this.snapshots = snapshots;
    this.snapshotLog = snapshotLog;
    this.previousFiles = previousFiles;

    // changes are carried through until metadata is read from a file
    this.changes = changes;

    this.snapshotsById = indexAndValidateSnapshots(snapshots, lastSequenceNumber);
    this.schemasById = indexSchemas();
    this.specsById = indexSpecs(specs);
    this.sortOrdersById = indexSortOrders(sortOrders);

    HistoryEntry last = null;
    for (HistoryEntry logEntry : snapshotLog) {
      if (last != null) {
        Preconditions.checkArgument(
            (logEntry.timestampMillis() - last.timestampMillis()) >= -ONE_MINUTE,
            "[BUG] Expected sorted snapshot log entries.");
      }
      last = logEntry;
    }
    if (last != null) {
      Preconditions.checkArgument(
          // commits can happen concurrently from different machines.
          // A tolerance helps us avoid failure for small clock skew
          lastUpdatedMillis - last.timestampMillis() >= -ONE_MINUTE,
          "Invalid update timestamp %s: before last snapshot log entry at %s",
          lastUpdatedMillis, last.timestampMillis());
    }

    MetadataLogEntry previous = null;
    for (MetadataLogEntry metadataEntry : previousFiles) {
      if (previous != null) {
        Preconditions.checkArgument(
            // commits can happen concurrently from different machines.
            // A tolerance helps us avoid failure for small clock skew
            (metadataEntry.timestampMillis() - previous.timestampMillis()) >= -ONE_MINUTE,
            "[BUG] Expected sorted previous metadata log entries.");
      }
      previous = metadataEntry;
    }
      // Make sure that this update's lastUpdatedMillis is > max(previousFile's timestamp)
    if (previous != null) {
      Preconditions.checkArgument(
          // commits can happen concurrently from different machines.
          // A tolerance helps us avoid failure for small clock skew
          lastUpdatedMillis - previous.timestampMillis >= -ONE_MINUTE,
          "Invalid update timestamp %s: before the latest metadata log entry timestamp %s",
          lastUpdatedMillis, previous.timestampMillis);
    }

    Preconditions.checkArgument(
        currentSnapshotId < 0 || snapshotsById.containsKey(currentSnapshotId),
        "Invalid table metadata: Cannot find current version");
  }

  public int formatVersion() {
    return formatVersion;
  }

  public String metadataFileLocation() {
    return metadataFileLocation;
  }

  public String uuid() {
    return uuid;
  }

  public long lastSequenceNumber() {
    return lastSequenceNumber;
  }

  public long nextSequenceNumber() {
    return formatVersion > 1 ? lastSequenceNumber + 1 : INITIAL_SEQUENCE_NUMBER;
  }

  public long lastUpdatedMillis() {
    return lastUpdatedMillis;
  }

  public int lastColumnId() {
    return lastColumnId;
  }

  public Schema schema() {
    return schemasById.get(currentSchemaId);
  }

  public List<Schema> schemas() {
    return schemas;
  }

  public Map<Integer, Schema> schemasById() {
    return schemasById;
  }

  public int currentSchemaId() {
    return currentSchemaId;
  }

  public PartitionSpec spec() {
    return specsById.get(defaultSpecId);
  }

  public PartitionSpec spec(int id) {
    return specsById.get(id);
  }

  public List<PartitionSpec> specs() {
    return specs;
  }

  public Map<Integer, PartitionSpec> specsById() {
    return specsById;
  }

  int lastAssignedPartitionId() {
    return lastAssignedPartitionId;
  }

  public int defaultSpecId() {
    return defaultSpecId;
  }

  public int defaultSortOrderId() {
    return defaultSortOrderId;
  }

  public SortOrder sortOrder() {
    return sortOrdersById.get(defaultSortOrderId);
  }

  public List<SortOrder> sortOrders() {
    return sortOrders;
  }

  public Map<Integer, SortOrder> sortOrdersById() {
    return sortOrdersById;
  }

  public String location() {
    return location;
  }

  public Map<String, String> properties() {
    return properties;
  }

  public String property(String property, String defaultValue) {
    return properties.getOrDefault(property, defaultValue);
  }

  public boolean propertyAsBoolean(String property, boolean defaultValue) {
    return PropertyUtil.propertyAsBoolean(properties, property, defaultValue);
  }

  public int propertyAsInt(String property, int defaultValue) {
    return PropertyUtil.propertyAsInt(properties, property, defaultValue);
  }

  public long propertyAsLong(String property, long defaultValue) {
    return PropertyUtil.propertyAsLong(properties, property, defaultValue);
  }

  public Snapshot snapshot(long snapshotId) {
    return snapshotsById.get(snapshotId);
  }

  public Snapshot currentSnapshot() {
    return snapshotsById.get(currentSnapshotId);
  }

  public List<Snapshot> snapshots() {
    return snapshots;
  }

  public List<HistoryEntry> snapshotLog() {
    return snapshotLog;
  }

  public List<MetadataLogEntry> previousFiles() {
    return previousFiles;
  }

  public List<MetadataUpdate> changes() {
    return changes;
  }

  public TableMetadata withUUID() {
    return new Builder(this).assignUUID().build();
  }

  public TableMetadata updateSchema(Schema newSchema, int newLastColumnId) {
    return new Builder(this).setCurrentSchema(newSchema, newLastColumnId).build();
  }

  // The caller is responsible to pass a newPartitionSpec with correct partition field IDs
  public TableMetadata updatePartitionSpec(PartitionSpec newPartitionSpec) {
    return new Builder(this).setDefaultPartitionSpec(newPartitionSpec).build();
  }

  public TableMetadata replaceSortOrder(SortOrder newOrder) {
    return new Builder(this).setDefaultSortOrder(newOrder).build();
  }

  public TableMetadata addStagedSnapshot(Snapshot snapshot) {
    return new Builder(this).addSnapshot(snapshot).build();
  }

  public TableMetadata replaceCurrentSnapshot(Snapshot snapshot) {
    return new Builder(this).setCurrentSnapshot(snapshot).build();
  }

  public TableMetadata removeSnapshotsIf(Predicate<Snapshot> removeIf) {
    List<Snapshot> toRemove = snapshots.stream().filter(removeIf).collect(Collectors.toList());
    return new Builder(this).removeSnapshots(toRemove).build();
  }

  public TableMetadata replaceProperties(Map<String, String> rawProperties) {
    ValidationException.check(rawProperties != null, "Cannot set properties to null");
    Map<String, String> newProperties = unreservedProperties(rawProperties);

    Set<String> removed = Sets.newHashSet(properties.keySet());
    Map<String, String> updated = Maps.newHashMap();
    for (Map.Entry<String, String> entry : newProperties.entrySet()) {
      removed.remove(entry.getKey());
      String current = properties.get(entry.getKey());
      if (current == null || !current.equals(entry.getValue())) {
        updated.put(entry.getKey(), entry.getValue());
      }
    }

    int newFormatVersion = PropertyUtil.propertyAsInt(rawProperties, TableProperties.FORMAT_VERSION, formatVersion);

    return new Builder(this)
        .setProperties(updated)
        .removeProperties(removed)
        .upgradeFormatVersion(newFormatVersion)
        .build();
  }

  private PartitionSpec reassignPartitionIds(PartitionSpec partitionSpec, TypeUtil.NextID nextID) {
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(partitionSpec.schema())
        .withSpecId(partitionSpec.specId());

    if (formatVersion > 1) {
      // for v2 and later, reuse any existing field IDs, but reproduce the same spec
      Map<Pair<Integer, String>, Integer> transformToFieldId = specs.stream()
          .flatMap(spec -> spec.fields().stream())
          .collect(Collectors.toMap(
              field -> Pair.of(field.sourceId(), field.transform().toString()),
              PartitionField::fieldId,
              Math::max));

      for (PartitionField field : partitionSpec.fields()) {
        // reassign the partition field ids
        int partitionFieldId = transformToFieldId.computeIfAbsent(
            Pair.of(field.sourceId(), field.transform().toString()), k -> nextID.get());
        specBuilder.add(
            field.sourceId(),
            partitionFieldId,
            field.name(),
            field.transform());
      }

    } else {
      // for v1, preserve the existing spec and carry forward all fields, replacing missing fields with void
      Map<Pair<Integer, String>, PartitionField> newFields = Maps.newLinkedHashMap();
      for (PartitionField newField : partitionSpec.fields()) {
        newFields.put(Pair.of(newField.sourceId(), newField.transform().toString()), newField);
      }
      List<String> newFieldNames = newFields.values().stream().map(PartitionField::name).collect(Collectors.toList());

      for (PartitionField field : spec().fields()) {
        // ensure each field is either carried forward or replaced with void
        PartitionField newField = newFields.remove(Pair.of(field.sourceId(), field.transform().toString()));
        if (newField != null) {
          // copy the new field with the existing field ID
          specBuilder.add(newField.sourceId(), field.fieldId(), newField.name(), newField.transform());
        } else {
          // Rename old void transforms that would otherwise conflict
          String voidName = newFieldNames.contains(field.name()) ? field.name() + "_" + field.fieldId() : field.name();
          specBuilder.add(field.sourceId(), field.fieldId(), voidName, Transforms.alwaysNull());
        }
      }

      // add any remaining new fields at the end and assign new partition field IDs
      for (PartitionField newField : newFields.values()) {
        specBuilder.add(newField.sourceId(), nextID.get(), newField.name(), newField.transform());
      }
    }

    return specBuilder.build();
  }

  // The caller is responsible to pass a updatedPartitionSpec with correct partition field IDs
  public TableMetadata buildReplacement(Schema updatedSchema, PartitionSpec updatedPartitionSpec,
                                        SortOrder updatedSortOrder, String newLocation,
                                        Map<String, String> updatedProperties) {
    ValidationException.check(formatVersion > 1 || PartitionSpec.hasSequentialIds(updatedPartitionSpec),
        "Spec does not use sequential IDs that are required in v1: %s", updatedPartitionSpec);

    AtomicInteger newLastColumnId = new AtomicInteger(lastColumnId);
    Schema freshSchema = TypeUtil.assignFreshIds(updatedSchema, schema(), newLastColumnId::incrementAndGet);

    // rebuild the partition spec using the new column ids and reassign partition field ids to align with existing
    // partition specs in the table
    PartitionSpec freshSpec = reassignPartitionIds(
        freshSpec(INITIAL_SPEC_ID, freshSchema, updatedPartitionSpec),
        new AtomicInteger(lastAssignedPartitionId)::incrementAndGet);

    // rebuild the sort order using new column ids
    SortOrder freshSortOrder = freshSortOrder(INITIAL_SORT_ORDER_ID, freshSchema, updatedSortOrder);

    // check if there is format version override
    int newFormatVersion = PropertyUtil.propertyAsInt(updatedProperties, TableProperties.FORMAT_VERSION, formatVersion);

    return new Builder(this)
        .upgradeFormatVersion(newFormatVersion)
        .setCurrentSnapshot(null)
        .setCurrentSchema(freshSchema, newLastColumnId.get())
        .setDefaultPartitionSpec(freshSpec)
        .setDefaultSortOrder(freshSortOrder)
        .setLocation(newLocation)
        .setProperties(unreservedProperties(updatedProperties))
        .build();
  }

  public TableMetadata updateLocation(String newLocation) {
    return new Builder(this).setLocation(newLocation).build();
  }

  public TableMetadata upgradeToFormatVersion(int newFormatVersion) {
    return new Builder(this).upgradeFormatVersion(newFormatVersion).build();
  }

  private static PartitionSpec updateSpecSchema(Schema schema, PartitionSpec partitionSpec) {
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema)
        .withSpecId(partitionSpec.specId());

    // add all the fields to the builder. IDs should not change.
    for (PartitionField field : partitionSpec.fields()) {
      specBuilder.add(field.sourceId(), field.fieldId(), field.name(), field.transform());
    }

    // build without validation because the schema may have changed in a way that makes this spec invalid. the spec
    // should still be preserved so that older metadata can be interpreted.
    return specBuilder.buildUnchecked();
  }

  private static SortOrder updateSortOrderSchema(Schema schema, SortOrder sortOrder) {
    SortOrder.Builder builder = SortOrder.builderFor(schema).withOrderId(sortOrder.orderId());

    // add all the fields to the builder. IDs should not change.
    for (SortField field : sortOrder.fields()) {
      builder.addSortField(field.transform(), field.sourceId(), field.direction(), field.nullOrder());
    }

    // build without validation because the schema may have changed in a way that makes this order invalid. the order
    // should still be preserved so that older metadata can be interpreted.
    return builder.buildUnchecked();
  }

  private static PartitionSpec freshSpec(int specId, Schema schema, PartitionSpec partitionSpec) {
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema)
        .withSpecId(specId);

    for (PartitionField field : partitionSpec.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = partitionSpec.schema().findColumnName(field.sourceId());
      specBuilder.add(
          schema.findField(sourceName).fieldId(),
          field.fieldId(),
          field.name(),
          field.transform().toString());
    }

    return specBuilder.build();
  }

  private static SortOrder freshSortOrder(int orderId, Schema schema, SortOrder sortOrder) {
    SortOrder.Builder builder = SortOrder.builderFor(schema);

    if (sortOrder.isSorted()) {
      builder.withOrderId(orderId);
    }

    for (SortField field : sortOrder.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = sortOrder.schema().findColumnName(field.sourceId());
      // reassign all sort fields with fresh sort field IDs
      int newSourceId = schema.findField(sourceName).fieldId();
      builder.addSortField(
          field.transform().toString(),
          newSourceId,
          field.direction(),
          field.nullOrder());
    }

    return builder.build();
  }

  private static Map<Long, Snapshot> indexAndValidateSnapshots(List<Snapshot> snapshots, long lastSequenceNumber) {
    ImmutableMap.Builder<Long, Snapshot> builder = ImmutableMap.builder();
    for (Snapshot snap : snapshots) {
      ValidationException.check(snap.sequenceNumber() <= lastSequenceNumber,
          "Invalid snapshot with sequence number %s greater than last sequence number %s",
          snap.sequenceNumber(), lastSequenceNumber);
      builder.put(snap.snapshotId(), snap);
    }
    return builder.build();
  }

  private Map<Integer, Schema> indexSchemas() {
    ImmutableMap.Builder<Integer, Schema> builder = ImmutableMap.builder();
    for (Schema schema : schemas) {
      builder.put(schema.schemaId(), schema);
    }
    return builder.build();
  }

  private static Map<Integer, PartitionSpec> indexSpecs(List<PartitionSpec> specs) {
    ImmutableMap.Builder<Integer, PartitionSpec> builder = ImmutableMap.builder();
    for (PartitionSpec spec : specs) {
      builder.put(spec.specId(), spec);
    }
    return builder.build();
  }

  private static Map<Integer, SortOrder> indexSortOrders(List<SortOrder> sortOrders) {
    ImmutableMap.Builder<Integer, SortOrder> builder = ImmutableMap.builder();
    for (SortOrder sortOrder : sortOrders) {
      builder.put(sortOrder.orderId(), sortOrder);
    }
    return builder.build();
  }

  public static Builder buildFrom(TableMetadata base) {
    return new Builder(base);
  }

  public static class Builder {
    private final TableMetadata base;
    private int formatVersion;
    private String uuid;
    private Long lastUpdatedMillis;
    private String location;
    private long lastSequenceNumber;
    private int lastColumnId;
    private int currentSchemaId;
    private final List<Schema> schemas;
    private int defaultSpecId;
    private List<PartitionSpec> specs;
    private int lastAssignedPartitionId;
    private int defaultSortOrderId;
    private List<SortOrder> sortOrders;
    private final Map<String, String> properties;
    private long currentSnapshotId;
    private List<Snapshot> snapshots;

    // change tracking
    private final List<MetadataUpdate> changes;
    private final int startingChangeCount;
    private boolean discardChanges = false;

    // handled in build
    private final List<HistoryEntry> snapshotLog;
    private final String previousFileLocation;
    private final List<MetadataLogEntry> previousFiles;

    // indexes for convenience
    private final Map<Long, Snapshot> snapshotsById;
    private final Map<Integer, Schema> schemasById;
    private final Map<Integer, PartitionSpec> specsById;
    private final Map<Integer, SortOrder> sortOrdersById;

    private Builder(TableMetadata base) {
      this.base = base;
      this.formatVersion = base.formatVersion;
      this.uuid = base.uuid;
      this.lastUpdatedMillis = null;
      this.location = base.location;
      this.lastSequenceNumber = base.lastSequenceNumber;
      this.lastColumnId = base.lastColumnId;
      this.currentSchemaId = base.currentSchemaId;
      this.schemas = Lists.newArrayList(base.schemas);
      this.defaultSpecId = base.defaultSpecId;
      this.specs = Lists.newArrayList(base.specs);
      this.lastAssignedPartitionId = base.lastAssignedPartitionId;
      this.defaultSortOrderId = base.defaultSortOrderId;
      this.sortOrders = Lists.newArrayList(base.sortOrders);
      this.properties = Maps.newHashMap(base.properties);
      this.currentSnapshotId = base.currentSnapshotId;
      this.snapshots = Lists.newArrayList(base.snapshots);
      this.changes = Lists.newArrayList(base.changes);
      this.startingChangeCount = changes.size();

      this.snapshotLog = Lists.newArrayList(base.snapshotLog);
      this.previousFileLocation = base.metadataFileLocation;
      this.previousFiles = base.previousFiles;

      this.snapshotsById = Maps.newHashMap(base.snapshotsById);
      this.schemasById = Maps.newHashMap(base.schemasById);
      this.specsById = Maps.newHashMap(base.specsById);
      this.sortOrdersById = Maps.newHashMap(base.sortOrdersById);
    }

    public Builder assignUUID() {
      if (uuid == null) {
        this.uuid = UUID.randomUUID().toString();
        changes.add(new MetadataUpdate.AssignUUID(uuid));
      }

      return this;
    }

    public Builder upgradeFormatVersion(int newFormatVersion) {
      Preconditions.checkArgument(newFormatVersion <= SUPPORTED_TABLE_FORMAT_VERSION,
          "Cannot upgrade table to unsupported format version: v%s (supported: v%s)",
          newFormatVersion, SUPPORTED_TABLE_FORMAT_VERSION);
      Preconditions.checkArgument(newFormatVersion >= formatVersion,
          "Cannot downgrade v%s table to v%s", formatVersion, newFormatVersion);

      if (newFormatVersion == formatVersion) {
        return this;
      }

      this.formatVersion = newFormatVersion;
      changes.add(new MetadataUpdate.UpgradeFormatVersion(newFormatVersion));

      return this;
    }

    public Builder setCurrentSchema(Schema newSchema, int newLastColumnId) {
      setCurrentSchema(addSchemaInternal(newSchema, newLastColumnId));
      return this;
    }

    public Builder setCurrentSchema(int schemaId) {
      if (currentSchemaId == schemaId) {
        return this;
      }

      Schema schema = schemasById.get(schemaId);
      Preconditions.checkArgument(schema != null, "Cannot set current schema to unknown schema: %s", schemaId);

      // rebuild all the partition specs and sort orders for the new current schema
      this.specs = Lists.newArrayList(Iterables.transform(specs,
          spec -> updateSpecSchema(schema, spec)));
      specsById.clear();
      specsById.putAll(indexSpecs(specs));

      this.sortOrders = Lists.newArrayList(Iterables.transform(sortOrders,
          order -> updateSortOrderSchema(schema, order)));
      sortOrdersById.clear();
      sortOrdersById.putAll(indexSortOrders(sortOrders));

      this.currentSchemaId = schemaId;

      changes.add(new MetadataUpdate.SetCurrentSchema(schemaId));

      return this;
    }

    public Builder addSchema(Schema schema, int newLastColumnId) {
      addSchemaInternal(schema, newLastColumnId);
      return this;
    }

    public Builder setDefaultPartitionSpec(PartitionSpec spec) {
      setDefaultPartitionSpec(addPartitionSpecInternal(spec));
      return this;
    }

    public Builder setDefaultPartitionSpec(int specId) {
      if (defaultSpecId == specId) {
        // the new spec is already current and no change is needed
        return this;
      }

      this.defaultSpecId = specId;
      changes.add(new MetadataUpdate.SetDefaultPartitionSpec(specId));

      return this;
    }

    public Builder addPartitionSpec(PartitionSpec spec) {
      addPartitionSpecInternal(spec);
      return this;
    }

    public Builder setDefaultSortOrder(SortOrder order) {
      setDefaultSortOrder(addSortOrderInternal(order));
      return this;
    }

    public Builder setDefaultSortOrder(int sortOrderId) {
      if (sortOrderId == defaultSortOrderId) {
        return this;
      }

      this.defaultSortOrderId = sortOrderId;
      changes.add(new MetadataUpdate.SetDefaultSortOrder(sortOrderId));

      return this;
    }

    public Builder addSortOrder(SortOrder order) {
      addSortOrderInternal(order);
      return this;
    }

    public Builder addSnapshot(Snapshot snapshot) {
      if (snapshot == null || snapshotsById.containsKey(snapshot.snapshotId())) {
        // change is a noop
        return this;
      }

      ValidationException.check(formatVersion == 1 || snapshot.sequenceNumber() > lastSequenceNumber,
          "Cannot add snapshot with sequence number %s older than last sequence number %s",
          snapshot.sequenceNumber(), lastSequenceNumber);

      this.lastUpdatedMillis = snapshot.timestampMillis();
      this.lastSequenceNumber = snapshot.sequenceNumber();
      snapshots.add(snapshot);
      snapshotsById.put(snapshot.snapshotId(), snapshot);
      changes.add(new MetadataUpdate.AddSnapshot(snapshot));

      return this;
    }

    public Builder setCurrentSnapshot(Snapshot snapshot) {
      addSnapshot(snapshot);
      setCurrentSnapshot(snapshot, null);
      return this;
    }

    public Builder setCurrentSnapshot(long snapshotId) {
      if (currentSnapshotId == snapshotId) {
        // change is a noop
        return this;
      }

      Snapshot snapshot = snapshotsById.get(snapshotId);
      ValidationException.check(snapshot != null,
          "Cannot set current snapshot to unknown: %s", snapshotId);

      setCurrentSnapshot(snapshot, System.currentTimeMillis());

      return this;
    }

    public Builder removeSnapshots(List<Snapshot> snapshotsToRemove) {
      Set<Long> idsToRemove = snapshotsToRemove.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());

      List<Snapshot> retainedSnapshots = Lists.newArrayListWithExpectedSize(snapshots.size() - idsToRemove.size());
      for (Snapshot snapshot : snapshots) {
        long snapshotId = snapshot.snapshotId();
        if (idsToRemove.contains(snapshotId)) {
          snapshotsById.remove(snapshotId);
          changes.add(new MetadataUpdate.RemoveSnapshot(snapshotId));
        } else {
          retainedSnapshots.add(snapshot);
        }
      }

      this.snapshots = retainedSnapshots;
      if (!snapshotsById.containsKey(currentSnapshotId)) {
        setCurrentSnapshot(null, System.currentTimeMillis());
      }

      return this;
    }

    public Builder setProperties(Map<String, String> updated) {
      if (updated.isEmpty()) {
        return this;
      }

      properties.putAll(updated);
      changes.add(new MetadataUpdate.SetProperties(updated));

      return this;
    }

    public Builder removeProperties(Set<String> removed) {
      if (removed.isEmpty()) {
        return this;
      }

      removed.forEach(properties::remove);
      changes.add(new MetadataUpdate.RemoveProperties(removed));

      return this;
    }

    public Builder setLocation(String newLocation) {
      if (location != null && location.equals(newLocation)) {
        return this;
      }

      this.location = newLocation;
      changes.add(new MetadataUpdate.SetLocation(newLocation));

      return this;
    }

    public Builder discardChanges() {
      this.discardChanges = true;
      return this;
    }

    public TableMetadata build() {
      if (changes.size() == startingChangeCount && !(discardChanges && changes.size() > 0)) {
        return base;
      }

      if (lastUpdatedMillis == null) {
        this.lastUpdatedMillis = System.currentTimeMillis();
      }

      Schema schema = schemasById.get(currentSchemaId);
      PartitionSpec.checkCompatibility(specsById.get(defaultSpecId), schema);
      SortOrder.checkCompatibility(sortOrdersById.get(defaultSortOrderId), schema);

      List<MetadataLogEntry> metadataHistory = addPreviousFile(
          previousFiles, previousFileLocation, base.lastUpdatedMillis(), properties);
      List<HistoryEntry> newSnapshotLog = updateSnapshotLog(snapshotLog, snapshotsById, currentSnapshotId, changes);

      return new TableMetadata(
          null,
          formatVersion,
          uuid,
          location,
          lastSequenceNumber,
          lastUpdatedMillis,
          lastColumnId,
          currentSchemaId,
          ImmutableList.copyOf(schemas),
          defaultSpecId,
          ImmutableList.copyOf(specs),
          lastAssignedPartitionId,
          defaultSortOrderId,
          ImmutableList.copyOf(sortOrders),
          ImmutableMap.copyOf(properties),
          currentSnapshotId,
          ImmutableList.copyOf(snapshots),
          ImmutableList.copyOf(newSnapshotLog),
          ImmutableList.copyOf(metadataHistory),
          discardChanges ? ImmutableList.of() : ImmutableList.copyOf(changes)
      );
    }

    private int addSchemaInternal(Schema schema, int newLastColumnId) {
      Preconditions.checkArgument(newLastColumnId >= lastColumnId,
          "Invalid last column ID: %s < %s (previous last column ID)", newLastColumnId, lastColumnId);

      int newSchemaId = reuseOrCreateNewSchemaId(schema);
      boolean schemaFound = schemasById.containsKey(newSchemaId);
      if (schemaFound && newLastColumnId == lastColumnId) {
        // the new spec and last column id is already current and no change is needed
        return newSchemaId;
      }

      this.lastColumnId = newLastColumnId;

      Schema newSchema;
      if (newSchemaId != schema.schemaId()) {
        newSchema = new Schema(newSchemaId, schema.columns(), schema.identifierFieldIds());
      } else {
        newSchema = schema;
      }

      if (!schemaFound) {
        schemas.add(newSchema);
        schemasById.put(newSchema.schemaId(), newSchema);
      }

      changes.add(new MetadataUpdate.AddSchema(newSchema, lastColumnId));

      return newSchemaId;
    }

    private int reuseOrCreateNewSchemaId(Schema newSchema) {
      // if the schema already exists, use its id; otherwise use the highest id + 1
      int newSchemaId = currentSchemaId;
      for (Schema schema : schemas) {
        if (schema.sameSchema(newSchema)) {
          return schema.schemaId();
        } else if (schema.schemaId() >= newSchemaId) {
          newSchemaId = schema.schemaId() + 1;
        }
      }
      return newSchemaId;
    }

    private int addPartitionSpecInternal(PartitionSpec spec) {
      int newSpecId = reuseOrCreateNewSpecId(spec);
      if (specsById.containsKey(newSpecId)) {
        return newSpecId;
      }

      Schema schema = schemasById.get(currentSchemaId);
      PartitionSpec.checkCompatibility(spec, schema);
      ValidationException.check(formatVersion > 1 || PartitionSpec.hasSequentialIds(spec),
          "Spec does not use sequential IDs that are required in v1: %s", spec);

      PartitionSpec newSpec = freshSpec(newSpecId, schema, spec);
      this.lastAssignedPartitionId = Math.max(lastAssignedPartitionId, newSpec.lastAssignedFieldId());
      specs.add(newSpec);
      specsById.put(newSpecId, newSpec);

      changes.add(new MetadataUpdate.AddPartitionSpec(newSpec));

      return newSpecId;
    }

    private int reuseOrCreateNewSpecId(PartitionSpec newSpec) {
      // if the spec already exists, use the same ID. otherwise, use 1 more than the highest ID.
      int newSpecId = INITIAL_SPEC_ID;
      for (PartitionSpec spec : specs) {
        if (newSpec.compatibleWith(spec)) {
          return spec.specId();
        } else if (newSpecId <= spec.specId()) {
          newSpecId = spec.specId() + 1;
        }
      }

      return newSpecId;
    }

    private int addSortOrderInternal(SortOrder order) {
      int newOrderId = reuseOrCreateNewSortOrderId(order);
      if (sortOrdersById.containsKey(newOrderId)) {
        return newOrderId;
      }

      Schema schema = schemasById.get(currentSchemaId);
      SortOrder.checkCompatibility(order, schema);

      SortOrder newOrder;
      if (order.isUnsorted()) {
        newOrder = SortOrder.unsorted();
      } else {
        // rebuild the sort order using new column ids
        newOrder = freshSortOrder(newOrderId, schema, order);
      }

      sortOrders.add(newOrder);
      sortOrdersById.put(newOrderId, newOrder);

      changes.add(new MetadataUpdate.AddSortOrder(newOrder));

      return newOrderId;
    }

    private int reuseOrCreateNewSortOrderId(SortOrder newOrder) {
      if (newOrder.isUnsorted()) {
        return SortOrder.unsorted().orderId();
      }

      // determine the next order id
      int newOrderId = INITIAL_SORT_ORDER_ID;
      for (SortOrder order : sortOrders) {
        if (order.sameOrder(newOrder)) {
          return order.orderId();
        } else if (newOrderId <= order.orderId()) {
          newOrderId = order.orderId() + 1;
        }
      }

      return newOrderId;
    }

    private void setCurrentSnapshot(Snapshot snapshot, Long currentTimestampMillis) {
      if (snapshot == null) {
        this.currentSnapshotId = -1;
        snapshotLog.clear();
        changes.add(new MetadataUpdate.SetCurrentSnapshot(null));
        return;
      }

      if (currentSnapshotId == snapshot.snapshotId()) {
        return;
      }

      ValidationException.check(formatVersion == 1 || snapshot.sequenceNumber() <= lastSequenceNumber,
          "Last sequence number %s is less than existing snapshot sequence number %s",
          lastSequenceNumber, snapshot.sequenceNumber());

      this.lastUpdatedMillis = currentTimestampMillis != null ? currentTimestampMillis : snapshot.timestampMillis();
      this.currentSnapshotId = snapshot.snapshotId();
      snapshotLog.add(new SnapshotLogEntry(lastUpdatedMillis, snapshot.snapshotId()));
      changes.add(new MetadataUpdate.SetCurrentSnapshot(snapshot.snapshotId()));
    }

    private static List<MetadataLogEntry> addPreviousFile(
        List<MetadataLogEntry> previousFiles, String previousFileLocation, long timestampMillis,
        Map<String, String> properties) {
      if (previousFileLocation == null) {
        return previousFiles;
      }

      int maxSize = Math.max(1, PropertyUtil.propertyAsInt(properties,
          TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, TableProperties.METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT));

      List<MetadataLogEntry> newMetadataLog;
      if (previousFiles.size() >= maxSize) {
        int removeIndex = previousFiles.size() - maxSize + 1;
        newMetadataLog = Lists.newArrayList(previousFiles.subList(removeIndex, previousFiles.size()));
      } else {
        newMetadataLog = Lists.newArrayList(previousFiles);
      }
      newMetadataLog.add(new MetadataLogEntry(timestampMillis, previousFileLocation));

      return newMetadataLog;
    }

    /**
     * Finds intermediate snapshots that have not been committed as the current snapshot.
     *
     * @return a set of snapshot ids for all added snapshots that were later replaced as the current snapshot in changes
     */
    private static Set<Long> intermediateSnapshotIdSet(List<MetadataUpdate> changes, long currentSnapshotId) {
      Set<Long> addedSnapshotIds = Sets.newHashSet();
      Set<Long> intermediateSnapshotIds = Sets.newHashSet();
      for (MetadataUpdate update : changes) {
        if (update instanceof MetadataUpdate.AddSnapshot) {
          // adds must always come before set current snapshot
          MetadataUpdate.AddSnapshot addSnapshot = (MetadataUpdate.AddSnapshot) update;
          addedSnapshotIds.add(addSnapshot.snapshot().snapshotId());
        } else if (update instanceof MetadataUpdate.SetCurrentSnapshot) {
          Long snapshotId = ((MetadataUpdate.SetCurrentSnapshot) update).snapshotId();
          if (snapshotId != null && addedSnapshotIds.contains(snapshotId) && snapshotId != currentSnapshotId) {
            intermediateSnapshotIds.add(snapshotId);
          }
        }
      }

      return intermediateSnapshotIds;
    }

    private static List<HistoryEntry> updateSnapshotLog(
        List<HistoryEntry> snapshotLog, Map<Long, Snapshot> snapshotsById, long currentSnapshotId,
        List<MetadataUpdate> changes) {
      // find intermediate snapshots to suppress incorrect entries in the snapshot log.
      //
      // transactions can create snapshots that are never the current snapshot because several changes are combined
      // by the transaction into one table metadata update. when each intermediate snapshot is added to table metadata,
      // it is added to the snapshot log, assuming that it will be the current snapshot. when there are multiple
      // snapshot updates, the log must be corrected by suppressing the intermediate snapshot entries.
      //
      // a snapshot is an intermediate snapshot if it was added but is not the current snapshot.
      Set<Long> intermediateSnapshotIds = intermediateSnapshotIdSet(changes, currentSnapshotId);

      // update the snapshot log
      List<HistoryEntry> newSnapshotLog = Lists.newArrayList();
      for (HistoryEntry logEntry : snapshotLog) {
        long snapshotId = logEntry.snapshotId();
        if (snapshotsById.containsKey(snapshotId) && !intermediateSnapshotIds.contains(snapshotId)) {
          // copy the log entries that are still valid
          newSnapshotLog.add(logEntry);
        } else {
          // any invalid entry causes the history before it to be removed. otherwise, there could be
          // history gaps that cause time-travel queries to produce incorrect results. for example,
          // if history is [(t1, s1), (t2, s2), (t3, s3)] and s2 is removed, the history cannot be
          // [(t1, s1), (t3, s3)] because it appears that s3 was current during the time between t2
          // and t3 when in fact s2 was the current snapshot.
          newSnapshotLog.clear();
        }
      }

      if (snapshotsById.get(currentSnapshotId) != null) {
        ValidationException.check(Iterables.getLast(newSnapshotLog).snapshotId() == currentSnapshotId,
            "Cannot set invalid snapshot log: latest entry is not the current snapshot");
      }

      return newSnapshotLog;
    }
  }
}
