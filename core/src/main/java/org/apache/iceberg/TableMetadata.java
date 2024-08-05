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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableSupplier;

/** Metadata for a table. */
public class TableMetadata implements Serializable {
  static final long INITIAL_SEQUENCE_NUMBER = 0;
  static final long INVALID_SEQUENCE_NUMBER = -1;
  static final int DEFAULT_TABLE_FORMAT_VERSION = 2;
  static final int SUPPORTED_TABLE_FORMAT_VERSION = 3;
  static final int INITIAL_SPEC_ID = 0;
  static final int INITIAL_SORT_ORDER_ID = 1;
  static final int INITIAL_SCHEMA_ID = 0;

  private static final long ONE_MINUTE = TimeUnit.MINUTES.toMillis(1);

  public static TableMetadata newTableMetadata(
      Schema schema,
      PartitionSpec spec,
      SortOrder sortOrder,
      String location,
      Map<String, String> properties) {
    int formatVersion =
        PropertyUtil.propertyAsInt(
            properties, TableProperties.FORMAT_VERSION, DEFAULT_TABLE_FORMAT_VERSION);
    return newTableMetadata(
        schema, spec, sortOrder, location, persistedProperties(properties), formatVersion);
  }

  public static TableMetadata newTableMetadata(
      Schema schema, PartitionSpec spec, String location, Map<String, String> properties) {
    return newTableMetadata(schema, spec, SortOrder.unsorted(), location, properties);
  }

  private static Map<String, String> unreservedProperties(Map<String, String> rawProperties) {
    return rawProperties.entrySet().stream()
        .filter(e -> !TableProperties.RESERVED_PROPERTIES.contains(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static Map<String, String> persistedProperties(Map<String, String> rawProperties) {
    Map<String, String> persistedProperties = Maps.newHashMap();

    // explicitly set defaults that apply only to new tables
    persistedProperties.put(
        TableProperties.PARQUET_COMPRESSION,
        TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0);

    rawProperties.entrySet().stream()
        .filter(entry -> !TableProperties.RESERVED_PROPERTIES.contains(entry.getKey()))
        .forEach(entry -> persistedProperties.put(entry.getKey(), entry.getValue()));

    return persistedProperties;
  }

  static TableMetadata newTableMetadata(
      Schema schema,
      PartitionSpec spec,
      SortOrder sortOrder,
      String location,
      Map<String, String> properties,
      int formatVersion) {
    Preconditions.checkArgument(
        properties.keySet().stream().noneMatch(TableProperties.RESERVED_PROPERTIES::contains),
        "Table properties should not contain reserved properties, but got %s",
        properties);

    // reassign all column ids to ensure consistency
    AtomicInteger lastColumnId = new AtomicInteger(0);
    Schema freshSchema =
        TypeUtil.assignFreshIds(INITIAL_SCHEMA_ID, schema, lastColumnId::incrementAndGet);

    // rebuild the partition spec using the new column ids
    PartitionSpec.Builder specBuilder =
        PartitionSpec.builderFor(freshSchema).withSpecId(INITIAL_SPEC_ID);
    for (PartitionField field : spec.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = schema.findColumnName(field.sourceId());
      // reassign all partition fields with fresh partition field Ids to ensure consistency
      specBuilder.add(freshSchema.findField(sourceName).fieldId(), field.name(), field.transform());
    }
    PartitionSpec freshSpec = specBuilder.build();

    // rebuild the sort order using the new column ids
    int freshSortOrderId = sortOrder.isUnsorted() ? sortOrder.orderId() : INITIAL_SORT_ORDER_ID;
    SortOrder freshSortOrder = freshSortOrder(freshSortOrderId, freshSchema, sortOrder);

    // Validate the metrics configuration. Note: we only do this on new tables to we don't
    // break existing tables.
    MetricsConfig.fromProperties(properties).validateReferencedColumns(schema);

    return new Builder()
        .setInitialFormatVersion(formatVersion)
        .setCurrentSchema(freshSchema, lastColumnId.get())
        .setDefaultPartitionSpec(freshSpec)
        .setDefaultSortOrder(freshSortOrder)
        .setLocation(location)
        .setProperties(properties)
        .build();
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
      return timestampMillis == that.timestampMillis && java.util.Objects.equals(file, that.file);
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
  private final Map<Integer, Schema> schemasById;
  private final Map<Integer, PartitionSpec> specsById;
  private final Map<Integer, SortOrder> sortOrdersById;
  private final List<HistoryEntry> snapshotLog;
  private final List<MetadataLogEntry> previousFiles;
  private final List<StatisticsFile> statisticsFiles;
  private final List<PartitionStatisticsFile> partitionStatisticsFiles;
  private final List<MetadataUpdate> changes;
  private SerializableSupplier<List<Snapshot>> snapshotsSupplier;
  private volatile List<Snapshot> snapshots;
  private volatile Map<Long, Snapshot> snapshotsById;
  private volatile Map<String, SnapshotRef> refs;
  private volatile boolean snapshotsLoaded;

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  TableMetadata(
      String metadataFileLocation,
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
      SerializableSupplier<List<Snapshot>> snapshotsSupplier,
      List<HistoryEntry> snapshotLog,
      List<MetadataLogEntry> previousFiles,
      Map<String, SnapshotRef> refs,
      List<StatisticsFile> statisticsFiles,
      List<PartitionStatisticsFile> partitionStatisticsFiles,
      List<MetadataUpdate> changes) {
    Preconditions.checkArgument(
        specs != null && !specs.isEmpty(), "Partition specs cannot be null or empty");
    Preconditions.checkArgument(
        sortOrders != null && !sortOrders.isEmpty(), "Sort orders cannot be null or empty");
    Preconditions.checkArgument(
        formatVersion <= SUPPORTED_TABLE_FORMAT_VERSION,
        "Unsupported format version: v%s",
        formatVersion);
    Preconditions.checkArgument(
        formatVersion == 1 || uuid != null, "UUID is required in format v%s", formatVersion);
    Preconditions.checkArgument(
        formatVersion > 1 || lastSequenceNumber == 0,
        "Sequence number must be 0 in v1: %s",
        lastSequenceNumber);
    Preconditions.checkArgument(
        metadataFileLocation == null || changes.isEmpty(),
        "Cannot create TableMetadata with a metadata location and changes");

    this.metadataFileLocation = metadataFileLocation;
    this.formatVersion = formatVersion;
    this.uuid = uuid;
    this.location = location != null ? LocationUtil.stripTrailingSlash(location) : null;
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
    this.snapshotsSupplier = snapshotsSupplier;
    this.snapshotsLoaded = snapshotsSupplier == null;
    this.snapshotLog = snapshotLog;
    this.previousFiles = previousFiles;

    // changes are carried through until metadata is read from a file
    this.changes = changes;

    this.snapshotsById = indexAndValidateSnapshots(snapshots, lastSequenceNumber);
    this.schemasById = indexSchemas();
    this.specsById = indexSpecs(specs);
    this.sortOrdersById = indexSortOrders(sortOrders);
    this.refs = validateRefs(currentSnapshotId, refs, snapshotsById);
    this.statisticsFiles = ImmutableList.copyOf(statisticsFiles);
    this.partitionStatisticsFiles = ImmutableList.copyOf(partitionStatisticsFiles);

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
          lastUpdatedMillis,
          last.timestampMillis());
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
          lastUpdatedMillis,
          previous.timestampMillis);
    }

    validateCurrentSnapshot();
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

  public int lastAssignedPartitionId() {
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
    if (!snapshotsById.containsKey(snapshotId)) {
      ensureSnapshotsLoaded();
    }

    return snapshotsById.get(snapshotId);
  }

  public Snapshot currentSnapshot() {
    return snapshotsById.get(currentSnapshotId);
  }

  public List<Snapshot> snapshots() {
    ensureSnapshotsLoaded();

    return snapshots;
  }

  private synchronized void ensureSnapshotsLoaded() {
    if (!snapshotsLoaded) {
      List<Snapshot> loadedSnapshots = Lists.newArrayList(snapshotsSupplier.get());
      loadedSnapshots.removeIf(s -> s.sequenceNumber() > lastSequenceNumber);

      this.snapshots = ImmutableList.copyOf(loadedSnapshots);
      this.snapshotsById = indexAndValidateSnapshots(snapshots, lastSequenceNumber);
      validateCurrentSnapshot();

      this.refs = validateRefs(currentSnapshotId, refs, snapshotsById);

      this.snapshotsLoaded = true;
      this.snapshotsSupplier = null;
    }
  }

  public SnapshotRef ref(String name) {
    return refs.get(name);
  }

  public Map<String, SnapshotRef> refs() {
    return refs;
  }

  public List<StatisticsFile> statisticsFiles() {
    return statisticsFiles;
  }

  public List<PartitionStatisticsFile> partitionStatisticsFiles() {
    return partitionStatisticsFiles;
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

  public TableMetadata addPartitionSpec(PartitionSpec newPartitionSpec) {
    return new Builder(this).addPartitionSpec(newPartitionSpec).build();
  }

  public TableMetadata replaceSortOrder(SortOrder newOrder) {
    return new Builder(this).setDefaultSortOrder(newOrder).build();
  }

  public TableMetadata removeSnapshotsIf(Predicate<Snapshot> removeIf) {
    List<Snapshot> toRemove = snapshots().stream().filter(removeIf).collect(Collectors.toList());
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

    int newFormatVersion =
        PropertyUtil.propertyAsInt(rawProperties, TableProperties.FORMAT_VERSION, formatVersion);

    return new Builder(this)
        .setProperties(updated)
        .removeProperties(removed)
        .upgradeFormatVersion(newFormatVersion)
        .build();
  }

  private void validateCurrentSnapshot() {
    Preconditions.checkArgument(
        currentSnapshotId < 0 || snapshotsById.containsKey(currentSnapshotId),
        "Invalid table metadata: Cannot find current version");
  }

  private PartitionSpec reassignPartitionIds(PartitionSpec partitionSpec, TypeUtil.NextID nextID) {
    PartitionSpec.Builder specBuilder =
        PartitionSpec.builderFor(partitionSpec.schema()).withSpecId(partitionSpec.specId());

    if (formatVersion > 1) {
      // for v2 and later, reuse any existing field IDs, but reproduce the same spec
      Map<Pair<Integer, String>, Integer> transformToFieldId =
          specs.stream()
              .flatMap(spec -> spec.fields().stream())
              .collect(
                  Collectors.toMap(
                      field -> Pair.of(field.sourceId(), field.transform().toString()),
                      PartitionField::fieldId,
                      Math::max));

      for (PartitionField field : partitionSpec.fields()) {
        // reassign the partition field ids
        int partitionFieldId =
            transformToFieldId.computeIfAbsent(
                Pair.of(field.sourceId(), field.transform().toString()), k -> nextID.get());
        specBuilder.add(field.sourceId(), partitionFieldId, field.name(), field.transform());
      }

    } else {
      // for v1, preserve the existing spec and carry forward all fields, replacing missing fields
      // with void
      Map<Pair<Integer, String>, PartitionField> newFields = Maps.newLinkedHashMap();
      for (PartitionField newField : partitionSpec.fields()) {
        newFields.put(Pair.of(newField.sourceId(), newField.transform().toString()), newField);
      }
      List<String> newFieldNames =
          newFields.values().stream().map(PartitionField::name).collect(Collectors.toList());

      for (PartitionField field : spec().fields()) {
        // ensure each field is either carried forward or replaced with void
        PartitionField newField =
            newFields.remove(Pair.of(field.sourceId(), field.transform().toString()));
        if (newField != null) {
          // copy the new field with the existing field ID
          specBuilder.add(
              newField.sourceId(), field.fieldId(), newField.name(), newField.transform());
        } else {
          // Rename old void transforms that would otherwise conflict
          String voidName =
              newFieldNames.contains(field.name())
                  ? field.name() + "_" + field.fieldId()
                  : field.name();
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
  public TableMetadata buildReplacement(
      Schema updatedSchema,
      PartitionSpec updatedPartitionSpec,
      SortOrder updatedSortOrder,
      String newLocation,
      Map<String, String> updatedProperties) {
    ValidationException.check(
        formatVersion > 1 || PartitionSpec.hasSequentialIds(updatedPartitionSpec),
        "Spec does not use sequential IDs that are required in v1: %s",
        updatedPartitionSpec);

    AtomicInteger newLastColumnId = new AtomicInteger(lastColumnId);
    Schema freshSchema =
        TypeUtil.assignFreshIds(updatedSchema, schema(), newLastColumnId::incrementAndGet);

    // rebuild the partition spec using the new column ids and reassign partition field ids to align
    // with existing
    // partition specs in the table
    PartitionSpec freshSpec =
        reassignPartitionIds(
            freshSpec(INITIAL_SPEC_ID, freshSchema, updatedPartitionSpec),
            new AtomicInteger(lastAssignedPartitionId)::incrementAndGet);

    // rebuild the sort order using new column ids
    SortOrder freshSortOrder = freshSortOrder(INITIAL_SORT_ORDER_ID, freshSchema, updatedSortOrder);

    // check if there is format version override
    int newFormatVersion =
        PropertyUtil.propertyAsInt(
            updatedProperties, TableProperties.FORMAT_VERSION, formatVersion);

    return new Builder(this)
        .upgradeFormatVersion(newFormatVersion)
        .resetMainBranch()
        .setCurrentSchema(freshSchema, newLastColumnId.get())
        .setDefaultPartitionSpec(freshSpec)
        .setDefaultSortOrder(freshSortOrder)
        .setLocation(newLocation)
        .setProperties(persistedProperties(updatedProperties))
        .build();
  }

  public TableMetadata updateLocation(String newLocation) {
    return new Builder(this).setLocation(newLocation).build();
  }

  public TableMetadata upgradeToFormatVersion(int newFormatVersion) {
    return new Builder(this).upgradeFormatVersion(newFormatVersion).build();
  }

  private static PartitionSpec updateSpecSchema(Schema schema, PartitionSpec partitionSpec) {
    PartitionSpec.Builder specBuilder =
        PartitionSpec.builderFor(schema).withSpecId(partitionSpec.specId());

    // add all the fields to the builder. IDs should not change.
    for (PartitionField field : partitionSpec.fields()) {
      specBuilder.add(field.sourceId(), field.fieldId(), field.name(), field.transform());
    }

    // build without validation because the schema may have changed in a way that makes this spec
    // invalid. the spec
    // should still be preserved so that older metadata can be interpreted.
    return specBuilder.buildUnchecked();
  }

  private static SortOrder updateSortOrderSchema(Schema schema, SortOrder sortOrder) {
    SortOrder.Builder builder = SortOrder.builderFor(schema).withOrderId(sortOrder.orderId());

    // add all the fields to the builder. IDs should not change.
    for (SortField field : sortOrder.fields()) {
      builder.addSortField(
          field.transform(), field.sourceId(), field.direction(), field.nullOrder());
    }

    // build without validation because the schema may have changed in a way that makes this order
    // invalid. the order
    // should still be preserved so that older metadata can be interpreted.
    return builder.buildUnchecked();
  }

  private static PartitionSpec freshSpec(int specId, Schema schema, PartitionSpec partitionSpec) {
    UnboundPartitionSpec.Builder specBuilder = UnboundPartitionSpec.builder().withSpecId(specId);

    for (PartitionField field : partitionSpec.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = partitionSpec.schema().findColumnName(field.sourceId());

      final int fieldId;
      if (sourceName != null) {
        fieldId = schema.findField(sourceName).fieldId();
      } else {
        // In the case of a null sourceName, the column has been deleted.
        // This only happens in V1 tables where the reference is still around as a void transform
        fieldId = field.sourceId();
      }
      specBuilder.addField(field.transform().toString(), fieldId, field.fieldId(), field.name());
    }

    return specBuilder.build().bind(schema);
  }

  private static SortOrder freshSortOrder(int orderId, Schema schema, SortOrder sortOrder) {
    UnboundSortOrder.Builder builder = UnboundSortOrder.builder();

    if (sortOrder.isSorted()) {
      builder.withOrderId(orderId);
    }

    for (SortField field : sortOrder.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = sortOrder.schema().findColumnName(field.sourceId());
      // reassign all sort fields with fresh sort field IDs
      int newSourceId = schema.findField(sourceName).fieldId();
      builder.addSortField(
          field.transform().toString(), newSourceId, field.direction(), field.nullOrder());
    }

    return builder.build().bind(schema);
  }

  private static Map<Long, Snapshot> indexAndValidateSnapshots(
      List<Snapshot> snapshots, long lastSequenceNumber) {
    ImmutableMap.Builder<Long, Snapshot> builder = ImmutableMap.builder();
    for (Snapshot snap : snapshots) {
      ValidationException.check(
          snap.sequenceNumber() <= lastSequenceNumber,
          "Invalid snapshot with sequence number %s greater than last sequence number %s",
          snap.sequenceNumber(),
          lastSequenceNumber);
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

  private static Map<String, SnapshotRef> validateRefs(
      Long currentSnapshotId,
      Map<String, SnapshotRef> inputRefs,
      Map<Long, Snapshot> snapshotsById) {
    for (SnapshotRef ref : inputRefs.values()) {
      Preconditions.checkArgument(
          snapshotsById.containsKey(ref.snapshotId()),
          "Snapshot for reference %s does not exist in the existing snapshots list",
          ref);
    }

    SnapshotRef main = inputRefs.get(SnapshotRef.MAIN_BRANCH);
    if (currentSnapshotId != -1) {
      Preconditions.checkArgument(
          main == null || currentSnapshotId == main.snapshotId(),
          "Current snapshot ID does not match main branch (%s != %s)",
          currentSnapshotId,
          main != null ? main.snapshotId() : null);
    } else {
      Preconditions.checkArgument(
          main == null, "Current snapshot is not set, but main branch exists: %s", main);
    }

    return inputRefs;
  }

  public static Builder buildFrom(TableMetadata base) {
    return new Builder(base);
  }

  public static Builder buildFromEmpty() {
    return new Builder(DEFAULT_TABLE_FORMAT_VERSION);
  }

  public static Builder buildFromEmpty(int formatVersion) {
    return new Builder(formatVersion);
  }

  public static class Builder {
    private static final int LAST_ADDED = -1;

    private final TableMetadata base;
    private String metadataLocation;
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
    private SerializableSupplier<List<Snapshot>> snapshotsSupplier;
    private final Map<String, SnapshotRef> refs;
    private final Map<Long, List<StatisticsFile>> statisticsFiles;
    private final Map<Long, List<PartitionStatisticsFile>> partitionStatisticsFiles;
    private boolean suppressHistoricalSnapshots = false;

    // change tracking
    private final List<MetadataUpdate> changes;
    private final int startingChangeCount;
    private boolean discardChanges = false;
    private Integer lastAddedSchemaId = null;
    private Integer lastAddedSpecId = null;
    private Integer lastAddedOrderId = null;

    // handled in build
    private final List<HistoryEntry> snapshotLog;
    private String previousFileLocation;
    private final List<MetadataLogEntry> previousFiles;

    // indexes for convenience
    private final Map<Long, Snapshot> snapshotsById;
    private final Map<Integer, Schema> schemasById;
    private final Map<Integer, PartitionSpec> specsById;
    private final Map<Integer, SortOrder> sortOrdersById;

    private Builder() {
      this(DEFAULT_TABLE_FORMAT_VERSION);
    }

    private Builder(int formatVersion) {
      this.base = null;
      this.formatVersion = formatVersion;
      this.lastSequenceNumber = INITIAL_SEQUENCE_NUMBER;
      this.uuid = UUID.randomUUID().toString();
      this.schemas = Lists.newArrayList();
      this.specs = Lists.newArrayList();
      this.sortOrders = Lists.newArrayList();
      this.properties = Maps.newHashMap();
      this.snapshots = Lists.newArrayList();
      this.currentSnapshotId = -1;
      this.changes = Lists.newArrayList();
      this.startingChangeCount = 0;
      this.snapshotLog = Lists.newArrayList();
      this.previousFiles = Lists.newArrayList();
      this.refs = Maps.newHashMap();
      this.statisticsFiles = Maps.newHashMap();
      this.partitionStatisticsFiles = Maps.newHashMap();
      this.snapshotsById = Maps.newHashMap();
      this.schemasById = Maps.newHashMap();
      this.specsById = Maps.newHashMap();
      this.sortOrdersById = Maps.newHashMap();
    }

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
      this.snapshots = Lists.newArrayList(base.snapshots());
      this.changes = Lists.newArrayList(base.changes);
      this.startingChangeCount = changes.size();

      this.snapshotLog = Lists.newArrayList(base.snapshotLog);
      this.previousFileLocation = base.metadataFileLocation;
      this.previousFiles = base.previousFiles;
      this.refs = Maps.newHashMap(base.refs);
      this.statisticsFiles = indexStatistics(base.statisticsFiles);
      this.partitionStatisticsFiles = indexPartitionStatistics(base.partitionStatisticsFiles);
      this.snapshotsById = Maps.newHashMap(base.snapshotsById);
      this.schemasById = Maps.newHashMap(base.schemasById);
      this.specsById = Maps.newHashMap(base.specsById);
      this.sortOrdersById = Maps.newHashMap(base.sortOrdersById);
    }

    public Builder withMetadataLocation(String newMetadataLocation) {
      this.metadataLocation = newMetadataLocation;
      return this;
    }

    public Builder assignUUID() {
      if (uuid == null) {
        this.uuid = UUID.randomUUID().toString();
        changes.add(new MetadataUpdate.AssignUUID(uuid));
      }

      return this;
    }

    public Builder assignUUID(String newUuid) {
      Preconditions.checkArgument(newUuid != null, "Cannot set uuid to null");

      if (!newUuid.equals(uuid)) {
        this.uuid = newUuid;
        changes.add(new MetadataUpdate.AssignUUID(uuid));
      }

      return this;
    }

    // it is only safe to set the format version directly while creating tables
    // in all other cases, use upgradeFormatVersion
    private Builder setInitialFormatVersion(int newFormatVersion) {
      Preconditions.checkArgument(
          newFormatVersion <= SUPPORTED_TABLE_FORMAT_VERSION,
          "Unsupported format version: v%s (supported: v%s)",
          newFormatVersion,
          SUPPORTED_TABLE_FORMAT_VERSION);
      this.formatVersion = newFormatVersion;
      return this;
    }

    public Builder upgradeFormatVersion(int newFormatVersion) {
      Preconditions.checkArgument(
          newFormatVersion <= SUPPORTED_TABLE_FORMAT_VERSION,
          "Cannot upgrade table to unsupported format version: v%s (supported: v%s)",
          newFormatVersion,
          SUPPORTED_TABLE_FORMAT_VERSION);
      Preconditions.checkArgument(
          newFormatVersion >= formatVersion,
          "Cannot downgrade v%s table to v%s",
          formatVersion,
          newFormatVersion);

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
      if (schemaId == -1) {
        ValidationException.check(
            lastAddedSchemaId != null, "Cannot set last added schema: no schema has been added");
        return setCurrentSchema(lastAddedSchemaId);
      }

      if (currentSchemaId == schemaId) {
        return this;
      }

      Schema schema = schemasById.get(schemaId);
      Preconditions.checkArgument(
          schema != null, "Cannot set current schema to unknown schema: %s", schemaId);

      // rebuild all the partition specs and sort orders for the new current schema
      this.specs =
          Lists.newArrayList(Iterables.transform(specs, spec -> updateSpecSchema(schema, spec)));
      specsById.clear();
      specsById.putAll(indexSpecs(specs));

      this.sortOrders =
          Lists.newArrayList(
              Iterables.transform(sortOrders, order -> updateSortOrderSchema(schema, order)));
      sortOrdersById.clear();
      sortOrdersById.putAll(indexSortOrders(sortOrders));

      this.currentSchemaId = schemaId;

      if (lastAddedSchemaId != null && lastAddedSchemaId == schemaId) {
        changes.add(new MetadataUpdate.SetCurrentSchema(LAST_ADDED));
      } else {
        changes.add(new MetadataUpdate.SetCurrentSchema(schemaId));
      }

      return this;
    }

    public Builder addSchema(Schema schema, int newLastColumnId) {
      // TODO: remove requirement for newLastColumnId
      addSchemaInternal(schema, newLastColumnId);
      return this;
    }

    public Builder setDefaultPartitionSpec(PartitionSpec spec) {
      setDefaultPartitionSpec(addPartitionSpecInternal(spec));
      return this;
    }

    public Builder setDefaultPartitionSpec(int specId) {
      if (specId == -1) {
        ValidationException.check(
            lastAddedSpecId != null, "Cannot set last added spec: no spec has been added");
        return setDefaultPartitionSpec(lastAddedSpecId);
      }

      if (defaultSpecId == specId) {
        // the new spec is already current and no change is needed
        return this;
      }

      this.defaultSpecId = specId;
      if (lastAddedSpecId != null && lastAddedSpecId == specId) {
        changes.add(new MetadataUpdate.SetDefaultPartitionSpec(LAST_ADDED));
      } else {
        changes.add(new MetadataUpdate.SetDefaultPartitionSpec(specId));
      }

      return this;
    }

    public Builder addPartitionSpec(UnboundPartitionSpec spec) {
      addPartitionSpecInternal(spec.bind(schemasById.get(currentSchemaId)));
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
      if (sortOrderId == -1) {
        ValidationException.check(
            lastAddedOrderId != null,
            "Cannot set last added sort order: no sort order has been added");
        return setDefaultSortOrder(lastAddedOrderId);
      }

      if (sortOrderId == defaultSortOrderId) {
        return this;
      }

      this.defaultSortOrderId = sortOrderId;
      if (lastAddedOrderId != null && lastAddedOrderId == sortOrderId) {
        changes.add(new MetadataUpdate.SetDefaultSortOrder(LAST_ADDED));
      } else {
        changes.add(new MetadataUpdate.SetDefaultSortOrder(sortOrderId));
      }

      return this;
    }

    public Builder addSortOrder(UnboundSortOrder order) {
      addSortOrderInternal(order.bind(schemasById.get(currentSchemaId)));
      return this;
    }

    public Builder addSortOrder(SortOrder order) {
      addSortOrderInternal(order);
      return this;
    }

    public Builder addSnapshot(Snapshot snapshot) {
      if (snapshot == null) {
        // change is a noop
        return this;
      }

      ValidationException.check(
          !schemas.isEmpty(), "Attempting to add a snapshot before a schema is added");
      ValidationException.check(
          !specs.isEmpty(), "Attempting to add a snapshot before a partition spec is added");
      ValidationException.check(
          !sortOrders.isEmpty(), "Attempting to add a snapshot before a sort order is added");

      ValidationException.check(
          !snapshotsById.containsKey(snapshot.snapshotId()),
          "Snapshot already exists for id: %s",
          snapshot.snapshotId());

      ValidationException.check(
          formatVersion == 1
              || snapshot.sequenceNumber() > lastSequenceNumber
              || snapshot.parentId() == null,
          "Cannot add snapshot with sequence number %s older than last sequence number %s",
          snapshot.sequenceNumber(),
          lastSequenceNumber);

      this.lastUpdatedMillis = snapshot.timestampMillis();
      this.lastSequenceNumber = snapshot.sequenceNumber();
      snapshots.add(snapshot);
      snapshotsById.put(snapshot.snapshotId(), snapshot);
      changes.add(new MetadataUpdate.AddSnapshot(snapshot));

      return this;
    }

    public Builder setSnapshotsSupplier(SerializableSupplier<List<Snapshot>> snapshotsSupplier) {
      this.snapshotsSupplier = snapshotsSupplier;
      return this;
    }

    public Builder setBranchSnapshot(Snapshot snapshot, String branch) {
      addSnapshot(snapshot);
      setBranchSnapshotInternal(snapshot, branch);
      return this;
    }

    public Builder setBranchSnapshot(long snapshotId, String branch) {
      SnapshotRef ref = refs.get(branch);
      if (ref != null && ref.snapshotId() == snapshotId) {
        // change is a noop
        return this;
      }

      Snapshot snapshot = snapshotsById.get(snapshotId);
      ValidationException.check(
          snapshot != null, "Cannot set %s to unknown snapshot: %s", branch, snapshotId);

      setBranchSnapshotInternal(snapshot, branch);

      return this;
    }

    public Builder setRef(String name, SnapshotRef ref) {
      SnapshotRef existingRef = refs.get(name);
      if (existingRef != null && existingRef.equals(ref)) {
        return this;
      }

      long snapshotId = ref.snapshotId();
      Snapshot snapshot = snapshotsById.get(snapshotId);
      ValidationException.check(
          snapshot != null, "Cannot set %s to unknown snapshot: %s", name, snapshotId);
      if (isAddedSnapshot(snapshotId)) {
        this.lastUpdatedMillis = snapshot.timestampMillis();
      }

      if (SnapshotRef.MAIN_BRANCH.equals(name)) {
        this.currentSnapshotId = ref.snapshotId();
        if (lastUpdatedMillis == null) {
          this.lastUpdatedMillis = System.currentTimeMillis();
        }

        snapshotLog.add(new SnapshotLogEntry(lastUpdatedMillis, ref.snapshotId()));
      }

      refs.put(name, ref);
      MetadataUpdate.SetSnapshotRef refUpdate =
          new MetadataUpdate.SetSnapshotRef(
              name,
              ref.snapshotId(),
              ref.type(),
              ref.minSnapshotsToKeep(),
              ref.maxSnapshotAgeMs(),
              ref.maxRefAgeMs());
      changes.add(refUpdate);
      return this;
    }

    public Builder removeRef(String name) {
      if (SnapshotRef.MAIN_BRANCH.equals(name)) {
        this.currentSnapshotId = -1;
        snapshotLog.clear();
      }

      SnapshotRef ref = refs.remove(name);
      if (ref != null) {
        changes.add(new MetadataUpdate.RemoveSnapshotRef(name));
      }

      return this;
    }

    private Builder resetMainBranch() {
      this.currentSnapshotId = -1;
      SnapshotRef ref = refs.remove(SnapshotRef.MAIN_BRANCH);
      if (ref != null) {
        changes.add(new MetadataUpdate.RemoveSnapshotRef(SnapshotRef.MAIN_BRANCH));
      }

      return this;
    }

    public Builder setStatistics(long snapshotId, StatisticsFile statisticsFile) {
      Preconditions.checkNotNull(statisticsFile, "statisticsFile is null");
      Preconditions.checkArgument(
          snapshotId == statisticsFile.snapshotId(),
          "snapshotId does not match: %s vs %s",
          snapshotId,
          statisticsFile.snapshotId());
      statisticsFiles.put(statisticsFile.snapshotId(), ImmutableList.of(statisticsFile));
      changes.add(new MetadataUpdate.SetStatistics(snapshotId, statisticsFile));
      return this;
    }

    public Builder removeStatistics(long snapshotId) {
      if (statisticsFiles.remove(snapshotId) == null) {
        return this;
      }
      changes.add(new MetadataUpdate.RemoveStatistics(snapshotId));
      return this;
    }

    /**
     * Suppresses snapshots that are historical, removing the metadata for lazy snapshot loading.
     *
     * <p>Note that the snapshots are not considered removed from metadata and no RemoveSnapshot
     * changes are created.
     *
     * <p>A snapshot is historical if no ref directly references its ID.
     *
     * @return this for method chaining
     */
    public Builder suppressHistoricalSnapshots() {
      this.suppressHistoricalSnapshots = true;
      Set<Long> refSnapshotIds =
          refs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());
      Set<Long> suppressedSnapshotIds = Sets.difference(snapshotsById.keySet(), refSnapshotIds);
      rewriteSnapshotsInternal(suppressedSnapshotIds, true);
      return this;
    }

    public Builder setPartitionStatistics(PartitionStatisticsFile file) {
      Preconditions.checkNotNull(file, "partition statistics file is null");
      partitionStatisticsFiles.put(file.snapshotId(), ImmutableList.of(file));
      changes.add(new MetadataUpdate.SetPartitionStatistics(file));
      return this;
    }

    public Builder removePartitionStatistics(long snapshotId) {
      if (partitionStatisticsFiles.remove(snapshotId) == null) {
        return this;
      }

      changes.add(new MetadataUpdate.RemovePartitionStatistics(snapshotId));
      return this;
    }

    public Builder removeSnapshots(List<Snapshot> snapshotsToRemove) {
      Set<Long> idsToRemove =
          snapshotsToRemove.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
      return removeSnapshots(idsToRemove);
    }

    public Builder removeSnapshots(Collection<Long> idsToRemove) {
      return rewriteSnapshotsInternal(idsToRemove, false);
    }

    /**
     * Rewrite this builder's snapshots by removing the snapshots for a list of IDs.
     *
     * <p>If suppress is true, changes are not created.
     *
     * @param idsToRemove collection of snapshot IDs to remove from this builder
     * @param suppress whether the operation is suppressing snapshots (retains history) or removing
     * @return this for method chaining
     */
    private Builder rewriteSnapshotsInternal(Collection<Long> idsToRemove, boolean suppress) {
      List<Snapshot> retainedSnapshots =
          Lists.newArrayListWithExpectedSize(snapshots.size() - idsToRemove.size());
      for (Snapshot snapshot : snapshots) {
        long snapshotId = snapshot.snapshotId();
        if (idsToRemove.contains(snapshotId)) {
          snapshotsById.remove(snapshotId);
          if (!suppress) {
            changes.add(new MetadataUpdate.RemoveSnapshot(snapshotId));
          }
          removeStatistics(snapshotId);
          removePartitionStatistics(snapshotId);
        } else {
          retainedSnapshots.add(snapshot);
        }
      }

      this.snapshots = retainedSnapshots;

      // remove any refs that are no longer valid
      Set<String> danglingRefs = Sets.newHashSet();
      for (Map.Entry<String, SnapshotRef> refEntry : refs.entrySet()) {
        if (!snapshotsById.containsKey(refEntry.getValue().snapshotId())) {
          danglingRefs.add(refEntry.getKey());
        }
      }

      danglingRefs.forEach(this::removeRef);

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

    public Builder setPreviousFileLocation(String previousFileLocation) {
      this.previousFileLocation = previousFileLocation;
      return this;
    }

    private boolean hasChanges() {
      return changes.size() != startingChangeCount
          || (discardChanges && !changes.isEmpty())
          || metadataLocation != null
          || suppressHistoricalSnapshots
          || null != snapshotsSupplier;
    }

    public TableMetadata build() {
      if (!hasChanges()) {
        return base;
      }

      if (lastUpdatedMillis == null) {
        this.lastUpdatedMillis = System.currentTimeMillis();
      }

      // when associated with a metadata file, table metadata must have no changes so that the
      // metadata matches exactly
      // what is in the metadata file, which does not store changes. metadata location with changes
      // is inconsistent.
      Preconditions.checkArgument(
          changes.isEmpty() || discardChanges || metadataLocation == null,
          "Cannot set metadata location with changes to table metadata: %s changes",
          changes.size());

      Schema schema = schemasById.get(currentSchemaId);
      PartitionSpec.checkCompatibility(specsById.get(defaultSpecId), schema);
      SortOrder.checkCompatibility(sortOrdersById.get(defaultSortOrderId), schema);

      List<MetadataLogEntry> metadataHistory;
      if (base == null) {
        metadataHistory = Lists.newArrayList();
      } else {
        metadataHistory =
            addPreviousFile(
                previousFiles, previousFileLocation, base.lastUpdatedMillis(), properties);
      }
      List<HistoryEntry> newSnapshotLog =
          updateSnapshotLog(snapshotLog, snapshotsById, currentSnapshotId, changes);

      return new TableMetadata(
          metadataLocation,
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
          snapshotsSupplier,
          ImmutableList.copyOf(newSnapshotLog),
          ImmutableList.copyOf(metadataHistory),
          ImmutableMap.copyOf(refs),
          statisticsFiles.values().stream().flatMap(List::stream).collect(Collectors.toList()),
          partitionStatisticsFiles.values().stream()
              .flatMap(List::stream)
              .collect(Collectors.toList()),
          discardChanges ? ImmutableList.of() : ImmutableList.copyOf(changes));
    }

    private int addSchemaInternal(Schema schema, int newLastColumnId) {
      Preconditions.checkArgument(
          newLastColumnId >= lastColumnId,
          "Invalid last column ID: %s < %s (previous last column ID)",
          newLastColumnId,
          lastColumnId);

      int newSchemaId = reuseOrCreateNewSchemaId(schema);
      boolean schemaFound = schemasById.containsKey(newSchemaId);
      if (schemaFound && newLastColumnId == lastColumnId) {
        // the new spec and last column id is already current and no change is needed
        // update lastAddedSchemaId if the schema was added in this set of changes (since it is now
        // the last)
        boolean isNewSchema =
            lastAddedSchemaId != null
                && changes(MetadataUpdate.AddSchema.class)
                    .anyMatch(added -> added.schema().schemaId() == newSchemaId);
        this.lastAddedSchemaId = isNewSchema ? newSchemaId : null;
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

      this.lastAddedSchemaId = newSchemaId;

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
        // update lastAddedSpecId if the spec was added in this set of changes (since it is now the
        // last)
        boolean isNewSpec =
            lastAddedSpecId != null
                && changes(MetadataUpdate.AddPartitionSpec.class)
                    .anyMatch(added -> added.spec().specId() == lastAddedSpecId);
        this.lastAddedSpecId = isNewSpec ? newSpecId : null;
        return newSpecId;
      }

      Schema schema = schemasById.get(currentSchemaId);
      PartitionSpec.checkCompatibility(spec, schema);
      ValidationException.check(
          formatVersion > 1 || PartitionSpec.hasSequentialIds(spec),
          "Spec does not use sequential IDs that are required in v1: %s",
          spec);

      PartitionSpec newSpec = freshSpec(newSpecId, schema, spec);
      this.lastAssignedPartitionId =
          Math.max(lastAssignedPartitionId, newSpec.lastAssignedFieldId());
      specs.add(newSpec);
      specsById.put(newSpecId, newSpec);

      changes.add(new MetadataUpdate.AddPartitionSpec(newSpec));

      this.lastAddedSpecId = newSpecId;

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
        // update lastAddedOrderId if the order was added in this set of changes (since it is now
        // the last)
        boolean isNewOrder =
            lastAddedOrderId != null
                && changes(MetadataUpdate.AddSortOrder.class)
                    .anyMatch(added -> added.sortOrder().orderId() == lastAddedOrderId);
        this.lastAddedOrderId = isNewOrder ? newOrderId : null;
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

      this.lastAddedOrderId = newOrderId;

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

    private void setBranchSnapshotInternal(Snapshot snapshot, String branch) {
      long replacementSnapshotId = snapshot.snapshotId();
      SnapshotRef ref = refs.get(branch);
      if (ref != null) {
        ValidationException.check(ref.isBranch(), "Cannot update branch: %s is a tag", branch);
        if (ref.snapshotId() == replacementSnapshotId) {
          return;
        }
      }

      ValidationException.check(
          formatVersion == 1 || snapshot.sequenceNumber() <= lastSequenceNumber,
          "Last sequence number %s is less than existing snapshot sequence number %s",
          lastSequenceNumber,
          snapshot.sequenceNumber());

      SnapshotRef newRef;
      if (ref != null) {
        newRef = SnapshotRef.builderFrom(ref, replacementSnapshotId).build();
      } else {
        newRef = SnapshotRef.branchBuilder(replacementSnapshotId).build();
      }

      setRef(branch, newRef);
    }

    private static List<MetadataLogEntry> addPreviousFile(
        List<MetadataLogEntry> previousFiles,
        String previousFileLocation,
        long timestampMillis,
        Map<String, String> properties) {
      if (previousFileLocation == null) {
        return previousFiles;
      }

      int maxSize =
          Math.max(
              1,
              PropertyUtil.propertyAsInt(
                  properties,
                  TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
                  TableProperties.METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT));

      List<MetadataLogEntry> newMetadataLog;
      if (previousFiles.size() >= maxSize) {
        int removeIndex = previousFiles.size() - maxSize + 1;
        newMetadataLog =
            Lists.newArrayList(previousFiles.subList(removeIndex, previousFiles.size()));
      } else {
        newMetadataLog = Lists.newArrayList(previousFiles);
      }
      newMetadataLog.add(new MetadataLogEntry(timestampMillis, previousFileLocation));

      return newMetadataLog;
    }

    /**
     * Finds intermediate snapshots that have not been committed as the current snapshot.
     *
     * <p>Transactions can create snapshots that are never the current snapshot because several
     * changes are combined by the transaction into one table metadata update. when each
     * intermediate snapshot is added to table metadata, it is added to the snapshot log, assuming
     * that it will be the current snapshot. when there are multiple snapshot updates, the log must
     * be corrected by suppressing the intermediate snapshot entries.
     *
     * <p>A snapshot is an intermediate snapshot if it was added but is not the current snapshot.
     *
     * @return a set of snapshot ids for all added snapshots that were later replaced as the current
     *     snapshot in changes
     */
    private static Set<Long> intermediateSnapshotIdSet(
        List<MetadataUpdate> changes, long currentSnapshotId) {
      Set<Long> addedSnapshotIds = Sets.newHashSet();
      Set<Long> intermediateSnapshotIds = Sets.newHashSet();
      for (MetadataUpdate update : changes) {
        if (update instanceof MetadataUpdate.AddSnapshot) {
          // adds must always come before set current snapshot
          MetadataUpdate.AddSnapshot addSnapshot = (MetadataUpdate.AddSnapshot) update;
          addedSnapshotIds.add(addSnapshot.snapshot().snapshotId());
        } else if (update instanceof MetadataUpdate.SetSnapshotRef) {
          MetadataUpdate.SetSnapshotRef setRef = (MetadataUpdate.SetSnapshotRef) update;
          long snapshotId = setRef.snapshotId();
          if (addedSnapshotIds.contains(snapshotId)
              && SnapshotRef.MAIN_BRANCH.equals(setRef.name())
              && snapshotId != currentSnapshotId) {
            intermediateSnapshotIds.add(snapshotId);
          }
        }
      }

      return intermediateSnapshotIds;
    }

    private static List<HistoryEntry> updateSnapshotLog(
        List<HistoryEntry> snapshotLog,
        Map<Long, Snapshot> snapshotsById,
        long currentSnapshotId,
        List<MetadataUpdate> changes) {
      Set<Long> intermediateSnapshotIds = intermediateSnapshotIdSet(changes, currentSnapshotId);
      boolean hasIntermediateSnapshots = !intermediateSnapshotIds.isEmpty();
      boolean hasRemovedSnapshots =
          changes.stream().anyMatch(MetadataUpdate.RemoveSnapshot.class::isInstance);

      if (!hasIntermediateSnapshots && !hasRemovedSnapshots) {
        return snapshotLog;
      }

      // update the snapshot log
      List<HistoryEntry> newSnapshotLog = Lists.newArrayList();
      for (HistoryEntry logEntry : snapshotLog) {
        long snapshotId = logEntry.snapshotId();
        if (snapshotsById.containsKey(snapshotId)) {
          if (!intermediateSnapshotIds.contains(snapshotId)) {
            // copy the log entries that are still valid
            newSnapshotLog.add(logEntry);
          }
        } else if (hasRemovedSnapshots) {
          // any invalid entry causes the history before it to be removed. otherwise, there could be
          // history gaps that cause time-travel queries to produce incorrect results. for example,
          // if history is [(t1, s1), (t2, s2), (t3, s3)] and s2 is removed, the history cannot be
          // [(t1, s1), (t3, s3)] because it appears that s3 was current during the time between t2
          // and t3 when in fact s2 was the current snapshot.
          newSnapshotLog.clear();
        }
      }

      if (snapshotsById.get(currentSnapshotId) != null) {
        ValidationException.check(
            Iterables.getLast(newSnapshotLog).snapshotId() == currentSnapshotId,
            "Cannot set invalid snapshot log: latest entry is not the current snapshot");
      }

      return newSnapshotLog;
    }

    private static Map<Long, List<StatisticsFile>> indexStatistics(List<StatisticsFile> files) {
      return files.stream().collect(Collectors.groupingBy(StatisticsFile::snapshotId));
    }

    private static Map<Long, List<PartitionStatisticsFile>> indexPartitionStatistics(
        List<PartitionStatisticsFile> files) {
      return files.stream().collect(Collectors.groupingBy(PartitionStatisticsFile::snapshotId));
    }

    private boolean isAddedSnapshot(long snapshotId) {
      return changes(MetadataUpdate.AddSnapshot.class)
          .anyMatch(add -> add.snapshot().snapshotId() == snapshotId);
    }

    private <U extends MetadataUpdate> Stream<U> changes(Class<U> updateClass) {
      return changes.stream().filter(updateClass::isInstance).map(updateClass::cast);
    }
  }
}
