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

package com.netflix.iceberg;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.types.TypeUtil;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * Metadata for a table.
 */
public class TableMetadata {
  static final int TABLE_FORMAT_VERSION = 1;

  public static TableMetadata newTableMetadata(TableOperations ops,
                                               Schema schema,
                                               PartitionSpec spec,
                                               String location) {
    return newTableMetadata(ops, schema, spec, location, ImmutableMap.of());
  }

  public static TableMetadata newTableMetadata(TableOperations ops,
                                               Schema schema,
                                               PartitionSpec spec,
                                               String location,
                                               Map<String, String> properties) {
    // reassign all column ids to ensure consistency
    AtomicInteger lastColumnId = new AtomicInteger(0);
    Schema freshSchema = TypeUtil.assignFreshIds(schema, lastColumnId::incrementAndGet);

    // rebuild the partition spec using the new column ids
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(freshSchema);
    for (PartitionField field : spec.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = schema.findColumnName(field.sourceId());
      specBuilder.add(
          freshSchema.findField(sourceName).fieldId(),
          field.name(),
          field.transform().toString());
    }
    PartitionSpec freshSpec = specBuilder.build();

    return new TableMetadata(ops, null, location,
        System.currentTimeMillis(),
        lastColumnId.get(), freshSchema, 0, ImmutableMap.of(0, freshSpec),
        ImmutableMap.copyOf(properties), -1, ImmutableList.of(), ImmutableList.of());
  }

  public static class SnapshotLogEntry {
    private final long timestampMillis;
    private final long snapshotId;

    SnapshotLogEntry(long timestampMillis, long snapshotId) {
      this.timestampMillis = timestampMillis;
      this.snapshotId = snapshotId;
    }

    public long timestampMillis() {
      return timestampMillis;
    }

    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
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
      return Objects.toStringHelper(this)
          .add("timestampMillis", timestampMillis)
          .add("snapshotId", snapshotId)
          .toString();
    }
  }

  private final TableOperations ops;
  private final InputFile file;

  // stored metadata
  private final String location;
  private final long lastUpdatedMillis;
  private final int lastColumnId;
  private final Schema schema;
  private final int defaultSpecId;
  private final Map<Integer, PartitionSpec> specs;
  private final Map<String, String> properties;
  private final long currentSnapshotId;
  private final List<Snapshot> snapshots;
  private final Map<Long, Snapshot> snapshotsById;
  private final List<SnapshotLogEntry> snapshotLog;

  TableMetadata(TableOperations ops,
                InputFile file,
                String location,
                long lastUpdatedMillis,
                int lastColumnId,
                Schema schema,
                int defaultSpecId,
                Map<Integer, PartitionSpec> specs,
                Map<String, String> properties,
                long currentSnapshotId,
                List<Snapshot> snapshots,
                List<SnapshotLogEntry> snapshotLog) {
    this.ops = ops;
    this.file = file;
    this.location = location;
    this.lastUpdatedMillis = lastUpdatedMillis;
    this.lastColumnId = lastColumnId;
    this.schema = schema;
    this.specs = specs;
    this.defaultSpecId = defaultSpecId;
    this.properties = properties;
    this.currentSnapshotId = currentSnapshotId;
    this.snapshots = snapshots;
    this.snapshotLog = snapshotLog;

    ImmutableMap.Builder<Long, Snapshot> builder = ImmutableMap.builder();
    for (Snapshot version : snapshots) {
      builder.put(version.snapshotId(), version);
    }
    this.snapshotsById = builder.build();

    SnapshotLogEntry last = null;
    for (SnapshotLogEntry logEntry : snapshotLog) {
      if (last != null) {
        Preconditions.checkArgument(
            (logEntry.timestampMillis() - last.timestampMillis()) >= 0,
            "[BUG] Expected sorted snapshot log entries.");
      }
      last = logEntry;
    }

    Preconditions.checkArgument(
        currentSnapshotId < 0 || snapshotsById.containsKey(currentSnapshotId),
        "Invalid table metadata: Cannot find current version");
  }

  public InputFile file() {
    return file;
  }

  public long lastUpdatedMillis() {
    return lastUpdatedMillis;
  }

  public int lastColumnId() {
    return lastColumnId;
  }

  public Schema schema() {
    return schema;
  }

  public PartitionSpec spec() {
    return specs.get(defaultSpecId);
  }

  public int defaultSpecId() {
    return defaultSpecId;
  }

  public PartitionSpec spec(int id) {
    return specs.get(id);
  }

  public Map<Integer, PartitionSpec> specs() {
    return specs;
  }

  public String location() {
    return location;
  }

  public Map<String, String> properties() {
    return properties;
  }

  public int propertyAsInt(String property, int defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Integer.parseInt(properties.get(property));
    }
    return defaultValue;
  }

  public long propertyAsLong(String property, long defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Long.parseLong(properties.get(property));
    }
    return defaultValue;
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

  public List<SnapshotLogEntry> snapshotLog() {
    return snapshotLog;
  }

  public TableMetadata updateTableLocation(String newLocation) {
    return new TableMetadata(ops, null, newLocation,
        System.currentTimeMillis(), lastColumnId, schema, defaultSpecId, specs, properties,
        currentSnapshotId, snapshots, snapshotLog);
  }

  public TableMetadata updateSchema(Schema schema, int lastColumnId) {
    PartitionSpec.checkCompatibility(spec(), schema);
    return new TableMetadata(ops, null, location,
        System.currentTimeMillis(), lastColumnId, schema, defaultSpecId, specs, properties,
        currentSnapshotId, snapshots, snapshotLog);
  }

  public TableMetadata updatePartitionSpec(PartitionSpec spec) {
    PartitionSpec.checkCompatibility(spec, schema);

    // if the spec already exists, use the same ID. otherwise, use 1 more than the highest ID.
    int newDefaultSpecId = 0;
    for (Map.Entry<Integer, PartitionSpec> entry : specs.entrySet()) {
      if (spec.equals(entry.getValue())) {
        newDefaultSpecId = entry.getKey();
        break;
      } else if (newDefaultSpecId <= entry.getKey()) {
        newDefaultSpecId = entry.getKey() + 1;
      }
    }

    Preconditions.checkArgument(defaultSpecId != newDefaultSpecId,
        "Cannot set default partition spec to the current default");

    Map<Integer, PartitionSpec> newSpecs = Maps.newHashMap();
    newSpecs.putAll(specs);
    newSpecs.put(newDefaultSpecId, spec);

    return new TableMetadata(ops, null, location,
        System.currentTimeMillis(), lastColumnId, schema, newDefaultSpecId,
        ImmutableMap.copyOf(newSpecs), properties,
        currentSnapshotId, snapshots, snapshotLog);
  }

  public TableMetadata replaceCurrentSnapshot(Snapshot snapshot) {
    List<Snapshot> newSnapshots = ImmutableList.<Snapshot>builder()
        .addAll(snapshots)
        .add(snapshot)
        .build();
    List<SnapshotLogEntry> newSnapshotLog = ImmutableList.<SnapshotLogEntry>builder()
        .addAll(snapshotLog)
        .add(new SnapshotLogEntry(snapshot.timestampMillis(), snapshot.snapshotId()))
        .build();
    return new TableMetadata(ops, null, location,
        snapshot.timestampMillis(), lastColumnId, schema, defaultSpecId, specs, properties,
        snapshot.snapshotId(), newSnapshots, newSnapshotLog);
  }

  public TableMetadata removeSnapshotsIf(Predicate<Snapshot> removeIf) {
    List<Snapshot> filtered = Lists.newArrayListWithExpectedSize(snapshots.size());
    for (Snapshot snapshot : snapshots) {
      // keep the current snapshot and any snapshots that do not match the removeIf condition
      if (snapshot.snapshotId() == currentSnapshotId || !removeIf.test(snapshot)) {
        filtered.add(snapshot);
      }
    }

    // update the snapshot log
    Set<Long> validIds = Sets.newHashSet(Iterables.transform(filtered, Snapshot::snapshotId));
    List<SnapshotLogEntry> newSnapshotLog = Lists.newArrayList();
    for (SnapshotLogEntry logEntry : snapshotLog) {
      if (validIds.contains(logEntry.snapshotId())) {
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

    return new TableMetadata(ops, null, location,
        System.currentTimeMillis(), lastColumnId, schema, defaultSpecId, specs, properties,
        currentSnapshotId, filtered, ImmutableList.copyOf(newSnapshotLog));
  }

  public TableMetadata rollbackTo(Snapshot snapshot) {
    ValidationException.check(snapshotsById.containsKey(snapshot.snapshotId()),
        "Cannot set current snapshot to unknown: %s", snapshot.snapshotId());

    long nowMillis = System.currentTimeMillis();
    List<SnapshotLogEntry> newSnapshotLog = ImmutableList.<SnapshotLogEntry>builder()
        .addAll(snapshotLog)
        .add(new SnapshotLogEntry(nowMillis, snapshot.snapshotId()))
        .build();

    return new TableMetadata(ops, null, location,
        nowMillis, lastColumnId, schema, defaultSpecId, specs, properties,
        snapshot.snapshotId(), snapshots, newSnapshotLog);
  }

  public TableMetadata replaceProperties(Map<String, String> newProperties) {
    ValidationException.check(newProperties != null, "Cannot set properties to null");
    return new TableMetadata(ops, null, location,
        System.currentTimeMillis(), lastColumnId, schema, defaultSpecId, specs, newProperties,
        currentSnapshotId, snapshots, snapshotLog);
  }

  public TableMetadata removeSnapshotLogEntries(Set<Long> snapshotIds) {
    List<SnapshotLogEntry> newSnapshotLog = Lists.newArrayList();
    for (SnapshotLogEntry logEntry : snapshotLog) {
      if (!snapshotIds.contains(logEntry.snapshotId())) {
        // copy the log entries that are still valid
        newSnapshotLog.add(logEntry);
      }
    }

    ValidationException.check(currentSnapshotId < 0 || // not set
            Iterables.getLast(newSnapshotLog).snapshotId() == currentSnapshotId,
        "Cannot set invalid snapshot log: latest entry is not the current snapshot");
    return new TableMetadata(ops, null, location,
        System.currentTimeMillis(), lastColumnId, schema, defaultSpecId, specs, properties,
        currentSnapshotId, snapshots, newSnapshotLog);
  }

  public TableMetadata buildReplacement(Schema schema, PartitionSpec partitionSpec,
                                        Map<String, String> properties) {
    AtomicInteger lastColumnId = new AtomicInteger(0);
    Schema freshSchema = TypeUtil.assignFreshIds(schema, lastColumnId::incrementAndGet);

    // rebuild the partition spec using the new column ids
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(freshSchema);
    for (PartitionField field : partitionSpec.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = schema.findColumnName(field.sourceId());
      specBuilder.add(
          freshSchema.findField(sourceName).fieldId(),
          field.name(),
          field.transform().toString());
    }
    PartitionSpec freshSpec = specBuilder.build();

    // if the spec already exists, use the same ID. otherwise, use 1 more than the highest ID.
    int specId = 0;
    for (Map.Entry<Integer, PartitionSpec> entry : specs.entrySet()) {
      if (freshSpec.equals(entry.getValue())) {
        specId = entry.getKey();
        break;
      } else if (specId <= entry.getKey()) {
        specId = entry.getKey() + 1;
      }
    }

    Map<Integer, PartitionSpec> newSpecs = Maps.newHashMap();
    newSpecs.putAll(specs);
    newSpecs.put(specId, freshSpec);

    Map<String, String> newProperties = Maps.newHashMap();
    newProperties.putAll(this.properties);
    newProperties.putAll(properties);

    return new TableMetadata(ops, null, location,
        System.currentTimeMillis(), lastColumnId.get(), freshSchema,
        specId, ImmutableMap.copyOf(newSpecs), ImmutableMap.copyOf(newProperties),
        -1, snapshots, ImmutableList.of());
  }
}
