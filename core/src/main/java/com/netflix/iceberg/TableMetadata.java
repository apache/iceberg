/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.types.TypeUtil;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * Metadata for a table.
 */
public class TableMetadata {
  static final int TABLE_FORMAT_VERSION = 1;

  public static TableMetadata newTableMetadata(TableOperations ops, Schema schema, PartitionSpec spec) {
    // reassign all column ids to ensure consistency
    AtomicInteger lastColumnId = new AtomicInteger(0);
    Schema freshSchema = TypeUtil.reassignIds(schema, lastColumnId::incrementAndGet);

    // rebuild the partition spec using the new column ids
    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(freshSchema);
    for (PartitionField field : spec.fields()) {
      // look up the name of the source field in the old schema to get the new schema's id
      String sourceName = schema.getColumnName(field.sourceId());
      specBuilder.add(
          freshSchema.getColumn(sourceName).fieldId(),
          field.name(),
          field.transform().toString());
    }
    PartitionSpec freshSpec = specBuilder.build();

    return new TableMetadata(ops, null,
        System.currentTimeMillis(),
        lastColumnId.get(), freshSchema, freshSpec,
        ImmutableMap.of(), -1, ImmutableList.of());
  }

  private final TableOperations ops;
  // TODO: should this reference its own file? (maybe: it could be an easy way to track it)
  private final InputFile file;
  private final long lastUpdatedMillis;
  private final int lastColumnId;
  private final Schema schema;
  private final PartitionSpec spec;
  private final Map<String, String> properties;
  private final long currentSnapshotId;
  private final List<Snapshot> snapshots;
  private final Map<Long, Snapshot> snapshotsById;

  TableMetadata(TableOperations ops,
                InputFile file,
                long lastUpdatedMillis,
                int lastColumnId,
                Schema schema,
                PartitionSpec spec,
                Map<String, String> properties,
                long currentSnapshotId,
                List<Snapshot> snapshots) {
    this.ops = ops;
    this.file = file;
    this.lastUpdatedMillis = lastUpdatedMillis;
    this.lastColumnId = lastColumnId;
    this.schema = schema;
    this.spec = spec;
    this.properties = properties;
    this.currentSnapshotId = currentSnapshotId;
    this.snapshots = snapshots;

    ImmutableMap.Builder<Long, Snapshot> builder = ImmutableMap.builder();
    for (Snapshot version : snapshots) {
      builder.put(version.snapshotId(), version);
    }
    this.snapshotsById = builder.build();

    Preconditions.checkArgument(
        snapshotsById.isEmpty() || snapshotsById.containsKey(currentSnapshotId),
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
    return spec;
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

  public Snapshot snapshot(long snapshotId) {
    return snapshotsById.get(snapshotId);
  }

  public Snapshot currentSnapshot() {
    return snapshotsById.get(currentSnapshotId);
  }

  public List<Snapshot> snapshots() {
    return snapshots;
  }

  public TableMetadata updateSchema(Schema schema, int lastColumnId) {
    PartitionSpec.checkCompatibility(spec, schema);
    return new TableMetadata(ops, null,
        System.currentTimeMillis(), lastColumnId, schema, spec, properties, currentSnapshotId,
        snapshots);
  }

  public TableMetadata addSnapshot(Snapshot snapshot) {
    List<Snapshot> newSnapshots = ImmutableList.<Snapshot>builder()
        .addAll(snapshots)
        .add(snapshot)
        .build();
    return new TableMetadata(ops, null,
        snapshot.timestampMillis(), lastColumnId, schema, spec, properties, snapshot.snapshotId(),
        newSnapshots);
  }

  public TableMetadata removeSnapshotsIf(Predicate<Snapshot> filter) {
    List<Snapshot> filtered = Lists.newArrayListWithExpectedSize(snapshots.size());
    for (Snapshot snapshot : snapshots) {
      if (!filter.test(snapshot)) {
        // the snapshot should be retained
        filtered.add(snapshot);
      } else {
        // the snapshot will be removed. verify that this isn't current snapshot
        ValidationException.check(snapshot.snapshotId() != currentSnapshotId,
            "Cannot remove the current table snapshot: %s", currentSnapshotId);
      }
    }

    return new TableMetadata(ops, null,
        System.currentTimeMillis(), lastColumnId, schema, spec, properties, currentSnapshotId,
        filtered);
  }

  public TableMetadata rollbackTo(Snapshot snapshot) {
    ValidationException.check(snapshotsById.containsKey(snapshot.snapshotId()),
        "Cannot set current snapshot to unknown: %s", snapshot.snapshotId());

    return new TableMetadata(ops, null,
        System.currentTimeMillis(), lastColumnId, schema, spec, properties, snapshot.snapshotId(),
        snapshots);
  }

  public TableMetadata replaceProperties(Map<String, String> newProperties) {
    ValidationException.check(newProperties != null, "Cannot set properties to null");
    return new TableMetadata(ops, null,
        System.currentTimeMillis(), lastColumnId, schema, spec, newProperties, currentSnapshotId,
        snapshots);
  }

}
