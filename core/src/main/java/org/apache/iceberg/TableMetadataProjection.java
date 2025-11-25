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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * A projection wrapper for {@link TableMetadata} that transforms snapshots on-demand.
 *
 * <p>This projection follows the same pattern as StructProjection, applying transformations lazily
 * when snapshot-related methods are accessed. Unlike embedding transformation logic in
 * TableMetadata itself, this keeps the transformation concerns separate.
 *
 * <p>The projection is lightweight - it extends TableMetadata and overrides only snapshot-related
 * methods, allowing all other metadata (schemas, specs, properties, etc.) to be inherited
 * naturally.
 *
 * <p>Example use cases:
 *
 * <ul>
 *   <li>Filtering sensitive data from snapshot summaries before exposing metadata
 *   <li>Enriching snapshots with additional computed properties
 *   <li>Testing scenarios requiring modified snapshot metadata without persisting changes
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>
 * // Remove sensitive data from snapshot summaries
 * TableMetadata cleaned = TableMetadataProjection.create(
 *     originalMetadata,
 *     snapshot -&gt; {
 *       if (snapshot instanceof BaseSnapshot) {
 *         BaseSnapshot base = (BaseSnapshot) snapshot;
 *         Map&lt;String, String&gt; filteredSummary = filterSensitiveKeys(base.summary());
 *         return new BaseSnapshot(
 *             base.sequenceNumber(), base.snapshotId(), base.parentId(),
 *             base.timestampMillis(), base.operation(), filteredSummary,
 *             base.schemaId(), base.manifestListLocation(),
 *             base.firstRowId(), base.addedRows(), base.keyId());
 *       }
 *       return snapshot;
 *     });
 * </pre>
 */
public class TableMetadataProjection extends TableMetadata {

  /**
   * Creates a projecting wrapper for {@link TableMetadata} that transforms snapshots.
   *
   * @param metadata the base table metadata to wrap
   * @param snapshotTransformer function to transform each snapshot
   * @return a projection that applies the transformer to all snapshots
   */
  public static TableMetadata create(
      TableMetadata metadata, Function<Snapshot, Snapshot> snapshotTransformer) {
    Preconditions.checkArgument(metadata != null, "Metadata cannot be null");
    Preconditions.checkArgument(snapshotTransformer != null, "Snapshot transformer cannot be null");
    return new TableMetadataProjection(metadata, snapshotTransformer);
  }

  private final Function<Snapshot, Snapshot> snapshotTransformer;
  private volatile List<Snapshot> transformedSnapshots;
  private volatile Map<Long, Snapshot> transformedSnapshotsById;
  private volatile Map<String, SnapshotRef> transformedRefs;

  private TableMetadataProjection(
      TableMetadata base, Function<Snapshot, Snapshot> snapshotTransformer) {
    // Call super constructor with all fields from base metadata
    // Pass null for snapshots since we'll provide them via override
    super(
        base.metadataFileLocation(),
        base.formatVersion(),
        base.uuid(),
        base.location(),
        base.lastSequenceNumber(),
        base.lastUpdatedMillis(),
        base.lastColumnId(),
        base.currentSchemaId(),
        base.schemas(),
        base.defaultSpecId(),
        base.specs(),
        base.lastAssignedPartitionId(),
        base.defaultSortOrderId(),
        base.sortOrders(),
        base.properties(),
        base.currentSnapshotId(), // Get raw field value without triggering loading
        null, // snapshots will be provided via override
        base::snapshots, // lazy supplier to trigger transformation
        base.snapshotLog(),
        base.previousFiles(),
        base.refs(),
        base.statisticsFiles(),
        base.partitionStatisticsFiles(),
        base.nextRowId(),
        base.encryptionKeys(),
        base.changes());

    this.snapshotTransformer = snapshotTransformer;
  }

  @Override
  public List<Snapshot> snapshots() {
    if (transformedSnapshots == null) {
      synchronized (this) {
        if (transformedSnapshots == null) {
          // Trigger lazy loading via super, which will call our supplier
          List<Snapshot> baseSnapshots = super.snapshots();
          transformedSnapshots =
              baseSnapshots.stream()
                  .map(snapshotTransformer)
                  .collect(ImmutableList.toImmutableList());
        }
      }
    }
    return transformedSnapshots;
  }

  @Override
  public Snapshot snapshot(long snapshotId) {
    // Build index lazily and look up transformed snapshot
    if (transformedSnapshotsById == null) {
      synchronized (this) {
        if (transformedSnapshotsById == null) {
          transformedSnapshotsById =
              snapshots().stream()
                  .collect(ImmutableMap.toImmutableMap(Snapshot::snapshotId, s -> s));
        }
      }
    }
    return transformedSnapshotsById.get(snapshotId);
  }

  @Override
  public Snapshot currentSnapshot() {
    long currentId = currentSnapshotId();
    if (currentId == -1) {
      return null;
    }

    // Trigger loading if needed and return transformed snapshot
    return snapshot(currentId);
  }

  @Override
  public Map<String, SnapshotRef> refs() {
    if (transformedRefs == null) {
      synchronized (this) {
        if (transformedRefs == null) {
          // Rebuild refs to point to transformed snapshots
          // Filter out any refs that point to snapshots that no longer exist after transformation
          Map<Long, Snapshot> transformedById = transformedSnapshotsById();
          transformedRefs =
              super.refs().entrySet().stream()
                  .filter(entry -> transformedById.containsKey(entry.getValue().snapshotId()))
                  .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
      }
    }
    return transformedRefs;
  }

  @Override
  public SnapshotRef ref(String name) {
    return refs().get(name);
  }

  private Map<Long, Snapshot> transformedSnapshotsById() {
    if (transformedSnapshotsById == null) {
      synchronized (this) {
        if (transformedSnapshotsById == null) {
          transformedSnapshotsById =
              snapshots().stream()
                  .collect(ImmutableMap.toImmutableMap(Snapshot::snapshotId, s -> s));
        }
      }
    }
    return transformedSnapshotsById;
  }

  // All other methods (schemas, specs, properties, statistics, etc.) are
  // automatically inherited from TableMetadata without any delegation needed
}
