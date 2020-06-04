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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;

class IncrementalDataTableScan extends DataTableScan {
  private long fromSnapshotId;
  private long toSnapshotId;

  IncrementalDataTableScan(TableOperations ops, Table table, Schema schema, Expression rowFilter,
                           boolean ignoreResiduals, boolean caseSensitive, boolean colStats,
                           Collection<String> selectedColumns, ImmutableMap<String, String> options,
                           long fromSnapshotId, long toSnapshotId) {
    super(ops, table, null, schema, rowFilter, ignoreResiduals, caseSensitive, colStats, selectedColumns, options);
    validateSnapshotIds(table, fromSnapshotId, toSnapshotId);
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;
  }

  @Override
  public TableScan asOfTime(long timestampMillis) {
    throw new UnsupportedOperationException(String.format(
        "Cannot scan table as of time %s: configured for incremental data in snapshots (%s, %s]",
        timestampMillis, fromSnapshotId, toSnapshotId));
  }

  @Override
  public TableScan useSnapshot(long scanSnapshotId) {
    throw new UnsupportedOperationException(String.format(
        "Cannot scan table using scan snapshot id %s: configured for incremental data in snapshots (%s, %s]",
        scanSnapshotId, fromSnapshotId, toSnapshotId));
  }

  @Override
  public TableScan appendsBetween(long newFromSnapshotId, long newToSnapshotId) {
    validateSnapshotIdsRefinement(newFromSnapshotId, newToSnapshotId);
    return new IncrementalDataTableScan(
        tableOps(), table(), schema(), filter(), shouldIgnoreResiduals(),
        isCaseSensitive(), colStats(), selectedColumns(), options(),
        newFromSnapshotId, newToSnapshotId);
  }

  @Override
  public TableScan appendsAfter(long newFromSnapshotId) {
    final Snapshot currentSnapshot = table().currentSnapshot();
    Preconditions.checkState(currentSnapshot != null,
        "Cannot scan appends after %s, there is no current snapshot", newFromSnapshotId);
    return appendsBetween(newFromSnapshotId, currentSnapshot.snapshotId());
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    //TODO publish an incremental appends scan event
    List<Snapshot> snapshots = snapshotsWithin(table(), fromSnapshotId, toSnapshotId);
    Set<Long> snapshotIds = Sets.newHashSet(Iterables.transform(snapshots, Snapshot::snapshotId));
    Set<ManifestFile> manifests = FluentIterable
        .from(snapshots)
        .transformAndConcat(s -> s.dataManifests())
        .filter(manifestFile -> snapshotIds.contains(manifestFile.snapshotId()))
        .toSet();

    ManifestGroup manifestGroup = new ManifestGroup(tableOps().io(), manifests)
        .caseSensitive(isCaseSensitive())
        .select(colStats() ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
        .filterData(filter())
        .filterManifestEntries(
            manifestEntry ->
                snapshotIds.contains(manifestEntry.snapshotId()) &&
                manifestEntry.status() == ManifestEntry.Status.ADDED)
        .specsById(tableOps().current().specsById())
        .ignoreDeleted();

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (PLAN_SCANS_WITH_WORKER_POOL && manifests.size() > 1) {
      manifestGroup = manifestGroup.planWith(ThreadPools.getWorkerPool());
    }

    return manifestGroup.planFiles();
  }

  @Override
  @SuppressWarnings("checkstyle:HiddenField")
  protected TableScan newRefinedScan(
          TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
          boolean ignoreResiduals, boolean caseSensitive, boolean colStats, Collection<String> selectedColumns,
          ImmutableMap<String, String> options) {
    return new IncrementalDataTableScan(
            ops, table, schema, rowFilter, ignoreResiduals, caseSensitive, colStats, selectedColumns, options,
            fromSnapshotId, toSnapshotId);
  }

  private static List<Snapshot> snapshotsWithin(Table table, long fromSnapshotId, long toSnapshotId) {
    List<Long> snapshotIds = SnapshotUtil.snapshotIdsBetween(table, fromSnapshotId, toSnapshotId);
    List<Snapshot> snapshots = Lists.newArrayList();
    for (Long snapshotId : snapshotIds) {
      Snapshot snapshot = table.snapshot(snapshotId);
      // for now, incremental scan supports only appends
      if (snapshot.operation().equals(DataOperations.APPEND)) {
        snapshots.add(snapshot);
      } else if (snapshot.operation().equals(DataOperations.OVERWRITE)) {
        throw new UnsupportedOperationException(
            String.format("Found %s operation, cannot support incremental data in snapshots (%s, %s]",
                DataOperations.OVERWRITE, fromSnapshotId, toSnapshotId));
      }
    }
    return snapshots;
  }

  private void validateSnapshotIdsRefinement(long newFromSnapshotId, long newToSnapshotId) {
    Set<Long> snapshotIdsRange = Sets.newHashSet(
        SnapshotUtil.snapshotIdsBetween(table(), fromSnapshotId, toSnapshotId));
    // since snapshotIdsBetween return ids in range (fromSnapshotId, toSnapshotId]
    snapshotIdsRange.add(fromSnapshotId);
    Preconditions.checkArgument(
        snapshotIdsRange.contains(newFromSnapshotId),
        "from snapshot id %s not in existing snapshot ids range (%s, %s]",
        newFromSnapshotId, fromSnapshotId, newToSnapshotId);
    Preconditions.checkArgument(
        snapshotIdsRange.contains(newToSnapshotId),
        "to snapshot id %s not in existing snapshot ids range (%s, %s]",
        newToSnapshotId, fromSnapshotId, toSnapshotId);
  }

  private static void validateSnapshotIds(Table table, long fromSnapshotId, long toSnapshotId) {
    Preconditions.checkArgument(fromSnapshotId != toSnapshotId, "from and to snapshot ids cannot be the same");
    Preconditions.checkArgument(
        table.snapshot(fromSnapshotId) != null, "from snapshot %s does not exist", fromSnapshotId);
    Preconditions.checkArgument(
        table.snapshot(toSnapshotId) != null, "to snapshot %s does not exist", toSnapshotId);
    Preconditions.checkArgument(SnapshotUtil.ancestorOf(table, toSnapshotId, fromSnapshotId),
        "from snapshot %s is not an ancestor of to snapshot  %s", fromSnapshotId, toSnapshotId);
  }
}
