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
import java.util.Set;
import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

class BaseIncrementalAppendScan extends BaseScan<IncrementalAppendScan> implements IncrementalAppendScan {

  BaseIncrementalAppendScan(TableOperations ops, Table table) {
    this(ops, table, table.schema(), new TableScanContext());
  }

  BaseIncrementalAppendScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  @Override
  protected IncrementalAppendScan newRefinedScan(
      TableOperations newOps, Table newTable, Schema newSchema, TableScanContext newContext) {
    return new BaseIncrementalAppendScan(newOps, newTable, newSchema, newContext);
  }

  @Override
  public IncrementalAppendScan fromSnapshotInclusive(long fromSnapshotId) {
    Snapshot startingSnapshotInclusive = table().snapshot(fromSnapshotId);
    Preconditions.checkArgument(startingSnapshotInclusive != null,
        "Cannot find the starting snapshot: %d", fromSnapshotId);
    return newRefinedScan(tableOps(), table(), schema(), context().fromSnapshotIdInclusive(fromSnapshotId));
  }

  @Override
  public IncrementalAppendScan fromSnapshotExclusive(long fromSnapshotId) {
    Preconditions.checkArgument(table().snapshot(fromSnapshotId) != null,
        "Cannot find starting snapshot: %d", fromSnapshotId);
    return newRefinedScan(tableOps(), table(), schema(), context().fromSnapshotId(fromSnapshotId));
  }

  @Override
  public IncrementalAppendScan toSnapshot(long toSnapshotId) {
    Preconditions.checkArgument(table().snapshot(toSnapshotId) != null,
        "Cannot find end snapshot: %d", toSnapshotId);
    return newRefinedScan(tableOps(), table(), schema(),  context().toSnapshotId(toSnapshotId));
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    long toSnapshotIdInclusive;
    if (context().toSnapshotId() != null) {
      toSnapshotIdInclusive = context().toSnapshotId();
    } else if (table().currentSnapshot() != null) {
      toSnapshotIdInclusive = table().currentSnapshot().snapshotId();
    } else {
      throw new IllegalArgumentException("End snapshot is not set and table has no current snapshot");
    }


    if (context().fromSnapshotId() != null) {
      Preconditions.checkArgument(
          SnapshotUtil.isAncestorOf(table(), toSnapshotIdInclusive, context().fromSnapshotId()),
          "Starting snapshot (%s) %s is not an ancestor of end snapshot %s",
          context().fromSnapshotInclusive() ? "inclusive" : "exclusive",
          context().fromSnapshotId(), toSnapshotIdInclusive);
    }

    Long fromSnapshotIdExclusive = context().fromSnapshotId();
    if (context().fromSnapshotId() != null && context().fromSnapshotInclusive()) {
      // for inclusive behavior fromSnapshotIdExclusive is set to the parent snapshot id, which can be null.
      // appendsBetween handles null fromSnapshotIdExclusive properly by finding the ancestors of end snapshot.
      fromSnapshotIdExclusive = table().snapshot(context().fromSnapshotId()).parentId();
    }

    notifyIncrementalScanEvent(table(), context(), toSnapshotIdInclusive);

    // appendsBetween handles null fromSnapshotId (exclusive) properly
    List<Snapshot> snapshots = appendsBetween(table(), fromSnapshotIdExclusive, toSnapshotIdInclusive);
    if (snapshots.isEmpty()) {
      return CloseableIterable.empty();
    }

    return planFilesFromSnapshots(snapshots);
  }

  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    CloseableIterable<FileScanTask> fileScanTasks = planFiles();
    CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(fileScanTasks, targetSplitSize());
    return TableScanUtil.planTasks(splitFiles, targetSplitSize(), splitLookback(), splitOpenFileCost());
  }

  private CloseableIterable<FileScanTask> planFilesFromSnapshots(List<Snapshot> snapshots) {
    Set<Long> snapshotIds = Sets.newHashSet(Iterables.transform(snapshots, Snapshot::snapshotId));
    Set<ManifestFile> manifests = FluentIterable
        .from(snapshots)
        .transformAndConcat(Snapshot::dataManifests)
        .filter(manifestFile -> snapshotIds.contains(manifestFile.snapshotId()))
        .toSet();

    ManifestGroup manifestGroup = new ManifestGroup(tableOps().io(), manifests)
        .caseSensitive(context().caseSensitive())
        .select(context().returnColumnStats() ? DataTableScan.SCAN_WITH_STATS_COLUMNS : DataTableScan.SCAN_COLUMNS)
        .filterData(context().rowFilter())
        .filterManifestEntries(
            manifestEntry ->
                snapshotIds.contains(manifestEntry.snapshotId()) &&
                    manifestEntry.status() == ManifestEntry.Status.ADDED)
        .specsById(tableOps().current().specsById())
        .ignoreDeleted();

    if (context().ignoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (manifests.size() > 1 &&
        (DataTableScan.PLAN_SCANS_WITH_WORKER_POOL || context().planWithCustomizedExecutor())) {
      manifestGroup = manifestGroup.planWith(context().planExecutor());
    }

    return manifestGroup.planFiles();
  }

  private static void notifyIncrementalScanEvent(
      Table table, TableScanContext context, long toSnapshotIdInclusive) {
    long fromSnapshotId;
    boolean fromSnapshotInclusive;
    if (context.fromSnapshotId() != null) {
      fromSnapshotId = context.fromSnapshotId();
      fromSnapshotInclusive = context.fromSnapshotInclusive();
    } else {
      Snapshot oldestAncestorSnapshot = SnapshotUtil.oldestAncestor(toSnapshotIdInclusive, table::snapshot);
      fromSnapshotId = oldestAncestorSnapshot.snapshotId();
      fromSnapshotInclusive = true;
    }

    Listeners.notifyAll(new IncrementalScanEvent(table.name(), fromSnapshotId,
        toSnapshotIdInclusive, context.rowFilter(), table.schema(), fromSnapshotInclusive));
  }

  private static List<Snapshot> appendsBetween(Table table, Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {
    List<Snapshot> snapshots = Lists.newArrayList();
    for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(
        toSnapshotIdInclusive, fromSnapshotIdExclusive, table::snapshot)) {
      if (snapshot.operation().equals(DataOperations.APPEND)) {
        snapshots.add(snapshot);
      }
    }

    return snapshots;
  }
}
