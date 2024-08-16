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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestGroup.CreateTasksFunction;
import org.apache.iceberg.ManifestGroup.TaskContext;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

class BaseIncrementalChangelogScan
    extends BaseIncrementalScan<
        IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>>
    implements IncrementalChangelogScan {

  BaseIncrementalChangelogScan(Table table) {
    this(table, table.schema(), TableScanContext.empty());
  }

  private BaseIncrementalChangelogScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  @Override
  protected IncrementalChangelogScan newRefinedScan(
      Table newTable, Schema newSchema, TableScanContext newContext) {
    return new BaseIncrementalChangelogScan(newTable, newSchema, newContext);
  }

  @Override
  protected CloseableIterable<ChangelogScanTask> doPlanFiles(
      Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {

    Deque<Snapshot> changelogSnapshots =
        orderedChangelogSnapshots(fromSnapshotIdExclusive, toSnapshotIdInclusive);

    if (changelogSnapshots.isEmpty()) {
      return CloseableIterable.empty();
    }

    Set<ManifestFile> newDataManifests = Sets.newHashSet();
    Set<ManifestFile> newDeleteManifests = Sets.newHashSet();
    Map<Long, Snapshot> addedToChangedSnapshots = Maps.newHashMap();
    for (Snapshot snapshot : changelogSnapshots) {
      List<ManifestFile> dataManifests = snapshot.dataManifests(table().io());
      for (ManifestFile manifest : dataManifests) {
        if (!newDataManifests.contains(manifest)) {
          addedToChangedSnapshots.put(manifest.snapshotId(), snapshot);
          newDataManifests.add(manifest);
        }
      }
      newDeleteManifests.addAll(snapshot.deleteManifests(table().io()));
    }

    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), newDataManifests, newDeleteManifests)
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .filterManifestEntries(entry -> addedToChangedSnapshots.containsKey(entry.snapshotId()))
            .ignoreExisting()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (newDataManifests.size() > 1 && shouldPlanWithExecutor()) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }

    return manifestGroup.plan(
        new CreateDataFileChangeTasks(changelogSnapshots, addedToChangedSnapshots));
  }

  @Override
  public CloseableIterable<ScanTaskGroup<ChangelogScanTask>> planTasks() {
    return TableScanUtil.planTaskGroups(
        planFiles(), targetSplitSize(), splitLookback(), splitOpenFileCost());
  }

  // builds a collection of changelog snapshots (oldest to newest)
  // the order of the snapshots is important as it is used to determine change ordinals
  private Deque<Snapshot> orderedChangelogSnapshots(Long fromIdExcl, long toIdIncl) {
    Deque<Snapshot> changelogSnapshots = new ArrayDeque<>();

    for (Snapshot snapshot : SnapshotUtil.ancestorsBetween(table(), toIdIncl, fromIdExcl)) {
      if (!snapshot.operation().equals(DataOperations.REPLACE)) {
        changelogSnapshots.addFirst(snapshot);
      }
    }

    return changelogSnapshots;
  }

  private Set<Long> toSnapshotIds(Collection<Snapshot> snapshots) {
    return snapshots.stream().map(Snapshot::snapshotId).collect(Collectors.toSet());
  }

  private static Map<Long, Integer> computeSnapshotOrdinals(Deque<Snapshot> snapshots) {
    Map<Long, Integer> snapshotOrdinals = Maps.newHashMap();

    int ordinal = 0;

    for (Snapshot snapshot : snapshots) {
      snapshotOrdinals.put(snapshot.snapshotId(), ordinal++);
    }

    return snapshotOrdinals;
  }

  private static class CreateDataFileChangeTasks implements CreateTasksFunction<ChangelogScanTask> {

    private final Map<Long, Integer> snapshotOrdinals;
    private final Map<Long, Snapshot> addedToChangedSnapshots;

    CreateDataFileChangeTasks(
        Deque<Snapshot> snapshots, Map<Long, Snapshot> addedToChangedSnapshots) {
      this.snapshotOrdinals = computeSnapshotOrdinals(snapshots);
      this.addedToChangedSnapshots = addedToChangedSnapshots;
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context) {

      return CloseableIterable.filter(
          CloseableIterable.transform(
              entries,
              entry -> {
                long snapshotId = entry.snapshotId();
                Snapshot snapshot = addedToChangedSnapshots.get(snapshotId);
                long commitSnapshotId = snapshot.snapshotId();
                int changeOrdinal = snapshotOrdinals.get(snapshot.snapshotId());
                DataFile dataFile = entry.file().copy(context.shouldKeepStats());
                DeleteFile[] deleteFiles = context.deletes().forDataFile(dataFile);
                List<DeleteFile> addedDeletes = Lists.newArrayList();
                List<DeleteFile> existingDeletes = Lists.newArrayList();
                for (DeleteFile file : deleteFiles) {
                  if (file.dataSequenceNumber() == snapshot.sequenceNumber()) {
                    addedDeletes.add(file);
                  } else {
                    existingDeletes.add(file);
                  }
                }

                switch (entry.status()) {
                  case ADDED:
                    if (snapshotId == commitSnapshotId) {
                      return new BaseAddedRowsScanTask(
                          changeOrdinal,
                          commitSnapshotId,
                          dataFile,
                          addedDeletes.toArray(new DeleteFile[0]),
                          context.schemaAsString(),
                          context.specAsString(),
                          context.residuals());
                    } else if (deleteFiles.length > 0) {
                      return new BaseDeletedRowsScanTask(
                          changeOrdinal,
                          commitSnapshotId,
                          dataFile,
                          addedDeletes.toArray(new DeleteFile[0]),
                          existingDeletes.toArray(new DeleteFile[0]),
                          context.schemaAsString(),
                          context.specAsString(),
                          context.residuals());
                    } else {
                      return null;
                    }

                  case DELETED:
                    return new BaseDeletedDataFileScanTask(
                        changeOrdinal,
                        commitSnapshotId,
                        dataFile,
                        existingDeletes.toArray(new DeleteFile[0]),
                        context.schemaAsString(),
                        context.specAsString(),
                        context.residuals());

                  default:
                    throw new IllegalArgumentException(
                        "Unexpected entry status: " + entry.status());
                }
              }),
          Objects::nonNull);
    }
  }
}
