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
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.ManifestGroup.CreateTasksFunction;
import org.apache.iceberg.ManifestGroup.TaskContext;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;

class BaseIncrementalChangelogScan
    extends BaseIncrementalScan<
        IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>>
    implements IncrementalChangelogScan {

  BaseIncrementalChangelogScan(TableOperations ops, Table table) {
    this(ops, table, table.schema(), new TableScanContext());
  }

  BaseIncrementalChangelogScan(
      TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  @Override
  protected IncrementalChangelogScan newRefinedScan(
      TableOperations newOps, Table newTable, Schema newSchema, TableScanContext newContext) {
    return new BaseIncrementalChangelogScan(newOps, newTable, newSchema, newContext);
  }

  @Override
  protected CloseableIterable<ChangelogScanTask> doPlanFiles(
      Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {

    Deque<Snapshot> changelogSnapshots =
        orderedChangelogSnapshots(fromSnapshotIdExclusive, toSnapshotIdInclusive);

    if (changelogSnapshots.isEmpty()) {
      return CloseableIterable.empty();
    }

    Map<Long, SnapshotInfo> changelogSnapshotInfos = computeSnapshotInfos(table(), changelogSnapshots);

    boolean containAddedDeleteFiles = false;
    boolean containRemovedDataFiles = false;
    for (SnapshotInfo info : changelogSnapshotInfos.values()) {
      containAddedDeleteFiles = containAddedDeleteFiles || !info.addedDeleteFiles().isEmpty();
      containRemovedDataFiles = containRemovedDataFiles || info.hasRemovedDataFiles();
    }

    Set<ManifestFile> dataManifests;
    Set<ManifestFile> deleteManifests;
    if (containAddedDeleteFiles) {
      // scan all dataFiles to locate the deleted record and ensure that this record has not been deleted before
      dataManifests = Sets.newHashSet(changelogSnapshots.getLast().dataManifests(tableOps().io()));
      deleteManifests = Sets.newHashSet(changelogSnapshots.getLast().deleteManifests(tableOps().io()));
    } else {
      dataManifests = FluentIterable.from(changelogSnapshots)
          .transformAndConcat(snapshot -> snapshot.dataManifests(table().io()))
          .filter(manifest -> changelogSnapshotInfos.containsKey(manifest.snapshotId()))
          .toSet();

      // scan all deleteFiles to locate the deleted records when there are removed data files
      deleteManifests = !containRemovedDataFiles ? ImmutableSet.of() :
          Sets.newHashSet(changelogSnapshots.getLast().deleteManifests(tableOps().io()));
    }

    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), dataManifests, deleteManifests)
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter());

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (dataManifests.size() > 1 && shouldPlanWithExecutor()) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }

    return manifestGroup.plan(new CreateChangelogScanTaskTasks(changelogSnapshotInfos));
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

  private static Map<Long, SnapshotInfo> computeSnapshotInfos(Table table, Deque<Snapshot> snapshots) {
    Map<Long, SnapshotInfo> snapshotInfos = Maps.newHashMap();

    int ordinal = 0;
    for (Snapshot snapshot : snapshots) {
      Set<CharSequence> removedDataFiles = FluentIterable
          .from(snapshot.removedDataFiles(table.io()))
          .transform(ContentFile::path)
          .toSet();

      Set<CharSequence> addedDeleteFiles = FluentIterable
          .from(snapshot.addedDeleteFiles(table.io()))
          .transform(ContentFile::path)
          .toSet();
      snapshotInfos.put(snapshot.snapshotId(),
          new SnapshotInfo(snapshot.snapshotId(), ordinal++, removedDataFiles.isEmpty(), addedDeleteFiles));
    }

    return snapshotInfos;
  }

  private static class CreateChangelogScanTaskTasks implements CreateTasksFunction<ChangelogScanTask> {
    private final Map<Long, Integer> snapshotOrdinals = Maps.newHashMap();
    private final Map<CharSequence, Long> addedDeleteSnapshotIds = Maps.newHashMap();

    CreateChangelogScanTaskTasks(Map<Long, SnapshotInfo> snapshotInfos) {
      for (Map.Entry<Long, SnapshotInfo> kv : snapshotInfos.entrySet()) {
        snapshotOrdinals.put(kv.getKey(), kv.getValue().ordinals());
        kv.getValue().addedDeleteFiles().forEach(file -> addedDeleteSnapshotIds.put(file, kv.getKey()));
      }
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context) {

      return CloseableIterable.filter(CloseableIterable.transform(
          entries,
          entry -> {
            long commitSnapshotId = entry.snapshotId();
            DataFile dataFile = entry.file().copy(context.shouldKeepStats());
            DeleteFile[] deleteFiles = context.deletes().forEntry(entry);

            switch (entry.status()) {
              case ADDED:
                return snapshotOrdinals.containsKey(commitSnapshotId) ?
                  getAddedRowsScanTask(commitSnapshotId, dataFile, deleteFiles, context) :
                  getDeletedRowsScanTask(commitSnapshotId, dataFile, deleteFiles, context);
              case DELETED:
                return getDeletedDataFileScanTask(commitSnapshotId, dataFile, deleteFiles, context);
              case EXISTING:
                return getDeletedRowsScanTask(commitSnapshotId, dataFile, deleteFiles, context);
              default:
                throw new IllegalArgumentException("Unexpected entry status: " + entry.status());
            }
          }), Objects::nonNull);
    }

    private ChangelogScanTask getAddedRowsScanTask(
        long commitSnapshotId, DataFile dataFile, DeleteFile[] deleteFiles, TaskContext context) {
      return new BaseAddedRowsScanTask(
          snapshotOrdinals.get(commitSnapshotId),
          commitSnapshotId,
          dataFile,
          deleteFiles,
          context.schemaAsString(),
          context.specAsString(),
          context.residuals());
    }

    private ChangelogScanTask getDeletedDataFileScanTask(
        long commitSnapshotId, DataFile dataFile, DeleteFile[] deleteFiles, TaskContext context) {
      if (snapshotOrdinals.containsKey(commitSnapshotId)) {
        return new BaseDeletedDataFileScanTask(
            snapshotOrdinals.get(commitSnapshotId),
            commitSnapshotId,
            dataFile,
            deleteFiles,
            context.schemaAsString(),
            context.specAsString(),
            context.residuals());
      } else {
        // ignore removed data files.
        return null;
      }
    }

    private ChangelogScanTask getDeletedRowsScanTask(
        long commitSnapshotId, DataFile dataFile, DeleteFile[] deleteFiles, TaskContext context) {
      int changeOrdinal = 0;
      List<DeleteFile> addedDeletes = Lists.newArrayList();
      List<DeleteFile> existingDeletes = Lists.newArrayList();
      for (DeleteFile deleteFile : deleteFiles) {
        if (addedDeleteSnapshotIds.containsKey(deleteFile.path())) {
          // use the ordinal of the last added delete file as the final change ordinal.
          long snapshotId = addedDeleteSnapshotIds.get(deleteFile.path());
          changeOrdinal = Math.max(changeOrdinal, snapshotOrdinals.get(snapshotId));

          addedDeletes.add(deleteFile);
        } else {
          existingDeletes.add(deleteFile);
        }
      }

      if (!addedDeletes.isEmpty()) {
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
        // ignore this data file for not contain any new deleted rows.
        return null;
      }
    }
  }

  private static class SnapshotInfo {
    private final long snapshotId;
    private final int ordinals;
    private final boolean hasRemovedDataFiles;
    private final Set<CharSequence> addedDeleteFiles;

    private SnapshotInfo(long snapshotId, int ordinals, boolean hasRemovedFiles, Set<CharSequence> deleteFiles) {
      this.snapshotId = snapshotId;
      this.ordinals = ordinals;
      this.hasRemovedDataFiles = hasRemovedFiles;
      this.addedDeleteFiles = deleteFiles;
    }

    public long snapshotId() {
      return snapshotId;
    }

    public int ordinals() {
      return ordinals;
    }

    public boolean hasRemovedDataFiles() {
      return hasRemovedDataFiles;
    }

    public Set<CharSequence> addedDeleteFiles() {
      return addedDeleteFiles;
    }
  }
}
