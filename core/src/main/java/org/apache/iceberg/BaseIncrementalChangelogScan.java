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
import org.apache.iceberg.ManifestGroup.CreateTasksFunction;
import org.apache.iceberg.ManifestGroup.TaskContext;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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

    Map<Long, Integer> snapshotOrdinals = computeSnapshotOrdinals(changelogSnapshots);

    Iterable<CloseableIterable<ChangelogScanTask>> plans =
        FluentIterable.from(changelogSnapshots)
            .transform(
                snapshot -> {
                  List<ManifestFile> dataManifests = snapshot.dataManifests(table().io());
                  List<ManifestFile> deleteManifests = snapshot.deleteManifests(table().io());

                  ManifestGroup manifestGroup =
                      new ManifestGroup(table().io(), dataManifests, deleteManifests)
                          .specsById(table().specs())
                          .caseSensitive(isCaseSensitive())
                          .select(scanColumns())
                          .filterData(filter())
                          .columnsToKeepStats(columnsToKeepStats());

                  if (shouldIgnoreResiduals()) {
                    manifestGroup = manifestGroup.ignoreResiduals();
                  }

                  if (dataManifests.size() > 1 && shouldPlanWithExecutor()) {
                    manifestGroup = manifestGroup.planWith(planExecutor());
                  }

                  long snapshotId = snapshot.snapshotId();
                  long sequenceNumber = snapshot.sequenceNumber();
                  int changeOrdinal = snapshotOrdinals.get(snapshotId);
                  return manifestGroup.plan(
                      new CreateDataFileChangeTasks(snapshotId, sequenceNumber, changeOrdinal));
                });

    return CloseableIterable.concat(plans);
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

  private static Map<Long, Integer> computeSnapshotOrdinals(Deque<Snapshot> snapshots) {
    Map<Long, Integer> snapshotOrdinals = Maps.newHashMap();

    int ordinal = 0;

    for (Snapshot snapshot : snapshots) {
      snapshotOrdinals.put(snapshot.snapshotId(), ordinal++);
    }

    return snapshotOrdinals;
  }

  private static class DummyChangelogScanTask implements ChangelogScanTask {
    public static final DummyChangelogScanTask INSTANCE = new DummyChangelogScanTask();

    private DummyChangelogScanTask() {}

    @Override
    public ChangelogOperation operation() {
      return ChangelogOperation.DELETE;
    }

    @Override
    public int changeOrdinal() {
      return 0;
    }

    @Override
    public long commitSnapshotId() {
      return 0L;
    }
  }

  private static class CreateDataFileChangeTasks implements CreateTasksFunction<ChangelogScanTask> {
    private final long snapshotId;
    private final long sequenceNumber;
    private final int changeOrdinal;

    CreateDataFileChangeTasks(long snapshotId, long sequenceNumber, int changeOrdinal) {
      this.snapshotId = snapshotId;
      this.sequenceNumber = sequenceNumber;
      this.changeOrdinal = changeOrdinal;
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context) {

      CloseableIterable<ChangelogScanTask> tasks =
          CloseableIterable.transform(
              entries,
              entry -> {
                long entrySnapshotId = entry.snapshotId();
                DataFile dataFile = entry.file().copy(context.shouldKeepStats());
                DeleteFile[] deleteFiles = context.deletes().forEntry(entry);
                List<DeleteFile> added = Lists.newArrayList();
                List<DeleteFile> existing = Lists.newArrayList();
                for (DeleteFile deleteFile : deleteFiles) {
                  if (sequenceNumber == deleteFile.dataSequenceNumber()) {
                    added.add(deleteFile);
                  } else {
                    existing.add(deleteFile);
                  }
                }
                DeleteFile[] addedDeleteFiles = added.toArray(new DeleteFile[0]);
                DeleteFile[] existingDeleteFiles = existing.toArray(new DeleteFile[0]);

                switch (entry.status()) {
                  case ADDED:
                    if (entrySnapshotId == snapshotId) {
                      return new BaseAddedRowsScanTask(
                          changeOrdinal,
                          snapshotId,
                          dataFile,
                          addedDeleteFiles,
                          context.schemaAsString(),
                          context.specAsString(),
                          context.residuals());
                    } else {
                      // the data file is added before the snapshot we're processing
                      if (addedDeleteFiles.length == 0) {
                        return DummyChangelogScanTask.INSTANCE;
                      } else {
                        return new BaseDeletedRowsScanTask(
                            changeOrdinal,
                            snapshotId,
                            dataFile,
                            addedDeleteFiles,
                            existingDeleteFiles,
                            context.schemaAsString(),
                            context.specAsString(),
                            context.residuals());
                      }
                    }

                  case DELETED:
                    if (entrySnapshotId == snapshotId) {
                      return new BaseDeletedDataFileScanTask(
                          changeOrdinal,
                          snapshotId,
                          dataFile,
                          existingDeleteFiles,
                          context.schemaAsString(),
                          context.specAsString(),
                          context.residuals());
                    } else {
                      return DummyChangelogScanTask.INSTANCE;
                    }

                  case EXISTING:
                    if (addedDeleteFiles.length == 0) {
                      return DummyChangelogScanTask.INSTANCE;
                    } else {
                      return new BaseDeletedRowsScanTask(
                          changeOrdinal,
                          snapshotId,
                          dataFile,
                          addedDeleteFiles,
                          existingDeleteFiles,
                          context.schemaAsString(),
                          context.specAsString(),
                          context.residuals());
                    }

                  default:
                    throw new IllegalArgumentException(
                        "Unexpected entry status: " + entry.status());
                }
              });
      return CloseableIterable.filter(tasks, task -> (task != DummyChangelogScanTask.INSTANCE));
    }
  }
}
