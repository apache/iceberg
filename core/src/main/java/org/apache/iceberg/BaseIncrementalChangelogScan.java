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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
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
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
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

    Set<Long> changelogSnapshotIds = toSnapshotIds(changelogSnapshots);
    Snapshot toSnapshot = table().snapshot(toSnapshotIdInclusive);

    Set<ManifestFile> newDataManifests =
        FluentIterable.from(changelogSnapshots)
            .transformAndConcat(snapshot -> snapshot.dataManifests(table().io()))
            .filter(manifest -> changelogSnapshotIds.contains(manifest.snapshotId()))
            .toSet();

    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), newDataManifests, ImmutableList.of())
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .filterManifestEntries(entry -> changelogSnapshotIds.contains(entry.snapshotId()))
            .ignoreExisting();

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (newDataManifests.size() > 1 && shouldPlanWithExecutor()) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }

    ImmutableSet<ManifestFile> allDeleteManifestFiles =
        FluentIterable.from(toSnapshot.deleteManifests(table().io())).toSet();

    // collect delete manifest files between fromSnapshotIdExclusive and to toSnapshotIdInclusive
    Set<ManifestFile> newDeleteManifests =
        allDeleteManifestFiles.stream()
            .filter(manifest -> changelogSnapshotIds.contains(manifest.snapshotId()))
            .collect(Collectors.toSet());

    if (!newDeleteManifests.isEmpty()) {
      // build a map from snapshot to delete entries
      Map<Snapshot, List<ManifestEntry<DeleteFile>>> deleteEntriesMap = Maps.newHashMap();
      newDeleteManifests.forEach(
          file -> {
            ManifestReader<DeleteFile> deleteFiles =
                ManifestFiles.readDeleteManifest(file, table().io(), table().specs());
            try (CloseableIterable<ManifestEntry<DeleteFile>> entries = deleteFiles.entries()) {
              List<ManifestEntry<DeleteFile>> entryList = Lists.newArrayList();
              entries.forEach(entry -> entryList.add(entry.copy()));
              deleteEntriesMap.put(table().snapshot(file.snapshotId()), entryList);
            } catch (IOException e) {
              throw new RuntimeException("Failed to close delete entries.");
            }
          });

      ImmutableSet<ManifestFile> allDataManifestFiles =
          FluentIterable.from(toSnapshot.dataManifests(table().io())).toSet();

      ManifestGroup deleteManifestGroup =
          new ManifestGroup(table().io(), allDataManifestFiles, allDeleteManifestFiles)
              .specsById(table().specs())
              .caseSensitive(isCaseSensitive())
              .select(scanColumns())
              .filterData(filter())
              .ignoreExisting();

      if (shouldIgnoreResiduals()) {
        deleteManifestGroup = deleteManifestGroup.ignoreResiduals();
      }

      if (shouldPlanWithExecutor()) {
        deleteManifestGroup = deleteManifestGroup.planWith(planExecutor());
      }

      // combine data file change tasks and delete rows scan tasks
      return CloseableIterable.withNoopClose(
          Iterables.concat(
              manifestGroup.plan(new CreateDataFileChangeTasks(changelogSnapshots)),
              deleteManifestGroup.plan(
                  new CreateDeletedRowsScanTask(changelogSnapshots, deleteEntriesMap))));
    }

    return manifestGroup.plan(new CreateDataFileChangeTasks(changelogSnapshots));
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
    private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

    private final Map<Long, Integer> snapshotOrdinals;

    CreateDataFileChangeTasks(Deque<Snapshot> snapshots) {
      this.snapshotOrdinals = computeSnapshotOrdinals(snapshots);
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context) {

      return CloseableIterable.transform(
          entries,
          entry -> {
            long commitSnapshotId = entry.snapshotId();
            int changeOrdinal = snapshotOrdinals.get(commitSnapshotId);
            DataFile dataFile = entry.file().copy(context.shouldKeepStats());

            switch (entry.status()) {
              case ADDED:
                return new BaseAddedRowsScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    NO_DELETES,
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              case DELETED:
                return new BaseDeletedDataFileScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    NO_DELETES,
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              default:
                throw new IllegalArgumentException("Unexpected entry status: " + entry.status());
            }
          });
    }
  }

  private static class CreateDeletedRowsScanTask implements CreateTasksFunction<ChangelogScanTask> {
    private final Map<Long, Integer> snapshotOrdinals;
    private final Map<Snapshot, List<ManifestEntry<DeleteFile>>> newDeleteEntries;

    CreateDeletedRowsScanTask(
        Deque<Snapshot> snapshots, Map<Snapshot, List<ManifestEntry<DeleteFile>>> deleteEntries) {
      this.snapshotOrdinals = computeSnapshotOrdinals(snapshots);
      this.newDeleteEntries = deleteEntries;
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> dataEntries, TaskContext ctx) {
      List<CloseableIterable<ChangelogScanTask>> taskIteratorList = Lists.newArrayList();
      newDeleteEntries.forEach(
          (snapshot, deleteEntries) ->
              taskIteratorList.add(transformTask(dataEntries, ctx, snapshot, deleteEntries)));

      return CloseableIterable.filter(CloseableIterable.concat(taskIteratorList), Objects::nonNull);
    }

    private CloseableIterable<ChangelogScanTask> transformTask(
        CloseableIterable<ManifestEntry<DataFile>> dataEntries,
        TaskContext ctx,
        Snapshot snapshot,
        List<ManifestEntry<DeleteFile>> deleteEntries) {
      return CloseableIterable.transform(
          dataEntries,
          dataEntry -> {
            DataFile dataFile = dataEntry.file().copy(ctx.shouldKeepStats());
            DeleteFile[] allDeleteFiles =
                ctx.deletes().forDataFile(dataEntry.dataSequenceNumber(), dataFile);
            Set<DeleteFile> newDeleteFileSet = Sets.newHashSet();
            deleteEntries.forEach(deleteEntry -> newDeleteFileSet.add(deleteEntry.file()));
            List<DeleteFile> addedDeleteFileList = Lists.newArrayList();
            List<DeleteFile> existingDeleteFileList = Lists.newArrayList();

            Arrays.stream(allDeleteFiles)
                .forEach(
                    file -> {
                      if (newDeleteFileSet.contains(file)) {
                        addedDeleteFileList.add(file);
                      } else if (file.dataSequenceNumber() < snapshot.sequenceNumber()) {
                        existingDeleteFileList.add(file);
                      }
                    });

            // If no deleted files match, no task is created, null will be filtered.
            if (addedDeleteFileList.isEmpty()) {
              return null;
            }

            return new BaseDeletedRowsScanTask(
                snapshotOrdinals.get(snapshot.snapshotId()),
                snapshot.snapshotId(),
                dataFile,
                addedDeleteFileList.toArray(new DeleteFile[0]),
                existingDeleteFileList.toArray(new DeleteFile[0]),
                ctx.schemaAsString(),
                ctx.specAsString(),
                ctx.residuals());
          });
    }
  }
}
