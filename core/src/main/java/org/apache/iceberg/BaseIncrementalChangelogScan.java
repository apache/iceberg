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
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.ManifestGroup.CreateTasksFunction;
import org.apache.iceberg.ManifestGroup.TaskContext;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BaseIncrementalChangelogScan
    extends BaseIncrementalScan<
        IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>>
    implements IncrementalChangelogScan {

  private static final Logger LOG = LoggerFactory.getLogger(BaseIncrementalChangelogScan.class);

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

  // Counts how many times the existing (pre-range) delete index was actually built across all
  // planFiles() calls on this scan (accessed via package-private methods for testing)
  private final AtomicInteger existingDeleteIndexBuildCallCount = new AtomicInteger(0);

  @Override
  protected CloseableIterable<ChangelogScanTask> doPlanFiles(
      Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {

    Deque<Snapshot> changelogSnapshots =
        orderedChangelogSnapshots(fromSnapshotIdExclusive, toSnapshotIdInclusive);

    if (changelogSnapshots.isEmpty()) {
      return CloseableIterable.empty();
    }

    // Read each snapshot's new delete manifests once, collecting added and removed delete files
    DeleteFileChanges deleteFileChanges = collectDeleteFileChanges(changelogSnapshots);
    Map<Long, DeleteFileIndex> addedDeletesBySnapshot = deleteFileChanges.addedBySnapshot;

    boolean rangeHasDeleteFiles =
        hasDeleteFileChanges(deleteFileChanges) || hasExistingDeletes(fromSnapshotIdExclusive);

    if (rangeHasDeleteFiles && !includeDeleteFiles()) {
      throw new UnsupportedOperationException(
          String.format(
              "Changelog scans cannot be planned when delete files apply to the scan range "
                  + "(e.g. they produce %s, which not all engines can execute). Set table "
                  + "property or scan option '%s' to true to enable delete file support in "
                  + "changelog scans",
              DeletedRowsScanTask.class.getSimpleName(),
              TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES));
    }

    // Collect data file changes in the range and derive the scope of existing deletes that can
    // apply to tasks, so that building the existing delete index only reads relevant delete files.
    // Both outputs only feed the delete file machinery, so when no delete file exists before the
    // range and none is added or removed within it, this extra manifest pass is skipped: every
    // cumulative index can only be empty and no DeletedRowsScanTask can be planned
    ChangedDataFiles changedDataFiles = ChangedDataFiles.empty(table().specs());
    ExistingDeleteScope existingDeleteScope = null;
    if (rangeHasDeleteFiles) {
      changedDataFiles = collectChangedDataFiles(changelogSnapshots);
      existingDeleteScope = buildExistingDeleteScope(changedDataFiles, addedDeletesBySnapshot);
    }

    // Tracks all deletes that applied before each snapshot in the range; the existing (pre-range)
    // delete index is built lazily on first use and memoized for this invocation only, as a later
    // plan of the same scan may resolve a different range
    ExistingDeleteIndexHolder existingDeleteIndexHolder =
        new ExistingDeleteIndexHolder(fromSnapshotIdExclusive, existingDeleteScope);
    CumulativeDeleteIndexes cumulativeDeleteIndexes =
        new CumulativeDeleteIndexes(
            changelogSnapshots,
            addedDeletesBySnapshot,
            deleteFileChanges.removedBySnapshot,
            existingDeleteIndexHolder::get);

    Map<Long, Integer> snapshotOrdinals = computeSnapshotOrdinals(changelogSnapshots);

    // The task factory derives everything it needs from the manifest entry, so a single instance
    // is shared by every snapshot in the range
    CreateDataFileChangeTasks createDataFileChangeTasks =
        new CreateDataFileChangeTasks(
            snapshotOrdinals, addedDeletesBySnapshot, cumulativeDeleteIndexes);

    // Plan progressively: walk snapshots oldest to newest, emitting each snapshot's data file
    // tasks and then its deleted-rows tasks before moving on. Tasks come out ordered by change
    // ordinal by construction, and consumers stream them without materializing the whole range
    List<CloseableIterable<ChangelogScanTask>> perSnapshotTasks = Lists.newArrayList();
    for (Snapshot snapshot : changelogSnapshots) {
      perSnapshotTasks.add(planDataFileTasks(snapshot, createDataFileChangeTasks));
      perSnapshotTasks.add(
          planDeletedRowsTasksForSnapshot(
              snapshot,
              addedDeletesBySnapshot,
              cumulativeDeleteIndexes,
              changedDataFiles,
              snapshotOrdinals));
    }

    return CloseableIterable.concat(perSnapshotTasks);
  }

  /**
   * Plans ADDED and DELETED data file tasks for one snapshot from the data manifests that snapshot
   * wrote. ADDED entries in a manifest carry the snapshot ID that committed them (or inherit the
   * manifest's), and DELETED entries carry the deleting snapshot's ID, so filtering both manifests
   * and entries by this snapshot's ID partitions the range's changes exactly.
   */
  private CloseableIterable<ChangelogScanTask> planDataFileTasks(
      Snapshot snapshot, CreateDataFileChangeTasks createTasks) {
    long snapshotId = snapshot.snapshotId();

    List<ManifestFile> newDataManifests =
        snapshot.dataManifests(table().io()).stream()
            .filter(
                manifest -> manifest.snapshotId() != null && manifest.snapshotId() == snapshotId)
            .toList();

    if (newDataManifests.isEmpty()) {
      return CloseableIterable.empty();
    }

    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), newDataManifests, ImmutableList.of())
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .filterManifestEntries(
                entry -> entry.snapshotId() != null && entry.snapshotId() == snapshotId)
            .ignoreExisting()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (newDataManifests.size() > 1 && shouldPlanWithExecutor()) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }

    return manifestGroup.plan(createTasks);
  }

  /**
   * Plans DeletedRowsScanTask instances for EXISTING data files affected by this snapshot's added
   * delete files. The scan of the snapshot's live data manifests is deferred until the returned
   * iterable is consumed.
   */
  private CloseableIterable<ChangelogScanTask> planDeletedRowsTasksForSnapshot(
      Snapshot snapshot,
      Map<Long, DeleteFileIndex> addedDeletesBySnapshot,
      CumulativeDeleteIndexes cumulativeDeleteIndexes,
      ChangedDataFiles changedDataFiles,
      Map<Long, Integer> snapshotOrdinals) {
    DeleteFileIndex addedDeleteIndex = addedDeletesBySnapshot.get(snapshot.snapshotId());
    if (addedDeleteIndex == null || addedDeleteIndex.isEmpty()) {
      return CloseableIterable.empty();
    }

    // re-iterating the returned iterable replans this snapshot's tasks (same as ManifestGroup.plan)
    return CloseableIterable.withNoopClose(
        () -> {
          // A task for an EXISTING file requires one of this snapshot's added deletes to apply,
          // so data manifests are pruned to the partitions of this snapshot's added delete files
          // and, when every added delete is file-scoped, entries to the referenced files
          PartitionSet snapshotDeletePartitions = addedDeletePartitions(addedDeleteIndex);
          Set<String> candidateLocations = fileScopedTargets(addedDeleteIndex);

          List<ChangelogScanTask> tasks = Lists.newArrayList();
          processSnapshotForDeletedRowsTasks(
              snapshot,
              addedDeleteIndex,
              cumulativeDeleteIndexes,
              changedDataFiles.statusBySnapshot.getOrDefault(snapshot.snapshotId(), Map.of()),
              snapshotOrdinals,
              snapshotDeletePartitions,
              candidateLocations,
              tasks);
          return tasks.iterator();
        });
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

  /**
   * Builds a delete file index for existing deletes that were present before the start snapshot.
   * These deletes should be applied to data files but should not generate DELETE changelog rows.
   * Manifests that cannot hold a delete relevant to the range are pruned before the index builder
   * reads the rest, and files outside the affected scope are skipped as entries are read.
   */
  private DeleteFileIndex buildExistingDeleteIndex(
      Long fromSnapshotIdExclusive, ExistingDeleteScope scope) {
    if (fromSnapshotIdExclusive == null) {
      return DeleteFileIndex.emptyIndex();
    }
    Snapshot fromSnapshot = table().snapshot(fromSnapshotIdExclusive);
    Preconditions.checkState(
        fromSnapshot != null, "Cannot find starting snapshot: %s", fromSnapshotIdExclusive);

    List<ManifestFile> deleteManifests = fromSnapshot.deleteManifests(table().io());
    if (deleteManifests.isEmpty()) {
      return DeleteFileIndex.emptyIndex();
    }

    // Prune manifests that cannot contain deletes for any partition affected by the scan range
    if (scope != null) {
      deleteManifests =
          pruneManifestsByAffectedPartitions(deleteManifests, scope.allAffectedPartitions);
    }

    if (deleteManifests.isEmpty()) {
      return DeleteFileIndex.emptyIndex();
    }

    // filterData prunes manifests and entries against the scan filter with per-spec cached
    // evaluators and keeps minimal stats. Entry pruning also runs the filter against each delete
    // file's own stats, which may drop a delete file whose bounds cannot match the filter; that is
    // only safe because the task residual re-applies the filter to the rows a task emits, so
    // ignoreResiduals must be forwarded (as ManifestGroup does) to disable it when the residual is
    // dropped. Deliberately no planWith: this may run lazily under the cumulative-index monitor on
    // shared worker-pool threads, and nested submission to the same pool can starve it
    DeleteFileIndex.Builder builder =
        DeleteFileIndex.builderFor(table().io(), deleteManifests)
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .filterData(filter());

    if (shouldIgnoreResiduals()) {
      builder.ignoreResiduals();
    }

    if (scope != null) {
      // skip delete files that cannot apply to any task in the scan range
      builder.deleteFilePredicate(scope::keeps);
    }

    return builder.build();
  }

  /**
   * Builds the existing (pre-range) delete index at most once per planFiles() invocation, so that
   * concurrent planning threads share one index while a later plan of the same scan, which may
   * resolve a different range and scope, builds its own.
   */
  private class ExistingDeleteIndexHolder {
    private final Long fromSnapshotIdExclusive;
    private final ExistingDeleteScope scope;
    private DeleteFileIndex index = null;

    ExistingDeleteIndexHolder(Long fromSnapshotIdExclusive, ExistingDeleteScope scope) {
      this.fromSnapshotIdExclusive = fromSnapshotIdExclusive;
      this.scope = scope;
    }

    synchronized DeleteFileIndex get() {
      if (index == null) {
        this.index = buildExistingDeleteIndex(fromSnapshotIdExclusive, scope);
        existingDeleteIndexBuildCallCount.incrementAndGet();
      }

      return index;
    }
  }

  // Visible for testing
  int existingDeleteIndexBuildCount() {
    return existingDeleteIndexBuildCallCount.get();
  }

  // Visible for testing
  boolean wasExistingDeleteIndexBuilt() {
    return existingDeleteIndexBuildCallCount.get() > 0;
  }

  /** Added delete indexes and removed delete files, per snapshot, read in one manifest pass. */
  private static class DeleteFileChanges {
    private final Map<Long, DeleteFileIndex> addedBySnapshot;
    private final Map<Long, List<DeleteFile>> removedBySnapshot;

    DeleteFileChanges(
        Map<Long, DeleteFileIndex> addedBySnapshot, Map<Long, List<DeleteFile>> removedBySnapshot) {
      this.addedBySnapshot = addedBySnapshot;
      this.removedBySnapshot = removedBySnapshot;
    }
  }

  /**
   * Reads the delete manifests written by each changelog snapshot exactly once, splitting live
   * entries into the snapshot's added delete files (indexed) and removed delete files.
   */
  private DeleteFileChanges collectDeleteFileChanges(Deque<Snapshot> changelogSnapshots) {
    Map<Long, DeleteFileIndex> addedBySnapshot = Maps.newConcurrentMap();
    Map<Long, List<DeleteFile>> removedBySnapshot = Maps.newConcurrentMap();

    // one evaluator of the scan filter per partition spec, shared by all manifests and snapshots
    Map<Integer, Evaluator> partitionEvaluators = Maps.newConcurrentMap();

    Tasks.foreach(changelogSnapshots)
        .retry(3)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(planExecutor())
        .onFailure(
            (snapshot, exc) ->
                LOG.warn(
                    "Failed to read delete manifests for snapshot {}", snapshot.snapshotId(), exc))
        .run(
            snapshot -> {
              long snapshotId = snapshot.snapshotId();
              List<ManifestFile> newDeleteManifests =
                  snapshot.deleteManifests(table().io()).stream()
                      .filter(
                          manifest ->
                              manifest.snapshotId() != null && manifest.snapshotId() == snapshotId)
                      .toList();

              List<DeleteFile> added = Lists.newArrayList();
              List<DeleteFile> removed = Lists.newArrayList();
              for (ManifestFile manifest : newDeleteManifests) {
                readDeleteManifest(manifest, snapshotId, partitionEvaluators, added, removed);
              }

              addedBySnapshot.put(
                  snapshotId,
                  added.isEmpty()
                      ? DeleteFileIndex.emptyIndex()
                      : DeleteFileIndex.builderFor(added)
                          .specsById(table().specs())
                          .caseSensitive(isCaseSensitive())
                          .build());
              removedBySnapshot.put(snapshotId, removed);
            });

    return new DeleteFileChanges(addedBySnapshot, removedBySnapshot);
  }

  /**
   * Reads one delete manifest, collecting the delete files this snapshot added and removed. Only
   * entries committed by the given snapshot are considered; EXISTING entries carried over from
   * earlier snapshots keep their original snapshot ID and are skipped.
   */
  private void readDeleteManifest(
      ManifestFile manifest,
      long snapshotId,
      Map<Integer, Evaluator> partitionEvaluators,
      List<DeleteFile> added,
      List<DeleteFile> removed) {
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, table().io(), table().specs())) {
      for (ManifestEntry<DeleteFile> entry : reader.entries()) {
        if (entry.snapshotId() == null || entry.snapshotId() != snapshotId) {
          continue;
        }

        DeleteFile file = entry.file();

        // Apply partition pruning - skip delete files that cannot match the scan filter
        if (!partitionMatchesFilter(file, partitionEvaluators)) {
          continue;
        }

        Set<Integer> columns =
            file.content() == FileContent.POSITION_DELETES
                ? Set.of(MetadataColumns.DELETE_FILE_PATH.fieldId())
                : Set.copyOf(file.equalityFieldIds());
        DeleteFile copied = ContentFileUtil.copy(file, true, columns);

        if (entry.status() == ManifestEntry.Status.DELETED) {
          removed.add(copied);
        } else {
          added.add(copied);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read delete manifest: " + manifest.path(), e);
    }
  }

  /**
   * Returns true if the delete file's partition may contain rows matching the scan filter, so that
   * delete files that cannot apply to any scanned row are skipped. The scan filter is projected
   * into each spec's partition space once and the resulting evaluator is cached in the given map,
   * which may be shared by concurrent readers.
   */
  private boolean partitionMatchesFilter(DeleteFile file, Map<Integer, Evaluator> evaluatorCache) {
    Expression currentFilter = filter();
    if (currentFilter.equals(Expressions.alwaysTrue())) {
      return true;
    }

    PartitionSpec spec = table().specs().get(file.specId());
    if (spec == null || spec.isUnpartitioned()) {
      // if the spec is unknown or the table is unpartitioned, be conservative and keep the file
      return true;
    }

    try {
      Evaluator evaluator =
          evaluatorCache.computeIfAbsent(
              file.specId(),
              ignored -> {
                // an inclusive projection turns predicates on source columns into predicates on
                // partition values, so non-identity transforms are handled correctly
                Expression partitionFilter =
                    Projections.inclusive(spec, isCaseSensitive()).project(currentFilter);
                return new Evaluator(spec.partitionType(), partitionFilter, isCaseSensitive());
              });
      return evaluator.eval(file.partition());
    } catch (Exception e) {
      // if the filter cannot be projected or evaluated, be conservative and keep the file
      return true;
    }
  }

  /**
   * Returns the partitions of the added delete files, or an empty set when some delete has global
   * reach (an unpartitioned spec) and data manifests cannot be pruned by partition.
   */
  private PartitionSet addedDeletePartitions(DeleteFileIndex addedDeleteIndex) {
    PartitionSet partitions = PartitionSet.create(table().specs());
    for (DeleteFile file : addedDeleteIndex.referencedDeleteFiles()) {
      PartitionSpec spec = table().specs().get(file.specId());
      if (spec == null || spec.isUnpartitioned()) {
        // an empty set disables partition pruning
        return PartitionSet.create(table().specs());
      }

      partitions.add(file.specId(), file.partition());
    }

    return partitions;
  }

  /**
   * Returns the data file locations referenced by the added delete files when every added delete is
   * file-scoped (a DV or a single-file position delete), or null when some added delete may apply
   * to files other than the ones it references.
   */
  private static Set<String> fileScopedTargets(DeleteFileIndex addedDeleteIndex) {
    Set<String> locations = Sets.newHashSet();
    for (DeleteFile file : addedDeleteIndex.referencedDeleteFiles()) {
      String referencedLocation = ContentFileUtil.referencedDataFileLocation(file);
      if (referencedLocation == null) {
        return null;
      }

      locations.add(referencedLocation);
    }

    return locations;
  }

  /** Data file changes observed across the changelog snapshots. */
  private static class ChangedDataFiles {
    private final Map<Long, Map<String, ManifestEntry.Status>> statusBySnapshot;
    private final Set<String> deletedFileLocations;
    private final PartitionSet deletedFilePartitions;

    ChangedDataFiles(
        Map<Long, Map<String, ManifestEntry.Status>> statusBySnapshot,
        Set<String> deletedFileLocations,
        PartitionSet deletedFilePartitions) {
      this.statusBySnapshot = statusBySnapshot;
      this.deletedFileLocations = deletedFileLocations;
      this.deletedFilePartitions = deletedFilePartitions;
    }

    static ChangedDataFiles empty(Map<Integer, PartitionSpec> specsById) {
      return new ChangedDataFiles(Map.of(), Set.of(), PartitionSet.create(specsById));
    }
  }

  /** Returns true if any snapshot in the range added or removed a delete file. */
  private static boolean hasDeleteFileChanges(DeleteFileChanges deleteFileChanges) {
    for (DeleteFileIndex addedDeletes : deleteFileChanges.addedBySnapshot.values()) {
      if (!addedDeletes.isEmpty()) {
        return true;
      }
    }

    for (List<DeleteFile> removedDeletes : deleteFileChanges.removedBySnapshot.values()) {
      if (!removedDeletes.isEmpty()) {
        return true;
      }
    }

    return false;
  }

  /** Returns true if the snapshot the range starts from has any delete files. */
  private boolean hasExistingDeletes(Long fromSnapshotIdExclusive) {
    if (fromSnapshotIdExclusive == null) {
      return false;
    }

    Snapshot fromSnapshot = table().snapshot(fromSnapshotIdExclusive);
    return fromSnapshot != null && !fromSnapshot.deleteManifests(table().io()).isEmpty();
  }

  private boolean includeDeleteFiles() {
    boolean tableValue =
        PropertyUtil.propertyAsBoolean(
            table().properties(),
            TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES,
            TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES_DEFAULT);
    return PropertyUtil.propertyAsBoolean(
        options(), TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES, tableValue);
  }

  /**
   * Collects, for each snapshot, the statuses of data files changed in it, along with the locations
   * and partitions of data files removed anywhere in the scan range.
   */
  private ChangedDataFiles collectChangedDataFiles(Deque<Snapshot> changelogSnapshots) {

    Map<Long, Map<String, ManifestEntry.Status>> statusBySnapshot = Maps.newConcurrentMap();
    Queue<Set<String>> localDeletedLocations = new ConcurrentLinkedQueue<>();
    Queue<PartitionSet> localDeletedPartitions = new ConcurrentLinkedQueue<>();

    Tasks.foreach(changelogSnapshots)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(planExecutor())
        .run(
            snapshot -> {
              long snapshotId = snapshot.snapshotId();
              Map<String, ManifestEntry.Status> fileStatuses = Maps.newHashMap();
              Set<String> deletedLocations = Sets.newHashSet();
              PartitionSet deletedPartitions = PartitionSet.create(table().specs());

              List<ManifestFile> changedDataManifests =
                  snapshot.dataManifests(table().io()).stream()
                      .filter(
                          manifest ->
                              manifest.snapshotId() != null && manifest.snapshotId() == snapshotId)
                      .toList();

              if (!changedDataManifests.isEmpty()) {
                ManifestGroup changedGroup =
                    new ManifestGroup(table().io(), changedDataManifests, ImmutableList.of())
                        .specsById(table().specs())
                        .caseSensitive(isCaseSensitive())
                        .select(scanColumns())
                        .filterData(filter())
                        .ignoreExisting()
                        .columnsToKeepStats(columnsToKeepStats());

                try (CloseableIterable<ManifestEntry<DataFile>> entries = changedGroup.entries()) {
                  for (ManifestEntry<DataFile> entry : entries) {
                    if (entry.snapshotId() != null && entry.snapshotId() == snapshotId) {
                      fileStatuses.put(entry.file().location(), entry.status());
                      if (entry.status() == ManifestEntry.Status.DELETED) {
                        // copy the file: the reader reuses the entry, so the partition struct
                        // must not be stored as a live reference
                        DataFile deletedFile = entry.file().copyWithoutStats();
                        deletedLocations.add(deletedFile.location());
                        deletedPartitions.add(deletedFile.specId(), deletedFile.partition());
                      }
                    }
                  }
                } catch (Exception e) {
                  throw new RuntimeException(
                      "Failed to collect file statuses for snapshot " + snapshot.snapshotId(), e);
                }
              }

              statusBySnapshot.put(snapshot.snapshotId(), fileStatuses);
              localDeletedLocations.add(deletedLocations);
              localDeletedPartitions.add(deletedPartitions);
            });

    Set<String> deletedFileLocations = Sets.newHashSet();
    localDeletedLocations.forEach(deletedFileLocations::addAll);

    PartitionSet deletedFilePartitions = PartitionSet.create(table().specs());
    localDeletedPartitions.forEach(deletedFilePartitions::addAll);

    return new ChangedDataFiles(statusBySnapshot, deletedFileLocations, deletedFilePartitions);
  }

  /**
   * Scope of existing (pre-range) delete files that can apply to changelog tasks in the scan range.
   * Existing deletes are only attached to data files that were removed in the range or hit by newly
   * added deletes, so delete files outside this scope can be skipped while building the existing
   * delete index:
   *
   * <ul>
   *   <li>file-scoped position deletes and DVs are kept only if they reference an affected data
   *       file location, or lie in a partition where a partition-scoped delete was added
   *   <li>equality deletes and partition-scoped position deletes are kept only if they have global
   *       reach or lie in an affected partition
   * </ul>
   */
  private static class ExistingDeleteScope {
    private final Map<Integer, PartitionSpec> specsById;
    private final PartitionSet allAffectedPartitions;
    private final PartitionSet broadDeletePartitions;
    private final Set<String> affectedDataFileLocations;

    ExistingDeleteScope(
        Map<Integer, PartitionSpec> specsById,
        PartitionSet allAffectedPartitions,
        PartitionSet broadDeletePartitions,
        Set<String> affectedDataFileLocations) {
      this.specsById = specsById;
      this.allAffectedPartitions = allAffectedPartitions;
      this.broadDeletePartitions = broadDeletePartitions;
      this.affectedDataFileLocations = affectedDataFileLocations;
    }

    boolean keeps(DeleteFile file) {
      String referencedLocation = ContentFileUtil.referencedDataFileLocation(file);
      if (referencedLocation != null) {
        // file-scoped position delete or DV
        return affectedDataFileLocations.contains(referencedLocation)
            || broadDeletePartitions.contains(file.specId(), file.partition());
      }

      // equality delete or partition-scoped position delete
      PartitionSpec spec = specsById.get(file.specId());
      return spec == null
          || spec.isUnpartitioned()
          || allAffectedPartitions.contains(file.specId(), file.partition());
    }
  }

  /**
   * Derives the scope of existing deletes that can apply to tasks in the scan range from the
   * removed data files and the added delete files. Returns null when scoping is not possible
   * because some added delete has global reach and can affect any data file.
   */
  private ExistingDeleteScope buildExistingDeleteScope(
      ChangedDataFiles changedDataFiles, Map<Long, DeleteFileIndex> addedDeletesBySnapshot) {
    PartitionSet allAffected = PartitionSet.create(table().specs());
    PartitionSet broadDeletePartitions = PartitionSet.create(table().specs());
    Set<String> affectedLocations = Sets.newHashSet(changedDataFiles.deletedFileLocations);

    allAffected.addAll(changedDataFiles.deletedFilePartitions);

    for (DeleteFileIndex index : addedDeletesBySnapshot.values()) {
      for (DeleteFile file : index.referencedDeleteFiles()) {
        PartitionSpec spec = table().specs().get(file.specId());
        String referencedLocation = ContentFileUtil.referencedDataFileLocation(file);
        if (referencedLocation != null) {
          affectedLocations.add(referencedLocation);
        } else if (spec == null || spec.isUnpartitioned()) {
          // an equality delete or a partition-scoped position delete with global reach can
          // affect any data file, so existing deletes cannot be scoped
          return null;
        } else {
          broadDeletePartitions.add(file.specId(), file.partition());
        }

        allAffected.add(file.specId(), file.partition());
      }
    }

    return new ExistingDeleteScope(
        table().specs(), allAffected, broadDeletePartitions, affectedLocations);
  }

  /**
   * Prunes manifests that cannot contain files in any affected partition. A partitioned manifest
   * whose spec has no affected partitions is pruned: every delete-to-data match requires equal spec
   * IDs (file-scoped deletes share their target data file's spec and partition, and
   * partition-scoped lookups are keyed by the data file's spec), so neither an affected delete file
   * nor a data file matched by this range's deletes can live in a manifest of an unaffected spec.
   * Deletes with global reach disable this pruning upstream (null scope for existing deletes; an
   * empty partition set for per-snapshot data manifest pruning).
   */
  private List<ManifestFile> pruneManifestsByAffectedPartitions(
      List<ManifestFile> manifests, PartitionSet affectedPartitions) {
    if (affectedPartitions.isEmpty()) {
      return manifests;
    }

    Map<Integer, Expression> partitionExprsBySpec =
        buildAffectedPartitionExpressions(affectedPartitions);
    if (partitionExprsBySpec == null) {
      // some affected partition cannot be expressed; skip pruning
      return manifests;
    }

    try {
      Map<Integer, ManifestEvaluator> evaluatorsBySpec = Maps.newHashMap();
      List<ManifestFile> pruned = Lists.newArrayList();
      for (ManifestFile manifest : manifests) {
        PartitionSpec spec = table().specs().get(manifest.partitionSpecId());
        if (spec == null || spec.isUnpartitioned()) {
          pruned.add(manifest);
          continue;
        }

        Expression expr = partitionExprsBySpec.get(manifest.partitionSpecId());
        if (expr == null) {
          // no affected partitions in this manifest's spec: see method javadoc
          continue;
        }

        ManifestEvaluator evaluator = evaluatorsBySpec.get(manifest.partitionSpecId());
        if (evaluator == null) {
          evaluator = ManifestEvaluator.forPartitionFilter(expr, spec, isCaseSensitive());
          evaluatorsBySpec.put(manifest.partitionSpecId(), evaluator);
        }

        if (evaluator.eval(manifest)) {
          pruned.add(manifest);
        }
      }

      return pruned;
    } catch (Exception e) {
      // if the partition expressions cannot be evaluated, be conservative and keep all manifests
      LOG.warn("Failed to prune manifests by affected partitions, skipping pruning", e);
      return manifests;
    }
  }

  /**
   * Builds partition-space expressions (one per spec) matching the affected partition tuples. The
   * expressions reference partition field names and compare transformed partition values directly,
   * so they are correct for non-identity transforms such as bucket or truncate. Returns null if
   * some affected partition cannot be translated to an expression and pruning must be skipped.
   */
  private Map<Integer, Expression> buildAffectedPartitionExpressions(
      PartitionSet affectedPartitions) {
    Map<Integer, Expression> exprsBySpec = Maps.newHashMap();

    for (Pair<Integer, StructLike> pair : affectedPartitions) {
      int specId = pair.first();
      PartitionSpec spec = table().specs().get(specId);
      if (spec == null || spec.isUnpartitioned()) {
        // a partition tuple without a known partitioned spec may affect any manifest
        return null;
      }

      StructLike partition = pair.second();
      Expression tupleExpr = Expressions.alwaysTrue();
      for (int pos = 0; pos < spec.fields().size(); pos++) {
        String name = spec.partitionType().fields().get(pos).name();
        Object value = partition.get(pos, Object.class);
        if (value == null) {
          tupleExpr = Expressions.and(tupleExpr, Expressions.isNull(name));
        } else if (isNaN(value)) {
          // NaN cannot be expressed as an equality predicate
          return null;
        } else {
          tupleExpr = Expressions.and(tupleExpr, Expressions.equal(name, value));
        }
      }

      exprsBySpec.merge(specId, tupleExpr, Expressions::or);
    }

    return exprsBySpec;
  }

  private static boolean isNaN(Object value) {
    return (value instanceof Double && ((Double) value).isNaN())
        || (value instanceof Float && ((Float) value).isNaN());
  }

  /**
   * Tracks the delete files that applied before each snapshot in the scan range and lazily builds a
   * per-snapshot index of them.
   *
   * <p>The index for a snapshot combines deletes that existed before the scan range with deletes
   * added by earlier snapshots in the range. Delete files that were removed by earlier snapshots in
   * the range are excluded, so removed delete files are never assigned to tasks.
   *
   * <p>Snapshots are consumed oldest to newest, so this class keeps a single advancing cursor over
   * the range instead of materializing per-snapshot state: one running list of accumulated added
   * deletes, one running set of removed delete paths, and only the most recently built index.
   * Within one snapshot, planning worker threads may request the same index concurrently; those
   * calls are serialized by this object's monitor.
   */
  private class CumulativeDeleteIndexes {
    private final Supplier<DeleteFileIndex> existingDeleteIndexSupplier;
    private final List<Long> orderedSnapshotIds;
    private final Map<Long, DeleteFileIndex> addedDeletesBySnapshot;
    private final Map<Long, List<DeleteFile>> removedDeletesBySnapshot;

    // cursor state: position in orderedSnapshotIds of the next snapshot whose effects to apply,
    // together with the state produced by all snapshots before that position
    private int cursor = 0;
    private final List<DeleteFile> accumulatedAddedDeletes = Lists.newArrayList();
    private final Set<String> removedPaths = Sets.newHashSet();
    private Long latestIndexSnapshotId = null;
    private DeleteFileIndex latestIndex = null;

    CumulativeDeleteIndexes(
        Deque<Snapshot> snapshots,
        Map<Long, DeleteFileIndex> addedDeletesBySnapshot,
        Map<Long, List<DeleteFile>> removedDeletesBySnapshot,
        Supplier<DeleteFileIndex> existingDeleteIndexSupplier) {
      this.existingDeleteIndexSupplier = existingDeleteIndexSupplier;
      this.orderedSnapshotIds = snapshots.stream().map(Snapshot::snapshotId).toList();
      this.addedDeletesBySnapshot = addedDeletesBySnapshot;
      this.removedDeletesBySnapshot = removedDeletesBySnapshot;
    }

    /** Returns an index of all delete files that applied before the given snapshot. */
    synchronized DeleteFileIndex deletesBefore(long snapshotId) {
      if (latestIndexSnapshotId != null && latestIndexSnapshotId == snapshotId) {
        return latestIndex;
      }

      int position = orderedSnapshotIds.indexOf(snapshotId);
      Preconditions.checkArgument(
          position >= 0, "Cannot find snapshot %s in the changelog range", snapshotId);

      // a consumer that re-iterates the plan may ask for a snapshot the cursor already passed;
      // restarting from the beginning is correct, just slower
      if (position < cursor) {
        resetCursor();
      }

      // apply the effects of every snapshot strictly before the requested one
      while (cursor < position) {
        advanceCursor(orderedSnapshotIds.get(cursor));
        cursor += 1;
      }

      this.latestIndex = buildIndex();
      this.latestIndexSnapshotId = snapshotId;
      return latestIndex;
    }

    private void resetCursor() {
      this.cursor = 0;
      accumulatedAddedDeletes.clear();
      removedPaths.clear();
    }

    /** Applies one snapshot's delete file removals and additions to the running state. */
    private void advanceCursor(long snapshotId) {
      List<DeleteFile> removedDeletes =
          removedDeletesBySnapshot.getOrDefault(snapshotId, List.of());
      if (!removedDeletes.isEmpty()) {
        Set<String> currentRemovedPaths = Sets.newHashSet();
        for (DeleteFile removed : removedDeletes) {
          currentRemovedPaths.add(removed.location());
        }

        // the cumulative set is still needed to filter pre-range existing delete files, but only
        // this snapshot's removals may evict accumulated in-range deletes: a path removed earlier
        // and re-added later in the range is live again
        removedPaths.addAll(currentRemovedPaths);
        accumulatedAddedDeletes.removeIf(file -> currentRemovedPaths.contains(file.location()));
      }

      DeleteFileIndex addedDeleteIndex = addedDeletesBySnapshot.get(snapshotId);
      if (addedDeleteIndex != null && !addedDeleteIndex.isEmpty()) {
        for (DeleteFile file : addedDeleteIndex.referencedDeleteFiles()) {
          accumulatedAddedDeletes.add(file);
        }
      }
    }

    private DeleteFileIndex buildIndex() {
      List<DeleteFile> candidates = Lists.newArrayList();
      for (DeleteFile file : existingDeleteIndexSupplier.get().referencedDeleteFiles()) {
        if (!removedPaths.contains(file.location())) {
          candidates.add(file);
        }
      }

      candidates.addAll(accumulatedAddedDeletes);

      if (candidates.isEmpty()) {
        return DeleteFileIndex.emptyIndex();
      }

      // A data file has at most one live DV, but when a DV was replaced by a snapshot this scan
      // does not track (e.g. a mid-range REPLACE rewrote it), both versions may be accumulated.
      // Keep only the newest DV per referenced data file: DVs are cumulative, so the newest one
      // carries all previously deleted positions. Candidates are ordered oldest to newest.
      //
      // When a skipped REPLACE snapshot rewrote a delete file, the DV retained here is the
      // pre-range one, which is content-equivalent to the rewritten one but may already have been
      // physically removed by expire_snapshots; execution then fails on a missing file. This is an
      // accepted limitation, discussed on the PR.
      List<DeleteFile> deleteFiles = Lists.newArrayList();
      Map<String, DeleteFile> dvByReferencedFile = Maps.newHashMap();
      for (DeleteFile file : candidates) {
        if (ContentFileUtil.isDV(file)) {
          dvByReferencedFile.put(file.referencedDataFile(), file);
        } else {
          deleteFiles.add(file);
        }
      }

      deleteFiles.addAll(dvByReferencedFile.values());

      return DeleteFileIndex.builderFor(deleteFiles)
          .specsById(table().specs())
          .caseSensitive(isCaseSensitive())
          .build();
    }
  }

  /**
   * Processes data files for a snapshot to create DeletedRowsScanTask for existing files affected
   * by new delete files.
   */
  private void processSnapshotForDeletedRowsTasks(
      Snapshot snapshot,
      DeleteFileIndex addedDeleteIndex,
      CumulativeDeleteIndexes cumulativeDeleteIndexes,
      Map<String, ManifestEntry.Status> currentSnapshotFiles,
      Map<Long, Integer> snapshotOrdinals,
      PartitionSet snapshotDeletePartitions,
      Set<String> candidateLocations,
      List<ChangelogScanTask> tasks) {

    // Get all data files that exist in this snapshot, pruned to the partitions of this
    // snapshot's added delete files
    List<ManifestFile> allDataManifests = snapshot.dataManifests(table().io());
    List<ManifestFile> prunedManifests =
        pruneManifestsByAffectedPartitions(allDataManifests, snapshotDeletePartitions);

    ManifestGroup allDataGroup =
        new ManifestGroup(table().io(), prunedManifests, ImmutableList.of())
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .ignoreDeleted()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      allDataGroup = allDataGroup.ignoreResiduals();
    }

    String schemaString = SchemaParser.toJson(schema());

    // Cache per specId - same for all files with same specId
    Map<Integer, String> specStringCache = Maps.newHashMap();
    Map<Integer, ResidualEvaluator> residualCache = Maps.newHashMap();
    Expression residualFilter = shouldIgnoreResiduals() ? Expressions.alwaysTrue() : filter();

    // Track files already processed within this snapshot
    Set<String> alreadyProcessedPaths = Sets.newHashSet();

    try (CloseableIterable<ManifestEntry<DataFile>> entries = allDataGroup.entries()) {
      for (ManifestEntry<DataFile> entry : entries) {
        DataFile dataFile = entry.file();
        String filePath = dataFile.location();

        // When every added delete is file-scoped, only the referenced files can produce tasks
        if (candidateLocations != null && !candidateLocations.contains(filePath)) {
          continue;
        }

        // Skip if this file was ADDED or DELETED in this snapshot
        // (those are handled by CreateDataFileChangeTasks)
        if (currentSnapshotFiles.containsKey(filePath)) {
          continue;
        }

        // Skip if we already created a task for this file in this snapshot
        // Note: alreadyProcessedPaths is local to this snapshot's processing
        if (alreadyProcessedPaths.contains(filePath)) {
          continue;
        }

        // Check if this data file is affected by newly added delete files
        DeleteFile[] addedDeletes = addedDeleteIndex.forEntry(entry);
        if (addedDeletes.length == 0) {
          continue;
        }

        // This data file was EXISTING but has new delete files applied.
        // Attach all deletes that applied before this snapshot so that rows deleted earlier are
        // not emitted again; the underlying index is built lazily on first use
        DeleteFileIndex deletesBefore =
            cumulativeDeleteIndexes.deletesBefore(snapshot.snapshotId());
        DeleteFile[] existingDeletes =
            deletesBefore.isEmpty() ? new DeleteFile[0] : deletesBefore.forEntry(entry);

        // Create a DeletedRowsScanTask
        int changeOrdinal = snapshotOrdinals.get(snapshot.snapshotId());

        // Use cached values (calculate once per specId)
        int specId = dataFile.specId();
        String specString =
            specStringCache.computeIfAbsent(
                specId, id -> PartitionSpecParser.toJson(table().specs().get(id)));
        ResidualEvaluator residuals =
            residualCache.computeIfAbsent(
                specId,
                id -> {
                  PartitionSpec spec = table().specs().get(id);
                  return ResidualEvaluator.of(spec, residualFilter, isCaseSensitive());
                });

        tasks.add(
            new BaseDeletedRowsScanTask(
                changeOrdinal,
                snapshot.snapshotId(),
                ContentFileUtil.copy(dataFile, shouldReturnColumnStats(), columnsToKeepStats()),
                addedDeletes,
                existingDeletes,
                schemaString,
                specString,
                residuals));

        // Mark this file as processed for this snapshot
        alreadyProcessedPaths.add(filePath);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to plan deleted rows tasks", e);
    }
  }

  private static class CreateDataFileChangeTasks implements CreateTasksFunction<ChangelogScanTask> {
    private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

    private final Map<Long, Integer> snapshotOrdinals;
    private final Map<Long, DeleteFileIndex> addedDeletesBySnapshot;
    private final CumulativeDeleteIndexes cumulativeDeleteIndexes;

    CreateDataFileChangeTasks(
        Map<Long, Integer> snapshotOrdinals,
        Map<Long, DeleteFileIndex> addedDeletesBySnapshot,
        CumulativeDeleteIndexes cumulativeDeleteIndexes) {
      this.snapshotOrdinals = snapshotOrdinals;
      this.addedDeletesBySnapshot = addedDeletesBySnapshot;
      this.cumulativeDeleteIndexes = cumulativeDeleteIndexes;
    }

    @Override
    public CloseableIterable<ChangelogScanTask> apply(
        CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext context) {

      return CloseableIterable.transform(
          entries,
          entry -> {
            long commitSnapshotId = entry.snapshotId();
            int changeOrdinal = snapshotOrdinals.get(commitSnapshotId);
            DataFile dataFile =
                ContentFileUtil.copy(
                    entry.file(), context.shouldKeepStats(), context.columnsToKeepStats());

            switch (entry.status()) {
              case ADDED:
                // For ADDED data files, attach delete files added in this snapshot
                DeleteFile[] addedFileDeletes = deletesForAddedFile(entry, commitSnapshotId);
                return new BaseAddedRowsScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    addedFileDeletes,
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              case DELETED:
                // For DELETED data files, attach ALL deletes that were present up to deletion
                // This includes existing deletes AND deletes added in the scan range
                DeleteFile[] deletedFileDeletes = deletesForDeletedFile(entry, commitSnapshotId);
                return new BaseDeletedDataFileScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    deletedFileDeletes,
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              default:
                throw new IllegalArgumentException("Unexpected entry status: " + entry.status());
            }
          });
    }

    /**
     * Gets delete files that apply to an ADDED data file. Only includes deletes added in the same
     * snapshot as the file.
     */
    private DeleteFile[] deletesForAddedFile(ManifestEntry<DataFile> entry, long commitSnapshotId) {
      DeleteFileIndex addedDeleteIndex = addedDeletesBySnapshot.get(commitSnapshotId);
      return addedDeleteIndex == null || addedDeleteIndex.isEmpty()
          ? NO_DELETES
          : addedDeleteIndex.forEntry(entry);
    }

    /**
     * Gets all delete files that were applied to a DELETED data file up to the point it was
     * deleted. This includes existing deletes and all deletes added in the scan range up to (but
     * not including) the deletion snapshot. The underlying per-snapshot index is built once and
     * reused for all entries deleted in the same snapshot.
     */
    private DeleteFile[] deletesForDeletedFile(
        ManifestEntry<DataFile> entry, long deletionSnapshotId) {
      DeleteFileIndex deletesBefore = cumulativeDeleteIndexes.deletesBefore(deletionSnapshotId);
      return deletesBefore.isEmpty() ? NO_DELETES : deletesBefore.forEntry(entry);
    }
  }
}
