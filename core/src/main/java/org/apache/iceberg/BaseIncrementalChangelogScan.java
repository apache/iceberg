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
import java.util.Set;
import java.util.stream.Collectors;
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
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
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

    Set<ManifestFile> newDataManifests =
        FluentIterable.from(changelogSnapshots)
            .transformAndConcat(snapshot -> snapshot.dataManifests(table().io()))
            .filter(manifest -> changelogSnapshotIds.contains(manifest.snapshotId()))
            .toSet();

    // Build delete file index for existing deletes (before the start snapshot)
    DeleteFileIndex existingDeleteIndex = buildExistingDeleteIndex(fromSnapshotIdExclusive);

    // Build per-snapshot delete file indexes for added deletes
    Map<Long, DeleteFileIndex> addedDeletesBySnapshot = buildAddedDeleteIndexes(changelogSnapshots);

    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), newDataManifests, ImmutableList.of())
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .filterManifestEntries(entry -> changelogSnapshotIds.contains(entry.snapshotId()))
            .ignoreExisting()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (newDataManifests.size() > 1 && shouldPlanWithExecutor()) {
      manifestGroup = manifestGroup.planWith(planExecutor());
    }

    // Plan data file tasks (ADDED and DELETED)
    CloseableIterable<ChangelogScanTask> dataFileTasks =
        manifestGroup.plan(
            new CreateDataFileChangeTasks(
                changelogSnapshots, existingDeleteIndex, addedDeletesBySnapshot));

    // Find EXISTING data files affected by newly added delete files and create tasks for them
    CloseableIterable<ChangelogScanTask> deletedRowsTasks =
        planDeletedRowsTasks(
            changelogSnapshots, existingDeleteIndex, addedDeletesBySnapshot, changelogSnapshotIds);

    return CloseableIterable.concat(ImmutableList.of(dataFileTasks, deletedRowsTasks));
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

  /**
   * Builds a delete file index for existing deletes that were present before the start snapshot.
   * These deletes should be applied to data files but should not generate DELETE changelog rows.
   * Uses manifest pruning and caching to optimize performance.
   */
  private DeleteFileIndex buildExistingDeleteIndex(Long fromSnapshotIdExclusive) {
    if (fromSnapshotIdExclusive == null) {
      return DeleteFileIndex.builderFor(ImmutableList.of()).build();
    }

    Snapshot fromSnapshot = table().snapshot(fromSnapshotIdExclusive);
    Preconditions.checkState(
        fromSnapshot != null, "Cannot find starting snapshot: %s", fromSnapshotIdExclusive);

    List<ManifestFile> existingDeleteManifests = fromSnapshot.deleteManifests(table().io());
    if (existingDeleteManifests.isEmpty()) {
      return DeleteFileIndex.builderFor(ImmutableList.of()).build();
    }

    // Prune manifests based on partition filter to avoid processing irrelevant manifests
    List<ManifestFile> prunedManifests = pruneManifestsByPartition(existingDeleteManifests);
    if (prunedManifests.isEmpty()) {
      return DeleteFileIndex.builderFor(ImmutableList.of()).build();
    }

    // Load delete files with caching to avoid redundant manifest parsing
    List<DeleteFile> deleteFiles = loadDeleteFilesWithCache(prunedManifests);

    return DeleteFileIndex.builderFor(deleteFiles)
        .specsById(table().specs())
        .caseSensitive(isCaseSensitive())
        .build();
  }

  /**
   * Builds per-snapshot delete file indexes for newly added delete files in each changelog
   * snapshot. These deletes should generate DELETE changelog rows. Uses caching to avoid re-parsing
   * manifests.
   */
  private Map<Long, DeleteFileIndex> buildAddedDeleteIndexes(Deque<Snapshot> changelogSnapshots) {
    Map<Long, DeleteFileIndex> addedDeletesBySnapshot = Maps.newHashMap();

    for (Snapshot snapshot : changelogSnapshots) {
      List<ManifestFile> snapshotDeleteManifests = snapshot.deleteManifests(table().io());
      if (snapshotDeleteManifests.isEmpty()) {
        addedDeletesBySnapshot.put(
            snapshot.snapshotId(), DeleteFileIndex.builderFor(ImmutableList.of()).build());
        continue;
      }

      // Filter to only include delete files added in this snapshot
      List<ManifestFile> addedDeleteManifests =
          FluentIterable.from(snapshotDeleteManifests)
              .filter(manifest -> manifest.snapshotId().equals(snapshot.snapshotId()))
              .toList();

      if (addedDeleteManifests.isEmpty()) {
        addedDeletesBySnapshot.put(
            snapshot.snapshotId(), DeleteFileIndex.builderFor(ImmutableList.of()).build());
      } else {
        // Load delete files with caching to avoid redundant manifest parsing
        List<DeleteFile> deleteFiles = loadDeleteFilesWithCache(addedDeleteManifests);

        DeleteFileIndex index =
            DeleteFileIndex.builderFor(deleteFiles)
                .specsById(table().specs())
                .caseSensitive(isCaseSensitive())
                .build();
        addedDeletesBySnapshot.put(snapshot.snapshotId(), index);
      }
    }

    return addedDeletesBySnapshot;
  }

  /**
   * Plans tasks for EXISTING data files that are affected by newly added delete files. These files
   * were not added or deleted in the changelog snapshot range, but have new delete files applied to
   * them.
   */
  private CloseableIterable<ChangelogScanTask> planDeletedRowsTasks(
      Deque<Snapshot> changelogSnapshots,
      DeleteFileIndex existingDeleteIndex,
      Map<Long, DeleteFileIndex> addedDeletesBySnapshot,
      Set<Long> changelogSnapshotIds) {

    Map<Long, Integer> snapshotOrdinals = computeSnapshotOrdinals(changelogSnapshots);
    List<ChangelogScanTask> tasks = Lists.newArrayList();

    // Build a map of file statuses for each snapshot
    Map<Long, Map<String, ManifestEntry.Status>> fileStatusBySnapshot =
        buildFileStatusBySnapshot(changelogSnapshots, changelogSnapshotIds);

    // Process snapshots in order, tracking which files have been handled
    Set<String> alreadyProcessedPaths = Sets.newHashSet();

    // Accumulate actual DeleteFile entries chronologically
    List<DeleteFile> accumulatedDeletes = Lists.newArrayList();

    // Start with deletes from before the changelog range
    // Apply partition pruning to only accumulate relevant delete files
    if (!existingDeleteIndex.isEmpty()) {
      for (DeleteFile df : existingDeleteIndex.referencedDeleteFiles()) {
        if (partitionMatchesFilter(df)) {
          accumulatedDeletes.add(df);
        }
      }
    }

    for (Snapshot snapshot : changelogSnapshots) {
      DeleteFileIndex addedDeleteIndex = addedDeletesBySnapshot.get(snapshot.snapshotId());
      if (addedDeleteIndex.isEmpty()) {
        continue;
      }

      // Build cumulative delete index for this snapshot from accumulated deletes
      DeleteFileIndex cumulativeDeleteIndex = buildCumulativeDeleteIndex(accumulatedDeletes);

      // Process data files for this snapshot
      processSnapshotForDeletedRowsTasks(
          snapshot,
          addedDeleteIndex,
          cumulativeDeleteIndex,
          fileStatusBySnapshot.get(snapshot.snapshotId()),
          alreadyProcessedPaths,
          snapshotOrdinals,
          tasks);

      // Accumulate this snapshot's added deletes for subsequent snapshots
      // Apply partition pruning to only accumulate relevant delete files
      for (DeleteFile df : addedDeleteIndex.referencedDeleteFiles()) {
        if (partitionMatchesFilter(df)) {
          accumulatedDeletes.add(df);
        }
      }
    }

    return CloseableIterable.withNoopClose(tasks);
  }

  /**
   * Builds a map of file statuses for each snapshot, tracking which files were added or deleted in
   * each snapshot.
   */
  private Map<Long, Map<String, ManifestEntry.Status>> buildFileStatusBySnapshot(
      Deque<Snapshot> changelogSnapshots, Set<Long> changelogSnapshotIds) {

    Map<Long, Map<String, ManifestEntry.Status>> fileStatusBySnapshot = Maps.newHashMap();

    for (Snapshot snapshot : changelogSnapshots) {
      Map<String, ManifestEntry.Status> fileStatuses = Maps.newHashMap();

      List<ManifestFile> changedDataManifests =
          FluentIterable.from(snapshot.dataManifests(table().io()))
              .filter(manifest -> manifest.snapshotId().equals(snapshot.snapshotId()))
              .toList();

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
          if (changelogSnapshotIds.contains(entry.snapshotId())) {
            fileStatuses.put(entry.file().location(), entry.status());
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to collect file statuses", e);
      }

      fileStatusBySnapshot.put(snapshot.snapshotId(), fileStatuses);
    }

    return fileStatusBySnapshot;
  }

  /** Builds a cumulative delete index from the accumulated list of delete files. */
  private DeleteFileIndex buildCumulativeDeleteIndex(List<DeleteFile> accumulatedDeletes) {
    if (accumulatedDeletes.isEmpty()) {
      return DeleteFileIndex.builderFor(ImmutableList.of()).build();
    }

    return DeleteFileIndex.builderFor(accumulatedDeletes)
        .specsById(table().specs())
        .caseSensitive(isCaseSensitive())
        .build();
  }

  /**
   * Processes data files for a snapshot to create DeletedRowsScanTask for existing files affected
   * by new delete files.
   */
  private void processSnapshotForDeletedRowsTasks(
      Snapshot snapshot,
      DeleteFileIndex addedDeleteIndex,
      DeleteFileIndex cumulativeDeleteIndex,
      Map<String, ManifestEntry.Status> currentSnapshotFiles,
      Set<String> alreadyProcessedPaths,
      Map<Long, Integer> snapshotOrdinals,
      List<ChangelogScanTask> tasks) {

    // Get all data files that exist in this snapshot
    List<ManifestFile> allDataManifests = snapshot.dataManifests(table().io());
    ManifestGroup allDataGroup =
        new ManifestGroup(table().io(), allDataManifests, ImmutableList.of())
            .specsById(table().specs())
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .ignoreDeleted()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
      allDataGroup = allDataGroup.ignoreResiduals();
    }

    try (CloseableIterable<ManifestEntry<DataFile>> entries = allDataGroup.entries()) {
      for (ManifestEntry<DataFile> entry : entries) {
        DataFile dataFile = entry.file();
        String filePath = dataFile.location();

        // Skip if this file was ADDED or DELETED in this snapshot
        // (those are handled by CreateDataFileChangeTasks)
        if (currentSnapshotFiles.containsKey(filePath)) {
          continue;
        }

        // Skip if we already created a task for this file in this snapshot
        String key = snapshot.snapshotId() + ":" + filePath;
        if (alreadyProcessedPaths.contains(key)) {
          continue;
        }

        // Check if this data file is affected by newly added delete files
        DeleteFile[] addedDeletes = addedDeleteIndex.forEntry(entry);
        if (addedDeletes.length == 0) {
          continue;
        }

        // This data file was EXISTING but has new delete files applied
        // Get existing deletes from before this snapshot (cumulative)
        DeleteFile[] existingDeletes =
            cumulativeDeleteIndex.isEmpty()
                ? new DeleteFile[0]
                : cumulativeDeleteIndex.forEntry(entry);

        // Create a DeletedRowsScanTask
        int changeOrdinal = snapshotOrdinals.get(snapshot.snapshotId());
        String schemaString = SchemaParser.toJson(schema());
        String specString = PartitionSpecParser.toJson(table().specs().get(dataFile.specId()));
        PartitionSpec spec = table().specs().get(dataFile.specId());
        Expression residualFilter = shouldIgnoreResiduals() ? Expressions.alwaysTrue() : filter();
        ResidualEvaluator residuals = ResidualEvaluator.of(spec, residualFilter, isCaseSensitive());

        tasks.add(
            new BaseDeletedRowsScanTask(
                changeOrdinal,
                snapshot.snapshotId(),
                dataFile.copy(shouldKeepStats()),
                addedDeletes,
                existingDeletes,
                schemaString,
                specString,
                residuals));

        // Mark this file+snapshot as processed
        alreadyProcessedPaths.add(key);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to plan deleted rows tasks", e);
    }
  }

  private boolean shouldKeepStats() {
    Set<Integer> columns = columnsToKeepStats();
    return columns != null && !columns.isEmpty();
  }

  /**
   * Loads delete files from manifests using a cache to avoid redundant manifest parsing. This
   * significantly improves planning performance when the same manifests are accessed across
   * multiple scans or within a scan range.
   *
   * @param manifests the delete manifests to load
   * @return list of delete files
   */
  private List<DeleteFile> loadDeleteFilesWithCache(List<ManifestFile> manifests) {
    DeleteManifestCache cache = DeleteManifestCache.instance();
    List<DeleteFile> allDeleteFiles = Lists.newArrayList();

    for (ManifestFile manifest : manifests) {
      // Try to get from cache first
      List<DeleteFile> cachedFiles = cache.get(manifest);

      if (cachedFiles != null) {
        // Cache hit - reuse parsed delete files
        allDeleteFiles.addAll(cachedFiles);
      } else {
        // Cache miss - parse manifest and populate cache
        List<DeleteFile> manifestDeleteFiles = loadDeleteFilesFromManifest(manifest);
        cache.put(manifest, manifestDeleteFiles);
        allDeleteFiles.addAll(manifestDeleteFiles);
      }
    }

    return allDeleteFiles;
  }

  /**
   * Prunes delete manifests based on partition filter to avoid processing irrelevant manifests.
   * This significantly improves performance when only a subset of partitions are relevant to the
   * scan.
   *
   * @param manifests all delete manifests to consider
   * @return list of manifests that might contain relevant delete files
   */
  private List<ManifestFile> pruneManifestsByPartition(List<ManifestFile> manifests) {
    Expression currentFilter = filter();

    // If there's no filter, return all manifests
    if (currentFilter == null || currentFilter.equals(Expressions.alwaysTrue())) {
      return manifests;
    }

    List<ManifestFile> prunedManifests = Lists.newArrayList();

    for (ManifestFile manifest : manifests) {
      PartitionSpec spec = table().specs().get(manifest.partitionSpecId());
      if (spec == null || spec.isUnpartitioned()) {
        // Include unpartitioned manifests
        prunedManifests.add(manifest);
      } else if (manifestOverlapsFilter(manifest, spec, currentFilter)) {
        // Check if manifest partition range overlaps with filter
        prunedManifests.add(manifest);
      }
    }

    return prunedManifests;
  }

  /**
   * Checks if a manifest's partition range overlaps with the given filter.
   *
   * @param manifest the manifest to check
   * @param spec the partition spec for the manifest
   * @param filter the scan filter
   * @return true if the manifest might contain matching partitions, false otherwise
   */
  private boolean manifestOverlapsFilter(
      ManifestFile manifest, PartitionSpec spec, Expression filter) {
    try {
      // Use inclusive projection to transform row filter to partition filter
      Expression partitionFilter = Projections.inclusive(spec, isCaseSensitive()).project(filter);

      // Create evaluator for the partition filter
      ManifestEvaluator evaluator =
          ManifestEvaluator.forPartitionFilter(partitionFilter, spec, isCaseSensitive());

      // Check if manifest could contain matching partitions
      return evaluator.eval(manifest);
    } catch (Exception e) {
      // If evaluation fails, be conservative and include the manifest
      return true;
    }
  }

  /**
   * Checks if a delete file's partition overlaps with the current scan filter. This enables
   * partition pruning to reduce memory footprint and planning overhead by skipping delete files
   * that cannot possibly match any rows in the scan.
   *
   * @param file the delete file to check
   * @return true if the delete file's partition might contain matching rows, false otherwise
   */
  private boolean partitionMatchesFilter(DeleteFile file) {
    // If there's no filter, all partitions match
    Expression currentFilter = filter();
    if (currentFilter == null || currentFilter.equals(Expressions.alwaysTrue())) {
      return true;
    }

    // Get the partition spec for this delete file
    PartitionSpec spec = table().specs().get(file.specId());
    if (spec == null || spec.isUnpartitioned()) {
      // If spec not found or table is unpartitioned, be conservative and include the file
      return true;
    }

    try {
      // Project the row filter to partition space using inclusive projection
      // This transforms expressions on source columns to expressions on partition columns
      Expression partitionFilter =
          Projections.inclusive(spec, isCaseSensitive()).project(currentFilter);

      // Evaluate the projected filter against the delete file's partition
      Evaluator evaluator = new Evaluator(spec.partitionType(), partitionFilter, isCaseSensitive());
      return evaluator.eval(file.partition());
    } catch (Exception e) {
      // If evaluation fails, be conservative and include the file
      return true;
    }
  }

  /**
   * Loads delete files from a single manifest, parsing the manifest entries.
   *
   * @param manifest the delete manifest to load
   * @return list of delete files from this manifest
   */
  private List<DeleteFile> loadDeleteFilesFromManifest(ManifestFile manifest) {
    List<DeleteFile> deleteFiles = Lists.newArrayList();

    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, table().io(), table().specs())) {
      for (ManifestEntry<DeleteFile> entry : reader.entries()) {
        if (entry.status() != ManifestEntry.Status.DELETED) {
          // Only include live delete files, copy with minimal stats to save memory
          DeleteFile file = entry.file();

          // Apply partition pruning - skip delete files that cannot match the scan filter
          if (!partitionMatchesFilter(file)) {
            continue;
          }

          Set<Integer> columns =
              file.content() == FileContent.POSITION_DELETES
                  ? Set.of(MetadataColumns.DELETE_FILE_PATH.fieldId())
                  : Set.copyOf(file.equalityFieldIds());
          deleteFiles.add(ContentFileUtil.copy(file, true, columns));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read delete manifest: " + manifest.path(), e);
    }

    return deleteFiles;
  }

  private static class CreateDataFileChangeTasks implements CreateTasksFunction<ChangelogScanTask> {
    private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

    private final Map<Long, Integer> snapshotOrdinals;
    private final DeleteFileIndex existingDeleteIndex;
    private final Map<Long, DeleteFileIndex> addedDeletesBySnapshot;

    CreateDataFileChangeTasks(
        Deque<Snapshot> snapshots,
        DeleteFileIndex existingDeleteIndex,
        Map<Long, DeleteFileIndex> addedDeletesBySnapshot) {
      this.snapshotOrdinals = computeSnapshotOrdinals(snapshots);
      this.existingDeleteIndex = existingDeleteIndex;
      this.addedDeletesBySnapshot = addedDeletesBySnapshot;
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
                // For ADDED data files, attach any delete files that apply to them
                // This includes both existing deletes and newly added deletes in this snapshot
                DeleteFile[] addedFileDeletes = getDeletesForAddedFile(entry, commitSnapshotId);
                return new BaseAddedRowsScanTask(
                    changeOrdinal,
                    commitSnapshotId,
                    dataFile,
                    addedFileDeletes,
                    context.schemaAsString(),
                    context.specAsString(),
                    context.residuals());

              case DELETED:
                // For DELETED data files, attach existing deletes (deletes that were present
                // before the file was deleted)
                DeleteFile[] deletedFileDeletes =
                    existingDeleteIndex.isEmpty()
                        ? NO_DELETES
                        : existingDeleteIndex.forEntry(entry);
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
     * Gets all delete files that apply to an ADDED data file. This includes existing deletes that
     * were present before this snapshot and any new deletes added in this snapshot.
     */
    private DeleteFile[] getDeletesForAddedFile(
        ManifestEntry<DataFile> entry, long commitSnapshotId) {
      DeleteFile[] existingDeletes =
          existingDeleteIndex.isEmpty() ? NO_DELETES : existingDeleteIndex.forEntry(entry);

      DeleteFileIndex addedDeleteIndex = addedDeletesBySnapshot.get(commitSnapshotId);
      DeleteFile[] addedDeletes =
          addedDeleteIndex == null || addedDeleteIndex.isEmpty()
              ? NO_DELETES
              : addedDeleteIndex.forEntry(entry);

      // If both are empty, return NO_DELETES
      if (existingDeletes.length == 0 && addedDeletes.length == 0) {
        return NO_DELETES;
      }

      // If only one has deletes, return that one
      if (existingDeletes.length == 0) {
        return addedDeletes;
      }
      if (addedDeletes.length == 0) {
        return existingDeletes;
      }

      // Merge both arrays
      DeleteFile[] allDeletes = new DeleteFile[existingDeletes.length + addedDeletes.length];
      System.arraycopy(existingDeletes, 0, allDeletes, 0, existingDeletes.length);
      System.arraycopy(addedDeletes, 0, allDeletes, existingDeletes.length, addedDeletes.length);
      return allDeletes;
    }
  }
}
