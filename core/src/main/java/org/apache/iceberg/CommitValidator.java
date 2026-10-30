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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;

class CommitValidator {
  // data is only added in "append" and "overwrite" operations
  private static final Set<String> VALIDATE_ADDED_FILES_OPERATIONS =
      ImmutableSet.of(DataOperations.APPEND, DataOperations.OVERWRITE);
  // data files are removed in "overwrite", "replace", and "delete"
  private static final Set<String> VALIDATE_DATA_FILES_EXIST_OPERATIONS =
      ImmutableSet.of(DataOperations.OVERWRITE, DataOperations.REPLACE, DataOperations.DELETE);
  private static final Set<String> VALIDATE_DATA_FILES_EXIST_SKIP_DELETE_OPERATIONS =
      ImmutableSet.of(DataOperations.OVERWRITE, DataOperations.REPLACE);
  // delete files can be added in "overwrite" or "delete" operations
  private static final Set<String> VALIDATE_ADDED_DELETE_FILES_OPERATIONS =
      ImmutableSet.of(DataOperations.OVERWRITE, DataOperations.DELETE);
  // DVs can be added in "overwrite", "delete", and "replace" operations
  private static final Set<String> VALIDATE_ADDED_DVS_OPERATIONS =
      ImmutableSet.of(DataOperations.OVERWRITE, DataOperations.DELETE, DataOperations.REPLACE);

  static CommitValidator newValidator() {
    return new CommitValidator();
  }

  private FileIO io;
  private Long startingSnapshotId = null;
  private boolean caseSensitive = true;
  private Expression conflictDetectionFilter = null;
  private ExecutorService workerPool = null;

  private TableMetadata base = null;
  private Snapshot parent = null;

  private CommitValidator() {}

  CommitValidator io(FileIO newIo) {
    this.io = newIo;
    return this;
  }

  CommitValidator startingSnapshotId(Long newStartingSnapshotId) {
    this.startingSnapshotId = newStartingSnapshotId;
    return this;
  }

  CommitValidator caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    return this;
  }

  CommitValidator conflictDetectionFilter(Expression filter) {
    this.conflictDetectionFilter = filter;
    return this;
  }

  CommitValidator scanWith(ExecutorService newWorkerPool) {
    this.workerPool = newWorkerPool;
    return this;
  }

  /**
   * Binds the base metadata and parent snapshot the checks run against. Needs to be invoked before
   * running validations.
   */
  CommitValidator setParent(TableMetadata newBase, Snapshot newParent) {
    this.base = newBase;
    this.parent = newParent;
    return this;
  }

  /**
   * Validates that no data files matching the conflict detection filter have been added to the
   * table since the starting snapshot.
   */
  void validateNoConflictingAddedDataFiles() {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        addedDataFiles(conflictDetectionFilter, null);

    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
      if (conflicts.hasNext()) {
        throw new ValidationException(
            "Found conflicting files that can contain records matching %s: %s",
            conflictDetectionFilter,
            Iterators.toString(
                Iterators.transform(conflicts, entry -> entry.file().location().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to validate no appends matching %s", conflictDetectionFilter), e);
    }
  }

  void validateNoConflictingAddedDataFiles(PartitionSet partitionSet) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries = addedDataFiles(null, partitionSet);

    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
      if (conflicts.hasNext()) {
        throw new ValidationException(
            "Found conflicting files that can contain records matching partitions %s: %s",
            partitionSet,
            Iterators.toString(
                Iterators.transform(conflicts, entry -> entry.file().location().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to validate no appends matching %s", partitionSet), e);
    }
  }

  private CloseableIterable<ManifestEntry<DataFile>> addedDataFiles(
      Expression dataFilter, PartitionSet partitionSet) {
    // if there is no current table state, no files have been added
    if (parent == null) {
      return CloseableIterable.empty();
    }

    List<ManifestFile> manifests = Lists.newArrayList();
    Set<Long> newSnapshots =
        validationHistory(
            VALIDATE_ADDED_FILES_OPERATIONS,
            snapshot -> addNewManifests(snapshot, snapshot.dataManifests(io), manifests));

    ManifestGroup manifestGroup =
        new ManifestGroup(io, manifests, ImmutableList.of())
            .caseSensitive(caseSensitive)
            .filterManifestEntries(entry -> newSnapshots.contains(entry.snapshotId()))
            .specsById(base.specsById())
            .ignoreDeleted()
            .ignoreExisting();

    if (dataFilter != null) {
      manifestGroup = manifestGroup.filterData(dataFilter);
    }

    if (partitionSet != null) {
      manifestGroup =
          manifestGroup.filterManifestEntries(
              entry -> partitionSet.contains(entry.file().specId(), entry.file().partition()));
    }

    return manifestGroup.entries();
  }

  /**
   * Validates that no data files matching the conflict detection filter have been deleted from the
   * table since the starting snapshot.
   */
  void validateNoConflictingDeletedDataFiles() {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        deletedDataFiles(conflictDetectionFilter, null);

    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
      if (conflicts.hasNext()) {
        throw new ValidationException(
            "Found conflicting deleted files that can contain records matching %s: %s",
            conflictDetectionFilter,
            Iterators.toString(
                Iterators.transform(conflicts, entry -> entry.file().location().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format(
              "Failed to validate no deleted data files matching %s", conflictDetectionFilter),
          e);
    }
  }

  void validateNoConflictingDeletedDataFiles(PartitionSet partitionSet) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        deletedDataFiles(null, partitionSet);

    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
      if (conflicts.hasNext()) {
        throw new ValidationException(
            "Found conflicting deleted files that can apply to records matching %s: %s",
            partitionSet,
            Iterators.toString(
                Iterators.transform(conflicts, entry -> entry.file().location().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to validate no appends matching %s", partitionSet), e);
    }
  }

  private CloseableIterable<ManifestEntry<DataFile>> deletedDataFiles(
      Expression dataFilter, PartitionSet partitionSet) {
    // if there is no current table state, no files have been deleted
    if (parent == null) {
      return CloseableIterable.empty();
    }

    List<ManifestFile> manifests = Lists.newArrayList();
    Set<Long> newSnapshots =
        validationHistory(
            VALIDATE_DATA_FILES_EXIST_OPERATIONS,
            snapshot -> addNewManifests(snapshot, snapshot.dataManifests(io), manifests));

    ManifestGroup manifestGroup =
        new ManifestGroup(io, manifests, ImmutableList.of())
            .caseSensitive(caseSensitive)
            .filterManifestEntries(entry -> newSnapshots.contains(entry.snapshotId()))
            .filterManifestEntries(entry -> entry.status().equals(ManifestEntry.Status.DELETED))
            .specsById(base.specsById())
            .ignoreExisting();

    if (dataFilter != null) {
      manifestGroup = manifestGroup.filterData(dataFilter);
    }

    if (partitionSet != null) {
      manifestGroup =
          manifestGroup.filterManifestEntries(
              entry -> partitionSet.contains(entry.file().specId(), entry.file().partition()));
    }

    return manifestGroup.entries();
  }

  /**
   * Validates that the given data files have not been removed from the table since the starting
   * snapshot.
   *
   * @param requiredDataFiles locations of data files that must still exist
   * @param skipDeletes when true, removals by delete operations are ignored
   */
  @SuppressWarnings("CollectionUndefinedEquality")
  void validateDataFilesExist(CharSequenceSet requiredDataFiles, boolean skipDeletes) {
    // if there is no current table state, no files have been removed
    if (parent == null) {
      return;
    }

    Set<String> matchingOperations =
        skipDeletes
            ? VALIDATE_DATA_FILES_EXIST_SKIP_DELETE_OPERATIONS
            : VALIDATE_DATA_FILES_EXIST_OPERATIONS;

    List<ManifestFile> manifests = Lists.newArrayList();
    Set<Long> newSnapshots =
        validationHistory(
            matchingOperations,
            snapshot -> addNewManifests(snapshot, snapshot.dataManifests(io), manifests));

    ManifestGroup matchingDeletesGroup =
        new ManifestGroup(io, manifests, ImmutableList.of())
            .filterManifestEntries(
                entry ->
                    entry.status() != ManifestEntry.Status.ADDED
                        && newSnapshots.contains(entry.snapshotId())
                        && requiredDataFiles.contains(entry.file().location()))
            .specsById(base.specsById())
            .ignoreExisting();

    if (conflictDetectionFilter != null) {
      matchingDeletesGroup.filterData(conflictDetectionFilter);
    }

    try (CloseableIterator<ManifestEntry<DataFile>> deletes =
        matchingDeletesGroup.entries().iterator()) {
      if (deletes.hasNext()) {
        throw new ValidationException(
            "Cannot commit, missing data files: %s",
            Iterators.toString(
                Iterators.transform(deletes, entry -> entry.file().location().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to validate required files exist", e);
    }
  }

  /**
   * Validates that no delete files matching the conflict detection filter have been added to the
   * table since the starting snapshot.
   */
  void validateNoConflictingAddedDeleteFiles() {
    Set<String> locations =
        referencedDeleteFileLocations(
            addedDeleteFilesIndexedPerSnapshot(conflictDetectionFilter, null));
    ValidationException.check(
        locations.isEmpty(),
        "Found new conflicting delete files that can apply to records matching %s: %s",
        conflictDetectionFilter,
        locations);
  }

  void validateNoConflictingAddedDeleteFiles(PartitionSet partitionSet) {
    Set<String> locations =
        referencedDeleteFileLocations(addedDeleteFilesIndexedPerSnapshot(null, partitionSet));
    ValidationException.check(
        locations.isEmpty(),
        "Found new conflicting delete files that can apply to records matching %s: %s",
        partitionSet,
        locations);
  }

  /**
   * Validates that no new delete files that must be applied to the given data files have been added
   * to the table since the starting snapshot.
   *
   * <p>When equality deletes are ignored, only position deletes fail the validation. Ignoring them
   * is safe when the added data files share the replaced files' data sequence number, since
   * equality deletes added at a higher sequence number still apply to the added files (for example,
   * when rewriting data files).
   *
   * @param dataFiles data files to validate have no new row deletes
   * @param ignoreEqualityDeletes whether to ignore equality deletes during validation
   */
  void validateNoConflictingDeletesForDataFiles(
      Iterable<DataFile> dataFiles, boolean ignoreEqualityDeletes) {
    // if there is no current table state, no files have been added
    if (parent == null || base.formatVersion() < 2) {
      return;
    }

    List<DeleteFileIndex> deleteIndexes =
        addedDeleteFilesIndexedPerSnapshot(conflictDetectionFilter, null);

    long startingSequenceNumber = startingSequenceNumber();
    for (DataFile dataFile : dataFiles) {
      for (DeleteFileIndex deletes : deleteIndexes) {
        // if any delete is found that applies to files written in or before the starting snapshot,
        // fail
        DeleteFile[] deleteFiles = deletes.forDataFile(startingSequenceNumber, dataFile);
        if (ignoreEqualityDeletes) {
          ValidationException.check(
              !containsPositionDeletes(deleteFiles),
              "Cannot commit, found new position delete for replaced data file: %s",
              dataFile);
        } else {
          ValidationException.check(
              deleteFiles.length == 0,
              "Cannot commit, found new delete for replaced data file: %s",
              dataFile);
        }
      }
    }
  }

  /**
   * Returns one index per snapshot in the validation window, covering the matching delete files
   * added since the starting snapshot.
   *
   * <p>Callers must check every index. A single index cannot hold them all because it allows at
   * most one deletion vector per data file. That holds within a snapshot but not across them: a
   * data file can be referenced by a replacement deletion vector in each snapshot in the window.
   *
   * @param dataFilter an expression used to find delete files
   * @param partitionSet a partition set used to find delete files
   */
  private List<DeleteFileIndex> addedDeleteFilesIndexedPerSnapshot(
      Expression dataFilter, PartitionSet partitionSet) {
    // if there is no current table state, no delete files have been added
    if (parent == null || base.formatVersion() < 2) {
      return ImmutableList.of();
    }

    List<ManifestFile> addedDeleteManifests = Lists.newArrayList();
    validationHistory(
        VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
        snapshot -> addNewManifests(snapshot, snapshot.deleteManifests(io), addedDeleteManifests));

    // the history collects a manifest only from the snapshot that added it, so grouping by
    // snapshot ID assigns each manifest to exactly one index and still reads it once.
    // LinkedHashMap keeps the history order, which keeps the failure message deterministic.
    Map<Long, List<ManifestFile>> deleteManifestsBySnapshot =
        addedDeleteManifests.stream()
            .collect(
                Collectors.groupingBy(
                    ManifestFile::snapshotId, LinkedHashMap::new, Collectors.toList()));

    long startingSequenceNumber = startingSequenceNumber();
    List<DeleteFileIndex> deleteIndexes = Lists.newArrayList();
    for (List<ManifestFile> deleteManifests : deleteManifestsBySnapshot.values()) {
      deleteIndexes.add(
          buildDeleteFileIndex(deleteManifests, startingSequenceNumber, dataFilter, partitionSet));
    }

    return deleteIndexes;
  }

  private DeleteFileIndex buildDeleteFileIndex(
      List<ManifestFile> deleteManifests,
      long startingSequenceNumber,
      Expression dataFilter,
      PartitionSet partitionSet) {
    DeleteFileIndex.Builder builder =
        DeleteFileIndex.builderFor(io, deleteManifests)
            .afterSequenceNumber(startingSequenceNumber)
            .caseSensitive(caseSensitive)
            .specsById(base.specsById());

    if (dataFilter != null) {
      builder.filterData(dataFilter);
    }

    if (partitionSet != null) {
      builder.filterPartitions(partitionSet);
    }

    return builder.build();
  }

  private static Set<String> referencedDeleteFileLocations(List<DeleteFileIndex> deleteIndexes) {
    Set<String> locations = Sets.newLinkedHashSet();
    for (DeleteFileIndex deletes : deleteIndexes) {
      for (DeleteFile deleteFile : deletes.referencedDeleteFiles()) {
        locations.add(deleteFile.location());
      }
    }

    return locations;
  }

  private static boolean containsPositionDeletes(DeleteFile[] deleteFiles) {
    for (DeleteFile deleteFile : deleteFiles) {
      if (deleteFile.content() == FileContent.POSITION_DELETES) {
        return true;
      }
    }

    return false;
  }

  /**
   * Validates that no deletion vectors have been concurrently added for the referenced data files
   * since the starting snapshot.
   *
   * @param referencedDataFiles data files referenced by this operation's deletion vectors
   */
  void validateNoConflictingAddedDVs(Set<String> referencedDataFiles) {
    // skip if there is no current table state or this operation doesn't add new DVs
    if (parent == null || referencedDataFiles.isEmpty()) {
      return;
    }

    List<ManifestFile> newDeleteManifests = Lists.newArrayList();
    Set<Long> newSnapshotIds =
        validationHistory(
            VALIDATE_ADDED_DVS_OPERATIONS,
            snapshot ->
                addNewManifests(snapshot, snapshot.deleteManifests(io), newDeleteManifests));

    Iterable<ManifestFile> matchingManifests =
        Iterables.filter(
            filterManifestsByPartition(conflictDetectionFilter, newDeleteManifests),
            ManifestFile::hasAddedFiles);

    Tasks.foreach(matchingManifests)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPool)
        .run(
            manifest ->
                validateNoConflictingAddedDVs(manifest, newSnapshotIds, referencedDataFiles));
  }

  private void validateNoConflictingAddedDVs(
      ManifestFile manifest, Set<Long> newSnapshotIds, Set<String> referencedDataFiles) {
    try (CloseableIterable<ManifestEntry<DeleteFile>> entries =
        ManifestFiles.readDeleteManifest(manifest, io, base.specsById())
            .filterRows(conflictDetectionFilter)
            .caseSensitive(caseSensitive)
            .liveEntries()) {

      for (ManifestEntry<DeleteFile> entry : entries) {
        DeleteFile file = entry.file();
        if (newSnapshotIds.contains(entry.snapshotId()) && ContentFileUtil.isDV(file)) {
          ValidationException.check(
              !referencedDataFiles.contains(file.referencedDataFile()),
              "Found concurrently added DV for %s: %s",
              file.referencedDataFile(),
              ContentFileUtil.dvDesc(file));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Iterable<ManifestFile> filterManifestsByPartition(
      Expression filter, List<ManifestFile> manifests) {
    if (filter == null || filter == Expressions.alwaysTrue()) {
      return manifests;
    }

    // if any concurrent manifest was written with a different partition spec, skip pruning
    // to avoid incorrectly excluding manifests when a spec change happened during validation
    int defaultSpecId = base.defaultSpecId();
    if (manifests.stream().anyMatch(m -> m.partitionSpecId() != defaultSpecId)) {
      return manifests;
    }

    Map<Integer, PartitionSpec> specsById = base.specsById();
    Map<Integer, ManifestEvaluator> evaluators = Maps.newHashMap();
    return Iterables.filter(
        manifests,
        manifest -> {
          ManifestEvaluator evaluator =
              evaluators.computeIfAbsent(
                  manifest.partitionSpecId(),
                  specId -> {
                    PartitionSpec spec = specsById.get(specId);
                    Expression partitionFilter =
                        Projections.inclusive(spec, caseSensitive).project(filter);
                    return ManifestEvaluator.forPartitionFilter(
                        partitionFilter, spec, caseSensitive);
                  });
          return evaluator.eval(manifest);
        });
  }

  private Set<Long> validationHistory(Set<String> matchingOperations, Consumer<Snapshot> consumer) {
    Set<Long> newSnapshots = Sets.newHashSet();

    Snapshot lastSnapshot = null;
    Iterable<Snapshot> snapshots =
        SnapshotUtil.ancestorsBetween(parent.snapshotId(), startingSnapshotId, base::snapshot);
    for (Snapshot currentSnapshot : snapshots) {
      lastSnapshot = currentSnapshot;

      if (matchingOperations.contains(currentSnapshot.operation())) {
        consumer.accept(currentSnapshot);
        newSnapshots.add(currentSnapshot.snapshotId());
      }
    }

    ValidationException.check(
        lastSnapshot == null || Objects.equals(lastSnapshot.parentId(), startingSnapshotId),
        "Cannot determine history between starting snapshot %s and the last known ancestor %s",
        startingSnapshotId,
        lastSnapshot != null ? lastSnapshot.snapshotId() : null);

    return newSnapshots;
  }

  private static void addNewManifests(
      Snapshot snapshot, Iterable<ManifestFile> manifests, List<ManifestFile> collector) {
    for (ManifestFile manifest : manifests) {
      if (manifest.snapshotId() == snapshot.snapshotId()) {
        collector.add(manifest);
      }
    }
  }

  private long startingSequenceNumber() {
    if (startingSnapshotId != null && base.snapshot(startingSnapshotId) != null) {
      return base.snapshot(startingSnapshotId).sequenceNumber();
    } else {
      return TableMetadata.INITIAL_SEQUENCE_NUMBER;
    }
  }
}
