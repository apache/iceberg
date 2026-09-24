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
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;

/**
 * Checks a commit for conflicts against the table history between a starting snapshot and the
 * current parent snapshot.
 */
class SnapshotValidator {
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

  private final TableOperations ops;
  private final boolean caseSensitive;

  SnapshotValidator(TableOperations ops, boolean caseSensitive) {
    this.ops = ops;
    this.caseSensitive = caseSensitive;
  }

  void validateAddedDataFiles(
      TableMetadata base, Long startingSnapshotId, PartitionSet partitionSet, Snapshot parent) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        addedDataFiles(base, startingSnapshotId, null, partitionSet, parent);

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

  void validateAddedDataFiles(
      TableMetadata base,
      Long startingSnapshotId,
      Expression conflictDetectionFilter,
      Snapshot parent) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        addedDataFiles(base, startingSnapshotId, conflictDetectionFilter, null, parent);

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

  private CloseableIterable<ManifestEntry<DataFile>> addedDataFiles(
      TableMetadata base,
      Long startingSnapshotId,
      Expression dataFilter,
      PartitionSet partitionSet,
      Snapshot parent) {
    // if there is no current table state, no files have been added
    if (parent == null) {
      return CloseableIterable.empty();
    }

    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(
            base,
            startingSnapshotId,
            VALIDATE_ADDED_FILES_OPERATIONS,
            ManifestContent.DATA,
            parent);
    List<ManifestFile> manifests = history.first();
    Set<Long> newSnapshots = history.second();

    ManifestGroup manifestGroup =
        new ManifestGroup(ops.io(), manifests, ImmutableList.of())
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

  void validateNoNewDeletesForDataFiles(
      TableMetadata base,
      Long startingSnapshotId,
      Expression dataFilter,
      Iterable<DataFile> dataFiles,
      boolean ignoreEqualityDeletes,
      Snapshot parent) {
    // if there is no current table state, no files have been added
    if (parent == null || base.formatVersion() < 2) {
      return;
    }

    List<DeleteFileIndex> deleteIndexes =
        addedDeleteFilesIndexedPerSnapshot(base, startingSnapshotId, dataFilter, null, parent);

    long startingSequenceNumber = startingSequenceNumber(base, startingSnapshotId);
    for (DataFile dataFile : dataFiles) {
      for (DeleteFileIndex deletes : deleteIndexes) {
        // if any delete is found that applies to files written in or before the starting snapshot,
        // fail
        DeleteFile[] deleteFiles = deletes.forDataFile(startingSequenceNumber, dataFile);
        // rewrites can omit equality-delete checks: when added files keep the replaced files' data
        // sequence number, higher-sequence equality deletes still apply to them, so there is no
        // RewriteFiles/RowDelta conflict; only a new position delete signals a real conflict
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

  private static boolean containsPositionDeletes(DeleteFile[] deleteFiles) {
    for (DeleteFile deleteFile : deleteFiles) {
      if (deleteFile.content() == FileContent.POSITION_DELETES) {
        return true;
      }
    }

    return false;
  }

  void validateNoNewDeleteFiles(
      TableMetadata base, Long startingSnapshotId, Expression dataFilter, Snapshot parent) {
    Set<String> locations =
        referencedDeleteFileLocations(
            addedDeleteFilesIndexedPerSnapshot(base, startingSnapshotId, dataFilter, null, parent));
    ValidationException.check(
        locations.isEmpty(),
        "Found new conflicting delete files that can apply to records matching %s: %s",
        dataFilter,
        locations);
  }

  void validateNoNewDeleteFiles(
      TableMetadata base, Long startingSnapshotId, PartitionSet partitionSet, Snapshot parent) {
    Set<String> locations =
        referencedDeleteFileLocations(
            addedDeleteFilesIndexedPerSnapshot(
                base, startingSnapshotId, null, partitionSet, parent));
    ValidationException.check(
        locations.isEmpty(),
        "Found new conflicting delete files that can apply to records matching %s: %s",
        partitionSet,
        locations);
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

  private List<DeleteFileIndex> addedDeleteFilesIndexedPerSnapshot(
      TableMetadata base,
      Long startingSnapshotId,
      Expression dataFilter,
      PartitionSet partitionSet,
      Snapshot parent) {
    // if there is no current table state, no delete files have been added
    if (parent == null || base.formatVersion() < 2) {
      return ImmutableList.of();
    }

    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(
            base,
            startingSnapshotId,
            VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
            ManifestContent.DELETES,
            parent);

    // the history collects a manifest only from the snapshot that added it, so grouping by
    // snapshot ID assigns each manifest to exactly one index and still reads it once.
    // LinkedHashMap keeps the history order, which keeps the failure message deterministic.
    Map<Long, List<ManifestFile>> deleteManifestsBySnapshot =
        history.first().stream()
            .collect(
                Collectors.groupingBy(
                    ManifestFile::snapshotId, LinkedHashMap::new, Collectors.toList()));

    long startingSequenceNumber = startingSequenceNumber(base, startingSnapshotId);
    List<DeleteFileIndex> deleteIndexes = Lists.newArrayList();
    for (List<ManifestFile> deleteManifests : deleteManifestsBySnapshot.values()) {
      deleteIndexes.add(
          buildDeleteFileIndex(deleteManifests, startingSequenceNumber, dataFilter, partitionSet));
    }

    return deleteIndexes;
  }

  void validateDeletedDataFiles(
      TableMetadata base, Long startingSnapshotId, Expression dataFilter, Snapshot parent) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        deletedDataFiles(base, startingSnapshotId, dataFilter, null, parent);

    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
      if (conflicts.hasNext()) {
        throw new ValidationException(
            "Found conflicting deleted files that can contain records matching %s: %s",
            dataFilter,
            Iterators.toString(
                Iterators.transform(conflicts, entry -> entry.file().location().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to validate no deleted data files matching %s", dataFilter), e);
    }
  }

  void validateDeletedDataFiles(
      TableMetadata base, Long startingSnapshotId, PartitionSet partitionSet, Snapshot parent) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        deletedDataFiles(base, startingSnapshotId, null, partitionSet, parent);

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
      TableMetadata base,
      Long startingSnapshotId,
      Expression dataFilter,
      PartitionSet partitionSet,
      Snapshot parent) {
    // if there is no current table state, no files have been deleted
    if (parent == null) {
      return CloseableIterable.empty();
    }

    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(
            base,
            startingSnapshotId,
            VALIDATE_DATA_FILES_EXIST_OPERATIONS,
            ManifestContent.DATA,
            parent);
    List<ManifestFile> manifests = history.first();
    Set<Long> newSnapshots = history.second();

    ManifestGroup manifestGroup =
        new ManifestGroup(ops.io(), manifests, ImmutableList.of())
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

  private long startingSequenceNumber(TableMetadata metadata, Long startingSnapshotId) {
    if (startingSnapshotId != null && metadata.snapshot(startingSnapshotId) != null) {
      Snapshot startingSnapshot = metadata.snapshot(startingSnapshotId);
      return startingSnapshot.sequenceNumber();
    } else {
      return TableMetadata.INITIAL_SEQUENCE_NUMBER;
    }
  }

  private DeleteFileIndex buildDeleteFileIndex(
      List<ManifestFile> deleteManifests,
      long startingSequenceNumber,
      Expression dataFilter,
      PartitionSet partitionSet) {
    DeleteFileIndex.Builder builder =
        DeleteFileIndex.builderFor(ops.io(), deleteManifests)
            .afterSequenceNumber(startingSequenceNumber)
            .caseSensitive(caseSensitive)
            .specsById(ops.current().specsById());

    if (dataFilter != null) {
      builder.filterData(dataFilter);
    }

    if (partitionSet != null) {
      builder.filterPartitions(partitionSet);
    }

    return builder.build();
  }

  @SuppressWarnings("CollectionUndefinedEquality")
  void validateDataFilesExist(
      TableMetadata base,
      Long startingSnapshotId,
      CharSequenceSet requiredDataFiles,
      boolean skipDeletes,
      Expression conflictDetectionFilter,
      Snapshot parent) {
    // if there is no current table state, no files have been removed
    if (parent == null) {
      return;
    }

    Set<String> matchingOperations =
        skipDeletes
            ? VALIDATE_DATA_FILES_EXIST_SKIP_DELETE_OPERATIONS
            : VALIDATE_DATA_FILES_EXIST_OPERATIONS;

    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(
            base, startingSnapshotId, matchingOperations, ManifestContent.DATA, parent);
    List<ManifestFile> manifests = history.first();
    Set<Long> newSnapshots = history.second();

    ManifestGroup matchingDeletesGroup =
        new ManifestGroup(ops.io(), manifests, ImmutableList.of())
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

  void validateAddedDVs(
      TableMetadata base,
      Long startingSnapshotId,
      Expression conflictDetectionFilter,
      Snapshot parent,
      Set<String> referencedDataFiles,
      ExecutorService workerPool) {
    // skip if there is no current table state or this operation doesn't add new DVs
    if (parent == null || referencedDataFiles.isEmpty()) {
      return;
    }

    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(
            base,
            startingSnapshotId,
            VALIDATE_ADDED_DVS_OPERATIONS,
            ManifestContent.DELETES,
            parent);
    List<ManifestFile> newDeleteManifests = history.first();
    Set<Long> newSnapshotIds = history.second();

    Iterable<ManifestFile> matchingManifests =
        Iterables.filter(
            filterManifestsByPartition(base, conflictDetectionFilter, newDeleteManifests),
            ManifestFile::hasAddedFiles);

    Tasks.foreach(matchingManifests)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPool)
        .run(
            manifest ->
                validateAddedDVs(
                    manifest, conflictDetectionFilter, newSnapshotIds, referencedDataFiles));
  }

  private void validateAddedDVs(
      ManifestFile manifest,
      Expression conflictDetectionFilter,
      Set<Long> newSnapshotIds,
      Set<String> referencedDataFiles) {
    try (CloseableIterable<ManifestEntry<DeleteFile>> entries =
        ManifestFiles.readDeleteManifest(manifest, ops.io(), ops.current().specsById())
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
      TableMetadata base, Expression conflictDetectionFilter, List<ManifestFile> manifests) {
    if (conflictDetectionFilter == null || conflictDetectionFilter == Expressions.alwaysTrue()) {
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
                        Projections.inclusive(spec, caseSensitive).project(conflictDetectionFilter);
                    return ManifestEvaluator.forPartitionFilter(
                        partitionFilter, spec, caseSensitive);
                  });
          return evaluator.eval(manifest);
        });
  }

  // returns newly added manifests and snapshot IDs between the starting and parent snapshots
  private Pair<List<ManifestFile>, Set<Long>> validationHistory(
      TableMetadata base,
      Long startingSnapshotId,
      Set<String> matchingOperations,
      ManifestContent content,
      Snapshot parent) {
    List<ManifestFile> manifests = Lists.newArrayList();
    Set<Long> newSnapshots = Sets.newHashSet();

    Snapshot lastSnapshot = null;
    Iterable<Snapshot> snapshots =
        SnapshotUtil.ancestorsBetween(parent.snapshotId(), startingSnapshotId, base::snapshot);
    for (Snapshot currentSnapshot : snapshots) {
      lastSnapshot = currentSnapshot;

      if (matchingOperations.contains(currentSnapshot.operation())) {
        newSnapshots.add(currentSnapshot.snapshotId());
        if (content == ManifestContent.DATA) {
          for (ManifestFile manifest : currentSnapshot.dataManifests(ops.io())) {
            if (manifest.snapshotId() == currentSnapshot.snapshotId()) {
              manifests.add(manifest);
            }
          }
        } else {
          for (ManifestFile manifest : currentSnapshot.deleteManifests(ops.io())) {
            if (manifest.snapshotId() == currentSnapshot.snapshotId()) {
              manifests.add(manifest);
            }
          }
        }
      }
    }

    ValidationException.check(
        lastSnapshot == null || Objects.equals(lastSnapshot.parentId(), startingSnapshotId),
        "Cannot determine history between starting snapshot %s and the last known ancestor %s",
        startingSnapshotId,
        lastSnapshot != null ? lastSnapshot.snapshotId() : null);

    return Pair.of(manifests, newSnapshots);
  }
}
