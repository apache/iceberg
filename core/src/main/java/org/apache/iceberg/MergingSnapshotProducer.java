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

import static org.apache.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT;
import static org.apache.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT_DEFAULT;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED;
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Predicate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class MergingSnapshotProducer<ThisT> extends SnapshotProducer<ThisT> {
  private static final Logger LOG = LoggerFactory.getLogger(MergingSnapshotProducer.class);

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

  private final String tableName;
  private final TableOperations ops;
  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
  private final ManifestMergeManager<DataFile> mergeManager;
  private final ManifestFilterManager<DataFile> filterManager;
  private final ManifestMergeManager<DeleteFile> deleteMergeManager;
  private final ManifestFilterManager<DeleteFile> deleteFilterManager;
  private final boolean snapshotIdInheritanceEnabled;

  // update data
  private final List<DataFile> newFiles = Lists.newArrayList();
  private Long newDataFilesDataSequenceNumber;
  private final Map<Integer, List<DeleteFile>> newDeleteFilesBySpec = Maps.newHashMap();
  private final List<ManifestFile> appendManifests = Lists.newArrayList();
  private final List<ManifestFile> rewrittenAppendManifests = Lists.newArrayList();
  private final SnapshotSummary.Builder addedFilesSummary = SnapshotSummary.builder();
  private final SnapshotSummary.Builder appendedManifestsSummary = SnapshotSummary.builder();
  private Expression deleteExpression = Expressions.alwaysFalse();
  private PartitionSpec dataSpec;

  // cache new manifests after writing
  private ManifestFile cachedNewManifest = null;
  private boolean hasNewFiles = false;

  // cache new manifests for delete files
  private final List<ManifestFile> cachedNewDeleteManifests = Lists.newLinkedList();
  private boolean hasNewDeleteFiles = false;

  private boolean caseSensitive = true;

  MergingSnapshotProducer(String tableName, TableOperations ops) {
    super(ops);
    this.tableName = tableName;
    this.ops = ops;
    this.dataSpec = null;
    long targetSizeBytes =
        ops.current()
            .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    int minCountToMerge =
        ops.current().propertyAsInt(MANIFEST_MIN_MERGE_COUNT, MANIFEST_MIN_MERGE_COUNT_DEFAULT);
    boolean mergeEnabled =
        ops.current()
            .propertyAsBoolean(
                TableProperties.MANIFEST_MERGE_ENABLED,
                TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT);
    this.mergeManager = new DataFileMergeManager(targetSizeBytes, minCountToMerge, mergeEnabled);
    this.filterManager = new DataFileFilterManager();
    this.deleteMergeManager =
        new DeleteFileMergeManager(targetSizeBytes, minCountToMerge, mergeEnabled);
    this.deleteFilterManager = new DeleteFileFilterManager();
    this.snapshotIdInheritanceEnabled =
        ops.current()
            .propertyAsBoolean(
                SNAPSHOT_ID_INHERITANCE_ENABLED, SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);
  }

  @Override
  public ThisT set(String property, String value) {
    summaryBuilder.set(property, value);
    return self();
  }

  public ThisT caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    filterManager.caseSensitive(isCaseSensitive);
    deleteFilterManager.caseSensitive(isCaseSensitive);
    return self();
  }

  protected boolean isCaseSensitive() {
    return caseSensitive;
  }

  protected PartitionSpec dataSpec() {
    Preconditions.checkState(
        dataSpec != null, "Cannot determine partition spec: no data files have been added");
    // the spec is set when the write is started
    return dataSpec;
  }

  protected Expression rowFilter() {
    return deleteExpression;
  }

  protected List<DataFile> addedFiles() {
    return ImmutableList.copyOf(newFiles);
  }

  protected void failAnyDelete() {
    filterManager.failAnyDelete();
    deleteFilterManager.failAnyDelete();
  }

  protected void failMissingDeletePaths() {
    filterManager.failMissingDeletePaths();
    deleteFilterManager.failMissingDeletePaths();
  }

  /**
   * Add a filter to match files to delete. A file will be deleted if all of the rows it contains
   * match this or any other filter passed to this method.
   *
   * @param expr an expression to match rows.
   */
  protected void deleteByRowFilter(Expression expr) {
    this.deleteExpression = expr;
    filterManager.deleteByRowFilter(expr);
    // if a delete file matches the row filter, then it can be deleted because the rows will also be
    // deleted
    deleteFilterManager.deleteByRowFilter(expr);
  }

  /** Add a partition tuple to drop from the table during the delete phase. */
  protected void dropPartition(int specId, StructLike partition) {
    // dropping the data in a partition also drops all deletes in the partition
    filterManager.dropPartition(specId, partition);
    deleteFilterManager.dropPartition(specId, partition);
  }

  /** Add a specific data file to be deleted in the new snapshot. */
  protected void delete(DataFile file) {
    filterManager.delete(file);
  }

  /** Add a specific delete file to be deleted in the new snapshot. */
  protected void delete(DeleteFile file) {
    deleteFilterManager.delete(file);
  }

  /** Add a specific data path to be deleted in the new snapshot. */
  protected void delete(CharSequence path) {
    // this is an old call that never worked for delete files and can only be used to remove data
    // files.
    filterManager.delete(path);
  }

  protected boolean deletesDataFiles() {
    return filterManager.containsDeletes();
  }

  protected boolean deletesDeleteFiles() {
    return deleteFilterManager.containsDeletes();
  }

  protected boolean addsDataFiles() {
    return newFiles.size() > 0;
  }

  protected boolean addsDeleteFiles() {
    return newDeleteFilesBySpec.size() > 0;
  }

  /** Add a data file to the new snapshot. */
  protected void add(DataFile file) {
    Preconditions.checkNotNull(file, "Invalid data file: null");
    setDataSpec(file);
    addedFilesSummary.addedFile(dataSpec(), file);
    hasNewFiles = true;
    newFiles.add(file);
  }

  /** Add a delete file to the new snapshot. */
  protected void add(DeleteFile file) {
    Preconditions.checkNotNull(file, "Invalid delete file: null");
    PartitionSpec fileSpec = ops.current().spec(file.specId());
    List<DeleteFile> deleteFiles =
        newDeleteFilesBySpec.computeIfAbsent(file.specId(), specId -> Lists.newArrayList());
    deleteFiles.add(file);
    addedFilesSummary.addedFile(fileSpec, file);
    hasNewDeleteFiles = true;
  }

  private void setDataSpec(DataFile file) {
    PartitionSpec fileSpec = ops.current().spec(file.specId());
    Preconditions.checkNotNull(
        fileSpec, "Cannot find partition spec for data file: %s", file.path());
    if (dataSpec == null) {
      dataSpec = fileSpec;
    } else if (dataSpec.specId() != file.specId()) {
      throw new ValidationException("Invalid data file, expected spec id: %d", dataSpec.specId());
    }
  }

  /** Add all files in a manifest to the new snapshot. */
  protected void add(ManifestFile manifest) {
    Preconditions.checkArgument(
        manifest.content() == ManifestContent.DATA, "Cannot append delete manifest: %s", manifest);
    if (snapshotIdInheritanceEnabled && manifest.snapshotId() == null) {
      appendedManifestsSummary.addedManifest(manifest);
      appendManifests.add(manifest);
    } else {
      // the manifest must be rewritten with this update's snapshot ID
      ManifestFile copiedManifest = copyManifest(manifest);
      rewrittenAppendManifests.add(copiedManifest);
    }
  }

  private ManifestFile copyManifest(ManifestFile manifest) {
    TableMetadata current = ops.current();
    InputFile toCopy = ops.io().newInputFile(manifest.path());
    OutputFile newManifestPath = newManifestOutput();
    return ManifestFiles.copyAppendManifest(
        current.formatVersion(),
        manifest.partitionSpecId(),
        toCopy,
        current.specsById(),
        newManifestPath,
        snapshotId(),
        appendedManifestsSummary);
  }

  /**
   * Validates that no files matching given partitions have been added to the table since a starting
   * snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param partitionSet a set of partitions to filter new conflicting data files
   * @param parent ending snapshot on the lineage being validated
   */
  protected void validateAddedDataFiles(
      TableMetadata base, Long startingSnapshotId, PartitionSet partitionSet, Snapshot parent) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        addedDataFiles(base, startingSnapshotId, null, partitionSet, parent);

    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
      if (conflicts.hasNext()) {
        throw new ValidationException(
            "Found conflicting files that can contain records matching partitions %s: %s",
            partitionSet,
            Iterators.toString(
                Iterators.transform(conflicts, entry -> entry.file().path().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to validate no appends matching %s", partitionSet), e);
    }
  }

  /**
   * Validates that no files matching a filter have been added to the table since a starting
   * snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param conflictDetectionFilter an expression used to find new conflicting data files
   */
  protected void validateAddedDataFiles(
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
                Iterators.transform(conflicts, entry -> entry.file().path().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to validate no appends matching %s", conflictDetectionFilter), e);
    }
  }

  /**
   * Returns an iterable of files matching a filter have been added to a branch since a starting
   * snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param dataFilter an expression used to find new data files
   * @param partitionSet a set of partitions to find new data files
   * @param parent ending snapshot of the branch
   */
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

  /**
   * Validates that no new delete files that must be applied to the given data files have been added
   * to the table since a starting snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param dataFiles data files to validate have no new row deletes
   * @param parent ending snapshot on the branch being validated
   */
  protected void validateNoNewDeletesForDataFiles(
      TableMetadata base, Long startingSnapshotId, Iterable<DataFile> dataFiles, Snapshot parent) {
    validateNoNewDeletesForDataFiles(
        base, startingSnapshotId, null, dataFiles, newDataFilesDataSequenceNumber != null, parent);
  }

  /**
   * Validates that no new delete files that must be applied to the given data files have been added
   * to the table since a starting snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param dataFilter a data filter
   * @param dataFiles data files to validate have no new row deletes
   * @param parent ending snapshot on the branch being validated
   */
  protected void validateNoNewDeletesForDataFiles(
      TableMetadata base,
      Long startingSnapshotId,
      Expression dataFilter,
      Iterable<DataFile> dataFiles,
      Snapshot parent) {
    validateNoNewDeletesForDataFiles(
        base, startingSnapshotId, dataFilter, dataFiles, false, parent);
  }

  /**
   * Validates that no new delete files that must be applied to the given data files have been added
   * to the table since a starting snapshot, with the option to ignore equality deletes during the
   * validation.
   *
   * <p>For example, in the case of rewriting data files, if the added data files have the same
   * sequence number as the replaced data files, equality deletes added at a higher sequence number
   * are still effective against the added data files, so there is no risk of commit conflict
   * between RewriteFiles and RowDelta. In cases like this, validation against equality delete files
   * can be omitted.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param dataFilter a data filter
   * @param dataFiles data files to validate have no new row deletes
   * @param ignoreEqualityDeletes whether equality deletes should be ignored in validation
   * @param parent ending snapshot on the branch being validated
   */
  private void validateNoNewDeletesForDataFiles(
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

    DeleteFileIndex deletes = addedDeleteFiles(base, startingSnapshotId, dataFilter, null, parent);

    long startingSequenceNumber = startingSequenceNumber(base, startingSnapshotId);
    for (DataFile dataFile : dataFiles) {
      // if any delete is found that applies to files written in or before the starting snapshot,
      // fail
      DeleteFile[] deleteFiles = deletes.forDataFile(startingSequenceNumber, dataFile);
      if (ignoreEqualityDeletes) {
        ValidationException.check(
            Arrays.stream(deleteFiles)
                .noneMatch(deleteFile -> deleteFile.content() == FileContent.POSITION_DELETES),
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

  /**
   * Validates that no delete files matching a filter have been added to the table since a starting
   * snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param dataFilter an expression used to find new conflicting delete files
   * @param parent ending snapshot on the branch being validated
   */
  protected void validateNoNewDeleteFiles(
      TableMetadata base, Long startingSnapshotId, Expression dataFilter, Snapshot parent) {
    DeleteFileIndex deletes = addedDeleteFiles(base, startingSnapshotId, dataFilter, null, parent);
    ValidationException.check(
        deletes.isEmpty(),
        "Found new conflicting delete files that can apply to records matching %s: %s",
        dataFilter,
        Iterables.transform(deletes.referencedDeleteFiles(), ContentFile::path));
  }

  /**
   * Validates that no delete files matching a partition set have been added to the table since a
   * starting snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param partitionSet a partition set used to find new conflicting delete files
   * @param parent ending snapshot on the branch being validated
   */
  protected void validateNoNewDeleteFiles(
      TableMetadata base, Long startingSnapshotId, PartitionSet partitionSet, Snapshot parent) {
    DeleteFileIndex deletes =
        addedDeleteFiles(base, startingSnapshotId, null, partitionSet, parent);
    ValidationException.check(
        deletes.isEmpty(),
        "Found new conflicting delete files that can apply to records matching %s: %s",
        partitionSet,
        Iterables.transform(deletes.referencedDeleteFiles(), ContentFile::path));
  }

  /**
   * Returns matching delete files have been added to the table since a starting snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param dataFilter an expression used to find delete files
   * @param partitionSet a partition set used to find delete files
   * @param parent parent snapshot of the branch
   */
  protected DeleteFileIndex addedDeleteFiles(
      TableMetadata base,
      Long startingSnapshotId,
      Expression dataFilter,
      PartitionSet partitionSet,
      Snapshot parent) {
    // if there is no current table state, return empty delete file index
    if (parent == null || base.formatVersion() < 2) {
      return DeleteFileIndex.builderFor(ops.io(), ImmutableList.of())
          .specsById(base.specsById())
          .build();
    }

    Pair<List<ManifestFile>, Set<Long>> history =
        validationHistory(
            base,
            startingSnapshotId,
            VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
            ManifestContent.DELETES,
            parent);
    List<ManifestFile> deleteManifests = history.first();

    long startingSequenceNumber = startingSequenceNumber(base, startingSnapshotId);
    return buildDeleteFileIndex(deleteManifests, startingSequenceNumber, dataFilter, partitionSet);
  }

  /**
   * Validates that no files matching a filter have been deleted from the table since a starting
   * snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param dataFilter an expression used to find deleted data files
   * @param parent ending snapshot on the branch being validated
   */
  protected void validateDeletedDataFiles(
      TableMetadata base, Long startingSnapshotId, Expression dataFilter, Snapshot parent) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        deletedDataFiles(base, startingSnapshotId, dataFilter, null, parent);

    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
      if (conflicts.hasNext()) {
        throw new ValidationException(
            "Found conflicting deleted files that can contain records matching %s: %s",
            dataFilter,
            Iterators.toString(
                Iterators.transform(conflicts, entry -> entry.file().path().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to validate no deleted data files matching %s", dataFilter), e);
    }
  }

  /**
   * Validates that no files matching a filter have been deleted from the table since a starting
   * snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param partitionSet a partition set used to find deleted data files
   * @param parent ending snapshot on the branch being validated
   */
  protected void validateDeletedDataFiles(
      TableMetadata base, Long startingSnapshotId, PartitionSet partitionSet, Snapshot parent) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        deletedDataFiles(base, startingSnapshotId, null, partitionSet, parent);

    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
      if (conflicts.hasNext()) {
        throw new ValidationException(
            "Found conflicting deleted files that can apply to records matching %s: %s",
            partitionSet,
            Iterators.toString(
                Iterators.transform(conflicts, entry -> entry.file().path().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to validate no appends matching %s", partitionSet), e);
    }
  }

  /**
   * Returns an iterable of files matching a filter have been added to the table since a starting
   * snapshot.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param dataFilter an expression used to find deleted data files
   * @param partitionSet a set of partitions to find deleted data files
   * @param parent ending snapshot on the branch being validated
   */
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

  /**
   * Sets a data sequence number for new data files.
   *
   * @param sequenceNumber a data sequence number
   * @deprecated since 1.3.0, will be removed in 1.4.0; use {@link
   *     #setNewDataFilesDataSequenceNumber(long)};
   */
  @Deprecated
  protected void setNewFilesSequenceNumber(long sequenceNumber) {
    setNewDataFilesDataSequenceNumber(sequenceNumber);
  }

  protected void setNewDataFilesDataSequenceNumber(long sequenceNumber) {
    this.newDataFilesDataSequenceNumber = sequenceNumber;
  }

  private long startingSequenceNumber(TableMetadata metadata, Long staringSnapshotId) {
    if (staringSnapshotId != null && metadata.snapshot(staringSnapshotId) != null) {
      Snapshot startingSnapshot = metadata.snapshot(staringSnapshotId);
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
  protected void validateDataFilesExist(
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
                        && requiredDataFiles.contains(entry.file().path()))
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
                Iterators.transform(deletes, entry -> entry.file().path().toString())));
      }

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to validate required files exist", e);
    }
  }

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

  @Override
  protected Map<String, String> summary() {
    summaryBuilder.setPartitionSummaryLimit(
        ops.current()
            .propertyAsInt(
                TableProperties.WRITE_PARTITION_SUMMARY_LIMIT,
                TableProperties.WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT));
    return summaryBuilder.build();
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
    // filter any existing manifests
    List<ManifestFile> filtered =
        filterManager.filterManifests(
            SnapshotUtil.schemaFor(base, targetBranch()),
            snapshot != null ? snapshot.dataManifests(ops.io()) : null);
    long minDataSequenceNumber =
        filtered.stream()
            .map(ManifestFile::minSequenceNumber)
            .filter(
                seq ->
                    seq
                        != ManifestWriter
                            .UNASSIGNED_SEQ) // filter out unassigned in rewritten manifests
            .reduce(base.lastSequenceNumber(), Math::min);
    deleteFilterManager.dropDeleteFilesOlderThan(minDataSequenceNumber);
    List<ManifestFile> filteredDeletes =
        deleteFilterManager.filterManifests(
            SnapshotUtil.schemaFor(base, targetBranch()),
            snapshot != null ? snapshot.deleteManifests(ops.io()) : null);

    // only keep manifests that have live data files or that were written by this commit
    Predicate<ManifestFile> shouldKeep =
        manifest ->
            manifest.hasAddedFiles()
                || manifest.hasExistingFiles()
                || manifest.snapshotId() == snapshotId();
    Iterable<ManifestFile> unmergedManifests =
        Iterables.filter(Iterables.concat(prepareNewManifests(), filtered), shouldKeep);
    Iterable<ManifestFile> unmergedDeleteManifests =
        Iterables.filter(Iterables.concat(prepareDeleteManifests(), filteredDeletes), shouldKeep);

    // update the snapshot summary
    summaryBuilder.clear();
    summaryBuilder.merge(addedFilesSummary);
    summaryBuilder.merge(appendedManifestsSummary);
    summaryBuilder.merge(filterManager.buildSummary(filtered));
    summaryBuilder.merge(deleteFilterManager.buildSummary(filteredDeletes));

    List<ManifestFile> manifests = Lists.newArrayList();
    Iterables.addAll(manifests, mergeManager.mergeManifests(unmergedManifests));
    Iterables.addAll(manifests, deleteMergeManager.mergeManifests(unmergedDeleteManifests));

    return manifests;
  }

  @Override
  public Object updateEvent() {
    long snapshotId = snapshotId();
    Snapshot justSaved = ops.refresh().snapshot(snapshotId);
    long sequenceNumber = TableMetadata.INVALID_SEQUENCE_NUMBER;
    Map<String, String> summary;
    if (justSaved == null) {
      // The snapshot just saved may not be present if the latest metadata couldn't be loaded due to
      // eventual
      // consistency problems in refresh.
      LOG.warn("Failed to load committed snapshot: omitting sequence number from notifications");
      summary = summary();
    } else {
      sequenceNumber = justSaved.sequenceNumber();
      summary = justSaved.summary();
    }

    return new CreateSnapshotEvent(tableName, operation(), snapshotId, sequenceNumber, summary);
  }

  private void cleanUncommittedAppends(Set<ManifestFile> committed) {
    if (cachedNewManifest != null && !committed.contains(cachedNewManifest)) {
      deleteFile(cachedNewManifest.path());
      this.cachedNewManifest = null;
    }

    ListIterator<ManifestFile> deleteManifestsIterator = cachedNewDeleteManifests.listIterator();
    while (deleteManifestsIterator.hasNext()) {
      ManifestFile deleteManifest = deleteManifestsIterator.next();
      if (!committed.contains(deleteManifest)) {
        deleteFile(deleteManifest.path());
        deleteManifestsIterator.remove();
      }
    }

    // rewritten manifests are always owned by the table
    for (ManifestFile manifest : rewrittenAppendManifests) {
      if (!committed.contains(manifest)) {
        deleteFile(manifest.path());
      }
    }

    // manifests that are not rewritten are only owned by the table if the commit succeeded
    if (!committed.isEmpty()) {
      // the commit succeeded if at least one manifest was committed
      // the table now owns appendManifests; clean up any that are not used
      for (ManifestFile manifest : appendManifests) {
        if (!committed.contains(manifest)) {
          deleteFile(manifest.path());
        }
      }
    }
  }

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
    mergeManager.cleanUncommitted(committed);
    filterManager.cleanUncommitted(committed);
    deleteMergeManager.cleanUncommitted(committed);
    deleteFilterManager.cleanUncommitted(committed);
    cleanUncommittedAppends(committed);
  }

  private Iterable<ManifestFile> prepareNewManifests() {
    Iterable<ManifestFile> newManifests;
    if (newFiles.size() > 0) {
      ManifestFile newManifest = newFilesAsManifest();
      newManifests =
          Iterables.concat(
              ImmutableList.of(newManifest), appendManifests, rewrittenAppendManifests);
    } else {
      newManifests = Iterables.concat(appendManifests, rewrittenAppendManifests);
    }

    return Iterables.transform(
        newManifests,
        manifest -> GenericManifestFile.copyOf(manifest).withSnapshotId(snapshotId()).build());
  }

  private ManifestFile newFilesAsManifest() {
    if (hasNewFiles && cachedNewManifest != null) {
      deleteFile(cachedNewManifest.path());
      cachedNewManifest = null;
    }

    if (cachedNewManifest == null) {
      try {
        ManifestWriter<DataFile> writer = newManifestWriter(dataSpec());
        try {
          if (newDataFilesDataSequenceNumber == null) {
            writer.addAll(newFiles);
          } else {
            newFiles.forEach(f -> writer.add(f, newDataFilesDataSequenceNumber));
          }
        } finally {
          writer.close();
        }

        this.cachedNewManifest = writer.toManifestFile();
        this.hasNewFiles = false;
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close manifest writer");
      }
    }

    return cachedNewManifest;
  }

  private Iterable<ManifestFile> prepareDeleteManifests() {
    if (newDeleteFilesBySpec.isEmpty()) {
      return ImmutableList.of();
    }

    return newDeleteFilesAsManifests();
  }

  private List<ManifestFile> newDeleteFilesAsManifests() {
    if (hasNewDeleteFiles && cachedNewDeleteManifests.size() > 0) {
      for (ManifestFile cachedNewDeleteManifest : cachedNewDeleteManifests) {
        deleteFile(cachedNewDeleteManifest.path());
      }
      // this triggers a rewrite of all delete manifests even if there is only one new delete file
      // if there is a relevant use case in the future, the behavior can be optimized
      cachedNewDeleteManifests.clear();
    }

    if (cachedNewDeleteManifests.isEmpty()) {
      newDeleteFilesBySpec.forEach(
          (specId, deleteFiles) -> {
            PartitionSpec spec = ops.current().spec(specId);
            try {
              ManifestWriter<DeleteFile> writer = newDeleteManifestWriter(spec);
              try {
                writer.addAll(deleteFiles);
              } finally {
                writer.close();
              }
              cachedNewDeleteManifests.add(writer.toManifestFile());
            } catch (IOException e) {
              throw new RuntimeIOException(e, "Failed to close manifest writer");
            }
          });

      this.hasNewDeleteFiles = false;
    }

    return cachedNewDeleteManifests;
  }

  private class DataFileFilterManager extends ManifestFilterManager<DataFile> {
    private DataFileFilterManager() {
      super(ops.current().specsById(), MergingSnapshotProducer.this::workerPool);
    }

    @Override
    protected void deleteFile(String location) {
      MergingSnapshotProducer.this.deleteFile(location);
    }

    @Override
    protected ManifestWriter<DataFile> newManifestWriter(PartitionSpec manifestSpec) {
      return MergingSnapshotProducer.this.newManifestWriter(manifestSpec);
    }

    @Override
    protected ManifestReader<DataFile> newManifestReader(ManifestFile manifest) {
      return MergingSnapshotProducer.this.newManifestReader(manifest);
    }
  }

  private class DataFileMergeManager extends ManifestMergeManager<DataFile> {
    DataFileMergeManager(long targetSizeBytes, int minCountToMerge, boolean mergeEnabled) {
      super(
          targetSizeBytes, minCountToMerge, mergeEnabled, MergingSnapshotProducer.this::workerPool);
    }

    @Override
    protected long snapshotId() {
      return MergingSnapshotProducer.this.snapshotId();
    }

    @Override
    protected PartitionSpec spec(int specId) {
      return ops.current().spec(specId);
    }

    @Override
    protected void deleteFile(String location) {
      MergingSnapshotProducer.this.deleteFile(location);
    }

    @Override
    protected ManifestWriter<DataFile> newManifestWriter(PartitionSpec manifestSpec) {
      return MergingSnapshotProducer.this.newManifestWriter(manifestSpec);
    }

    @Override
    protected ManifestReader<DataFile> newManifestReader(ManifestFile manifest) {
      return MergingSnapshotProducer.this.newManifestReader(manifest);
    }
  }

  private class DeleteFileFilterManager extends ManifestFilterManager<DeleteFile> {
    private DeleteFileFilterManager() {
      super(ops.current().specsById(), MergingSnapshotProducer.this::workerPool);
    }

    @Override
    protected void deleteFile(String location) {
      MergingSnapshotProducer.this.deleteFile(location);
    }

    @Override
    protected ManifestWriter<DeleteFile> newManifestWriter(PartitionSpec manifestSpec) {
      return MergingSnapshotProducer.this.newDeleteManifestWriter(manifestSpec);
    }

    @Override
    protected ManifestReader<DeleteFile> newManifestReader(ManifestFile manifest) {
      return MergingSnapshotProducer.this.newDeleteManifestReader(manifest);
    }
  }

  private class DeleteFileMergeManager extends ManifestMergeManager<DeleteFile> {
    DeleteFileMergeManager(long targetSizeBytes, int minCountToMerge, boolean mergeEnabled) {
      super(
          targetSizeBytes, minCountToMerge, mergeEnabled, MergingSnapshotProducer.this::workerPool);
    }

    @Override
    protected long snapshotId() {
      return MergingSnapshotProducer.this.snapshotId();
    }

    @Override
    protected PartitionSpec spec(int specId) {
      return ops.current().spec(specId);
    }

    @Override
    protected void deleteFile(String location) {
      MergingSnapshotProducer.this.deleteFile(location);
    }

    @Override
    protected ManifestWriter<DeleteFile> newManifestWriter(PartitionSpec manifestSpec) {
      return MergingSnapshotProducer.this.newDeleteManifestWriter(manifestSpec);
    }

    @Override
    protected ManifestReader<DeleteFile> newManifestReader(ManifestFile manifest) {
      return MergingSnapshotProducer.this.newDeleteManifestReader(manifest);
    }
  }
}
