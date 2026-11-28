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
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
  // DVs can be added in "overwrite", "delete", and "replace" operations
  private static final Set<String> VALIDATE_ADDED_DVS_OPERATIONS =
      ImmutableSet.of(DataOperations.OVERWRITE, DataOperations.DELETE, DataOperations.REPLACE);

  private final String tableName;
  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
  private final ProducerActions producer;
  private final RootManifestProducer rootManifestProducer;
  private final ManifestListProducer manifestListProducer;
  private boolean failAnyDelete = false;
  private boolean failMissingDeletePaths = false;

  // update data
  private final List<DataFile> appendedDataFiles = Lists.newArrayList();
  private Expression deleteExpression = Expressions.alwaysFalse();
  private final List<DeleteFile> addedDeleteFiles = Lists.newArrayList();
  private boolean deletesDataFiles = false;
  private boolean deletesDeleteFiles = false;

  private final SnapshotSummary.Builder addedDataFilesSummary = SnapshotSummary.builder();
  private final SnapshotSummary.Builder appendedManifestsSummary = SnapshotSummary.builder();

  private boolean caseSensitive = true;

  MergingSnapshotProducer(String tableName, TableOperations ops) {
    super(ops);
    this.tableName = tableName;
    if (ops.current().formatVersion() < 4) {
      this.manifestListProducer =
          new ManifestListProducer(tableName, ops, snapshotId(), formatVersion());
      this.rootManifestProducer = null;
      this.producer = manifestListProducer;
    } else {
      this.manifestListProducer = null;
      this.rootManifestProducer = new RootManifestProducer(ops, snapshotId());
      this.producer = rootManifestProducer;
    }
  }

  @Override
  public ThisT scanManifestsWith(ExecutorService pool) {
    Preconditions.checkArgument(pool != null, "Executor service cannot be null");
    if (manifestListProducer != null) {
      manifestListProducer.scanWith(pool);
    }

    super.scanManifestsWith(pool);
    return self();
  }

  @Override
  public ThisT writeManifestsWith(ExecutorService pool, int parallelism) {
    if (manifestListProducer != null) {
      manifestListProducer.writeWith(pool, parallelism);
    }

    return self();
  }

  @Override
  public ThisT deleteWith(Consumer<String> deleteCallback) {
    if (manifestListProducer != null) {
      manifestListProducer.deleteWith(deleteCallback);
    }

    super.deleteWith(deleteCallback);
    return self();
  }

  @Override
  public ThisT set(String property, String value) {
    summaryBuilder.set(property, value);
    return self();
  }

  public ThisT caseSensitive(boolean isCaseSensitive) {
    producer.caseSensitive(isCaseSensitive);
    this.caseSensitive = isCaseSensitive;
    return self();
  }

  protected boolean isCaseSensitive() {
    return caseSensitive;
  }

  protected PartitionSpec dataSpec() {
    Set<Integer> specIds =
        addedDataFiles().stream().map(DataFile::specId).collect(Collectors.toSet());
    Preconditions.checkState(
        !specIds.isEmpty(), "Cannot determine partition specs: no data files have been added");
    Preconditions.checkState(
        specIds.size() == 1,
        "Cannot return a single partition spec: data files with different partition specs have been added");
    return spec(Iterables.getOnlyElement(specIds));
  }

  protected Expression rowFilter() {
    return deleteExpression;
  }

  protected List<DataFile> addedDataFiles() {
    return appendedDataFiles;
  }

  protected void failAnyDelete() {
    this.failAnyDelete = true;
  }

  protected void failMissingDeletePaths() {
    this.failMissingDeletePaths = true;
  }

  /**
   * Add a filter to match files to delete. A file will be deleted if all of the rows it contains
   * match this or any other filter passed to this method.
   *
   * @param expr an expression to match rows.
   */
  protected void deleteByRowFilter(Expression expr) {
    producer.deleteByRowFilter(expr);
    this.deleteExpression = expr;
    this.deletesDataFiles = true;
  }

  /** Add a partition tuple to drop from the table during the delete phase. */
  protected void dropPartition(int specId, StructLike partition) {
    producer.deletePartition(specId, partition);
    this.deletesDataFiles = true;
  }

  /** Add a specific data file to be deleted in the new snapshot. */
  protected void delete(DataFile file) {
    producer.deleteDataFile(file);
    this.deletesDataFiles = true;
  }

  /** Add a specific delete file to be deleted in the new snapshot. */
  protected void delete(DeleteFile file) {
    producer.deleteDeleteFile(file);
    this.deletesDeleteFiles = true;
  }

  /** Add a specific data path to be deleted in the new snapshot. */
  protected void delete(CharSequence path) {
    // this is an old call that never worked for delete files and can only be used to remove data
    // files.
    producer.deleteDataFile(path);
  }

  protected boolean deletesDataFiles() {
    return deletesDataFiles;
  }

  protected boolean deletesDeleteFiles() {
    return deletesDeleteFiles;
  }

  protected boolean addsDataFiles() {
    return !appendedDataFiles.isEmpty();
  }

  protected boolean addsDeleteFiles() {
    return !addedDeleteFiles.isEmpty();
  }

  /** Add a data file to the new snapshot. */
  protected void add(DataFile file) {
    Preconditions.checkNotNull(file, "Invalid data file: null");
    producer.appendDataFile(file);
    appendedDataFiles.add(file);
    addedDataFilesSummary.addedFile(spec(file.specId()), file);
  }

  protected void add(DataFile file, long dataSequenceNumber) {
    Preconditions.checkNotNull(file, "Invalid data file: null");
    producer.appendDataFileAtSeq(file, dataSequenceNumber);
    appendedDataFiles.add(file);
    addedDataFilesSummary.addedFile(spec(file.specId()), file);
  }

  private PartitionSpec spec(int specId) {
    return ops().current().spec(specId);
  }

  /** Add a delete file to the new snapshot. */
  protected void add(DeleteFile file) {
    Preconditions.checkNotNull(file, "Invalid delete file: null");
    producer.addDeleteFile(file);
    addedDeleteFiles.add(file);
  }

  /** Add a delete file to the new snapshot. */
  protected void add(DeleteFile file, long dataSequenceNumber) {
    Preconditions.checkNotNull(file, "Invalid delete file: null");
    producer.addDeleteFileAtSeq(file, dataSequenceNumber);
    addedDeleteFiles.add(file);
  }

  private int formatVersion() {
    return ops().current().formatVersion();
  }

  /** Add all files in a manifest to the new snapshot. */
  protected void add(ManifestFile manifest) {
    Preconditions.checkArgument(
        manifest.content() == ManifestContent.DATA, "Cannot append delete manifest: %s", manifest);
    producer.appendManifest(manifest);
    appendedManifestsSummary.addedManifest(manifest);
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
    Long parentId = parent != null ? parent.snapshotId() : null;
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

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
    Long parentId = parent != null ? parent.snapshotId() : null;
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

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
        new ManifestGroup(ops().io(), manifests, ImmutableList.of())
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
   * @param newDataFilesDataSequenceNumber sequence number used for new data files or null
   * @param parent ending snapshot on the branch being validated
   */
  protected void validateNoNewDeletesForDataFiles(
      TableMetadata base,
      Long startingSnapshotId,
      Iterable<DataFile> dataFiles,
      Long newDataFilesDataSequenceNumber,
      Snapshot parent) {
    Long parentId = parent != null ? parent.snapshotId() : null;
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

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
    Long parentId = parent != null ? parent.snapshotId() : null;
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

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

    Long parentId = parent != null ? parent.snapshotId() : null;
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

    List<DeleteFileIndex> deleteIndexes =
        addedDeleteFilesIndexedPerSnapshot(base, startingSnapshotId, dataFilter, null, parent);

    long startingSequenceNumber = startingSequenceNumber(base, startingSnapshotId);
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

  private static boolean containsPositionDeletes(DeleteFile[] deleteFiles) {
    for (DeleteFile deleteFile : deleteFiles) {
      if (deleteFile.content() == FileContent.POSITION_DELETES) {
        return true;
      }
    }

    return false;
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
    Long parentId = parent != null ? parent.snapshotId() : null;
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

    Set<String> locations =
        referencedDeleteFileLocations(
            addedDeleteFilesIndexedPerSnapshot(base, startingSnapshotId, dataFilter, null, parent));
    ValidationException.check(
        locations.isEmpty(),
        "Found new conflicting delete files that can apply to records matching %s: %s",
        dataFilter,
        locations);
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
    Long parentId = parent != null ? parent.snapshotId() : null;
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

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

  /**
   * Returns one index per snapshot in the validation window, covering the matching delete files
   * added since a starting snapshot.
   *
   * <p>Callers must check every index. A single index cannot hold them all because it allows at
   * most one deletion vector per data file. That holds within a snapshot but not across them: a
   * data file can be referenced by a replacement deletion vector in each snapshot in the window.
   *
   * @param base table metadata to validate
   * @param startingSnapshotId id of the snapshot current at the start of the operation
   * @param dataFilter an expression used to find delete files
   * @param partitionSet a partition set used to find delete files
   * @param parent parent snapshot of the branch
   */
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
    Long parentId = parent != null ? parent.snapshotId() : null;
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

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
    Long parentId = parent != null ? parent.snapshotId() : null;
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

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
        new ManifestGroup(ops().io(), manifests, ImmutableList.of())
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
        DeleteFileIndex.builderFor(ops().io(), deleteManifests)
            .afterSequenceNumber(startingSequenceNumber)
            .caseSensitive(caseSensitive)
            .specsById(ops().current().specsById());

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

    Long parentId = parent != null ? parent.snapshotId() : null;
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

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
        new ManifestGroup(ops().io(), manifests, ImmutableList.of())
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

  // validates there are no concurrently added DVs for referenced data files
  protected void validateAddedDVs(
      TableMetadata base,
      Long startingSnapshotId,
      Expression conflictDetectionFilter,
      Snapshot parent) {
    Map<String, List<DeleteFile>> dvsByReferencedFile = Maps.newHashMap();
    for (DeleteFile deletes : addedDeleteFiles) {
      if (ContentFileUtil.isDV(deletes)) {
        List<DeleteFile> dvs =
            dvsByReferencedFile.computeIfAbsent(
                deletes.referencedDataFile(), ignored -> Lists.newArrayList());
        dvs.add(deletes);
      }
    }

    // skip if there is no current table state or this operation doesn't add new DVs
    if (parent == null || dvsByReferencedFile.isEmpty()) {
      return;
    }

    Long parentId = parent.snapshotId();
    Preconditions.checkArgument(
        base.formatVersion() < 4 || Objects.equals(parentId, startingSnapshotId),
        "Cannot validate concurrent commit (%s) in v4 table",
        parentId);

    // DVs for data files added in this operation can't conflict with concurrent commits
    Set<String> unmatchedReferencedDataFiles = Sets.newHashSet(dvsByReferencedFile.keySet());
    for (DataFile dataFile : appendedDataFiles) {
      if (unmatchedReferencedDataFiles.remove(dataFile.location())
          && unmatchedReferencedDataFiles.isEmpty()) {
        return;
      }
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
        .executeWith(workerPool())
        .run(manifest -> validateAddedDVs(manifest, conflictDetectionFilter, newSnapshotIds));
  }

  private void validateAddedDVs(
      ManifestFile manifest, Expression conflictDetectionFilter, Set<Long> newSnapshotIds) {
    Set<String> referencedFiles =
        addedDeleteFiles.stream()
            .filter(ContentFileUtil::isDV)
            .map(DeleteFile::referencedDataFile)
            .collect(Collectors.toSet());

    try (CloseableIterable<ManifestEntry<DeleteFile>> entries =
        ManifestFiles.readDeleteManifest(manifest, ops().io(), ops().current().specsById())
            .filterRows(conflictDetectionFilter)
            .caseSensitive(caseSensitive)
            .liveEntries()) {

      for (ManifestEntry<DeleteFile> entry : entries) {
        DeleteFile file = entry.file();
        if (newSnapshotIds.contains(entry.snapshotId()) && ContentFileUtil.isDV(file)) {
          ValidationException.check(
              !referencedFiles.contains(file.referencedDataFile()),
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
          for (ManifestFile manifest : currentSnapshot.dataManifests(ops().io())) {
            if (manifest.snapshotId() == currentSnapshot.snapshotId()) {
              manifests.add(manifest);
            }
          }
        } else {
          for (ManifestFile manifest : currentSnapshot.deleteManifests(ops().io())) {
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
        ops()
            .current()
            .propertyAsInt(
                TableProperties.WRITE_PARTITION_SUMMARY_LIMIT,
                TableProperties.WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT));
    return summaryBuilder.build();
  }

  @Override
  protected NewSnapshotMetadata writeRootManifest(
      TableMetadata base, Snapshot parent, long sequenceNumber) {
    for (DeleteFile file : addedDeleteFiles) {
      Preconditions.checkArgument(
          ContentFileUtil.isDV(file),
          "Must use DVs for deletes in v%s: %s",
          formatVersion(),
          file.location());
    }

    TrackedFile result = rootManifestProducer.apply(base, parent, sequenceNumber);

    summaryBuilder.clear();
    summaryBuilder.merge(addedDataFilesSummary);

    return new NewSnapshotMetadata(
        null,
        result.location(),
        result.fileSizeInBytes(),
        rootManifestProducer.assignedRowIds(),
        rootManifestProducer.manifestLocations());
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base, Snapshot parent) {
    ValidationException.check(
        base.formatVersion() < 4,
        "Cannot commit to table concurrently upgraded to v%s",
        base.formatVersion());

    List<ManifestFile> manifests = manifestListProducer.apply(base, parent, targetBranch());

    List<DataFile> deletedDataFiles = manifestListProducer.deletedDataFiles();
    List<DeleteFile> deletedDeleteFiles = manifestListProducer.deletedDeleteFiles();
    if (failAnyDelete) {
      ValidationException.check(
          deletedDataFiles.isEmpty(),
          "Operation would delete existing data: %s",
          Lists.transform(deletedDataFiles, DataFile::location));
      ValidationException.check(
          deletedDeleteFiles.isEmpty(),
          "Operation would delete existing delete files: %s",
          Lists.transform(deletedDeleteFiles, DeleteFile::location));
    }

    if (failMissingDeletePaths) {
      ValidationException.check(
          manifestListProducer.missingDeletedDataFiles().isEmpty(),
          "Missing required files to delete: %s",
          Lists.transform(manifestListProducer.missingDeletedDataFiles(), DataFile::location));
      ValidationException.check(
          manifestListProducer.missingDeletedDataLocations().isEmpty(),
          "Missing required files to delete: %s",
          manifestListProducer.missingDeletedDataLocations());
      ValidationException.check(
          manifestListProducer.missingDeletedDeleteFiles().isEmpty(),
          "Missing required files to delete: %s",
          Lists.transform(manifestListProducer.missingDeletedDeleteFiles(), DeleteFile::location));
    }

    // build the snapshot summary
    summaryBuilder.clear();
    summaryBuilder.merge(addedDataFilesSummary);
    summaryBuilder.merge(appendedManifestsSummary);
    manifestListProducer
        .addedDeleteFiles()
        .forEach(file -> summaryBuilder.addedFile(spec(file.specId()), file));
    deletedDataFiles.forEach(file -> summaryBuilder.deletedFile(spec(file.specId()), file));
    deletedDeleteFiles.forEach(file -> summaryBuilder.deletedFile(spec(file.specId()), file));
    summaryBuilder.incrementDuplicateDeletes(manifestListProducer.duplicateDeleteCount());
    updateManifestSummary(summaryBuilder, manifests, manifestListProducer.replacedManifestsCount());

    return manifests;
  }

  @Override
  protected void cleanUncommitted(Set<String> committed) {
    producer.cleanUncommitted(committed);
  }

  @Override
  public Object updateEvent() {
    long snapshotId = snapshotId();

    Snapshot justSaved = ops().current().snapshot(snapshotId);
    if (justSaved == null) {
      justSaved = ops().refresh().snapshot(snapshotId);
    }

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

  private void updateManifestSummary(
      SnapshotSummary.Builder builder, List<ManifestFile> manifests, int replacedManifestsCount) {
    int manifestsCreated = 0;
    int manifestsKept = 0;
    for (ManifestFile manifest : manifests) {
      if (snapshotId() == manifest.snapshotId()) {
        manifestsCreated += 1;
      } else if (null != manifest.snapshotId()) {
        manifestsKept += 1;
      }
    }

    builder.set(SnapshotSummary.CREATED_MANIFESTS_COUNT, String.valueOf(manifestsCreated));
    builder.set(SnapshotSummary.KEPT_MANIFESTS_COUNT, String.valueOf(manifestsKept));
    builder.set(SnapshotSummary.REPLACED_MANIFESTS_COUNT, String.valueOf(replacedManifestsCount));
  }
}
