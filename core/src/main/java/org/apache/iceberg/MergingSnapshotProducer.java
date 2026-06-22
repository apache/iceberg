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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Predicate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
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
  private final ManifestMergeManager<DataFile> mergeManager;
  private final ManifestFilterManager<DataFile> filterManager;
  private final ManifestMergeManager<DeleteFile> deleteMergeManager;
  private final ManifestFilterManager<DeleteFile> deleteFilterManager;
  private final AtomicInteger dvMergeAttempt = new AtomicInteger(0);

  // update data
  private final Map<Integer, DataFileSet> newDataFilesBySpec = Maps.newHashMap();
  private Long newDataFilesDataSequenceNumber;
  private final List<DeleteFile> v2Deletes = Lists.newArrayList();
  private final Map<String, List<DeleteFile>> dvsByReferencedFile = Maps.newLinkedHashMap();
  private final List<ManifestFile> appendManifests = Lists.newArrayList();
  private final List<ManifestFile> rewrittenAppendManifests = Lists.newArrayList();
  private final SnapshotSummary.Builder addedDataFilesSummary = SnapshotSummary.builder();
  private final SnapshotSummary.Builder addedDeleteFilesSummary = SnapshotSummary.builder();
  private final SnapshotSummary.Builder appendedManifestsSummary = SnapshotSummary.builder();
  private Expression deleteExpression = Expressions.alwaysFalse();

  // cache new data manifests after writing
  private final List<ManifestFile> cachedNewDataManifests = Lists.newLinkedList();
  private boolean hasNewDataFiles = false;

  // cache new manifests for delete files
  private final List<ManifestFile> cachedNewDeleteManifests = Lists.newLinkedList();
  private boolean hasNewDeleteFiles = false;

  // v4 colocated DV state: manifests rewritten with REPLACED/MODIFIED pairs
  private final List<ManifestFile> cachedDVRewrittenManifests = Lists.newLinkedList();
  // paths of original manifests that were replaced by DV-rewritten manifests
  private final Set<String> dvReplacedManifestPaths = Sets.newHashSet();
  // data file paths whose DVs were collapsed into data manifests (not written as delete manifests)
  private final Set<String> collapsedDVPaths = Sets.newHashSet();
  private boolean hasDVRewrittenManifests = false;
  // map of data file path → DV for data files being born with a DV in this commit
  private Map<String, DeleteFile> bornWithDVByPath = ImmutableMap.of();
  // Snapshot summary contributions for DVs that were collapsed into data manifests instead of
  // emitted as standalone position-delete manifests. Reuses the same DV/POSITION_DELETES counters
  // as v3 standalone DV writes so v4 commits produce identical summary metrics.
  private final SnapshotSummary.Builder collapsedDVAddedSummary = SnapshotSummary.builder();
  private final SnapshotSummary.Builder collapsedDVRemovedSummary = SnapshotSummary.builder();

  private boolean caseSensitive = true;

  MergingSnapshotProducer(String tableName, TableOperations ops) {
    super(ops);
    this.tableName = tableName;
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
    Set<Integer> specIds = newDataFilesBySpec.keySet();
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
    return newDataFilesBySpec.values().stream()
        .flatMap(Set::stream)
        .collect(ImmutableList.toImmutableList());
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
    return !newDataFilesBySpec.isEmpty();
  }

  protected boolean addsDeleteFiles() {
    return !v2Deletes.isEmpty()
        || dvsByReferencedFile.values().stream().anyMatch(dvs -> !dvs.isEmpty());
  }

  /** Add a data file to the new snapshot. */
  protected void add(DataFile file) {
    Preconditions.checkNotNull(file, "Invalid data file: null");
    PartitionSpec spec = spec(file.specId());
    Preconditions.checkArgument(
        spec != null,
        "Cannot find partition spec %s for data file: %s",
        file.specId(),
        file.location());

    DataFileSet dataFiles =
        newDataFilesBySpec.computeIfAbsent(spec.specId(), ignored -> DataFileSet.create());
    if (dataFiles.add(Delegates.suppressFirstRowId(file))) {
      addedDataFilesSummary.addedFile(spec, file);
      hasNewDataFiles = true;
    }
  }

  private PartitionSpec spec(int specId) {
    return ops().current().spec(specId);
  }

  /** Add a delete file to the new snapshot. */
  protected void add(DeleteFile file) {
    addInternal(Delegates.pendingDeleteFile(file, null));
  }

  /** Add a delete file to the new snapshot. */
  protected void add(DeleteFile file, long dataSequenceNumber) {
    addInternal(Delegates.pendingDeleteFile(file, dataSequenceNumber));
  }

  private void addInternal(DeleteFile file) {
    validateNewDeleteFile(file);
    PartitionSpec spec = spec(file.specId());
    Preconditions.checkArgument(
        spec != null,
        "Cannot find partition spec %s for delete file: %s",
        file.specId(),
        file.location());
    hasNewDeleteFiles = true;
    if (ContentFileUtil.isDV(file)) {
      List<DeleteFile> dvsForReferencedFile =
          dvsByReferencedFile.computeIfAbsent(
              file.referencedDataFile(), newFile -> Lists.newArrayList());
      dvsForReferencedFile.add(file);
    } else {
      v2Deletes.add(file);
    }
  }

  protected void validateNewDeleteFile(DeleteFile file) {
    Preconditions.checkNotNull(file, "Invalid delete file: null");
    validateDeleteFileForVersion(file, formatVersion());
  }

  private static void validateDeleteFileForVersion(DeleteFile file, int formatVersion) {
    switch (formatVersion) {
      case 1:
        throw new IllegalArgumentException("Deletes are supported in V2 and above");
      case 2:
        Preconditions.checkArgument(
            file.content() == FileContent.EQUALITY_DELETES || !ContentFileUtil.isDV(file),
            "Must not use DVs for position deletes in V2: %s",
            ContentFileUtil.dvDesc(file));
        break;
      case 3:
      case 4:
        Preconditions.checkArgument(
            file.content() == FileContent.EQUALITY_DELETES || ContentFileUtil.isDV(file),
            "Must use DVs for position deletes in V%s: %s",
            formatVersion,
            file.location());
        break;
      default:
        throw new IllegalArgumentException("Unsupported format version: " + formatVersion);
    }
  }

  private int formatVersion() {
    return ops().current().formatVersion();
  }

  /** Add all files in a manifest to the new snapshot. */
  protected void add(ManifestFile manifest) {
    Preconditions.checkArgument(
        manifest.content() == ManifestContent.DATA, "Cannot append delete manifest: %s", manifest);
    if (canInheritSnapshotId() && manifest.snapshotId() == null) {
      Preconditions.checkArgument(
          manifest.firstRowId() == null,
          "Cannot append manifest with assigned first_row_id: %s",
          manifest.firstRowId());
      appendedManifestsSummary.addedManifest(manifest);
      appendManifests.add(manifest);
    } else {
      // the manifest must be rewritten with this update's snapshot ID and null first_row_ids
      ManifestFile copiedManifest = copyManifest(manifest);
      rewrittenAppendManifests.add(copiedManifest);
    }
  }

  private ManifestFile copyManifest(ManifestFile manifest) {
    TableMetadata current = ops().current();
    InputFile toCopy = ops().io().newInputFile(manifest);
    EncryptedOutputFile newManifestFile = newManifestOutputFile();
    return ManifestFiles.copyAppendManifest(
        current.formatVersion(),
        manifest.partitionSpecId(),
        toCopy,
        current.specsById(),
        newManifestFile,
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
        Iterables.transform(deletes.referencedDeleteFiles(), ContentFile::location));
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
        Iterables.transform(deletes.referencedDeleteFiles(), ContentFile::location));
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
      return DeleteFileIndex.builderFor(ops().io(), ImmutableList.of())
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

  protected void setNewDataFilesDataSequenceNumber(long sequenceNumber) {
    this.newDataFilesDataSequenceNumber = sequenceNumber;
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
    // skip if there is no current table state or this operation doesn't add new DVs
    if (parent == null || dvsByReferencedFile.isEmpty()) {
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
        .executeWith(workerPool())
        .run(manifest -> validateAddedDVs(manifest, conflictDetectionFilter, newSnapshotIds));

    // v4 path: scan concurrent DATA manifests for colocated DVs. v4 stores DVs as MODIFIED
    // entries on the data manifest (REPLACED/MODIFIED pair); the v3 DELETE-content history
    // above does not see them.
    Pair<List<ManifestFile>, Set<Long>> dataHistory =
        validationHistory(
            base, startingSnapshotId, VALIDATE_ADDED_DVS_OPERATIONS, ManifestContent.DATA, parent);
    Iterable<ManifestFile> newDataManifestsWithDVs =
        Iterables.filter(
            filterManifestsByPartition(base, conflictDetectionFilter, dataHistory.first()),
            m -> m.replacedFilesCount() != null && m.replacedFilesCount() > 0);

    Tasks.foreach(newDataManifestsWithDVs)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPool())
        .run(this::validateConcurrentColocatedDVs);
  }

  private void validateConcurrentColocatedDVs(ManifestFile manifest) {
    try (CloseableIterable<DeleteFile> dvs =
        ManifestFiles.readColocatedDVs(manifest, ops().io(), ops().current().specsById())) {
      for (DeleteFile dv : dvs) {
        ValidationException.check(
            !dvsByReferencedFile.containsKey(dv.referencedDataFile()),
            "Found concurrently added DV for %s: %s",
            dv.referencedDataFile(),
            ContentFileUtil.dvDesc(dv));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void validateAddedDVs(
      ManifestFile manifest, Expression conflictDetectionFilter, Set<Long> newSnapshotIds) {
    try (CloseableIterable<ManifestEntry<DeleteFile>> entries =
        ManifestFiles.readDeleteManifest(manifest, ops().io(), ops().current().specsById())
            .filterRows(conflictDetectionFilter)
            .caseSensitive(caseSensitive)
            .liveEntries()) {

      for (ManifestEntry<DeleteFile> entry : entries) {
        DeleteFile file = entry.file();
        if (newSnapshotIds.contains(entry.snapshotId()) && ContentFileUtil.isDV(file)) {
          ValidationException.check(
              !dvsByReferencedFile.containsKey(file.referencedDataFile()),
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

  // guard buffered deletes against concurrent format upgrade
  private void validateDeleteFilesForVersion(int currentFormatVersion) {
    for (DeleteFile file : v2Deletes) {
      validateDeleteFileForVersion(file, currentFormatVersion);
    }
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
    validateDeleteFilesForVersion(base.formatVersion());
    // filter any existing manifests
    List<ManifestFile> filtered =
        filterManager.filterManifests(
            SnapshotUtil.schemaFor(base, targetBranch()),
            snapshot != null ? snapshot.dataManifests(ops().io()) : null);
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

    // retrieve the data files to be deleted from the DataFileFilterManager and pass it to the
    // DeleteFileFilterManager so that it can potentially remove orphaned DVs
    Set<DataFile> filesToBeDeleted = filterManager.filesToBeDeleted();
    deleteFilterManager.removeDanglingDeletesFor(filesToBeDeleted);

    // For v4 tables, collapse DVs into data leaf manifests (REPLACED/MODIFIED pairs) instead of
    // writing separate delete manifests.
    List<ManifestFile> dvRewrittenManifests = ImmutableList.of();
    List<ManifestFile> filteredWithoutDVReplaced = filtered;
    if (base.formatVersion() >= 4 && !dvsByReferencedFile.isEmpty()) {
      dvRewrittenManifests = prepareDVRewrittenManifests(base, filtered);
      if (!dvReplacedManifestPaths.isEmpty()) {
        filteredWithoutDVReplaced =
            filtered.stream()
                .filter(m -> !dvReplacedManifestPaths.contains(m.path()))
                .collect(ImmutableList.toImmutableList());
      }
    }

    List<ManifestFile> filteredDeletes =
        deleteFilterManager.filterManifests(
            SnapshotUtil.schemaFor(base, targetBranch()),
            snapshot != null ? snapshot.deleteManifests(ops().io()) : null);

    // only keep manifests that have live data files or that were written by this commit
    Predicate<ManifestFile> shouldKeep =
        manifest ->
            manifest.hasAddedFiles()
                || manifest.hasExistingFiles()
                || manifest.snapshotId() == snapshotId();
    Iterable<ManifestFile> unmergedManifests =
        Iterables.filter(
            Iterables.concat(
                prepareNewDataManifests(), dvRewrittenManifests, filteredWithoutDVReplaced),
            shouldKeep);
    Iterable<ManifestFile> unmergedDeleteManifests =
        Iterables.filter(Iterables.concat(prepareDeleteManifests(), filteredDeletes), shouldKeep);

    // update the snapshot summary
    summaryBuilder.clear();
    summaryBuilder.merge(addedDataFilesSummary);
    summaryBuilder.merge(addedDeleteFilesSummary);
    summaryBuilder.merge(collapsedDVAddedSummary);
    summaryBuilder.merge(collapsedDVRemovedSummary);
    summaryBuilder.merge(appendedManifestsSummary);
    summaryBuilder.merge(filterManager.buildSummary(filtered));
    summaryBuilder.merge(deleteFilterManager.buildSummary(filteredDeletes));

    List<ManifestFile> manifests = Lists.newArrayList();
    Iterables.addAll(manifests, mergeManager.mergeManifests(unmergedManifests));
    Iterables.addAll(manifests, deleteMergeManager.mergeManifests(unmergedDeleteManifests));

    // update created/kept/replaced manifest count
    // replaced manifests come from:
    // 1. filterManager - manifests rewritten to remove deleted files
    // 2. deleteFilterManager - delete manifests rewritten to remove deleted files
    // 3. mergeManager - data manifests merged via bin-packing
    // 4. deleteMergeManager - delete manifests merged via bin-packing
    // Note: rewrittenAppendManifests are NEW manifests (copies), not replaced ones
    int replacedManifestsCount =
        filterManager.replacedManifestsCount()
            + deleteFilterManager.replacedManifestsCount()
            + mergeManager.replacedManifestsCount()
            + deleteMergeManager.replacedManifestsCount();
    summaryBuilder.merge(buildManifestCountSummary(manifests, replacedManifestsCount));

    return manifests;
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

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
    mergeManager.cleanUncommitted(committed);
    filterManager.cleanUncommitted(committed);
    deleteMergeManager.cleanUncommitted(committed);
    deleteFilterManager.cleanUncommitted(committed);
    cleanUncommittedAppends(committed);
    deleteUncommitted(cachedDVRewrittenManifests, committed, true /* clear manifests */);
  }

  private void cleanUncommittedAppends(Set<ManifestFile> committed) {
    deleteUncommitted(cachedNewDataManifests, committed, true /* clear manifests */);
    deleteUncommitted(cachedNewDeleteManifests, committed, true /* clear manifests */);
    // rewritten manifests are always owned by the table
    deleteUncommitted(rewrittenAppendManifests, committed, false);

    // manifests that are not rewritten are only owned by the table if the commit succeeded
    if (!committed.isEmpty()) {
      // the commit succeeded if at least one manifest was committed
      // the table now owns appendManifests; clean up any that are not used
      deleteUncommitted(appendManifests, committed, false);
    }
  }

  private Iterable<ManifestFile> prepareNewDataManifests() {
    Iterable<ManifestFile> newManifests;
    if (!newDataFilesBySpec.isEmpty()) {
      List<ManifestFile> dataFileManifests = newDataFilesAsManifests();
      newManifests = Iterables.concat(dataFileManifests, appendManifests, rewrittenAppendManifests);
    } else {
      newManifests = Iterables.concat(appendManifests, rewrittenAppendManifests);
    }

    return Iterables.transform(
        newManifests,
        manifest -> GenericManifestFile.copyOf(manifest).withSnapshotId(snapshotId()).build());
  }

  private List<ManifestFile> newDataFilesAsManifests() {
    if (hasNewDataFiles && !cachedNewDataManifests.isEmpty()) {
      cachedNewDataManifests.forEach(file -> deleteFile(file.path()));
      cachedNewDataManifests.clear();
    }

    if (cachedNewDataManifests.isEmpty()) {
      newDataFilesBySpec.forEach(
          (specId, dataFiles) -> {
            List<ManifestFile> newDataManifests;
            if (!bornWithDVByPath.isEmpty()) {
              newDataManifests =
                  writeDataManifestsWithBornDVs(
                      dataFiles, newDataFilesDataSequenceNumber, spec(specId), bornWithDVByPath);
            } else {
              newDataManifests =
                  writeDataManifests(dataFiles, newDataFilesDataSequenceNumber, spec(specId));
            }

            cachedNewDataManifests.addAll(newDataManifests);
          });
      this.hasNewDataFiles = false;
    }

    return cachedNewDataManifests;
  }

  // Like writeDataManifests but uses addWithDV for files that are born with a DV.
  private List<ManifestFile> writeDataManifestsWithBornDVs(
      Iterable<DataFile> files,
      Long dataSeq,
      PartitionSpec spec,
      Map<String, DeleteFile> bornWithDVs) {
    ManifestWriter<DataFile> writer = newManifestWriter(spec);
    try {
      for (DataFile file : files) {
        String path = file.location().toString();
        DeleteFile dv = bornWithDVs.get(path);
        if (dv != null && writer instanceof ManifestWriter.V4Writer) {
          // Born-with-DV: emit a single ADDED entry with the DV embedded.
          DeletionVector dvStruct = toDeletionVector(dv);
          ((ManifestWriter.V4Writer) writer).addWithDV(file, dvStruct);
        } else if (dataSeq != null) {
          writer.add(file, dataSeq);
        } else {
          writer.add(file);
        }
      }
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close manifest writer for born-with-DV files", e);
      }
    }

    return ImmutableList.of(writer.toManifestFile());
  }

  private Iterable<ManifestFile> prepareDeleteManifests() {
    if (!addsDeleteFiles()) {
      return ImmutableList.of();
    }

    return newDeleteFilesAsManifests();
  }

  private List<ManifestFile> newDeleteFilesAsManifests() {
    if (hasNewDeleteFiles && !cachedNewDeleteManifests.isEmpty()) {
      for (ManifestFile cachedNewDeleteManifest : cachedNewDeleteManifests) {
        deleteFile(cachedNewDeleteManifest.path());
      }
      // this triggers a rewrite of all delete manifests even if there is only one new delete file
      // if there is a relevant use case in the future, the behavior can be optimized
      cachedNewDeleteManifests.clear();
      // On cache invalidation of delete files, clear the summary because any new DV could require a
      // merge,
      // and the summary cannot be generated until after merging is complete.
      addedDeleteFilesSummary.clear();
    }

    if (cachedNewDeleteManifests.isEmpty()) {
      // For v4: exclude DVs that were already collapsed into data manifests.
      Map<String, List<DeleteFile>> dvsToEmit;
      if (!collapsedDVPaths.isEmpty()) {
        dvsToEmit = Maps.newLinkedHashMap();
        for (Map.Entry<String, List<DeleteFile>> entry : dvsByReferencedFile.entrySet()) {
          if (!collapsedDVPaths.contains(entry.getKey())) {
            dvsToEmit.put(entry.getKey(), entry.getValue());
          }
        }
      } else {
        dvsToEmit = dvsByReferencedFile;
      }

      List<DeleteFile> mergedDVs = mergeDVs(dvsToEmit);
      Map<Integer, List<DeleteFile>> newDeleteFilesBySpec =
          Streams.stream(Iterables.concat(mergedDVs, DeleteFileSet.of(v2Deletes)))
              .collect(Collectors.groupingBy(ContentFile::specId));

      newDeleteFilesBySpec.forEach(
          (specId, deleteFiles) -> {
            PartitionSpec spec = ops().current().spec(specId);
            deleteFiles.forEach(file -> addedDeleteFilesSummary.addedFile(spec, file));
            List<ManifestFile> newDeleteManifests = writeDeleteManifests(deleteFiles, spec);
            cachedNewDeleteManifests.addAll(newDeleteManifests);
          });

      this.hasNewDeleteFiles = false;
    }

    return cachedNewDeleteManifests;
  }

  private List<DeleteFile> mergeDVs(Map<String, List<DeleteFile>> dvsMap) {
    for (Map.Entry<String, List<DeleteFile>> entry : dvsMap.entrySet()) {
      if (entry.getValue().size() > 1) {
        LOG.warn(
            "Merging {} duplicate DVs for data file {} in table {}.",
            entry.getValue().size(),
            entry.getKey(),
            tableName);
      }
    }

    if (dvsMap.isEmpty()) {
      return ImmutableList.of();
    }

    FileIO fileIO = EncryptingFileIO.combine(ops().io(), ops().encryption());

    String dvOutputLocation =
        ops()
            .locationProvider()
            .newDataLocation(
                FileFormat.PUFFIN.addExtension(
                    String.format(
                        "merged-dvs-%s-%s", snapshotId(), dvMergeAttempt.incrementAndGet())));

    return DVUtil.mergeAndWriteDVsIfRequired(
        dvsMap,
        dvOutputLocation,
        fileIO,
        ops().current().specsById(),
        ThreadPools.getDeleteWorkerPool());
  }

  /**
   * Prepares v4 DV-rewritten leaf manifests. For each data file being updated with a DV, rewrites
   * the leaf manifest that contains it: the existing entry becomes REPLACED, and a new MODIFIED
   * entry carries the DV. Returns the new leaf manifests (to be included in the snapshot).
   * Populates {@link #dvReplacedManifestPaths} with the paths of the original manifests that were
   * replaced, and {@link #collapsedDVPaths} with data file paths whose DVs were collapsed.
   */
  private List<ManifestFile> prepareDVRewrittenManifests(
      TableMetadata base, List<ManifestFile> filteredDataManifests) {
    if (hasDVRewrittenManifests && !cachedDVRewrittenManifests.isEmpty()) {
      cachedDVRewrittenManifests.forEach(m -> deleteFile(m.path()));
      cachedDVRewrittenManifests.clear();
      dvReplacedManifestPaths.clear();
      collapsedDVPaths.clear();
      collapsedDVAddedSummary.clear();
      collapsedDVRemovedSummary.clear();
    }

    if (!cachedDVRewrittenManifests.isEmpty()) {
      return cachedDVRewrittenManifests;
    }

    // Collect the paths of all newly-added data files in this commit (born-with-DV case).
    Set<String> newDataFilePaths = Sets.newHashSet();
    newDataFilesBySpec
        .values()
        .forEach(fileSet -> fileSet.forEach(f -> newDataFilePaths.add(f.location().toString())));

    // Merge DVs per referenced data file to get one DV per file.
    List<DeleteFile> mergedDVList = mergeDVs(dvsByReferencedFile);

    // Build a map: data file path → merged DV DeleteFile
    Map<String, DeleteFile> mergedDVByPath = Maps.newHashMap();
    for (DeleteFile dv : mergedDVList) {
      mergedDVByPath.put(dv.referencedDataFile().toString(), dv);
    }

    // For data files being born with a DV in this commit: store the DV so
    // newDataFilesAsManifests() can embed it. Mark them as collapsed to skip delete manifests.
    Map<String, DeleteFile> bornWithDV = Maps.newHashMap();
    for (Map.Entry<String, DeleteFile> entry : mergedDVByPath.entrySet()) {
      if (newDataFilePaths.contains(entry.getKey())) {
        bornWithDV.put(entry.getKey(), entry.getValue());
        collapsedDVPaths.add(entry.getKey());
        // Credit the colocated DV to the snapshot summary so v4 born-with-DV commits report the
        // same added-dvs / added-position-deletes / added-delete-files / added-files-size metrics
        // as v3 standalone DV writes.
        DeleteFile dv = entry.getValue();
        collapsedDVAddedSummary.addedFile(spec(dv.specId()), dv);
      }
    }

    if (!bornWithDV.isEmpty()) {
      // Update newDataFilesBySpec: replace data files that have a DV with DV-carrying versions.
      // We handle this by storing bornWithDV so newDataFilesAsManifests can access it.
      this.bornWithDVByPath = bornWithDV;
      this.hasNewDataFiles = true; // force rewrite
    }

    // For existing data files: find the manifests that contain them and rewrite with REPLACED/
    // MODIFIED pairs.
    Map<String, DeleteFile> dvsForExisting = Maps.newLinkedHashMap();
    for (Map.Entry<String, DeleteFile> entry : mergedDVByPath.entrySet()) {
      if (!newDataFilePaths.contains(entry.getKey())) {
        dvsForExisting.put(entry.getKey(), entry.getValue());
      }
    }

    if (!dvsForExisting.isEmpty()) {
      rewriteLeafManifestsWithDVs(base, filteredDataManifests, dvsForExisting);
    }

    this.hasDVRewrittenManifests = true;
    return cachedDVRewrittenManifests;
  }

  // Scans filteredDataManifests to find entries for the given data file paths, and rewrites those
  // manifests with REPLACED/MODIFIED pairs. New manifests go into cachedDVRewrittenManifests;
  // original manifest paths go into dvReplacedManifestPaths; affected DV paths go into
  // collapsedDVPaths.
  private void rewriteLeafManifestsWithDVs(
      TableMetadata base,
      List<ManifestFile> filteredDataManifests,
      Map<String, DeleteFile> dvsForExisting) {
    Map<Integer, PartitionSpec> specsById = base.specsById();

    // Track which referenced data file paths we still need to find.
    Set<String> remaining = Sets.newHashSet(dvsForExisting.keySet());

    for (ManifestFile manifest : filteredDataManifests) {
      if (remaining.isEmpty()) {
        break;
      }

      // Quick check: can this manifest contain any of the remaining paths?
      if (!manifestMightContain(manifest, remaining, specsById)) {
        continue;
      }

      // Read all entries from the manifest.
      List<ManifestEntry<DataFile>> entries = Lists.newArrayList();
      boolean manifestAffected = false;
      try (CloseableIterable<ManifestEntry<DataFile>> iter =
          ManifestFiles.read(manifest, ops().io(), specsById).entries()) {
        for (ManifestEntry<DataFile> entry : iter) {
          entries.add(entry.copy());
          if (entry.isLive() && remaining.contains(entry.file().location().toString())) {
            manifestAffected = true;
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to read manifest for DV collapse: " + manifest.path(), e);
      }

      if (!manifestAffected) {
        continue;
      }

      // Load any prior colocated DVs from this manifest so we can credit them as "removed" on the
      // summary when they are superseded by the new DV in this commit. ManifestFiles.read above
      // surfaces DV-bearing rows as plain DataFile entries, dropping the DV reference; the
      // ContentEntryReader path exposes the colocated DV as a DeleteFile.
      Map<String, DeleteFile> priorDVsByPath = Maps.newHashMap();
      try (CloseableIterable<DeleteFile> priorDVs =
          ManifestFiles.readColocatedDVs(
              manifest, EncryptingFileIO.combine(ops().io(), ops().encryption()), specsById)) {
        for (DeleteFile priorDV : priorDVs) {
          priorDVsByPath.put(priorDV.referencedDataFile().toString(), priorDV);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to read colocated DVs for DV collapse: " + manifest.path(), e);
      }

      // Rewrite this manifest: for each affected data file, emit REPLACED + MODIFIED pair.
      PartitionSpec spec = specsById.get(manifest.partitionSpecId());
      ManifestWriter<DataFile> writer = newManifestWriter(spec);
      Set<String> affectedInThisManifest = Sets.newHashSet();
      try {
        for (ManifestEntry<DataFile> entry : entries) {
          String path = entry.file().location().toString();
          // Drop non-live entries (DELETED rows from the snapshot that deleted the file, and
          // REPLACED rows from a prior commit that updated a DV — both project to non-live via
          // ContentEntryReader.toManifestStatus). They have already served their purpose in the
          // snapshot that produced them; subsequent leaf rewrites omit them per spec semantics
          // for DELETED (and by analogy for REPLACED, which is not-live for the same reason).
          if (!entry.isLive()) {
            continue;
          }

          DeleteFile dv = dvsForExisting.get(path);
          if (dv != null) {
            // Emit REPLACED (prior state, with prior DV if any) then MODIFIED (new state, with new
            // DV). Preserving the prior DV on the REPLACED row lets SnapshotChanges identify the
            // superseded DV as a "removed delete file" without walking the parent manifest.
            DeleteFile priorDV = priorDVsByPath.get(path);
            DeletionVector priorDvStruct = priorDV != null ? toDeletionVector(priorDV) : null;
            writer.replacedEntry(entry, priorDvStruct);
            DeletionVector dvStruct = toDeletionVector(dv);
            writer.modifiedEntry(entry, dvStruct);
            affectedInThisManifest.add(path);
            // Credit the new DV as added on the snapshot summary so v4 commits report the same
            // added-dvs / added-position-deletes / added-delete-files / added-files-size metrics
            // as v3 standalone DV writes.
            collapsedDVAddedSummary.addedFile(spec(dv.specId()), dv);
            // If the prior live entry already carried a colocated DV, credit it as removed: the
            // REPLACED row drops it and the MODIFIED row supersedes it with the new DV.
            if (priorDV != null) {
              collapsedDVRemovedSummary.deletedFile(spec(priorDV.specId()), priorDV);
            }
          } else if (entry.status() == ManifestEntry.Status.EXISTING) {
            writer.existing(entry);
          } else if (entry.status() == ManifestEntry.Status.ADDED) {
            writer.add(entry);
          }
        }
      } catch (Exception e) {
        try {
          writer.close();
        } catch (IOException closeEx) {
          e.addSuppressed(closeEx);
        }

        throw new RuntimeException("Failed to rewrite manifest with DVs: " + manifest.path(), e);
      }

      try {
        writer.close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close manifest writer for DV collapse", e);
      }

      ManifestFile rewritten =
          GenericManifestFile.copyOf(writer.toManifestFile()).withSnapshotId(snapshotId()).build();
      cachedDVRewrittenManifests.add(rewritten);
      dvReplacedManifestPaths.add(manifest.path());
      collapsedDVPaths.addAll(affectedInThisManifest);
      remaining.removeAll(affectedInThisManifest);
    }

    if (!remaining.isEmpty()) {
      LOG.warn(
          "Could not find leaf manifest entries for DV-referenced data files: {} in table {}",
          remaining,
          tableName);
    }
  }

  // Check whether a manifest might contain any of the given data file paths based on partition
  // summaries. Currently returns true (conservative) since path-based pruning is expensive.
  @SuppressWarnings("unused")
  private static boolean manifestMightContain(
      ManifestFile manifest, Set<String> paths, Map<Integer, PartitionSpec> specsById) {
    // Conservative: always scan. Future optimization: index by partition or file stats.
    return manifest.hasAddedFiles() || manifest.hasExistingFiles();
  }

  // Build a DeletionVectorStruct from a DV DeleteFile's Puffin blob reference.
  private static DeletionVector toDeletionVector(DeleteFile dv) {
    Preconditions.checkArgument(
        ContentFileUtil.isDV(dv), "Cannot build DeletionVector from non-DV delete file: %s", dv);
    Preconditions.checkArgument(
        dv.location() != null, "Invalid DV delete file: null location for %s", dv);
    Preconditions.checkArgument(
        dv.contentOffset() != null,
        "Invalid DV delete file: null content offset for %s",
        dv.location());
    Preconditions.checkArgument(
        dv.contentSizeInBytes() != null,
        "Invalid DV delete file: null content size for %s",
        dv.location());
    return DeletionVectorStruct.builder()
        .location(dv.location().toString())
        .offset(dv.contentOffset())
        .sizeInBytes(dv.contentSizeInBytes())
        .cardinality(dv.recordCount())
        .build();
  }

  private class DataFileFilterManager extends ManifestFilterManager<DataFile> {
    private DataFileFilterManager() {
      super(ops().current().specsById(), MergingSnapshotProducer.this::workerPool);
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

    @Override
    protected Set<DataFile> newFileSet() {
      return DataFileSet.create();
    }

    @Override
    protected void removeDanglingDeletesFor(Set<DataFile> dataFiles) {
      throw new UnsupportedOperationException("Cannot remove dangling deletes");
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
      return ops().current().spec(specId);
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
      return newManifestReader(manifest, true);
    }

    @Override
    protected ManifestReader<DataFile> newManifestReader(
        ManifestFile manifest, boolean isCommitted) {
      return ManifestFiles.read(manifest, ops().io(), ops().current().specsById(), isCommitted);
    }
  }

  private class DeleteFileFilterManager extends ManifestFilterManager<DeleteFile> {
    private DeleteFileFilterManager() {
      super(ops().current().specsById(), MergingSnapshotProducer.this::workerPool);
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

    @Override
    protected Set<DeleteFile> newFileSet() {
      return DeleteFileSet.create();
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
      return ops().current().spec(specId);
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
