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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Predicate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class MergingSnapshotProducer<ThisT> extends SnapshotProducer<ThisT> {
  private static final Logger LOG = LoggerFactory.getLogger(MergingSnapshotProducer.class);

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

  private SnapshotValidator validator() {
    return new SnapshotValidator(ops(), caseSensitive);
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
    validator().validateAddedDataFiles(base, startingSnapshotId, partitionSet, parent);
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
    validator().validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
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
    validator()
        .validateNoNewDeletesForDataFiles(
            base,
            startingSnapshotId,
            null,
            dataFiles,
            newDataFilesDataSequenceNumber != null,
            parent);
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
    validator()
        .validateNoNewDeletesForDataFiles(
            base, startingSnapshotId, dataFilter, dataFiles, false, parent);
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
    validator().validateNoNewDeleteFiles(base, startingSnapshotId, dataFilter, parent);
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
    validator().validateNoNewDeleteFiles(base, startingSnapshotId, partitionSet, parent);
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
    validator().validateDeletedDataFiles(base, startingSnapshotId, dataFilter, parent);
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
    validator().validateDeletedDataFiles(base, startingSnapshotId, partitionSet, parent);
  }

  protected void setNewDataFilesDataSequenceNumber(long sequenceNumber) {
    this.newDataFilesDataSequenceNumber = sequenceNumber;
  }

  protected void validateDataFilesExist(
      TableMetadata base,
      Long startingSnapshotId,
      CharSequenceSet requiredDataFiles,
      boolean skipDeletes,
      Expression conflictDetectionFilter,
      Snapshot parent) {
    validator()
        .validateDataFilesExist(
            base,
            startingSnapshotId,
            requiredDataFiles,
            skipDeletes,
            conflictDetectionFilter,
            parent);
  }

  // validates there are no concurrently added DVs for referenced data files
  protected void validateAddedDVs(
      TableMetadata base,
      Long startingSnapshotId,
      Expression conflictDetectionFilter,
      Snapshot parent) {
    validator()
        .validateAddedDVs(
            base,
            startingSnapshotId,
            conflictDetectionFilter,
            parent,
            dvsByReferencedFile.keySet(),
            workerPool());
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
        Iterables.filter(Iterables.concat(prepareNewDataManifests(), filtered), shouldKeep);
    Iterable<ManifestFile> unmergedDeleteManifests =
        Iterables.filter(Iterables.concat(prepareDeleteManifests(), filteredDeletes), shouldKeep);

    // update the snapshot summary
    summaryBuilder.clear();
    summaryBuilder.merge(addedDataFilesSummary);
    summaryBuilder.merge(addedDeleteFilesSummary);
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
            List<ManifestFile> newDataManifests =
                writeDataManifests(dataFiles, newDataFilesDataSequenceNumber, spec(specId));
            cachedNewDataManifests.addAll(newDataManifests);
          });
      this.hasNewDataFiles = false;
    }

    return cachedNewDataManifests;
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
      List<DeleteFile> mergedDVs = mergeDVs();
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

  private List<DeleteFile> mergeDVs() {
    for (Map.Entry<String, List<DeleteFile>> entry : dvsByReferencedFile.entrySet()) {
      if (entry.getValue().size() > 1) {
        LOG.warn(
            "Merging {} duplicate DVs for data file {} in table {}.",
            entry.getValue().size(),
            entry.getKey(),
            tableName);
      }
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
        dvsByReferencedFile,
        dvOutputLocation,
        fileIO,
        ops().current().specsById(),
        ThreadPools.getDeleteWorkerPool());
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
