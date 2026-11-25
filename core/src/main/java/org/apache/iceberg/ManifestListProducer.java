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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hadoop.util.Sets;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Predicate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ManifestListProducer implements ProducerActions {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestListProducer.class);

  private final String tableName;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final LocationProvider locationProvider;
  private final Map<Integer, PartitionSpec> specsById;
  private final long snapshotId;
  private final int formatVersion;
  private final boolean canInheritSnapshotId;
  private final ManifestWriteManager writeManager;
  private final ManifestMergeManager<DataFile> mergeManager;
  private final ManifestFilterManager<DataFile> filterManager;
  private final ManifestMergeManager<DeleteFile> deleteMergeManager;
  private final ManifestFilterManager<DeleteFile> deleteFilterManager;
  private final AtomicInteger dvMergeAttempt = new AtomicInteger(0);
  private Consumer<String> deleteFunc;
  private ExecutorService workerPool = ThreadPools.getWorkerPool();

  // update data
  private final Map<Integer, DataFileSet> newDataFilesBySpec = Maps.newHashMap();
  private final List<DeleteFile> v2Deletes = Lists.newArrayList();
  private final Map<String, List<DeleteFile>> dvsByReferencedFile = Maps.newLinkedHashMap();
  private final List<ManifestFile> appendManifests = Lists.newArrayList();
  private final List<ManifestFile> rewrittenAppendManifests = Lists.newArrayList();
  private Expression deleteExpression = Expressions.alwaysFalse();

  // cache new data manifests after writing
  private final List<ManifestFile> cachedNewDataManifests = Lists.newLinkedList();
  private boolean hasNewDataFiles = false;

  // cache new manifests for delete files
  private final List<ManifestFile> cachedNewDeleteManifests = Lists.newLinkedList();
  private boolean hasNewDeleteFiles = false;

  // operation results, set by apply
  private List<DeleteFile> addedDeleteFiles = Lists.newArrayList();
  private int duplicateDeleteCount = 0;
  private List<DataFile> deletedDataFiles = null;
  private List<DataFile> missingDataFiles = null;
  private List<String> missingDataLocations = null;
  private List<DeleteFile> missingDeleteFiles = null;
  private List<DeleteFile> deletedDeleteFiles = null;

  ManifestListProducer(String tableName, TableOperations ops, long snapshotId, int formatVersion) {
    this.tableName = tableName;
    this.io = ops.io();
    this.deleteFunc = io::deleteFile;
    this.encryption = ops.encryption();
    this.locationProvider = ops.locationProvider();
    this.specsById = ops.current().specsById();
    this.snapshotId = snapshotId;
    this.formatVersion = formatVersion;
    boolean snapshotIdInheritanceEnabled =
        ops.current()
            .propertyAsBoolean(
                SNAPSHOT_ID_INHERITANCE_ENABLED, SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);
    this.canInheritSnapshotId = ops.current().formatVersion() > 1 || snapshotIdInheritanceEnabled;
    long targetManifestSizeBytes =
        ops.current()
            .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    int minCountToMerge =
        ops.current().propertyAsInt(MANIFEST_MIN_MERGE_COUNT, MANIFEST_MIN_MERGE_COUNT_DEFAULT);
    boolean mergeEnabled =
        ops.current()
            .propertyAsBoolean(
                TableProperties.MANIFEST_MERGE_ENABLED,
                TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT);
    this.writeManager = new ManifestWriteManager(ops, snapshotId);
    this.mergeManager =
        new DataFileMergeManager(targetManifestSizeBytes, minCountToMerge, mergeEnabled);
    this.filterManager = new DataFileFilterManager();
    this.deleteMergeManager =
        new DeleteFileMergeManager(targetManifestSizeBytes, minCountToMerge, mergeEnabled);
    this.deleteFilterManager = new DeleteFileFilterManager();
  }

  int replacedManifestsCount() {
    // replaced manifests come from:
    // 1. filterManager - manifests rewritten to remove deleted files
    // 2. deleteFilterManager - delete manifests rewritten to remove deleted files
    // 3. mergeManager - data manifests merged via bin-packing
    // 4. deleteMergeManager - delete manifests merged via bin-packing
    // Note: rewrittenAppendManifests are NEW manifests (copies), not replaced ones
    return filterManager.replacedManifestsCount()
        + deleteFilterManager.replacedManifestsCount()
        + mergeManager.replacedManifestsCount()
        + deleteMergeManager.replacedManifestsCount();
  }

  List<DeleteFile> addedDeleteFiles() {
    return addedDeleteFiles;
  }

  int duplicateDeleteCount() {
    return duplicateDeleteCount;
  }

  List<DataFile> deletedDataFiles() {
    return deletedDataFiles;
  }

  List<DataFile> missingDeletedDataFiles() {
    return missingDataFiles;
  }

  List<String> missingDeletedDataLocations() {
    return missingDataLocations;
  }

  List<DeleteFile> missingDeletedDeleteFiles() {
    return missingDeleteFiles;
  }

  List<DeleteFile> deletedDeleteFiles() {
    return deletedDeleteFiles;
  }

  void deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
  }

  void scanWith(ExecutorService pool) {
    // match the behavior of SnapshotProducer that does not validate the thread pool
    if (pool != null) {
      this.workerPool = pool;
    }
  }

  void writeWith(ExecutorService pool, int parallelism) {
    writeManager.writeWith(pool, parallelism);
  }

  @Override
  public void caseSensitive(boolean isCaseSensitive) {
    filterManager.caseSensitive(isCaseSensitive);
    deleteFilterManager.caseSensitive(isCaseSensitive);
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

  /**
   * Add a filter to match files to delete. A file will be deleted if all of the rows it contains
   * match this or any other filter passed to this method.
   *
   * @param expr an expression to match rows.
   */
  @Override
  public void deleteByRowFilter(Expression expr) {
    this.deleteExpression = expr;
    filterManager.deleteByRowFilter(expr);
    // if a delete file matches the row filter, then it can be deleted because the rows will also be
    // deleted
    deleteFilterManager.deleteByRowFilter(expr);
  }

  /** Add a partition tuple to drop from the table during the delete phase. */
  @Override
  public void deletePartition(int specId, StructLike partition) {
    // dropping the data in a partition also drops all deletes in the partition
    filterManager.dropPartition(specId, partition);
    deleteFilterManager.dropPartition(specId, partition);
  }

  /** Add a specific data file to be deleted in the new snapshot. */
  @Override
  public void deleteDataFile(DataFile file) {
    filterManager.delete(file);
  }

  /** Add a specific delete file to be deleted in the new snapshot. */
  @Override
  public void deleteDeleteFile(DeleteFile file) {
    deleteFilterManager.delete(file);
  }

  /** Add a specific data path to be deleted in the new snapshot. */
  @Override
  public void deleteDataFile(CharSequence path) {
    // this is an old call that never worked for delete files and can only be used to remove data
    // files.
    filterManager.delete(path);
  }

  /** Add a data file to the new snapshot. */
  @Override
  public void appendDataFile(DataFile file) {
    appendInternal(Delegates.pendingDataFile(file, null));
  }

  /** Add a data file to the new snapshot at a specific sequence number. */
  @Override
  public void appendDataFileAtSeq(DataFile file, long dataSequenceNumber) {
    appendInternal(Delegates.pendingDataFile(file, dataSequenceNumber));
  }

  public void appendInternal(DataFile file) {
    Preconditions.checkNotNull(file, "Invalid data file: null");
    PartitionSpec spec = spec(file.specId());
    Preconditions.checkArgument(
        spec != null,
        "Cannot find partition spec %s for data file: %s",
        file.specId(),
        file.location());

    DataFileSet dataFiles =
        newDataFilesBySpec.computeIfAbsent(spec.specId(), ignored -> DataFileSet.create());
    if (dataFiles.add(file)) {
      hasNewDataFiles = true;
    }
  }

  private PartitionSpec spec(int specId) {
    return specsById.get(specId);
  }

  /** Add a delete file to the new snapshot. */
  @Override
  public void addDeleteFile(DeleteFile file) {
    addInternal(Delegates.pendingDeleteFile(file, null));
  }

  /** Add a delete file to the new snapshot. */
  @Override
  public void addDeleteFileAtSeq(DeleteFile file, long dataSequenceNumber) {
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
    validateDeleteFileForVersion(file, formatVersion);
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

  /** Add all files in a manifest to the new snapshot. */
  @Override
  public void appendManifest(ManifestFile manifest) {
    Preconditions.checkArgument(
        manifest.content() == ManifestContent.DATA, "Cannot append delete manifest: %s", manifest);
    if (canInheritSnapshotId && manifest.snapshotId() == null) {
      Preconditions.checkArgument(
          manifest.firstRowId() == null,
          "Cannot append manifest with assigned first_row_id: %s",
          manifest.firstRowId());
      appendManifests.add(manifest);
    } else {
      // the manifest must be rewritten with this update's snapshot ID and null first_row_ids
      ManifestFile copiedManifest = copyManifest(manifest);
      rewrittenAppendManifests.add(copiedManifest);
    }
  }

  private ManifestFile copyManifest(ManifestFile manifest) {
    InputFile toCopy = io.newInputFile(manifest);
    EncryptedOutputFile newManifestFile = writeManager.newManifestOutputFile();
    return ManifestFiles.copyAppendManifest(
        formatVersion,
        manifest.partitionSpecId(),
        toCopy,
        specsById,
        newManifestFile,
        snapshotId,
        SnapshotSummary.builder());
  }

  // guard buffered deletes against concurrent format upgrade
  private void validateDeleteFilesForVersion(int currentFormatVersion) {
    for (DeleteFile file : v2Deletes) {
      validateDeleteFileForVersion(file, currentFormatVersion);
    }
  }

  public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot, String targetBranch) {
    validateDeleteFilesForVersion(base.formatVersion());
    // filter any existing manifests
    List<ManifestFile> filtered =
        filterManager.filterManifests(
            SnapshotUtil.schemaFor(base, targetBranch),
            snapshot != null ? snapshot.dataManifests(io) : null);

    Set<DataFile> deletedFiles = filterManager.deletedFiles(filtered);
    Set<String> deletedLocations =
        Sets.newHashSet(Iterables.transform(deletedFiles, DataFile::location));
    this.deletedDataFiles = ImmutableList.copyOf(deletedFiles);
    this.missingDataFiles =
        ImmutableList.copyOf(Sets.difference(filterManager.filesToBeDeleted(), deletedFiles));
    this.missingDataLocations =
        ImmutableList.copyOf(
            Sets.difference(filterManager.locationsToBeDeleted(), deletedLocations));
    this.duplicateDeleteCount = filterManager.duplicateDeleteCount(filtered);

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
    deleteFilterManager.removeDanglingDeletesFor(deletedFiles);

    List<ManifestFile> filteredDeletes =
        deleteFilterManager.filterManifests(
            SnapshotUtil.schemaFor(base, targetBranch),
            snapshot != null ? snapshot.deleteManifests(io) : null);

    Set<DeleteFile> deletedDeletes = deleteFilterManager.deletedFiles(filteredDeletes);
    this.deletedDeleteFiles = ImmutableList.copyOf(deletedDeletes);
    this.missingDeleteFiles =
        ImmutableList.copyOf(
            Sets.difference(deleteFilterManager.filesToBeDeleted(), deletedDeletes));
    this.duplicateDeleteCount += deleteFilterManager.duplicateDeleteCount(filtered);

    // only keep manifests that have live data files or that were written by this commit
    Predicate<ManifestFile> shouldKeep =
        manifest ->
            manifest.hasAddedFiles()
                || manifest.hasExistingFiles()
                || manifest.snapshotId() == snapshotId;
    Iterable<ManifestFile> unmergedManifests =
        Iterables.filter(Iterables.concat(prepareNewDataManifests(), filtered), shouldKeep);
    Iterable<ManifestFile> unmergedDeleteManifests =
        Iterables.filter(Iterables.concat(prepareDeleteManifests(), filteredDeletes), shouldKeep);

    List<ManifestFile> manifests = Lists.newArrayList();
    Iterables.addAll(manifests, mergeManager.mergeManifests(unmergedManifests));
    Iterables.addAll(manifests, deleteMergeManager.mergeManifests(unmergedDeleteManifests));

    return manifests;
  }

  @Override
  public void cleanUncommitted(Set<String> committed) {
    mergeManager.cleanUncommitted(committed);
    filterManager.cleanUncommitted(committed);
    deleteMergeManager.cleanUncommitted(committed);
    deleteFilterManager.cleanUncommitted(committed);

    if (deleteUncommitted(cachedNewDataManifests, committed)) {
      cachedNewDataManifests.clear();
    }

    if (deleteUncommitted(cachedNewDeleteManifests, committed)) {
      cachedNewDeleteManifests.clear();
    }

    // rewritten manifests are always owned by the table
    deleteUncommitted(rewrittenAppendManifests, committed);

    // manifests that are not rewritten are only owned by the table if the commit succeeded
    if (!committed.isEmpty()) {
      // the commit succeeded if at least one manifest was committed
      // the table now owns appendManifests; clean up any that are not used
      deleteUncommitted(appendManifests, committed);
    }
  }

  /**
   * Deletes manifests that are not in the {@code committed} location set.
   *
   * @param manifests a collection of new {@link ManifestFile manifests}
   * @param committed a set of committed manifest locations
   * @return true if any manifest location was deleted
   */
  private boolean deleteUncommitted(Collection<ManifestFile> manifests, Set<String> committed) {
    boolean anyDeleted = false;
    for (ManifestFile manifest : manifests) {
      if (!committed.contains(manifest.path())) {
        deleteFunc.accept(manifest.path());
        anyDeleted = true;
      }
    }

    return anyDeleted;
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
        manifest -> GenericManifestFile.copyOf(manifest).withSnapshotId(snapshotId).build());
  }

  private List<ManifestFile> newDataFilesAsManifests() {
    if (hasNewDataFiles && !cachedNewDataManifests.isEmpty()) {
      cachedNewDataManifests.forEach(file -> deleteFunc.accept(file.path()));
      cachedNewDataManifests.clear();
    }

    if (cachedNewDataManifests.isEmpty()) {
      newDataFilesBySpec.forEach(
          (specId, dataFiles) -> {
            List<ManifestFile> newDataManifests =
                writeManager.writeDataManifests(dataFiles, spec(specId));
            cachedNewDataManifests.addAll(newDataManifests);
          });
      this.hasNewDataFiles = false;
    }

    return cachedNewDataManifests;
  }

  private Iterable<ManifestFile> prepareDeleteManifests() {
    if (v2Deletes.isEmpty() && dvsByReferencedFile.values().stream().allMatch(List::isEmpty)) {
      return ImmutableList.of();
    }

    return newDeleteFilesAsManifests();
  }

  private List<ManifestFile> newDeleteFilesAsManifests() {
    if (hasNewDeleteFiles && !cachedNewDeleteManifests.isEmpty()) {
      for (ManifestFile cachedNewDeleteManifest : cachedNewDeleteManifests) {
        deleteFunc.accept(cachedNewDeleteManifest.path());
      }
      // this triggers a rewrite of all delete manifests even if there is only one new delete file
      // if there is a relevant use case in the future, the behavior can be optimized
      cachedNewDeleteManifests.clear();
      // keep track of new delete files (after merging)
      this.addedDeleteFiles = Lists.newArrayList();
    }

    if (cachedNewDeleteManifests.isEmpty()) {
      List<DeleteFile> mergedDVs = mergeDVs();
      Map<Integer, List<DeleteFile>> newDeleteFilesBySpec =
          Streams.stream(Iterables.concat(mergedDVs, DeleteFileSet.of(v2Deletes)))
              .collect(Collectors.groupingBy(ContentFile::specId));

      newDeleteFilesBySpec.forEach(
          (specId, deleteFiles) -> {
            addedDeleteFiles.addAll(deleteFiles);
            List<ManifestFile> newDeleteManifests =
                writeManager.writeDeleteManifests(deleteFiles, spec(specId));
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

    FileIO fileIO = EncryptingFileIO.combine(io, encryption);

    String dvOutputLocation =
        locationProvider.newDataLocation(
            FileFormat.PUFFIN.addExtension(
                String.format("merged-dvs-%s-%s", snapshotId, dvMergeAttempt.incrementAndGet())));

    return DVUtil.mergeAndWriteDVsIfRequired(
        dvsByReferencedFile,
        dvOutputLocation,
        fileIO,
        specsById,
        ThreadPools.getDeleteWorkerPool());
  }

  private ManifestReader<DataFile> newManifestReader(ManifestFile manifest) {
    return ManifestFiles.read(manifest, io, specsById);
  }

  private ManifestReader<DeleteFile> newDeleteManifestReader(ManifestFile manifest) {
    return ManifestFiles.readDeleteManifest(manifest, io, specsById);
  }

  private class DataFileFilterManager extends ManifestFilterManager<DataFile> {
    private DataFileFilterManager() {
      super(specsById, () -> workerPool);
    }

    @Override
    protected void deleteFile(String location) {
      deleteFunc.accept(location);
    }

    @Override
    protected ManifestWriter<DataFile> newManifestWriter(PartitionSpec manifestSpec) {
      return writeManager.newManifestWriter(manifestSpec);
    }

    @Override
    protected ManifestReader<DataFile> newManifestReader(ManifestFile manifest) {
      return ManifestListProducer.this.newManifestReader(manifest);
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
      super(targetSizeBytes, minCountToMerge, mergeEnabled, () -> workerPool);
    }

    @Override
    protected long snapshotId() {
      return ManifestListProducer.this.snapshotId;
    }

    @Override
    protected PartitionSpec spec(int specId) {
      return ManifestListProducer.this.spec(specId);
    }

    @Override
    protected void deleteFile(String location) {
      deleteFunc.accept(location);
    }

    @Override
    protected ManifestWriter<DataFile> newManifestWriter(PartitionSpec manifestSpec) {
      return writeManager.newManifestWriter(manifestSpec);
    }

    @Override
    protected ManifestReader<DataFile> newManifestReader(ManifestFile manifest) {
      return newManifestReader(manifest, true);
    }

    @Override
    protected ManifestReader<DataFile> newManifestReader(
        ManifestFile manifest, boolean isCommitted) {
      return ManifestFiles.read(manifest, io, specsById, isCommitted);
    }
  }

  private class DeleteFileFilterManager extends ManifestFilterManager<DeleteFile> {
    private DeleteFileFilterManager() {
      super(specsById, () -> workerPool);
    }

    @Override
    protected void deleteFile(String location) {
      deleteFunc.accept(location);
    }

    @Override
    protected ManifestWriter<DeleteFile> newManifestWriter(PartitionSpec manifestSpec) {
      return writeManager.newDeleteManifestWriter(manifestSpec);
    }

    @Override
    protected ManifestReader<DeleteFile> newManifestReader(ManifestFile manifest) {
      return ManifestListProducer.this.newDeleteManifestReader(manifest);
    }

    @Override
    protected Set<DeleteFile> newFileSet() {
      return DeleteFileSet.create();
    }
  }

  private class DeleteFileMergeManager extends ManifestMergeManager<DeleteFile> {
    DeleteFileMergeManager(long targetSizeBytes, int minCountToMerge, boolean mergeEnabled) {
      super(targetSizeBytes, minCountToMerge, mergeEnabled, () -> workerPool);
    }

    @Override
    protected long snapshotId() {
      return snapshotId;
    }

    @Override
    protected PartitionSpec spec(int specId) {
      return ManifestListProducer.this.spec(specId);
    }

    @Override
    protected void deleteFile(String location) {
      deleteFunc.accept(location);
    }

    @Override
    protected ManifestWriter<DeleteFile> newManifestWriter(PartitionSpec manifestSpec) {
      return writeManager.newDeleteManifestWriter(manifestSpec);
    }

    @Override
    protected ManifestReader<DeleteFile> newManifestReader(ManifestFile manifest) {
      return ManifestListProducer.this.newDeleteManifestReader(manifest);
    }
  }
}
