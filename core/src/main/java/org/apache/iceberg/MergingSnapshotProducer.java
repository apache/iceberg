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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Predicate;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import static org.apache.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT;
import static org.apache.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT_DEFAULT;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED;
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT;

abstract class MergingSnapshotProducer<ThisT> extends SnapshotProducer<ThisT> {
  private final String tableName;
  private final TableOperations ops;
  private final PartitionSpec spec;
  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
  private final ManifestMergeManager<DataFile> mergeManager;
  private final ManifestFilterManager<DataFile> filterManager;
  private final ManifestMergeManager<DeleteFile> deleteMergeManager;
  private final ManifestFilterManager<DeleteFile> deleteFilterManager;
  private final boolean snapshotIdInheritanceEnabled;

  // update data
  private final List<DataFile> newFiles = Lists.newArrayList();
  private final List<DeleteFile> newDeleteFiles = Lists.newArrayList();
  private final List<ManifestFile> appendManifests = Lists.newArrayList();
  private final List<ManifestFile> rewrittenAppendManifests = Lists.newArrayList();
  private final SnapshotSummary.Builder addedFilesSummary = SnapshotSummary.builder();
  private final SnapshotSummary.Builder appendedManifestsSummary = SnapshotSummary.builder();
  private Expression deleteExpression = Expressions.alwaysFalse();

  // cache new manifests after writing
  private ManifestFile cachedNewManifest = null;
  private boolean hasNewFiles = false;
  private ManifestFile cachedNewDeleteManifest = null;
  private boolean hasNewDeleteFiles = false;

  MergingSnapshotProducer(String tableName, TableOperations ops) {
    super(ops);
    this.tableName = tableName;
    this.ops = ops;
    this.spec = ops.current().spec();
    long targetSizeBytes = ops.current()
        .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    int minCountToMerge = ops.current()
        .propertyAsInt(MANIFEST_MIN_MERGE_COUNT, MANIFEST_MIN_MERGE_COUNT_DEFAULT);
    boolean mergeEnabled = ops.current()
        .propertyAsBoolean(TableProperties.MANIFEST_MERGE_ENABLED, TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT);
    this.mergeManager = new DataFileMergeManager(targetSizeBytes, minCountToMerge, mergeEnabled);
    this.filterManager = new DataFileFilterManager();
    this.deleteMergeManager = new DeleteFileMergeManager(targetSizeBytes, minCountToMerge, mergeEnabled);
    this.deleteFilterManager = new DeleteFileFilterManager();
    this.snapshotIdInheritanceEnabled = ops.current()
        .propertyAsBoolean(SNAPSHOT_ID_INHERITANCE_ENABLED, SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);
  }

  @Override
  public ThisT set(String property, String value) {
    summaryBuilder.set(property, value);
    return self();
  }

  protected PartitionSpec writeSpec() {
    // the spec is set when the write is started
    return spec;
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
    // if a delete file matches the row filter, then it can be deleted because the rows will also be deleted
    deleteFilterManager.deleteByRowFilter(expr);
  }

  /**
   * Add a partition tuple to drop from the table during the delete phase.
   */
  protected void dropPartition(int specId, StructLike partition) {
    // dropping the data in a partition also drops all deletes in the partition
    filterManager.dropPartition(specId, partition);
    deleteFilterManager.dropPartition(specId, partition);
  }

  /**
   * Add a specific data file to be deleted in the new snapshot.
   */
  protected void delete(DataFile file) {
    filterManager.delete(file);
  }

  /**
   * Add a specific delete file to be deleted in the new snapshot.
   */
  protected void delete(DeleteFile file) {
    deleteFilterManager.delete(file);
  }

  /**
   * Add a specific data path to be deleted in the new snapshot.
   */
  protected void delete(CharSequence path) {
    // this is an old call that never worked for delete files and can only be used to remove data files.
    filterManager.delete(path);
  }

  /**
   * Add a data file to the new snapshot.
   */
  protected void add(DataFile file) {
    addedFilesSummary.addedFile(spec, file);
    hasNewFiles = true;
    newFiles.add(file);
  }

  /**
   * Add a delete file to the new snapshot.
   */
  protected void add(DeleteFile file) {
    addedFilesSummary.addedFile(spec, file);
    hasNewDeleteFiles = true;
    newDeleteFiles.add(file);
  }

  /**
   * Add all files in a manifest to the new snapshot.
   */
  protected void add(ManifestFile manifest) {
    Preconditions.checkArgument(manifest.content() == ManifestContent.DATA,
        "Cannot append delete manifest: %s", manifest);
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
        current.formatVersion(), toCopy, current.specsById(), newManifestPath, snapshotId(), appendedManifestsSummary);
  }

  @Override
  protected Map<String, String> summary() {
    return summaryBuilder.build();
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    Snapshot current = base.currentSnapshot();

    // filter any existing manifests
    List<ManifestFile> filtered = filterManager.filterManifests(
        base.schema(), current != null ? current.dataManifests() : null);
    long minDataSequenceNumber = filtered.stream()
        .map(ManifestFile::minSequenceNumber)
        .filter(seq -> seq > 0) // filter out unassigned sequence numbers in rewritten manifests
        .reduce(base.lastSequenceNumber(), Math::min);
    deleteFilterManager.dropDeleteFilesOlderThan(minDataSequenceNumber);
    List<ManifestFile> filteredDeletes = deleteFilterManager.filterManifests(
        base.schema(), current != null ? current.deleteManifests() : null);

    // only keep manifests that have live data files or that were written by this commit
    Predicate<ManifestFile> shouldKeep = manifest ->
        manifest.hasAddedFiles() || manifest.hasExistingFiles() || manifest.snapshotId() == snapshotId();
    Iterable<ManifestFile> unmergedManifests = Iterables.filter(
        Iterables.concat(prepareNewManifests(), filtered), shouldKeep);
    Iterable<ManifestFile> unmergedDeleteManifests = Iterables.filter(
        Iterables.concat(prepareDeleteManifests(), filteredDeletes), shouldKeep);

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
    long sequenceNumber = ops.refresh().snapshot(snapshotId).sequenceNumber();
    return new CreateSnapshotEvent(
        tableName,
        operation(),
        snapshotId,
        sequenceNumber,
        summary());
  }

  private void cleanUncommittedAppends(Set<ManifestFile> committed) {
    if (cachedNewManifest != null && !committed.contains(cachedNewManifest)) {
      deleteFile(cachedNewManifest.path());
      this.cachedNewManifest = null;
    }

    if (cachedNewDeleteManifest != null && !committed.contains(cachedNewDeleteManifest)) {
      deleteFile(cachedNewDeleteManifest.path());
      this.cachedNewDeleteManifest = null;
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
      newManifests = Iterables.concat(ImmutableList.of(newManifest), appendManifests, rewrittenAppendManifests);
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
        ManifestWriter<DataFile> writer = newManifestWriter(spec);
        try {
          writer.addAll(newFiles);
        } finally {
          writer.close();
        }

        this.cachedNewManifest = writer.toManifestFile();
        this.hasNewFiles = false;
      } catch (IOException e) {
        throw new RuntimeIOException("Failed to close manifest writer", e);
      }
    }

    return cachedNewManifest;
  }

  private Iterable<ManifestFile> prepareDeleteManifests() {
    if (newDeleteFiles.isEmpty()) {
      return ImmutableList.of();
    }

    return ImmutableList.of(newDeleteFilesAsManifest());
  }

  private ManifestFile newDeleteFilesAsManifest() {
    if (hasNewDeleteFiles && cachedNewDeleteManifest != null) {
      deleteFile(cachedNewDeleteManifest.path());
      cachedNewDeleteManifest = null;
    }

    if (cachedNewDeleteManifest == null) {
      try {
        ManifestWriter<DeleteFile> writer = newDeleteManifestWriter(spec);
        try {
          writer.addAll(newDeleteFiles);
        } finally {
          writer.close();
        }

        this.cachedNewDeleteManifest = writer.toManifestFile();
        this.hasNewDeleteFiles = false;
      } catch (IOException e) {
        throw new RuntimeIOException("Failed to close manifest writer", e);
      }
    }

    return cachedNewDeleteManifest;
  }

  private class DataFileFilterManager extends ManifestFilterManager<DataFile> {
    private DataFileFilterManager() {
      super(ops.current().specsById());
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
      super(targetSizeBytes, minCountToMerge, mergeEnabled);
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
      super(ops.current().specsById());
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
      super(targetSizeBytes, minCountToMerge, mergeEnabled);
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
