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
  private final boolean snapshotIdInheritanceEnabled;

  private class DataFileFilterManager extends ManifestFilterManager<DataFile> {
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
      return ManifestFiles.read(manifest, ops.io(), ops.current().specsById());
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
      return ManifestFiles.read(manifest, ops.io(), ops.current().specsById());
    }
  }

  // update data
  private final List<DataFile> newFiles = Lists.newArrayList();
  private final List<ManifestFile> appendManifests = Lists.newArrayList();
  private final List<ManifestFile> rewrittenAppendManifests = Lists.newArrayList();
  private final SnapshotSummary.Builder appendedManifestsSummary = SnapshotSummary.builder();
  private Expression deleteExpression = Expressions.alwaysFalse();

  // cache the new manifest once it is written
  private ManifestFile cachedNewManifest = null;
  private boolean hasNewFiles = false;

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
  }

  protected void failMissingDeletePaths() {
    filterManager.failMissingDeletePaths();
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
  }

  /**
   * Add a partition tuple to drop from the table during the delete phase.
   */
  protected void dropPartition(StructLike partition) {
    filterManager.dropPartition(partition);
  }

  /**
   * Add a specific path to be deleted in the new snapshot.
   */
  protected void delete(DataFile file) {
    filterManager.delete(file);
  }

  /**
   * Add a specific path to be deleted in the new snapshot.
   */
  protected void delete(CharSequence path) {
    filterManager.delete(path);
  }

  /**
   * Add a file to the new snapshot.
   */
  protected void add(DataFile file) {
    hasNewFiles = true;
    newFiles.add(file);
  }

  /**
   * Add all files in a manifest to the new snapshot.
   */
  protected void add(ManifestFile manifest) {
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
    summaryBuilder.clear();
    summaryBuilder.merge(appendedManifestsSummary);
    Iterable<ManifestFile> newManifests = prepareNewManifests();

    Snapshot current = base.currentSnapshot();

    // filter any existing manifests
    List<ManifestFile> filtered = filterManager.filterManifests(
        base.schema(), current != null ? current.dataManifests() : null);

    Iterable<ManifestFile> unmergedManifests = Iterables.filter(
        Iterables.concat(newManifests, filtered),
        // only keep manifests that have live data files or that were written by this commit
        manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles() || manifest.snapshotId() == snapshotId());

    summaryBuilder.merge(filterManager.buildSummary(unmergedManifests));

    List<ManifestFile> manifests = Lists.newArrayList();
    Iterables.addAll(manifests, mergeManager.mergeManifests(unmergedManifests));

    if (current != null) {
      manifests.addAll(current.deleteManifests());
    }

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
    cleanUncommittedAppends(committed);
  }

  private Iterable<ManifestFile> prepareNewManifests() {
    Iterable<ManifestFile> newManifests;
    if (newFiles.size() > 0) {
      // add all of the new files to the summary builder
      for (DataFile file : newFiles) {
        summaryBuilder.addedFile(spec, file);
      }

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
}
