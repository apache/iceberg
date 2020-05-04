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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.BinPacking.ListPacker;
import org.apache.iceberg.util.CharSequenceWrapper;
import org.apache.iceberg.util.ManifestFileUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT;
import static org.apache.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT_DEFAULT;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED;
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT;

abstract class MergingSnapshotProducer<ThisT> extends SnapshotProducer<ThisT> {
  private static final Logger LOG = LoggerFactory.getLogger(MergingSnapshotProducer.class);

  private static final Joiner COMMA = Joiner.on(",");

  protected static class DeleteException extends ValidationException {
    private final String partition;

    private DeleteException(String partition) {
      super("Operation would delete existing data");
      this.partition = partition;
    }

    public String partition() {
      return partition;
    }
  }

  private final String tableName;
  private final TableOperations ops;
  private final PartitionSpec spec;
  private final long manifestTargetSizeBytes;
  private final int minManifestsCountToMerge;
  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
  private final boolean mergeEnabled;
  private final boolean snapshotIdInheritanceEnabled;

  // update data
  private final List<DataFile> newFiles = Lists.newArrayList();
  private final List<ManifestFile> appendManifests = Lists.newArrayList();
  private final List<ManifestFile> rewrittenAppendManifests = Lists.newArrayList();
  private final SnapshotSummary.Builder appendedManifestsSummary = SnapshotSummary.builder();
  private final Set<CharSequenceWrapper> deletePaths = Sets.newHashSet();
  private final Set<StructLikeWrapper> deleteFilePartitions = Sets.newHashSet();
  private final Set<StructLikeWrapper> dropPartitions = Sets.newHashSet();
  private Expression deleteExpression = Expressions.alwaysFalse();
  private boolean hasPathOnlyDeletes = false;
  private boolean failAnyDelete = false;
  private boolean failMissingDeletePaths = false;

  // cache the new manifest once it is written
  private ManifestFile cachedNewManifest = null;
  private ManifestFile firstAppendedManifest = null;
  private boolean hasNewFiles = false;

  // cache merge results to reuse when retrying
  private final Map<List<ManifestFile>, ManifestFile> mergeManifests = Maps.newConcurrentMap();

  // cache filtered manifests to avoid extra work when commits fail.
  private final Map<ManifestFile, ManifestFile> filteredManifests = Maps.newConcurrentMap();

  // tracking where files were deleted to validate retries quickly
  private final Map<ManifestFile, Iterable<DataFile>> filteredManifestToDeletedFiles =
      Maps.newConcurrentMap();

  private boolean filterUpdated = false; // used to clear caches of filtered and merged manifests

  MergingSnapshotProducer(String tableName, TableOperations ops) {
    super(ops);
    this.tableName = tableName;
    this.ops = ops;
    this.spec = ops.current().spec();
    this.manifestTargetSizeBytes = ops.current()
        .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    this.minManifestsCountToMerge = ops.current()
        .propertyAsInt(MANIFEST_MIN_MERGE_COUNT, MANIFEST_MIN_MERGE_COUNT_DEFAULT);
    this.mergeEnabled = ops.current()
        .propertyAsBoolean(TableProperties.MANIFEST_MERGE_ENABLED, TableProperties.MANIFEST_MERGE_ENABLED_DEFAULT);
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
    Preconditions.checkNotNull(expr, "Cannot delete files using filter: null");
    this.filterUpdated = true;
    this.deleteExpression = Expressions.or(deleteExpression, expr);
  }

  /**
   * Add a partition tuple to drop from the table during the delete phase.
   */
  protected void dropPartition(StructLike partition) {
    dropPartitions.add(StructLikeWrapper.wrap(partition));
  }

  /**
   * Add a specific path to be deleted in the new snapshot.
   */
  protected void delete(DataFile file) {
    Preconditions.checkNotNull(file, "Cannot delete file: null");
    this.filterUpdated = true;
    deletePaths.add(CharSequenceWrapper.wrap(file.path()));
    deleteFilePartitions.add(StructLikeWrapper.wrap(file.partition()));
  }

  /**
   * Add a specific path to be deleted in the new snapshot.
   */
  protected void delete(CharSequence path) {
    Preconditions.checkNotNull(path, "Cannot delete file path: null");
    this.filterUpdated = true;
    this.hasPathOnlyDeletes = true;
    deletePaths.add(CharSequenceWrapper.wrap(path));
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
    ManifestFile appendedManifest;
    if (snapshotIdInheritanceEnabled && manifest.snapshotId() == null) {
      appendedManifestsSummary.addedManifest(manifest);
      appendManifests.add(manifest);
      appendedManifest = manifest;
    } else {
      // the manifest must be rewritten with this update's snapshot ID
      ManifestFile copiedManifest = copyManifest(manifest);
      rewrittenAppendManifests.add(copiedManifest);
      appendedManifest = copiedManifest;
    }

    // keep reference of the first appended manifest, so that we can avoid merging first bin(s)
    // which has the first appended manifest and have not crossed the limit of minManifestsCountToMerge
    if (firstAppendedManifest == null) {
      firstAppendedManifest = appendedManifest;
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

    if (filterUpdated) {
      cleanUncommittedFilters(SnapshotProducer.EMPTY_SET);
      this.filterUpdated = false;
    }

    Snapshot current = base.currentSnapshot();
    Map<Integer, List<ManifestFile>> groups = Maps.newTreeMap(Comparator.<Integer>reverseOrder());

    // use a common metrics evaluator for all manifests because it is bound to the table schema
    StrictMetricsEvaluator metricsEvaluator = new StrictMetricsEvaluator(
        ops.current().schema(), deleteExpression);

    // add the current spec as the first group. files are added to the beginning.
    try {
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

      // TODO: add sequence numbers here
      Iterable<ManifestFile> newManifestsWithMetadata = Iterables.transform(
          newManifests,
          manifest -> GenericManifestFile.copyOf(manifest).withSnapshotId(snapshotId()).build());

      // filter any existing manifests
      List<ManifestFile> filtered;
      if (current != null) {
        List<ManifestFile> manifests = current.manifests();
        filtered = Arrays.asList(filterManifests(metricsEvaluator, manifests));
      } else {
        filtered = ImmutableList.of();
      }

      Iterable<ManifestFile> unmergedManifests = Iterables.filter(
          Iterables.concat(newManifestsWithMetadata, filtered),
          // only keep manifests that have live data files or that were written by this commit
          manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles() || manifest.snapshotId() == snapshotId());

      Set<CharSequenceWrapper> deletedFiles = deletedFiles(unmergedManifests);

      List<ManifestFile> manifests = Lists.newArrayList();
      if (mergeEnabled) {
        groupManifestsByPartitionSpec(groups, unmergedManifests);
        for (Map.Entry<Integer, List<ManifestFile>> entry : groups.entrySet()) {
          Iterables.addAll(manifests, mergeGroup(entry.getKey(), entry.getValue()));
        }
      } else {
        Iterables.addAll(manifests, unmergedManifests);
      }

      ValidationException.check(!failMissingDeletePaths || deletedFiles.containsAll(deletePaths),
          "Missing required files to delete: %s",
          COMMA.join(Iterables.transform(Iterables.filter(deletePaths,
              path -> !deletedFiles.contains(path)),
              CharSequenceWrapper::get)));

      return manifests;

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create snapshot manifest list");
    }
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

  private ManifestFile[] filterManifests(StrictMetricsEvaluator metricsEvaluator, List<ManifestFile> manifests)
      throws IOException {
    ManifestFile[] filtered = new ManifestFile[manifests.size()];
    // open all of the manifest files in parallel, use index to avoid reordering
    Tasks.range(filtered.length)
        .stopOnFailure().throwFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .run(index -> {
          ManifestFile manifest = filterManifest(metricsEvaluator, manifests.get(index));
          filtered[index] = manifest;
        }, IOException.class);
    return filtered;
  }

  private Set<CharSequenceWrapper> deletedFiles(Iterable<ManifestFile> manifests) {
    Set<CharSequenceWrapper> deletedFiles = Sets.newHashSet();

    for (ManifestFile manifest : manifests) {
      PartitionSpec manifestSpec = ops.current().spec(manifest.partitionSpecId());
      Iterable<DataFile> manifestDeletes = filteredManifestToDeletedFiles.get(manifest);
      if (manifestDeletes != null) {
        for (DataFile file : manifestDeletes) {
          summaryBuilder.deletedFile(manifestSpec, file);
          deletedFiles.add(CharSequenceWrapper.wrap(file.path()));
        }
      }
    }

    return deletedFiles;
  }

  private void groupManifestsByPartitionSpec(Map<Integer, List<ManifestFile>> groups, Iterable<ManifestFile> filtered) {
    for (ManifestFile manifest : filtered) {
      List<ManifestFile> group = groups.get(manifest.partitionSpecId());
      if (group != null) {
        group.add(manifest);
      } else {
        group = Lists.newArrayList();
        group.add(manifest);
        groups.put(manifest.partitionSpecId(), group);
      }
    }
  }

  private void cleanUncommittedMerges(Set<ManifestFile> committed) {
    // iterate over a copy of entries to avoid concurrent modification
    List<Map.Entry<List<ManifestFile>, ManifestFile>> entries =
        Lists.newArrayList(mergeManifests.entrySet());

    for (Map.Entry<List<ManifestFile>, ManifestFile> entry : entries) {
      // delete any new merged manifests that aren't in the committed list
      ManifestFile merged = entry.getValue();
      if (!committed.contains(merged)) {
        deleteFile(merged.path());
        // remove the deleted file from the cache
        mergeManifests.remove(entry.getKey());
      }
    }
  }

  private void cleanUncommittedFilters(Set<ManifestFile> committed) {
    // iterate over a copy of entries to avoid concurrent modification
    List<Map.Entry<ManifestFile, ManifestFile>> filterEntries =
        Lists.newArrayList(filteredManifests.entrySet());

    for (Map.Entry<ManifestFile, ManifestFile> entry : filterEntries) {
      // remove any new filtered manifests that aren't in the committed list
      ManifestFile manifest = entry.getKey();
      ManifestFile filtered = entry.getValue();
      if (!committed.contains(filtered)) {
        // only delete if the filtered copy was created
        if (!manifest.equals(filtered)) {
          deleteFile(filtered.path());
        }

        // remove the entry from the cache
        filteredManifests.remove(manifest);
      }
    }
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
    cleanUncommittedMerges(committed);
    cleanUncommittedFilters(committed);
    cleanUncommittedAppends(committed);
  }

  private boolean canContainDeletedFiles(ManifestFile manifest) {
    boolean canContainExpressionDeletes;
    if (deleteExpression != null && deleteExpression != Expressions.alwaysFalse()) {
      ManifestEvaluator manifestEvaluator =
          ManifestEvaluator.forRowFilter(deleteExpression, ops.current().spec(), true);
      canContainExpressionDeletes = manifestEvaluator.eval(manifest);
    } else {
      canContainExpressionDeletes = false;
    }

    boolean canContainDroppedPartitions;
    if (dropPartitions.size() > 0) {
      canContainDroppedPartitions = ManifestFileUtil.canContainAny(
          manifest,
          Iterables.transform(dropPartitions, StructLikeWrapper::get),
          specId -> ops.current().spec(specId));
    } else {
      canContainDroppedPartitions = false;
    }

    boolean canContainDroppedFiles;
    if (hasPathOnlyDeletes) {
      canContainDroppedFiles = true;
    } else if (deletePaths.size() > 0) {
      // because there were no path-only deletes, the set of deleted file partitions is valid
      canContainDroppedFiles = ManifestFileUtil.canContainAny(
          manifest,
          Iterables.transform(deleteFilePartitions, StructLikeWrapper::get),
          specId -> ops.current().spec(specId));
    } else {
      canContainDroppedFiles = false;
    }

    return canContainExpressionDeletes || canContainDroppedPartitions || canContainDroppedFiles;
  }

  /**
   * @return a ManifestReader that is a filtered version of the input manifest.
   */
  private ManifestFile filterManifest(StrictMetricsEvaluator metricsEvaluator,
                                      ManifestFile manifest) throws IOException {
    ManifestFile cached = filteredManifests.get(manifest);
    if (cached != null) {
      return cached;
    }

    boolean hasLiveFiles = manifest.hasAddedFiles() || manifest.hasExistingFiles();
    if (!hasLiveFiles || !canContainDeletedFiles(manifest)) {
      filteredManifests.put(manifest, manifest);
      return manifest;
    }

    try (ManifestReader reader = ManifestFiles.read(manifest, ops.io(), ops.current().specsById())) {

      // this is reused to compare file paths with the delete set
      CharSequenceWrapper pathWrapper = CharSequenceWrapper.wrap("");

      // reused to compare file partitions with the drop set
      StructLikeWrapper partitionWrapper = StructLikeWrapper.wrap(null);

      // this assumes that the manifest doesn't have files to remove and streams through the
      // manifest without copying data. if a manifest does have a file to remove, this will break
      // out of the loop and move on to filtering the manifest.
      boolean hasDeletedFiles =
          manifestHasDeletedFiles(metricsEvaluator, reader, pathWrapper, partitionWrapper);

      if (!hasDeletedFiles) {
        filteredManifests.put(manifest, manifest);
        return manifest;
      }

      return filterManifestWithDeletedFiles(metricsEvaluator, manifest, reader, pathWrapper,
          partitionWrapper);
    }
  }

  private boolean manifestHasDeletedFiles(
      StrictMetricsEvaluator metricsEvaluator, ManifestReader reader,
      CharSequenceWrapper pathWrapper, StructLikeWrapper partitionWrapper) {
    Evaluator inclusive = extractInclusiveDeleteExpression(reader);
    Evaluator strict = extractStrictDeleteExpression(reader);
    boolean hasDeletedFiles = false;
    for (ManifestEntry entry : reader.entries()) {
      DataFile file = entry.file();
      boolean fileDelete = deletePaths.contains(pathWrapper.set(file.path())) ||
          dropPartitions.contains(partitionWrapper.set(file.partition()));
      if (fileDelete || inclusive.eval(file.partition())) {
        ValidationException.check(
            fileDelete || strict.eval(file.partition()) || metricsEvaluator.eval(file),
            "Cannot delete file where some, but not all, rows match filter %s: %s",
            this.deleteExpression, file.path());

        hasDeletedFiles = true;
        if (failAnyDelete) {
          throw new DeleteException(writeSpec().partitionToPath(file.partition()));
        }
        break; // as soon as a deleted file is detected, stop scanning
      }
    }
    return hasDeletedFiles;
  }

  private ManifestFile filterManifestWithDeletedFiles(
      StrictMetricsEvaluator metricsEvaluator, ManifestFile manifest, ManifestReader reader,
      CharSequenceWrapper pathWrapper, StructLikeWrapper partitionWrapper) throws IOException {
    Evaluator inclusive = extractInclusiveDeleteExpression(reader);
    Evaluator strict = extractStrictDeleteExpression(reader);
    // when this point is reached, there is at least one file that will be deleted in the
    // manifest. produce a copy of the manifest with all deleted files removed.
    List<DataFile> deletedFiles = Lists.newArrayList();
    Set<CharSequenceWrapper> deletedPaths = Sets.newHashSet();
    ManifestWriter writer = newManifestWriter(reader.spec());
    try {
      reader.entries().forEach(entry -> {
        DataFile file = entry.file();
        boolean fileDelete = deletePaths.contains(pathWrapper.set(file.path())) ||
            dropPartitions.contains(partitionWrapper.set(file.partition()));
        if (entry.status() != Status.DELETED) {
          if (fileDelete || inclusive.eval(file.partition())) {
            ValidationException.check(
                fileDelete || strict.eval(file.partition()) || metricsEvaluator.eval(file),
                "Cannot delete file where some, but not all, rows match filter %s: %s",
                this.deleteExpression, file.path());

            writer.delete(entry);

            CharSequenceWrapper wrapper = CharSequenceWrapper.wrap(entry.file().path());
            if (deletedPaths.contains(wrapper)) {
              LOG.warn("Deleting a duplicate path from manifest {}: {}",
                  manifest.path(), wrapper.get());
              summaryBuilder.incrementDuplicateDeletes();
            } else {
              // only add the file to deletes if it is a new delete
              // this keeps the snapshot summary accurate for non-duplicate data
              deletedFiles.add(entry.file().copyWithoutStats());
            }
            deletedPaths.add(wrapper);

          } else {
            writer.existing(entry);
          }
        }
      });
    } finally {
      writer.close();
    }

    // return the filtered manifest as a reader
    ManifestFile filtered = writer.toManifestFile();

    // update caches
    filteredManifests.put(manifest, filtered);
    filteredManifestToDeletedFiles.put(filtered, deletedFiles);

    return filtered;
  }

  private Evaluator extractStrictDeleteExpression(ManifestReader reader) {
    Expression strictExpr = Projections
        .strict(reader.spec())
        .project(deleteExpression);
    return new Evaluator(reader.spec().partitionType(), strictExpr);
  }

  private Evaluator extractInclusiveDeleteExpression(ManifestReader reader) {
    Expression inclusiveExpr = Projections
        .inclusive(reader.spec())
        .project(deleteExpression);
    return new Evaluator(reader.spec().partitionType(), inclusiveExpr);
  }

  @SuppressWarnings("unchecked")
  private Iterable<ManifestFile> mergeGroup(int specId, List<ManifestFile> group)
      throws IOException {
    // use a lookback of 1 to avoid reordering the manifests. using 1 also means this should pack
    // from the end so that the manifest that gets under-filled is the first one, which will be
    // merged the next time.
    ListPacker<ManifestFile> packer = new ListPacker<>(manifestTargetSizeBytes, 1, false);
    List<List<ManifestFile>> bins = packer.packEnd(group, manifest -> manifest.length());

    // process bins in parallel, but put results in the order of the bins into an array to preserve
    // the order of manifests and contents. preserving the order helps avoid random deletes when
    // data files are eventually aged off.
    List<ManifestFile>[] binResults = (List<ManifestFile>[])
        Array.newInstance(List.class, bins.size());
    Tasks.range(bins.size())
        .stopOnFailure().throwFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .run(index -> {
          List<ManifestFile> bin = bins.get(index);
          List<ManifestFile> outputManifests = Lists.newArrayList();
          binResults[index] = outputManifests;

          if (bin.size() == 1) {
            // no need to rewrite
            outputManifests.add(bin.get(0));
            return;
          }

          // if the bin has a new manifest (the new data files) or appended manifest file then only merge it
          // if the number of manifests is above the minimum count. this is applied only to bins with an in-memory
          // manifest so that large manifests don't prevent merging older groups.
          if ((bin.contains(cachedNewManifest) || bin.contains(firstAppendedManifest)) &&
              bin.size() < minManifestsCountToMerge) {
            // not enough to merge, add all manifest files to the output list
            outputManifests.addAll(bin);
          } else {
            // merge the group
            outputManifests.add(createManifest(specId, bin));
          }
        }, IOException.class);

    return Iterables.concat(binResults);
  }

  private ManifestFile createManifest(int specId, List<ManifestFile> bin) throws IOException {
    // if this merge was already rewritten, use the existing file.
    // if the new files are in this merge, then the ManifestFile for the new files has changed and
    // will be a cache miss.
    if (mergeManifests.containsKey(bin)) {
      return mergeManifests.get(bin);
    }

    ManifestWriter writer = newManifestWriter(ops.current().spec());
    try {
      for (ManifestFile manifest : bin) {
        try (ManifestReader reader = ManifestFiles.read(manifest, ops.io(), ops.current().specsById())) {
          for (ManifestEntry entry : reader.entries()) {
            if (entry.status() == Status.DELETED) {
              // suppress deletes from previous snapshots. only files deleted by this snapshot
              // should be added to the new manifest
              if (entry.snapshotId() == snapshotId()) {
                writer.addEntry(entry);
              }
            } else if (entry.status() == Status.ADDED && entry.snapshotId() == snapshotId()) {
              // adds from this snapshot are still adds, otherwise they should be existing
              writer.addEntry(entry);
            } else {
              // add all files from the old manifest as existing files
              writer.existing(entry);
            }
          }
        }
      }
    } finally {
      writer.close();
    }

    ManifestFile manifest = writer.toManifestFile();

    // update the cache
    mergeManifests.put(bin, manifest);

    return manifest;
  }

  private ManifestFile newFilesAsManifest() throws IOException {
    if (hasNewFiles && cachedNewManifest != null) {
      deleteFile(cachedNewManifest.path());
      cachedNewManifest = null;
    }

    if (cachedNewManifest == null) {
      ManifestWriter writer = newManifestWriter(spec);
      try {
        writer.addAll(newFiles);
      } finally {
        writer.close();
      }

      this.cachedNewManifest = writer.toManifestFile();
      this.hasNewFiles = false;
    }

    return cachedNewManifest;
  }
}
