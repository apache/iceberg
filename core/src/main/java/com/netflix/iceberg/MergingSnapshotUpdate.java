/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.netflix.iceberg.ManifestEntry.Status;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.expressions.Evaluator;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.Projections;
import com.netflix.iceberg.expressions.StrictMetricsEvaluator;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.util.BinPacking.ListPacker;
import com.netflix.iceberg.util.CharSequenceWrapper;
import com.netflix.iceberg.util.StructLikeWrapper;
import com.netflix.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.netflix.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT;
import static com.netflix.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT_DEFAULT;
import static com.netflix.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static com.netflix.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
import static com.netflix.iceberg.util.ThreadPools.getWorkerPool;

abstract class MergingSnapshotUpdate extends SnapshotUpdate {
  private final Logger LOG = LoggerFactory.getLogger(getClass());

  private static final long SIZE_PER_FILE = 100; // assume each file will be ~100 bytes
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

  private final TableOperations ops;
  private final PartitionSpec spec;
  private final long manifestTargetSizeBytes;
  private final int minManifestsCountToMerge;

  // update data
  private final AtomicInteger manifestCount = new AtomicInteger(0);
  private final List<DataFile> newFiles = Lists.newArrayList();
  private final Set<CharSequenceWrapper> deletePaths = Sets.newHashSet();
  private final Set<StructLikeWrapper> dropPartitions = Sets.newHashSet();
  private Expression deleteExpression = Expressions.alwaysFalse();
  private boolean failAnyDelete = false;
  private boolean failMissingDeletePaths = false;

  // cache merge results to reuse when retrying
  private final Map<List<String>, String> mergeManifests = Maps.newConcurrentMap();

  // cache filtered manifests to avoid extra work when commits fail.
  private final Map<String, ManifestReader> filteredManifests = Maps.newConcurrentMap();

  // tracking where files were deleted to validate retries quickly
  private final Map<String, Set<CharSequenceWrapper>> filteredManifestToDeletedFiles =
      Maps.newConcurrentMap();

  private boolean filterUpdated = false; // used to clear caches of filtered and merged manifests

  MergingSnapshotUpdate(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.spec = ops.current().spec();
    this.manifestTargetSizeBytes = ops.current()
        .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    this.minManifestsCountToMerge = ops.current()
        .propertyAsInt(MANIFEST_MIN_MERGE_COUNT, MANIFEST_MIN_MERGE_COUNT_DEFAULT);
  }

  protected PartitionSpec writeSpec() {
    // the spec is set when the write is started
    return spec;
  }

  protected Expression rowFilter() {
    return deleteExpression;
  }

  protected List<DataFile> addedFiles() {
    return newFiles;
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
  protected void delete(CharSequence path) {
    Preconditions.checkNotNull(path, "Cannot delete file path: null");
    this.filterUpdated = true;
    deletePaths.add(CharSequenceWrapper.wrap(path));
  }

  /**
   * Add a file to the new snapshot.
   */
  protected void add(DataFile file) {
    newFiles.add(file);
  }

  @Override
  public List<String> apply(TableMetadata base) {
    if (filterUpdated) {
      cleanUncommittedFilters(EMPTY_SET);
      this.filterUpdated = false;
    }

    Snapshot current = base.currentSnapshot();
    List<PartitionSpec> specs = Lists.newArrayList();
    List<List<ManifestReader>> groups = Lists.newArrayList();

    // use a common metrics evaluator for all manifests because it is bound to the table schema
    StrictMetricsEvaluator metricsEvaluator = new StrictMetricsEvaluator(
        ops.current().schema(), deleteExpression);

    // add the current spec as the first group. files are added to the beginning.
    if (newFiles.size() > 0) {
      specs.add(spec);
      groups.add(Lists.newArrayList());
      groups.get(0).add(newFilesAsManifest());
    }

    ConcurrentLinkedQueue<ManifestReader> toClose = new ConcurrentLinkedQueue<>();
    boolean threw = true;
    try {
      Set<CharSequenceWrapper> deletedFiles = Sets.newHashSet();

      // group manifests by compatible partition specs to be merged
      if (current != null) {
        List<String> manifests = current.manifests();
        ManifestReader[] readers = new ManifestReader[manifests.size()];
        // open all of the manifest files in parallel, use index to avoid reordering
        Tasks.range(readers.length)
            .stopOnFailure().throwFailureWhenFinished()
            .executeWith(getWorkerPool())
            .run(index -> {
              ManifestReader manifest = filterManifest(
                  deleteExpression, metricsEvaluator, ops.newInputFile(manifests.get(index)));
              readers[index] = manifest;
              toClose.add(manifest);
            });

        for (ManifestReader reader : readers) {
          if (reader.file() != null) {
            String location = reader.file().location();
            Set<CharSequenceWrapper> manifestDeletes = filteredManifestToDeletedFiles.get(location);
            if (manifestDeletes != null) {
              deletedFiles.addAll(manifestDeletes);
            }
          }

          int index = findMatch(specs, reader.spec());
          if (index < 0) {
            // not found, add a new one
            List<ManifestReader> newList = Lists.<ManifestReader>newArrayList(reader);
            specs.add(reader.spec());
            groups.add(newList);
          } else {
            // replace the reader spec with the later one
            specs.set(index, reader.spec());
            groups.get(index).add(reader);
          }
        }
      }

      List<String> manifests = Lists.newArrayList();
      for (int i = 0; i < specs.size(); i += 1) {
        for (String manifest : mergeGroup(specs.get(i), groups.get(i))) {
          manifests.add(manifest);
        }
      }

      ValidationException.check(!failMissingDeletePaths || deletedFiles.containsAll(deletePaths),
          "Missing required files to delete: %s",
          COMMA.join(transform(filter(deletePaths,
              path -> !deletedFiles.contains(path)),
              CharSequenceWrapper::get)));

      threw = false;

      return manifests;

    } finally {
      for (ManifestReader reader : toClose) {
        try {
          Closeables.close(reader, threw);
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
      }
    }
  }

  private void cleanUncommittedMerges(Set<String> committed) {
    List<Map.Entry<List<String>, String>> entries = Lists.newArrayList(mergeManifests.entrySet());
    for (Map.Entry<List<String>, String> entry : entries) {
      // delete any new merged manifests that aren't in the committed list
      String merged = entry.getValue();
      if (!committed.contains(merged)) {
        deleteFile(merged);
        // remove the deleted file from the cache
        mergeManifests.remove(entry.getKey());
      }
    }
  }

  private void cleanUncommittedFilters(Set<String> committed) {
    List<Map.Entry<String, ManifestReader>> filterEntries = Lists.newArrayList(filteredManifests.entrySet());
    for (Map.Entry<String, ManifestReader> entry : filterEntries) {
      // remove any new filtered manifests that aren't in the committed list
      String manifest = entry.getKey();
      ManifestReader filtered = entry.getValue();
      if (filtered != null) {
        String location = filtered.file().location();
        if (!manifest.equals(location) && !committed.contains(location)) {
          filteredManifests.remove(manifest);
          deleteFile(location);
        }
      }
    }
  }

  @Override
  protected void cleanUncommitted(Set<String> committed) {
    cleanUncommittedMerges(committed);
    cleanUncommittedFilters(committed);
  }

  private boolean nothingToFilter() {
    return (deleteExpression == null || deleteExpression == Expressions.alwaysFalse()) &&
        deletePaths.isEmpty() && dropPartitions.isEmpty();
  }

  /**
   * @return a ManifestReader that is a filtered version of the input manifest.
   */
  private ManifestReader filterManifest(Expression deleteExpression,
                                        StrictMetricsEvaluator metricsEvaluator,
                                        InputFile manifest) {
    ManifestReader cached = filteredManifests.get(manifest.location());
    if (cached != null) {
      return cached;
    }

    ManifestReader reader = ManifestReader.read(manifest);

    if (nothingToFilter()) {
      filteredManifests.put(manifest.location(), reader);
      return reader;
    }

    try {
      Expression inclusiveExpr = Projections
          .inclusive(reader.spec())
          .project(deleteExpression);
      Evaluator inclusive = new Evaluator(reader.spec().partitionType(), inclusiveExpr);

      Expression strictExpr = Projections
          .strict(reader.spec())
          .project(deleteExpression);
      Evaluator strict = new Evaluator(reader.spec().partitionType(), strictExpr);

      // this is reused to compare file paths with the delete set
      CharSequenceWrapper pathWrapper = CharSequenceWrapper.wrap("");

      // reused to compare file partitions with the drop set
      StructLikeWrapper partitionWrapper = StructLikeWrapper.wrap(null);

      // this assumes that the manifest doesn't have files to remove and streams through the
      // manifest without copying data. if a manifest does have a file to remove, this will break
      // out of the loop and move on to filtering the manifest.
      boolean hasDeletedFiles = false;
      Iterator<ManifestEntry> entries = reader.entries().iterator();
      try {
        while (entries.hasNext()) {
          ManifestEntry entry = entries.next();
          DataFile file = entry.file();
          boolean fileDelete = (deletePaths.contains(pathWrapper.set(file.path())) ||
              dropPartitions.contains(partitionWrapper.set(file.partition())));
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
      } finally {
        // the loop may have exited early. ensure the iterator is closed.
        if (entries instanceof Closeable) {
          ((Closeable) entries).close();
        }
      }

      if (!hasDeletedFiles) {
        return reader;
      }

      // when this point is reached, there is at least one file that will be deleted in the
      // manifest. produce a copy of the manifest with all deleted files removed.
      Set<CharSequenceWrapper> deletedPaths = Sets.newHashSet();
      OutputFile filteredCopy = manifestPath(manifestCount.getAndIncrement());
      try (ManifestWriter writer = new ManifestWriter(reader.spec(), filteredCopy, snapshotId())) {
        for (ManifestEntry entry : reader.entries()) {
          DataFile file = entry.file();
          boolean fileDelete = (deletePaths.contains(pathWrapper.set(file.path())) ||
              dropPartitions.contains(partitionWrapper.set(file.partition())));
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
                    manifest.location(), wrapper.get());
              }
              deletedPaths.add(wrapper);

            } else {
              writer.addExisting(entry);
            }
          }
        }
      }

      // close the reader now that it is no longer used and will not be returned
      reader.close();

      // return the filtered manifest as a reader
      ManifestReader filtered = ManifestReader.read(ops.newInputFile(filteredCopy.location()));

      // update caches
      filteredManifests.put(manifest.location(), filtered);
      filteredManifestToDeletedFiles.put(filteredCopy.location(), deletedPaths);

      return filtered;

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to filter manifest: %s", reader.file().location());
    }
  }

  @SuppressWarnings("unchecked")
  private Iterable<String> mergeGroup(PartitionSpec groupSpec, List<ManifestReader> group) {
    // use a lookback of 1 to avoid reordering the manifests. using 1 also means this should pack
    // from the end so that the manifest that gets under-filled is the first one, which will be
    // merged the next time.
    long newFilesSize = newFiles.size() * SIZE_PER_FILE;
    ListPacker<ManifestReader> packer = new ListPacker<>(manifestTargetSizeBytes, 1);
    List<List<ManifestReader>> bins = packer.packEnd(group,
        reader -> reader.file() != null ? reader.file().getLength() : newFilesSize);

    // process bins in parallel, but put results in the order of the bins into an array to preserve
    // the order of manifests and contents. preserving the order helps avoid random deletes when
    // data files are eventually aged off.
    List<String>[] binResults = (List<String>[]) Array.newInstance(List.class, bins.size());
    Tasks.range(bins.size())
        .stopOnFailure().throwFailureWhenFinished()
        .executeWith(getWorkerPool())
        .run(index -> {
          List<ManifestReader> bin = bins.get(index);
          List<String> outputManifests = Lists.newArrayList();
          binResults[index] = outputManifests;

          if (bin.size() == 1 && bin.get(0).file() != null) {
            // no need to rewrite
            outputManifests.add(bin.get(0).file().location());
            return;
          }

          boolean hasInMemoryManifest = false;
          for (ManifestReader reader : bin) {
            if (reader.file() == null) {
              hasInMemoryManifest = true;
            }
          }

          // if the bin has an in-memory manifest (the new data) then only merge it if the number of
          // manifests is above the minimum count. this is applied only to bins with an in-memory
          // manifest so that large manifests don't prevent merging older groups.
          if (hasInMemoryManifest && bin.size() < minManifestsCountToMerge) {
            for (ManifestReader reader : bin) {
              if (reader.file() != null) {
                outputManifests.add(reader.file().location());
              } else {
                // write the in-memory manifest
                outputManifests.add(createManifest(groupSpec, Collections.singletonList(reader)));
              }
            }
          } else {
            outputManifests.add(createManifest(groupSpec, bin));
          }
        });

    return Iterables.concat(binResults);
  }

  // NOTE: This assumes that any files that are added are in an in-memory manifest.
  private String createManifest(PartitionSpec binSpec, List<ManifestReader> bin) {
    List<String> key = cacheKey(bin);
    // if this merge was already rewritten, use the existing file.
    // if the new files are in this merge, the key is based on the number of new files so files
    // added after the last merge will cause a cache miss.
    if (mergeManifests.containsKey(key)) {
      return mergeManifests.get(key);
    }

    OutputFile out = manifestPath(manifestCount.getAndIncrement());

    try (ManifestWriter writer = new ManifestWriter(binSpec, out, snapshotId())) {

      for (ManifestReader reader : bin) {
        if (reader.file() != null) {
          for (ManifestEntry entry : reader.entries()) {
            if (entry.status() == Status.DELETED) {
              // suppress deletes from previous snapshots. only files deleted by this snapshot
              // should be added to the new manifest
              if (entry.snapshotId() == snapshotId()) {
                writer.add(entry);
              }
            } else {
              // add all files from the old manifest as existing files
              writer.addExisting(entry);
            }
          }
        } else {
          // if the files are in an in-memory manifest, then they are new
          writer.addEntries(reader.entries());
        }
      }

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write manifest: %s", out);
    }

    // update the cache
    mergeManifests.put(key, out.location());

    return out.location();
  }

  private ManifestReader newFilesAsManifest() {
    long id = snapshotId();
    ManifestEntry reused = new ManifestEntry(spec.partitionType());
    return ManifestReader.inMemory(spec,
        transform(newFiles, file -> {
          reused.wrapAppend(id, file);
          return reused;
        }));
  }

  private List<String> cacheKey(List<ManifestReader> group) {
    List<String> key = Lists.newArrayList();

    for (ManifestReader reader : group) {
      if (reader.file() != null) {
        key.add(reader.file().location());
      } else {
        // if the file is null, this is an in-memory reader
        // use the size to avoid collisions if retries have added files
        key.add("append-" + newFiles.size() + "-files");
      }
    }

    return key;
  }

  /**
   * Helper method to group manifests by compatible partition spec.
   * <p>
   * When a match is found, this will replace the current spec for the group with the query spec.
   * This is to produce manifests with the latest compatible spec.
   *
   * @param specs   a list of partition specs, corresponding to the groups of readers
   * @param spec    spec to be matched to a group
   * @return        group of readers files for this spec can be merged into
   */
  private static int findMatch(List<PartitionSpec> specs,
                               PartitionSpec spec) {
    // loop from last to first because later specs are most likely to match
    for (int i = specs.size() - 1; i >= 0; i -= 1) {
      if (specs.get(i).compatibleWith(spec)) {
        return i;
      }
    }

    return -1;
  }
}
