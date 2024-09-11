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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.CharSequenceWrapper;
import org.apache.iceberg.util.ManifestFileUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ManifestFilterManager<F extends ContentFile<F>> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestFilterManager.class);
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

  private final Map<Integer, PartitionSpec> specsById;
  private final PartitionSet deleteFilePartitions;
  private final PartitionSet dropPartitions;
  private final CharSequenceSet deletePaths = CharSequenceSet.empty();
  private Expression deleteExpression = Expressions.alwaysFalse();
  private long minSequenceNumber = 0;
  private boolean hasPathOnlyDeletes = false;
  private boolean failAnyDelete = false;
  private boolean failMissingDeletePaths = false;
  private int duplicateDeleteCount = 0;
  private boolean caseSensitive = true;

  // cache filtered manifests to avoid extra work when commits fail.
  private final Map<ManifestFile, ManifestFile> filteredManifests = Maps.newConcurrentMap();

  // tracking where files were deleted to validate retries quickly
  private final Map<ManifestFile, Iterable<F>> filteredManifestToDeletedFiles =
      Maps.newConcurrentMap();

  private final Supplier<ExecutorService> workerPoolSupplier;

  protected ManifestFilterManager(
      Map<Integer, PartitionSpec> specsById, Supplier<ExecutorService> executorSupplier) {
    this.specsById = specsById;
    this.deleteFilePartitions = PartitionSet.create(specsById);
    this.dropPartitions = PartitionSet.create(specsById);
    this.workerPoolSupplier = executorSupplier;
  }

  protected abstract void deleteFile(String location);

  protected abstract ManifestWriter<F> newManifestWriter(PartitionSpec spec);

  protected abstract ManifestReader<F> newManifestReader(ManifestFile manifest);

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
    invalidateFilteredCache();
    this.deleteExpression = Expressions.or(deleteExpression, expr);
  }

  /** Add a partition tuple to drop from the table during the delete phase. */
  protected void dropPartition(int specId, StructLike partition) {
    Preconditions.checkNotNull(partition, "Cannot delete files in invalid partition: null");
    invalidateFilteredCache();
    dropPartitions.add(specId, partition);
  }

  /**
   * Set the sequence number used to remove old delete files.
   *
   * <p>Delete files with a sequence number older than the given value will be removed. By setting
   * this to the sequence number of the oldest data file in the table, this will continuously remove
   * delete files that are no longer needed because deletes cannot match any existing rows in the
   * table.
   *
   * @param sequenceNumber a sequence number used to remove old delete files
   */
  protected void dropDeleteFilesOlderThan(long sequenceNumber) {
    Preconditions.checkArgument(
        sequenceNumber >= 0, "Invalid minimum data sequence number: %s", sequenceNumber);
    this.minSequenceNumber = sequenceNumber;
  }

  void caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
  }

  /** Add a specific path to be deleted in the new snapshot. */
  void delete(F file) {
    Preconditions.checkNotNull(file, "Cannot delete file: null");
    invalidateFilteredCache();
    deletePaths.add(file.path());
    deleteFilePartitions.add(file.specId(), file.partition());
  }

  /** Add a specific path to be deleted in the new snapshot. */
  void delete(CharSequence path) {
    Preconditions.checkNotNull(path, "Cannot delete file path: null");
    invalidateFilteredCache();
    this.hasPathOnlyDeletes = true;
    deletePaths.add(path);
  }

  boolean containsDeletes() {
    return !deletePaths.isEmpty()
        || deleteExpression != Expressions.alwaysFalse()
        || !dropPartitions.isEmpty();
  }

  /**
   * Filter deleted files out of a list of manifests.
   *
   * @param tableSchema the current table schema
   * @param manifests a list of manifests to be filtered
   * @return an array of filtered manifests
   */
  List<ManifestFile> filterManifests(Schema tableSchema, List<ManifestFile> manifests) {
    if (manifests == null || manifests.isEmpty()) {
      validateRequiredDeletes();
      return ImmutableList.of();
    }

    ManifestFile[] filtered = new ManifestFile[manifests.size()];
    // open all of the manifest files in parallel, use index to avoid reordering
    Tasks.range(filtered.length)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPoolSupplier.get())
        .run(
            index -> {
              ManifestFile manifest = filterManifest(tableSchema, manifests.get(index));
              filtered[index] = manifest;
            });

    validateRequiredDeletes(filtered);

    return Arrays.asList(filtered);
  }

  /**
   * Creates a snapshot summary builder with the files deleted from the set of filtered manifests.
   *
   * @param manifests a set of filtered manifests
   */
  SnapshotSummary.Builder buildSummary(Iterable<ManifestFile> manifests) {
    SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();

    for (ManifestFile manifest : manifests) {
      PartitionSpec manifestSpec = specsById.get(manifest.partitionSpecId());
      Iterable<F> manifestDeletes = filteredManifestToDeletedFiles.get(manifest);
      if (manifestDeletes != null) {
        for (F file : manifestDeletes) {
          summaryBuilder.deletedFile(manifestSpec, file);
        }
      }
    }

    summaryBuilder.incrementDuplicateDeletes(duplicateDeleteCount);

    return summaryBuilder;
  }

  /**
   * Throws a {@link ValidationException} if any deleted file was not present in a filtered
   * manifest.
   *
   * @param manifests a set of filtered manifests
   */
  @SuppressWarnings("CollectionUndefinedEquality")
  private void validateRequiredDeletes(ManifestFile... manifests) {
    if (failMissingDeletePaths) {
      CharSequenceSet deletedFiles = deletedFiles(manifests);
      ValidationException.check(
          deletedFiles.containsAll(deletePaths),
          "Missing required files to delete: %s",
          COMMA.join(Iterables.filter(deletePaths, path -> !deletedFiles.contains(path))));
    }
  }

  private CharSequenceSet deletedFiles(ManifestFile[] manifests) {
    CharSequenceSet deletedFiles = CharSequenceSet.empty();

    if (manifests != null) {
      for (ManifestFile manifest : manifests) {
        Iterable<F> manifestDeletes = filteredManifestToDeletedFiles.get(manifest);
        if (manifestDeletes != null) {
          for (F file : manifestDeletes) {
            deletedFiles.add(file.path());
          }
        }
      }
    }

    return deletedFiles;
  }

  /**
   * Deletes filtered manifests that were created by this class, but are not in the committed
   * manifest set.
   *
   * @param committed the set of manifest files that were committed
   */
  void cleanUncommitted(Set<ManifestFile> committed) {
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

  private void invalidateFilteredCache() {
    cleanUncommitted(SnapshotProducer.EMPTY_SET);
  }

  /**
   * @return a ManifestReader that is a filtered version of the input manifest.
   */
  private ManifestFile filterManifest(Schema tableSchema, ManifestFile manifest) {
    ManifestFile cached = filteredManifests.get(manifest);
    if (cached != null) {
      return cached;
    }

    boolean hasLiveFiles = manifest.hasAddedFiles() || manifest.hasExistingFiles();
    if (!hasLiveFiles || !canContainDeletedFiles(manifest)) {
      filteredManifests.put(manifest, manifest);
      return manifest;
    }

    try (ManifestReader<F> reader = newManifestReader(manifest)) {
      PartitionSpec spec = reader.spec();
      PartitionAndMetricsEvaluator evaluator =
          new PartitionAndMetricsEvaluator(tableSchema, spec, deleteExpression);

      // this assumes that the manifest doesn't have files to remove and streams through the
      // manifest without copying data. if a manifest does have a file to remove, this will break
      // out of the loop and move on to filtering the manifest.
      boolean hasDeletedFiles = manifestHasDeletedFiles(evaluator, reader);
      if (!hasDeletedFiles) {
        filteredManifests.put(manifest, manifest);
        return manifest;
      }

      return filterManifestWithDeletedFiles(evaluator, manifest, reader);

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest: %s", manifest);
    }
  }

  private boolean canContainDeletedFiles(ManifestFile manifest) {
    boolean canContainExpressionDeletes;
    if (deleteExpression != null && deleteExpression != Expressions.alwaysFalse()) {
      ManifestEvaluator manifestEvaluator =
          ManifestEvaluator.forRowFilter(
              deleteExpression, specsById.get(manifest.partitionSpecId()), caseSensitive);
      canContainExpressionDeletes = manifestEvaluator.eval(manifest);
    } else {
      canContainExpressionDeletes = false;
    }

    boolean canContainDroppedPartitions;
    if (!dropPartitions.isEmpty()) {
      canContainDroppedPartitions =
          ManifestFileUtil.canContainAny(manifest, dropPartitions, specsById);
    } else {
      canContainDroppedPartitions = false;
    }

    boolean canContainDroppedFiles;
    if (hasPathOnlyDeletes) {
      canContainDroppedFiles = true;
    } else if (!deletePaths.isEmpty()) {
      // because there were no path-only deletes, the set of deleted file partitions is valid
      canContainDroppedFiles =
          ManifestFileUtil.canContainAny(manifest, deleteFilePartitions, specsById);
    } else {
      canContainDroppedFiles = false;
    }

    boolean canContainDropBySeq =
        manifest.content() == ManifestContent.DELETES
            && manifest.minSequenceNumber() < minSequenceNumber;

    return canContainExpressionDeletes
        || canContainDroppedPartitions
        || canContainDroppedFiles
        || canContainDropBySeq;
  }

  @SuppressWarnings({"CollectionUndefinedEquality", "checkstyle:CyclomaticComplexity"})
  private boolean manifestHasDeletedFiles(
      PartitionAndMetricsEvaluator evaluator, ManifestReader<F> reader) {
    boolean isDelete = reader.isDeleteManifestReader();

    for (ManifestEntry<F> entry : reader.liveEntries()) {
      F file = entry.file();
      boolean markedForDelete =
          deletePaths.contains(file.path())
              || dropPartitions.contains(file.specId(), file.partition())
              || (isDelete
                  && entry.isLive()
                  && entry.dataSequenceNumber() > 0
                  && entry.dataSequenceNumber() < minSequenceNumber);

      if (markedForDelete || evaluator.rowsMightMatch(file)) {
        boolean allRowsMatch = markedForDelete || evaluator.rowsMustMatch(file);
        ValidationException.check(
            allRowsMatch
                || isDelete, // ignore delete files where some records may not match the expression
            "Cannot delete file where some, but not all, rows match filter %s: %s",
            this.deleteExpression,
            file.path());

        if (allRowsMatch) {
          if (failAnyDelete) {
            throw new DeleteException(reader.spec().partitionToPath(file.partition()));
          }

          // as soon as a deleted file is detected, stop scanning
          return true;
        }
      }
    }

    return false;
  }

  @SuppressWarnings({"CollectionUndefinedEquality", "checkstyle:CyclomaticComplexity"})
  private ManifestFile filterManifestWithDeletedFiles(
      PartitionAndMetricsEvaluator evaluator, ManifestFile manifest, ManifestReader<F> reader) {
    boolean isDelete = reader.isDeleteManifestReader();
    // when this point is reached, there is at least one file that will be deleted in the
    // manifest. produce a copy of the manifest with all deleted files removed.
    List<F> deletedFiles = Lists.newArrayList();
    Set<CharSequenceWrapper> deletedPaths = Sets.newHashSet();

    try {
      ManifestWriter<F> writer = newManifestWriter(reader.spec());
      try {
        reader
            .entries()
            .forEach(
                entry -> {
                  F file = entry.file();
                  boolean markedForDelete =
                      deletePaths.contains(file.path())
                          || dropPartitions.contains(file.specId(), file.partition())
                          || (isDelete
                              && entry.isLive()
                              && entry.dataSequenceNumber() > 0
                              && entry.dataSequenceNumber() < minSequenceNumber);
                  if (entry.status() != ManifestEntry.Status.DELETED) {
                    if (markedForDelete || evaluator.rowsMightMatch(file)) {
                      boolean allRowsMatch = markedForDelete || evaluator.rowsMustMatch(file);
                      ValidationException.check(
                          allRowsMatch
                              || isDelete, // ignore delete files where some records may not match
                          // the expression
                          "Cannot delete file where some, but not all, rows match filter %s: %s",
                          this.deleteExpression,
                          file.path());

                      if (allRowsMatch) {
                        writer.delete(entry);

                        CharSequenceWrapper wrapper = CharSequenceWrapper.wrap(entry.file().path());
                        if (deletedPaths.contains(wrapper)) {
                          LOG.warn(
                              "Deleting a duplicate path from manifest {}: {}",
                              manifest.path(),
                              wrapper.get());
                          duplicateDeleteCount += 1;
                        } else {
                          // only add the file to deletes if it is a new delete
                          // this keeps the snapshot summary accurate for non-duplicate data
                          deletedFiles.add(entry.file().copyWithoutStats());
                          deletedPaths.add(wrapper);
                        }
                      } else {
                        writer.existing(entry);
                      }

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

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest writer");
    }
  }

  // an evaluator that checks whether rows in a file may/must match a given expression
  // this class first partially evaluates the provided expression using the partition tuple
  // and then checks the remaining part of the expression using metrics evaluators
  private class PartitionAndMetricsEvaluator {
    private final Schema tableSchema;
    private final ResidualEvaluator residualEvaluator;
    private final StructLikeMap<Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator>>
        metricsEvaluators;

    PartitionAndMetricsEvaluator(Schema tableSchema, PartitionSpec spec, Expression expr) {
      this.tableSchema = tableSchema;
      this.residualEvaluator = ResidualEvaluator.of(spec, expr, caseSensitive);
      this.metricsEvaluators = StructLikeMap.create(spec.partitionType());
    }

    boolean rowsMightMatch(F file) {
      Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator> evaluators = metricsEvaluators(file);
      InclusiveMetricsEvaluator inclusiveMetricsEvaluator = evaluators.first();
      return inclusiveMetricsEvaluator.eval(file);
    }

    boolean rowsMustMatch(F file) {
      Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator> evaluators = metricsEvaluators(file);
      StrictMetricsEvaluator strictMetricsEvaluator = evaluators.second();
      return strictMetricsEvaluator.eval(file);
    }

    private Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator> metricsEvaluators(F file) {
      // ResidualEvaluator removes predicates in the expression using strict/inclusive projections
      // if strict projection returns true -> the pred would return true -> replace the pred with
      // true
      // if inclusive projection returns false -> the pred would return false -> replace the pred
      // with false
      // otherwise, keep the original predicate and proceed to other predicates in the expression
      // in other words, ResidualEvaluator returns a part of the expression that needs to be
      // evaluated
      // for rows in the given partition using metrics
      PartitionData partition = (PartitionData) file.partition();
      if (!metricsEvaluators.containsKey(partition)) {
        Expression residual = residualEvaluator.residualFor(partition);
        InclusiveMetricsEvaluator inclusive =
            new InclusiveMetricsEvaluator(tableSchema, residual, caseSensitive);
        StrictMetricsEvaluator strict =
            new StrictMetricsEvaluator(tableSchema, residual, caseSensitive);

        metricsEvaluators.put(
            partition.copy(), // The partition may be a re-used container so a copy is required
            Pair.of(inclusive, strict));
      }
      return metricsEvaluators.get(partition);
    }
  }
}
