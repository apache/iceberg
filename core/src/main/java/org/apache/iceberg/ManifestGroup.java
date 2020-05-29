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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;

class ManifestGroup {
  private static final Types.StructType EMPTY_STRUCT = Types.StructType.of();

  private final FileIO io;
  private final Set<ManifestFile> manifests;
  private Predicate<ManifestFile> manifestPredicate;
  private Predicate<ManifestEntry<DataFile>> manifestEntryPredicate;
  private Map<Integer, PartitionSpec> specsById;
  private Expression dataFilter;
  private Expression fileFilter;
  private Expression partitionFilter;
  private boolean ignoreDeleted;
  private boolean ignoreExisting;
  private List<String> columns;
  private boolean caseSensitive;
  private ExecutorService executorService;

  ManifestGroup(FileIO io, Iterable<ManifestFile> manifests) {
    this.io = io;
    this.manifests = Sets.newHashSet(manifests);
    this.dataFilter = Expressions.alwaysTrue();
    this.fileFilter = Expressions.alwaysTrue();
    this.partitionFilter = Expressions.alwaysTrue();
    this.ignoreDeleted = false;
    this.ignoreExisting = false;
    this.columns = BaseManifestReader.ALL_COLUMNS;
    this.caseSensitive = true;
    this.manifestPredicate = m -> true;
    this.manifestEntryPredicate = e -> true;
  }

  ManifestGroup specsById(Map<Integer, PartitionSpec> newSpecsById) {
    this.specsById = newSpecsById;
    return this;
  }

  ManifestGroup filterData(Expression newDataFilter) {
    this.dataFilter = Expressions.and(dataFilter, newDataFilter);
    return this;
  }

  ManifestGroup filterFiles(Expression newFileFilter) {
    this.fileFilter = Expressions.and(fileFilter, newFileFilter);
    return this;
  }

  ManifestGroup filterPartitions(Expression newPartitionFilter) {
    this.partitionFilter = Expressions.and(partitionFilter, newPartitionFilter);
    return this;
  }

  ManifestGroup filterManifests(Predicate<ManifestFile> newManifestPredicate) {
    this.manifestPredicate = manifestPredicate.and(newManifestPredicate);
    return this;
  }

  ManifestGroup filterManifestEntries(Predicate<ManifestEntry<DataFile>> newManifestEntryPredicate) {
    this.manifestEntryPredicate = manifestEntryPredicate.and(newManifestEntryPredicate);
    return this;
  }

  ManifestGroup ignoreDeleted() {
    this.ignoreDeleted = true;
    return this;
  }

  ManifestGroup ignoreExisting() {
    this.ignoreExisting = true;
    return this;
  }

  ManifestGroup select(List<String> newColumns) {
    this.columns = Lists.newArrayList(newColumns);
    return this;
  }

  ManifestGroup caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return this;
  }

  ManifestGroup planWith(ExecutorService newExecutorService) {
    this.executorService = newExecutorService;
    return this;
  }

  /**
   * Returns a iterable of scan tasks. It is safe to add entries of this iterable
   * to a collection as {@link DataFile} in each {@link FileScanTask} is defensively
   * copied.
   * @return a {@link CloseableIterable} of {@link FileScanTask}
   */
  public CloseableIterable<FileScanTask> planFiles() {
    LoadingCache<Integer, ResidualEvaluator> residualCache = Caffeine.newBuilder().build(specId -> {
      PartitionSpec spec = specsById.get(specId);
      return ResidualEvaluator.of(spec, dataFilter, caseSensitive);
    });
    boolean dropStats = BaseManifestReader.dropStats(dataFilter, columns);
    Iterable<CloseableIterable<FileScanTask>> tasks = entries((manifest, entries) -> {
      int partitionSpecId = manifest.partitionSpecId();
      PartitionSpec spec = specsById.get(partitionSpecId);
      String schemaString = SchemaParser.toJson(spec.schema());
      String specString = PartitionSpecParser.toJson(spec);
      ResidualEvaluator residuals = residualCache.get(partitionSpecId);
      if (dropStats) {
        return CloseableIterable.transform(entries, e -> new BaseFileScanTask(
            e.file().copyWithoutStats(), schemaString, specString, residuals));
      } else {
        return CloseableIterable.transform(entries, e -> new BaseFileScanTask(
            e.file().copy(), schemaString, specString, residuals));
      }
    });

    if (executorService != null) {
      return new ParallelIterable<>(tasks, executorService);
    } else {
      return CloseableIterable.concat(tasks);
    }
  }

 /**
   * Returns an iterable for manifest entries in the set of manifests.
   * <p>
   * Entries are not copied and it is the caller's responsibility to make defensive copies if
   * adding these entries to a collection.
   *
   * @return a CloseableIterable of manifest entries.
   */
  public CloseableIterable<ManifestEntry<DataFile>> entries() {
    return CloseableIterable.concat(entries((manifest, entries) -> entries));
  }

  private <T> Iterable<CloseableIterable<T>> entries(
      BiFunction<ManifestFile, CloseableIterable<ManifestEntry<DataFile>>, CloseableIterable<T>> entryFn) {
    LoadingCache<Integer, ManifestEvaluator> evalCache = specsById == null ?
        null : Caffeine.newBuilder().build(specId -> {
          PartitionSpec spec = specsById.get(specId);
          return ManifestEvaluator.forPartitionFilter(
              Expressions.and(partitionFilter, Projections.inclusive(spec, caseSensitive).project(dataFilter)),
              spec, caseSensitive);
        });

    Evaluator evaluator = new Evaluator(DataFile.getType(EMPTY_STRUCT), fileFilter, caseSensitive);

    Iterable<ManifestFile> matchingManifests = evalCache == null ? manifests : Iterables.filter(manifests,
        manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));

    if (ignoreDeleted) {
      // only scan manifests that have entries other than deletes
      // remove any manifests that don't have any existing or added files. if either the added or
      // existing files count is missing, the manifest must be scanned.
      matchingManifests = Iterables.filter(matchingManifests,
          manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles());
    }

    if (ignoreExisting) {
      // only scan manifests that have entries other than existing
      // remove any manifests that don't have any deleted or added files. if either the added or
      // deleted files count is missing, the manifest must be scanned.
      matchingManifests = Iterables.filter(matchingManifests,
          manifest -> manifest.hasAddedFiles() || manifest.hasDeletedFiles());
    }

    matchingManifests = Iterables.filter(matchingManifests, manifestPredicate::test);

    Iterable<CloseableIterable<T>> readers = Iterables.transform(
        matchingManifests,
        manifest -> {
          ManifestReader reader = ManifestFiles.read(manifest, io, specsById)
              .filterRows(dataFilter)
              .filterPartitions(partitionFilter)
              .caseSensitive(caseSensitive)
              .select(columns);

          CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
          if (ignoreDeleted) {
            entries = reader.liveEntries();
          }

          if (ignoreExisting) {
            entries = CloseableIterable.filter(entries,
                entry -> entry.status() != ManifestEntry.Status.EXISTING);
          }

          if (fileFilter != null && fileFilter != Expressions.alwaysTrue()) {
            entries = CloseableIterable.filter(entries,
                entry -> evaluator.eval((GenericDataFile) entry.file()));
          }

          entries = CloseableIterable.filter(entries, manifestEntryPredicate);
          return entryFn.apply(manifest, entries);
        });

    return readers;
  }
}
