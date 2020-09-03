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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;

class ManifestGroup {
  private static final Types.StructType EMPTY_STRUCT = Types.StructType.of();

  private final FileIO io;
  private final Set<ManifestFile> dataManifests;
  private final DeleteFileIndex.Builder deleteIndexBuilder;
  private Predicate<ManifestFile> manifestPredicate;
  private Predicate<ManifestEntry<DataFile>> manifestEntryPredicate;
  private Map<Integer, PartitionSpec> specsById;
  private Expression dataFilter;
  private Expression fileFilter;
  private Expression partitionFilter;
  private boolean ignoreDeleted;
  private boolean ignoreExisting;
  private boolean ignoreResiduals;
  private List<String> columns;
  private boolean caseSensitive;
  private ExecutorService executorService;
  private ManifestProcessor manifestProcessor;

  ManifestGroup(FileIO io, Iterable<ManifestFile> manifests) {
    this(io,
        Iterables.filter(manifests, manifest -> manifest.content() == ManifestContent.DATA),
        Iterables.filter(manifests, manifest -> manifest.content() == ManifestContent.DELETES));
  }

  ManifestGroup(FileIO io, Iterable<ManifestFile> dataManifests, Iterable<ManifestFile> deleteManifests) {
    this.io = io;
    this.dataManifests = Sets.newHashSet(dataManifests);
    this.deleteIndexBuilder = DeleteFileIndex.builderFor(io, deleteManifests);
    this.dataFilter = Expressions.alwaysTrue();
    this.fileFilter = Expressions.alwaysTrue();
    this.partitionFilter = Expressions.alwaysTrue();
    this.ignoreDeleted = false;
    this.ignoreExisting = false;
    this.ignoreResiduals = false;
    this.columns = ManifestReader.ALL_COLUMNS;
    this.caseSensitive = true;
    this.manifestPredicate = m -> true;
    this.manifestEntryPredicate = e -> true;
    this.manifestProcessor = new LocalManifestProcessor(io);
  }

  ManifestGroup withProcessor(ManifestProcessor processor){
    this.manifestProcessor = processor;
    return this;
  }

  ManifestGroup specsById(Map<Integer, PartitionSpec> newSpecsById) {
    this.specsById = newSpecsById;
    deleteIndexBuilder.specsById(newSpecsById);
    return this;
  }

  ManifestGroup filterData(Expression newDataFilter) {
    this.dataFilter = Expressions.and(dataFilter, newDataFilter);
    deleteIndexBuilder.filterData(newDataFilter);
    return this;
  }

  ManifestGroup filterFiles(Expression newFileFilter) {
    this.fileFilter = Expressions.and(fileFilter, newFileFilter);
    return this;
  }

  ManifestGroup filterPartitions(Expression newPartitionFilter) {
    this.partitionFilter = Expressions.and(partitionFilter, newPartitionFilter);
    deleteIndexBuilder.filterPartitions(newPartitionFilter);
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

  ManifestGroup ignoreResiduals() {
    this.ignoreResiduals = true;
    return this;
  }

  ManifestGroup select(List<String> newColumns) {
    this.columns = Lists.newArrayList(newColumns);
    return this;
  }

  ManifestGroup caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    deleteIndexBuilder.caseSensitive(newCaseSensitive);
    return this;
  }

  ManifestGroup planWith(ExecutorService newExecutorService) {
    this.executorService = newExecutorService;
    deleteIndexBuilder.planWith(newExecutorService);
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
      Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
      return ResidualEvaluator.of(spec, filter, caseSensitive);
    });

    DeleteFileIndex deleteFiles = deleteIndexBuilder.build();

    boolean dropStats = ManifestReader.dropStats(dataFilter, columns);
    if (!deleteFiles.isEmpty()) {
      select(Streams.concat(columns.stream(), ManifestReader.STATS_COLUMNS.stream()).collect(Collectors.toList()));
    }

    LoadingCache<Integer, SpecCacheEntry> specCache = Caffeine.newBuilder().build(
        specId -> {
          PartitionSpec spec = specsById.get(specId);
          return new SpecCacheEntry(SchemaParser.toJson(spec.schema()), PartitionSpecParser.toJson(spec),
              residualCache.get(specId));
        }
    );

    //Todo Make this cleaner (maybe go back to old method of two traversals?  Make api for BaseFileScanTask?
    //This will have different performance characteristics than the old version since we are doing a look for every
    // entry, but I think this will probably end up being essentially a noop with branch prediction since we look up
    // the same thing over and over in order.
    Iterable<CloseableIterable<FileScanTask>> tasks = entries(entries ->
        CloseableIterable.transform(entries, e ->
        {
          SpecCacheEntry cached = specCache.get(e.file().specId());
          DataFile file = (dropStats) ? e.file().copyWithoutStats() : e.file().copy();
          return new BaseFileScanTask(file, deleteFiles.forEntry(e), cached.schemaString, cached.specString,
              cached.residuals);
        }));

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
    return CloseableIterable.concat(entries(entry -> entry));
  }

  /*
  Generating the lambda in a static context allows us to ignore the serializability of this class
   */
  private static ManifestProcessor.Func<DataFile> generateManifestProcessorFunc(Map<Integer, PartitionSpec> specsById,
      Expression dataFilter, Expression partitionFilter, boolean caseSensitive, List<String> columns, boolean ignoreDeleted) {
    return (ManifestProcessor.Func<DataFile>) (manifest, processorIO) -> {
      ManifestReader<DataFile> reader = ManifestFiles.read(manifest, processorIO, specsById)
          .filterRows(dataFilter)
          .filterPartitions(partitionFilter)
          .caseSensitive(caseSensitive)
          .select(columns);

      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      if (ignoreDeleted) {
        entries = reader.liveEntries();
      }
      return entries;
    };
  }

  private <T> Iterable<CloseableIterable<T>> entries(
      Function<CloseableIterable<ManifestEntry<DataFile>>, CloseableIterable<T>> entryFn) {

    LoadingCache<Integer, ManifestEvaluator> evalCache = specsById == null ?
        null : Caffeine.newBuilder().build(specId -> {
          PartitionSpec spec = specsById.get(specId);
          return ManifestEvaluator.forPartitionFilter(
              Expressions.and(partitionFilter, Projections.inclusive(spec, caseSensitive).project(dataFilter)),
              spec, caseSensitive);
        });

    Evaluator evaluator = new Evaluator(DataFile.getType(EMPTY_STRUCT), fileFilter, caseSensitive);

    Iterable<ManifestFile> matchingManifests = evalCache == null ? dataManifests :
        Iterables.filter(dataManifests, manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));

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

    Iterable< CloseableIterable<ManifestEntry<DataFile>>> fileReader =
        manifestProcessor.readManifests(matchingManifests, generateManifestProcessorFunc(specsById, dataFilter,
            partitionFilter, caseSensitive, columns, ignoreDeleted));

    return Iterables.transform(
        fileReader,
        entries -> {
          if (ignoreExisting) {
            entries = CloseableIterable.filter(entries,
                entry -> entry.status() != ManifestEntry.Status.EXISTING);
          }

          if (fileFilter != null && fileFilter != Expressions.alwaysTrue()) {
            entries = CloseableIterable.filter(entries,
                entry -> evaluator.eval((GenericDataFile) entry.file()));
          }

          entries = CloseableIterable.filter(entries, manifestEntryPredicate);
          return entryFn.apply(entries);
        });
  }


  private class SpecCacheEntry {
    private final String schemaString;
    private final String specString;
    private final ResidualEvaluator residuals;

    SpecCacheEntry(String schemaString, String specString, ResidualEvaluator residuals) {
      this.schemaString = schemaString;
      this.specString = specString;
      this.residuals = residuals;
    }
  }

}
