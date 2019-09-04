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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

class ManifestGroup {
  private static final Types.StructType EMPTY_STRUCT = Types.StructType.of();

  private final TableOperations ops;
  private final Set<ManifestFile> manifests;
  private final Expression dataFilter;
  private final Expression fileFilter;
  private final Expression partitionFilter;
  private final boolean ignoreDeleted;
  private final boolean ignoreExisting;
  private final List<String> columns;
  private final boolean caseSensitive;

  private final LoadingCache<Integer, ManifestEvaluator> evalCache;

  ManifestGroup(TableOperations ops, Iterable<ManifestFile> manifests) {
    this(ops, Sets.newHashSet(manifests), Expressions.alwaysTrue(), Expressions.alwaysTrue(),
        Expressions.alwaysTrue(), false, false, ImmutableList.of("*"), true);
  }

  private ManifestGroup(TableOperations ops, Set<ManifestFile> manifests,
                        Expression dataFilter, Expression fileFilter, Expression partitionFilter,
                        boolean ignoreDeleted, boolean ignoreExisting, List<String> columns,
                        boolean caseSensitive) {
    this.ops = ops;
    this.manifests = manifests;
    this.dataFilter = dataFilter;
    this.fileFilter = fileFilter;
    this.partitionFilter = partitionFilter;
    this.ignoreDeleted = ignoreDeleted;
    this.ignoreExisting = ignoreExisting;
    this.columns = columns;
    this.caseSensitive = caseSensitive;
    this.evalCache = Caffeine.newBuilder().build(specId -> {
      PartitionSpec spec = ops.current().spec(specId);
      return ManifestEvaluator.forPartitionFilter(
          Expressions.and(partitionFilter, Projections.inclusive(spec).project(dataFilter)),
          spec, caseSensitive);
    });
  }

  public ManifestGroup caseSensitive(boolean filterCaseSensitive) {
    return new ManifestGroup(ops, manifests, dataFilter, fileFilter, partitionFilter,
        ignoreDeleted, ignoreExisting, columns, filterCaseSensitive);
  }

  public ManifestGroup filterData(Expression expr) {
    return new ManifestGroup(
        ops, manifests, Expressions.and(dataFilter, expr), fileFilter, partitionFilter,
        ignoreDeleted, ignoreExisting, columns, caseSensitive);
  }

  public ManifestGroup filterFiles(Expression expr) {
    return new ManifestGroup(
        ops, manifests, dataFilter, Expressions.and(fileFilter, expr), partitionFilter,
        ignoreDeleted, ignoreExisting, columns, caseSensitive);
  }

  public ManifestGroup filterPartitions(Expression expr) {
    return new ManifestGroup(
        ops, manifests, dataFilter, fileFilter, Expressions.and(partitionFilter, expr),
        ignoreDeleted, ignoreExisting, columns, caseSensitive);
  }

  public ManifestGroup ignoreDeleted() {
    return new ManifestGroup(ops, manifests, dataFilter, fileFilter, partitionFilter, true,
        ignoreExisting, columns, caseSensitive);
  }

  public ManifestGroup ignoreDeleted(boolean shouldIgnoreDeleted) {
    return new ManifestGroup(ops, manifests, dataFilter, fileFilter, partitionFilter,
        shouldIgnoreDeleted, ignoreExisting, columns, caseSensitive);
  }

  public ManifestGroup ignoreExisting() {
    return new ManifestGroup(ops, manifests, dataFilter, fileFilter, partitionFilter,
        ignoreDeleted, true, columns, caseSensitive);
  }

  public ManifestGroup ignoreExisting(boolean shouldIgnoreExisting) {
    return new ManifestGroup(ops, manifests, dataFilter, fileFilter, partitionFilter,
        ignoreDeleted, shouldIgnoreExisting, columns, caseSensitive);
  }

  public ManifestGroup select(List<String> columnNames) {
    return new ManifestGroup(
        ops, manifests, dataFilter, fileFilter, partitionFilter, ignoreDeleted, ignoreExisting,
        Lists.newArrayList(columnNames), caseSensitive);
  }

  public ManifestGroup select(String... columnNames) {
    return select(Arrays.asList(columnNames));
  }

  /**
   * Returns an iterable for manifest entries in the set of manifests.
   * <p>
   * Entries are not copied and it is the caller's responsibility to make defensive copies if
   * adding these entries to a collection.
   *
   * @return a CloseableIterable of manifest entries.
   */
  public CloseableIterable<ManifestEntry> entries() {
    Evaluator evaluator = new Evaluator(DataFile.getType(EMPTY_STRUCT), fileFilter, caseSensitive);

    Iterable<ManifestFile> matchingManifests = Iterables.filter(manifests,
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

    Iterable<CloseableIterable<ManifestEntry>> readers = Iterables.transform(
        matchingManifests,
        manifest -> {
          ManifestReader reader = ManifestReader.read(
              ops.io().newInputFile(manifest.path()),
              ops.current()::spec);

          FilteredManifest filtered = reader
              .filterRows(dataFilter)
              .filterPartitions(partitionFilter)
              .select(columns);

          CloseableIterable<ManifestEntry> entries = filtered.allEntries();
          if (ignoreDeleted) {
            entries = filtered.liveEntries();
          }

          if (ignoreExisting) {
            entries = CloseableIterable.filter(entries,
                entry -> entry.status() != ManifestEntry.Status.EXISTING);
          }

          if (fileFilter != null && fileFilter != Expressions.alwaysTrue()) {
            entries = CloseableIterable.filter(entries,
                entry -> evaluator.eval((GenericDataFile) entry.file()));
          }

          return entries;
        });

    return CloseableIterable.concat(readers);
  }
}
