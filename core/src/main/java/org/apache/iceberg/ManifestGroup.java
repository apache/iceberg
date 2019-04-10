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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

class ManifestGroup {
  private static final Types.StructType EMPTY_STRUCT = Types.StructType.of();

  private final TableOperations ops;
  private final Set<ManifestFile> manifests;
  private final Expression dataFilter;
  private final Expression fileFilter;
  private final boolean ignoreDeleted;
  private final List<String> columns;

  private final LoadingCache<Integer, InclusiveManifestEvaluator> evalCache = CacheBuilder
      .newBuilder()
      .build(new CacheLoader<Integer, InclusiveManifestEvaluator>() {
        @Override
        public InclusiveManifestEvaluator load(Integer specId) {
          PartitionSpec spec = ops.current().spec(specId);
          return new InclusiveManifestEvaluator(spec, dataFilter);
        }
      });

  ManifestGroup(TableOperations ops, Iterable<ManifestFile> manifests) {
    this(ops, Sets.newHashSet(manifests), Expressions.alwaysTrue(), Expressions.alwaysTrue(),
        false, ImmutableList.of("*"));
  }

  private ManifestGroup(TableOperations ops, Set<ManifestFile> manifests,
                        Expression dataFilter, Expression fileFilter, boolean ignoreDeleted,
                        List<String> columns) {
    this.ops = ops;
    this.manifests = manifests;
    this.dataFilter = dataFilter;
    this.fileFilter = fileFilter;
    this.ignoreDeleted = ignoreDeleted;
    this.columns = columns;
  }

  public ManifestGroup filterData(Expression expr) {
    return new ManifestGroup(
        ops, manifests, Expressions.and(dataFilter, expr), fileFilter, ignoreDeleted, columns);
  }

  public ManifestGroup filterFiles(Expression expr) {
    return new ManifestGroup(
        ops, manifests, dataFilter, Expressions.and(fileFilter, expr), ignoreDeleted, columns);
  }

  public ManifestGroup ignoreDeleted() {
    return new ManifestGroup(ops, manifests, dataFilter, fileFilter, true, columns);
  }

  public ManifestGroup select(List<String> columns) {
    return new ManifestGroup(
        ops, manifests, dataFilter, fileFilter, ignoreDeleted, Lists.newArrayList(columns));
  }

  public ManifestGroup select(String... columns) {
    return select(Arrays.asList(columns));
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
    Evaluator evaluator = new Evaluator(DataFile.getType(EMPTY_STRUCT), fileFilter);

    Iterable<ManifestFile> matchingManifests = Iterables.filter(manifests,
        manifest -> evalCache.getUnchecked(manifest.partitionSpecId()).eval(manifest));

    if (ignoreDeleted) {
      // remove any manifests that don't have any existing or added files. if either the added or
      // existing files count is missing, the manifest must be scanned.
      matchingManifests = Iterables.filter(manifests, manifest ->
          manifest.addedFilesCount() == null || manifest.existingFilesCount() == null ||
              manifest.addedFilesCount() + manifest.existingFilesCount() > 0);
    }

    Iterable<CloseableIterable<ManifestEntry>> readers = Iterables.transform(
        matchingManifests,
        manifest -> {
          ManifestReader reader = ManifestReader.read(
              ops.io().newInputFile(manifest.path()),
              ops.current()::spec);
          FilteredManifest filtered = reader.filterRows(dataFilter).select(columns);
          return CloseableIterable.combine(
              Iterables.filter(
                  ignoreDeleted ? filtered.liveEntries() : filtered.allEntries(),
                  entry -> evaluator.eval((GenericDataFile) entry.file())),
              reader);
        });

    return CloseableIterable.concat(readers);
  }
}
