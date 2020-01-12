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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;

class ManifestGroup {
  private static final Types.StructType EMPTY_STRUCT = Types.StructType.of();

  private final FileIO io;
  private final Set<ManifestFile> manifests;
  private final Map<Integer, PartitionSpec> specsById;
  private final Expression dataFilter;
  private final Expression fileFilter;
  private final Expression partitionFilter;
  private final boolean ignoreDeleted;
  private final boolean ignoreExisting;
  private final List<String> columns;
  private final boolean caseSensitive;

  private final LoadingCache<Integer, ManifestEvaluator> evalCache;

  private ManifestGroup(FileIO io, Set<ManifestFile> manifests, Map<Integer, PartitionSpec> specsById,
                        Expression dataFilter, Expression fileFilter, Expression partitionFilter,
                        boolean ignoreDeleted, boolean ignoreExisting, List<String> columns,
                        boolean caseSensitive) {
    this.io = io;
    this.manifests = manifests;
    this.specsById = specsById;
    this.dataFilter = dataFilter;
    this.fileFilter = fileFilter;
    this.partitionFilter = partitionFilter;
    this.ignoreDeleted = ignoreDeleted;
    this.ignoreExisting = ignoreExisting;
    this.columns = columns;
    this.caseSensitive = caseSensitive;
    if (specsById == null) {
      this.evalCache = null;
    } else {
      this.evalCache = Caffeine.newBuilder().build(specId -> {
        PartitionSpec spec = specsById.get(specId);
        return ManifestEvaluator.forPartitionFilter(
            Expressions.and(partitionFilter, Projections.inclusive(spec, caseSensitive).project(dataFilter)),
            spec, caseSensitive);
      });
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
  public CloseableIterable<ManifestEntry> entries() {
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

    Iterable<CloseableIterable<ManifestEntry>> readers = Iterables.transform(
        matchingManifests,
        manifest -> {
          ManifestReader reader = ManifestReader.read(
              io.newInputFile(manifest.path()),
              specsById);

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

  static Builder builder(FileIO io, Iterable<ManifestFile> manifests) {
    return new Builder(io, manifests);
  }

  static class Builder {
    private final FileIO io;
    private final Set<ManifestFile> manifests;
    private Map<Integer, PartitionSpec> specsById;
    private Expression dataFilter;
    private Expression fileFilter;
    private Expression partitionFilter;
    private boolean ignoreDeleted;
    private boolean ignoreExisting;
    private List<String> columns;
    private boolean caseSensitive;

    Builder(FileIO io, Iterable<ManifestFile> manifests) {
      this.io = io;
      this.manifests = Sets.newHashSet(manifests);
      this.dataFilter = Expressions.alwaysTrue();
      this.fileFilter = Expressions.alwaysTrue();
      this.partitionFilter = Expressions.alwaysTrue();
      this.ignoreDeleted = false;
      this.ignoreExisting = false;
      this.columns = ImmutableList.of("*");
      this.caseSensitive = true;
    }

    Builder specsById(Map<Integer, PartitionSpec> newSpecsById) {
      this.specsById = newSpecsById;
      return this;
    }

    Builder filterData(Expression newDataFilter) {
      this.dataFilter = Expressions.and(dataFilter, newDataFilter);
      return this;
    }

    Builder filterFiles(Expression newFileFilter) {
      this.fileFilter = Expressions.and(fileFilter, newFileFilter);
      return this;
    }

    Builder filterPartitions(Expression newPartitionFilter) {
      this.partitionFilter = Expressions.and(partitionFilter, newPartitionFilter);
      return this;
    }

    Builder ignoreDeleted() {
      this.ignoreDeleted = true;
      return this;
    }

    Builder ignoreExisting() {
      this.ignoreExisting = true;
      return this;
    }

    Builder select(List<String> newColumns) {
      this.columns = Lists.newArrayList(newColumns);
      return this;
    }

    Builder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    ManifestGroup build() {
      return new ManifestGroup(io, manifests, specsById, dataFilter, fileFilter, partitionFilter, ignoreDeleted,
          ignoreExisting, columns, caseSensitive);
    }
  }
}
