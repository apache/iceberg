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

package com.netflix.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.iceberg.expressions.Evaluator;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.types.Types;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

class ManifestGroup {
  private static final Types.StructType EMPTY_STRUCT = Types.StructType.of();

  private final TableOperations ops;
  private final Set<String> manifests;
  private final Expression dataFilter;
  private final Expression fileFilter;
  private final boolean ignoreDeleted;
  private final List<String> columns;

  ManifestGroup(TableOperations ops, Iterable<String> manifests) {
    this(ops, Sets.newHashSet(manifests), Expressions.alwaysTrue(), Expressions.alwaysTrue(),
        false, ImmutableList.of("*"));
  }

  private ManifestGroup(TableOperations ops, Set<String> manifests,
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
    List<Closeable> toClose = Lists.newArrayList();

    Iterable<Iterable<ManifestEntry>> readers = Iterables.transform(
        manifests,
        manifest -> {
          ManifestReader reader = ManifestReader.read(ops.fileIo().newInputFile(manifest));
          FilteredManifest filtered = reader.filterRows(dataFilter).select(columns);
          toClose.add(reader);
          return Iterables.filter(
              ignoreDeleted ? filtered.liveEntries() : filtered.allEntries(),
              entry -> evaluator.eval((GenericDataFile) entry.file()));
        });

    return CloseableIterable.combine(Iterables.concat(readers), toClose);
  }
}
