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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;

public class FilteredManifest implements Filterable<FilteredManifest> {
  private static final Set<String> STATS_COLUMNS = Sets.newHashSet(
      "value_counts", "null_value_counts", "lower_bounds", "upper_bounds");

  private final ManifestReader reader;
  private final Expression partFilter;
  private final Expression rowFilter;
  private final Schema fileSchema;
  private final Collection<String> columns;
  private final boolean caseSensitive;

  // lazy state
  private Evaluator lazyEvaluator = null;
  private InclusiveMetricsEvaluator lazyMetricsEvaluator = null;

  FilteredManifest(ManifestReader reader, Expression partFilter, Expression rowFilter,
                   Schema fileSchema, Collection<String> columns, boolean caseSensitive) {
    Preconditions.checkNotNull(reader, "ManifestReader cannot be null");
    this.reader = reader;
    this.partFilter = partFilter;
    this.rowFilter = rowFilter;
    this.fileSchema = fileSchema;
    this.columns = columns;
    this.caseSensitive = caseSensitive;
  }

  @Override
  public FilteredManifest select(Collection<String> selectedColumns) {
    return new FilteredManifest(reader, partFilter, rowFilter, fileSchema, selectedColumns, caseSensitive);
  }

  @Override
  public FilteredManifest project(Schema fileProjection) {
    return new FilteredManifest(reader, partFilter, rowFilter, fileProjection, columns, caseSensitive);
  }

  @Override
  public FilteredManifest filterPartitions(Expression expr) {
    return new FilteredManifest(
        reader, Expressions.and(partFilter, expr), rowFilter, fileSchema, columns, caseSensitive);
  }

  @Override
  public FilteredManifest filterRows(Expression expr) {
    return new FilteredManifest(
        reader, partFilter, Expressions.and(rowFilter, expr), fileSchema, columns, caseSensitive);
  }

  @Override
  public FilteredManifest caseSensitive(boolean isCaseSensitive) {
    return new FilteredManifest(reader, partFilter, rowFilter, fileSchema, columns, isCaseSensitive);
  }

  CloseableIterable<ManifestEntry> allEntries() {
    if ((rowFilter != null && rowFilter != Expressions.alwaysTrue()) ||
        (partFilter != null && partFilter != Expressions.alwaysTrue())) {
      Evaluator evaluator = evaluator();
      InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();

      return CloseableIterable.filter(reader.entries(projection(fileSchema, columns, caseSensitive)),
          entry -> entry != null &&
              evaluator.eval(entry.file().partition()) &&
              metricsEvaluator.eval(entry.file()));

    } else {
      return reader.entries(projection(fileSchema, columns, caseSensitive));
    }
  }

  CloseableIterable<ManifestEntry> liveEntries() {
    if ((rowFilter != null && rowFilter != Expressions.alwaysTrue()) ||
        (partFilter != null && partFilter != Expressions.alwaysTrue())) {
      Evaluator evaluator = evaluator();
      InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();

      return CloseableIterable.filter(reader.entries(projection(fileSchema, columns, caseSensitive)),
          entry -> entry != null &&
              entry.status() != Status.DELETED &&
              evaluator.eval(entry.file().partition()) &&
              metricsEvaluator.eval(entry.file()));

    } else {
      return CloseableIterable.filter(reader.entries(projection(fileSchema, columns, caseSensitive)),
          entry -> entry != null && entry.status() != Status.DELETED);
    }
  }

  @Override
  public Iterator<DataFile> iterator() {
    if ((rowFilter != null && rowFilter != Expressions.alwaysTrue()) ||
        (partFilter != null && partFilter != Expressions.alwaysTrue())) {
      Evaluator evaluator = evaluator();
      InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();

      // ensure stats columns are present for metrics evaluation
      List<String> projectColumns = Lists.newArrayList(columns);
      projectColumns.addAll(STATS_COLUMNS); // order doesn't matter

      // if no stats columns were projected, drop them
      boolean dropStats = Sets.intersection(Sets.newHashSet(columns), STATS_COLUMNS).isEmpty();

      return Iterators.transform(
          Iterators.filter(reader.iterator(partFilter, projection(fileSchema, projectColumns, caseSensitive)),
              input -> input != null &&
                  evaluator.eval(input.partition()) &&
                  metricsEvaluator.eval(input)),
          dropStats ? DataFile::copyWithoutStats : DataFile::copy);

    } else {
      return Iterators.transform(
          reader.iterator(partFilter, projection(fileSchema, columns, caseSensitive)),
          DataFile::copy);
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  private static Schema projection(Schema fileSchema, Collection<String> columns, boolean caseSensitive) {
    if (columns != null) {
      if (caseSensitive) {
        return fileSchema.select(columns);
      } else {
        return fileSchema.caseInsensitiveSelect(columns);
      }
    }

    return fileSchema;
  }

  private Evaluator evaluator() {
    if (lazyEvaluator == null) {
      Expression projected = Projections.inclusive(reader.spec(), caseSensitive).project(rowFilter);
      Expression finalPartFilter = Expressions.and(projected, partFilter);
      if (finalPartFilter != null) {
        this.lazyEvaluator = new Evaluator(reader.spec().partitionType(), finalPartFilter, caseSensitive);
      } else {
        this.lazyEvaluator = new Evaluator(reader.spec().partitionType(), Expressions.alwaysTrue(), caseSensitive);
      }
    }
    return lazyEvaluator;
  }

  private InclusiveMetricsEvaluator metricsEvaluator() {
    if (lazyMetricsEvaluator == null) {
      if (rowFilter != null) {
        this.lazyMetricsEvaluator = new InclusiveMetricsEvaluator(
            reader.spec().schema(), rowFilter, caseSensitive);
      } else {
        this.lazyMetricsEvaluator = new InclusiveMetricsEvaluator(
            reader.spec().schema(), Expressions.alwaysTrue(), caseSensitive);
      }
    }
    return lazyMetricsEvaluator;
  }
}
