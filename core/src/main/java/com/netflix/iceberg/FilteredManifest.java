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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.netflix.iceberg.expressions.Evaluator;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.InclusiveMetricsEvaluator;
import com.netflix.iceberg.expressions.Projections;
import java.util.Collection;
import java.util.Iterator;

public class FilteredManifest implements Filterable<FilteredManifest> {
  private final ManifestReader reader;
  private final Expression partFilter;
  private final Expression rowFilter;
  private final Collection<String> columns;

  FilteredManifest(ManifestReader reader, Expression partFilter, Expression rowFilter,
                   Collection<String> columns) {
    Preconditions.checkNotNull(reader, "ManifestReader cannot be null");
    this.reader = reader;
    this.partFilter = partFilter;
    this.rowFilter = rowFilter;
    this.columns = columns;
  }

  @Override
  public FilteredManifest select(Collection<String> columns) {
    return new FilteredManifest(reader, partFilter, rowFilter, columns);
  }

  @Override
  public FilteredManifest filterPartitions(Expression expr) {
    return new FilteredManifest(reader,
        Expressions.and(partFilter, expr),
        rowFilter,
        columns);
  }

  @Override
  public FilteredManifest filterRows(Expression expr) {
    Expression projected = Projections.inclusive(reader.spec()).project(expr);
    return new FilteredManifest(reader,
        Expressions.and(partFilter, projected),
        Expressions.and(rowFilter, expr),
        columns);
  }

  @Override
  public Iterator<DataFile> iterator() {
    Evaluator evaluator = new Evaluator(reader.spec().partitionType(), partFilter);
    InclusiveMetricsEvaluator metricsEvaluator =
        new InclusiveMetricsEvaluator(reader.spec().schema(), rowFilter);

    return Iterators.transform(
        Iterators.filter(reader.iterator(partFilter, columns),
            input -> (input != null &&
                evaluator.eval(input.partition()) &&
                metricsEvaluator.eval(input))),
        DataFile::copy);
  }
}
