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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.netflix.iceberg.expressions.Evaluator;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.Projections;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;

public class FilteredManifest implements Filterable<FilteredManifest> {
  private final ManifestReader reader;
  private final Expression partFilter;
  private final Collection<String> columns;

  FilteredManifest(ManifestReader reader, Expression filter, Collection<String> columns) {
    Preconditions.checkNotNull(reader, "ManifestReader cannot be null");
    this.reader = reader;
    this.partFilter = filter;
    this.columns = columns;
  }

  @Override
  public FilteredManifest select(Collection<String> columns) {
    return new FilteredManifest(reader, partFilter, columns);
  }

  @Override
  public FilteredManifest filterPartitions(Expression expr) {
    return new FilteredManifest(reader, Expressions.and(partFilter, expr), columns);
  }

  @Override
  public FilteredManifest filterRows(Expression expr) {
    return filterPartitions(Projections.inclusive(reader.spec()).project(expr));
  }

  @Override
  public Iterator<DataFile> iterator() {
    return Iterators.transform(
        Iterators.filter(reader.iterator(partFilter, columns), new Predicate<DataFile>() {
          private final Evaluator evaluator = new Evaluator(reader.spec().partitionType(), partFilter);

          @Override
          public boolean apply(DataFile input) {
            return input != null && evaluator.eval(input.partition());
          }
        }), new Function<DataFile, DataFile>() {
          @Nullable
          @Override
          public DataFile apply(@Nullable DataFile input) {
            return input.copy();
          }
        });
  }
}
