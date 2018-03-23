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

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.ResidualEvaluator;
import com.netflix.iceberg.io.ClosingIterable;
import com.netflix.iceberg.util.ParallelIterable;
import java.util.Collection;
import java.util.Collections;

import static com.netflix.iceberg.util.ThreadPools.getPlannerPool;
import static com.netflix.iceberg.util.ThreadPools.getWorkerPool;

/**
 * Base class for {@link TableScan} implementations.
 */
class BaseTableScan extends ClosingIterable implements TableScan {
  private final TableOperations ops;
  private final Table table;
  private final Collection<String> columns;
  private final Expression rowFilter;

  BaseTableScan(TableOperations ops, Table table) {
    this(ops, table, Filterable.ALL_COLUMNS, Expressions.alwaysTrue());
  }

  private BaseTableScan(TableOperations ops, Table table, Collection<String> columns, Expression rowFilter) {
    this.ops = ops;
    this.table = table;
    this.columns = columns;
    this.rowFilter = rowFilter;
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public TableScan select(Collection<String> columns) {
    return new BaseTableScan(ops, table, columns, rowFilter);
  }

  @Override
  public TableScan filter(Expression expr) {
    return new BaseTableScan(ops, table, columns, Expressions.and(rowFilter, expr));
  }

  @Override
  public Iterable<FileScanTask> planFiles() {
    Snapshot snapshot = ops.current().currentSnapshot();
    if (snapshot != null) {
      Iterable<Iterable<FileScanTask>> readers = Iterables.transform(
          snapshot.manifests(),
          manifest -> {
            ManifestReader reader = ManifestReader.read(ops.newInputFile(manifest));
            addCloseable(reader);
            String schemaString = SchemaParser.toJson(reader.spec().schema());
            String specString = PartitionSpecParser.toJson(reader.spec());
            ResidualEvaluator residuals = new ResidualEvaluator(reader.spec(), rowFilter);
            return Iterables.transform(
                reader.filterRows(rowFilter).select(columns),
                file -> new BaseFileScanTask(file, schemaString, specString, residuals)
            );
          });

      if (snapshot.manifests().size() > 1) {
        return new ParallelIterable<>(readers, getPlannerPool(), getWorkerPool());
      } else {
        return Iterables.concat(readers);
      }

    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public Iterable<CombinedScanTask> planTasks() {
    return Iterables.transform(planFiles(), BaseCombinedScanTask::new);
  }

  @Override
  public Expression filter() {
    return rowFilter;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("table", table)
        .add("columns", columns)
        .add("filter", rowFilter)
        .toString();
  }
}
