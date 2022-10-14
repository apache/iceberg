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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.ThreadPools;

/** Context object with optional arguments for a TableScan. */
final class TableScanContext {
  private final Long snapshotId;
  private final Expression rowFilter;
  private final boolean ignoreResiduals;
  private final boolean caseSensitive;
  private final boolean colStats;
  private final Schema projectedSchema;
  private final Collection<String> selectedColumns;
  private final ImmutableMap<String, String> options;
  private final Long fromSnapshotId;
  private final Long toSnapshotId;
  private final ExecutorService planExecutor;
  private final boolean fromSnapshotInclusive;
  private final MetricsReporter metricsReporter;

  TableScanContext() {
    this.snapshotId = null;
    this.rowFilter = Expressions.alwaysTrue();
    this.ignoreResiduals = false;
    this.caseSensitive = true;
    this.colStats = false;
    this.projectedSchema = null;
    this.selectedColumns = null;
    this.options = ImmutableMap.of();
    this.fromSnapshotId = null;
    this.toSnapshotId = null;
    this.planExecutor = null;
    this.fromSnapshotInclusive = false;
    this.metricsReporter = new LoggingMetricsReporter();
  }

  private TableScanContext(
      Long snapshotId,
      Expression rowFilter,
      boolean ignoreResiduals,
      boolean caseSensitive,
      boolean colStats,
      Schema projectedSchema,
      Collection<String> selectedColumns,
      ImmutableMap<String, String> options,
      Long fromSnapshotId,
      Long toSnapshotId,
      ExecutorService planExecutor,
      boolean fromSnapshotInclusive,
      MetricsReporter metricsReporter) {
    this.snapshotId = snapshotId;
    this.rowFilter = rowFilter;
    this.ignoreResiduals = ignoreResiduals;
    this.caseSensitive = caseSensitive;
    this.colStats = colStats;
    this.projectedSchema = projectedSchema;
    this.selectedColumns = selectedColumns;
    this.options = options;
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;
    this.planExecutor = planExecutor;
    this.fromSnapshotInclusive = fromSnapshotInclusive;
    this.metricsReporter = metricsReporter;
  }

  Long snapshotId() {
    return snapshotId;
  }

  TableScanContext useSnapshotId(Long scanSnapshotId) {
    return Builder.builder(this).snapshotId(scanSnapshotId).build();
  }

  Expression rowFilter() {
    return rowFilter;
  }

  TableScanContext filterRows(Expression filter) {
    return Builder.builder(this).rowFilter(filter).build();
  }

  boolean ignoreResiduals() {
    return ignoreResiduals;
  }

  TableScanContext ignoreResiduals(boolean shouldIgnoreResiduals) {
    return Builder.builder(this).ignoreResiduals(shouldIgnoreResiduals).build();
  }

  boolean caseSensitive() {
    return caseSensitive;
  }

  TableScanContext setCaseSensitive(boolean isCaseSensitive) {
    return Builder.builder(this).caseSensitive(isCaseSensitive).build();
  }

  boolean returnColumnStats() {
    return colStats;
  }

  TableScanContext shouldReturnColumnStats(boolean returnColumnStats) {
    return Builder.builder(this).colStats(returnColumnStats).build();
  }

  Collection<String> selectedColumns() {
    return selectedColumns;
  }

  TableScanContext selectColumns(Collection<String> columns) {
    Preconditions.checkState(
        projectedSchema == null, "Cannot select columns when projection schema is set");
    return Builder.builder(this).projectedSchema(null).selectedColumns(columns).build();
  }

  Schema projectedSchema() {
    return projectedSchema;
  }

  TableScanContext project(Schema schema) {
    Preconditions.checkState(
        selectedColumns == null, "Cannot set projection schema when columns are selected");
    return Builder.builder(this).projectedSchema(schema).selectedColumns(null).build();
  }

  Map<String, String> options() {
    return options;
  }

  TableScanContext withOption(String property, String value) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options);
    builder.put(property, value);

    return Builder.builder(this).options(builder.build()).build();
  }

  Long fromSnapshotId() {
    return fromSnapshotId;
  }

  TableScanContext fromSnapshotIdExclusive(long id) {
    return Builder.builder(this).fromSnapshotId(id).fromSnapshotInclusive(false).build();
  }

  TableScanContext fromSnapshotIdInclusive(long id) {
    return Builder.builder(this).fromSnapshotId(id).fromSnapshotInclusive(true).build();
  }

  boolean fromSnapshotInclusive() {
    return fromSnapshotInclusive;
  }

  Long toSnapshotId() {
    return toSnapshotId;
  }

  TableScanContext toSnapshotId(long id) {
    return Builder.builder(this).toSnapshotId(id).build();
  }

  ExecutorService planExecutor() {
    return Optional.ofNullable(planExecutor).orElseGet(ThreadPools::getWorkerPool);
  }

  boolean planWithCustomizedExecutor() {
    return planExecutor != null;
  }

  TableScanContext planWith(ExecutorService executor) {
    return Builder.builder(this).planExecutor(executor).build();
  }

  MetricsReporter metricsReporter() {
    return metricsReporter;
  }

  TableScanContext reportWith(MetricsReporter reporter) {
    return Builder.builder(this).metricsReporter(reporter).build();
  }

  private static final class Builder {
    private Long snapshotId;
    private Expression rowFilter;
    private boolean ignoreResiduals;
    private boolean caseSensitive;
    private boolean colStats;
    private Schema projectedSchema;
    private Collection<String> selectedColumns;
    private ImmutableMap<String, String> options;
    private Long fromSnapshotId;
    private Long toSnapshotId;
    private ExecutorService planExecutor;
    private boolean fromSnapshotInclusive;
    private MetricsReporter metricsReporter;

    Builder(TableScanContext context) {
      this.snapshotId(context.snapshotId)
          .rowFilter(context.rowFilter)
          .ignoreResiduals(context.ignoreResiduals)
          .caseSensitive(context.caseSensitive)
          .colStats(context.colStats)
          .projectedSchema(context.projectedSchema)
          .selectedColumns(context.selectedColumns)
          .options(context.options)
          .fromSnapshotId(context.fromSnapshotId)
          .toSnapshotId(context.toSnapshotId)
          .planExecutor(context.planExecutor)
          .fromSnapshotInclusive(context.fromSnapshotInclusive)
          .metricsReporter(context.metricsReporter);
    }

    static Builder builder(TableScanContext context) {
      return new Builder(context);
    }

    Builder snapshotId(Long newSnapshotId) {
      this.snapshotId = newSnapshotId;
      return this;
    }

    Builder rowFilter(Expression newRowFilter) {
      this.rowFilter = newRowFilter;
      return this;
    }

    Builder ignoreResiduals(boolean newIgnoreResiduals) {
      this.ignoreResiduals = newIgnoreResiduals;
      return this;
    }

    Builder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    Builder colStats(boolean newColStats) {
      this.colStats = newColStats;
      return this;
    }

    Builder projectedSchema(Schema newProjectedSchema) {
      this.projectedSchema = newProjectedSchema;
      return this;
    }

    Builder selectedColumns(Collection<String> newSelectedColumns) {
      this.selectedColumns = newSelectedColumns;
      return this;
    }

    Builder options(ImmutableMap<String, String> newOptions) {
      this.options = newOptions;
      return this;
    }

    Builder fromSnapshotId(Long newFromSnapshotId) {
      this.fromSnapshotId = newFromSnapshotId;
      return this;
    }

    Builder toSnapshotId(Long newToSnapshotId) {
      this.toSnapshotId = newToSnapshotId;
      return this;
    }

    Builder planExecutor(ExecutorService newPlanExecutor) {
      this.planExecutor = newPlanExecutor;
      return this;
    }

    Builder fromSnapshotInclusive(boolean newFromSnapshotInclusive) {
      this.fromSnapshotInclusive = newFromSnapshotInclusive;
      return this;
    }

    Builder metricsReporter(MetricsReporter newMetricsReporter) {
      this.metricsReporter = newMetricsReporter;
      return this;
    }

    TableScanContext build() {
      return new TableScanContext(
          snapshotId,
          rowFilter,
          ignoreResiduals,
          caseSensitive,
          colStats,
          projectedSchema,
          selectedColumns,
          options,
          fromSnapshotId,
          toSnapshotId,
          planExecutor,
          fromSnapshotInclusive,
          metricsReporter);
    }
  }
}
