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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;

abstract class BaseScan<ThisT, T extends ScanTask, G extends ScanTaskGroup<T>>
    implements Scan<ThisT, T, G> {

  protected static final List<String> SCAN_COLUMNS =
      ImmutableList.of(
          "snapshot_id",
          "file_path",
          "file_ordinal",
          "file_format",
          "block_size_in_bytes",
          "file_size_in_bytes",
          "record_count",
          "partition",
          "key_metadata",
          "split_offsets",
          "sort_order_id");

  private static final List<String> STATS_COLUMNS =
      ImmutableList.of(
          "value_counts",
          "null_value_counts",
          "nan_value_counts",
          "lower_bounds",
          "upper_bounds",
          "column_sizes",
          "geom_lower_bounds",
          "geom_upper_bounds");

  protected static final List<String> SCAN_WITH_STATS_COLUMNS =
      ImmutableList.<String>builder().addAll(SCAN_COLUMNS).addAll(STATS_COLUMNS).build();

  protected static final List<String> DELETE_SCAN_COLUMNS =
      ImmutableList.of(
          "snapshot_id",
          "content",
          "file_path",
          "file_ordinal",
          "file_format",
          "block_size_in_bytes",
          "file_size_in_bytes",
          "record_count",
          "partition",
          "key_metadata",
          "split_offsets",
          "equality_ids");

  protected static final List<String> DELETE_SCAN_WITH_STATS_COLUMNS =
      ImmutableList.<String>builder().addAll(DELETE_SCAN_COLUMNS).addAll(STATS_COLUMNS).build();

  protected static final boolean PLAN_SCANS_WITH_WORKER_POOL =
      SystemConfigs.SCAN_THREAD_POOL_ENABLED.value();

  private final Table table;
  private final Schema schema;
  private final TableScanContext context;

  protected BaseScan(Table table, Schema schema, TableScanContext context) {
    this.table = table;
    this.schema = schema;
    this.context = context;
  }

  public Table table() {
    return table;
  }

  protected FileIO io() {
    return table.io();
  }

  protected Schema tableSchema() {
    return schema;
  }

  protected TableScanContext context() {
    return context;
  }

  protected Map<String, String> options() {
    return context().options();
  }

  protected List<String> scanColumns() {
    return context.returnColumnStats() ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS;
  }

  protected boolean shouldReturnColumnStats() {
    return context().returnColumnStats();
  }

  protected Set<Integer> columnsToKeepStats() {
    return context().columnsToKeepStats();
  }

  protected boolean shouldIgnoreResiduals() {
    return context().ignoreResiduals();
  }

  protected Expression residualFilter() {
    return shouldIgnoreResiduals() ? Expressions.alwaysTrue() : filter();
  }

  protected boolean shouldPlanWithExecutor() {
    return PLAN_SCANS_WITH_WORKER_POOL || context().planWithCustomizedExecutor();
  }

  protected ExecutorService planExecutor() {
    return context().planExecutor();
  }

  protected abstract ThisT newRefinedScan(
      Table newTable, Schema newSchema, TableScanContext newContext);

  @Override
  public ThisT option(String property, String value) {
    return newRefinedScan(table, schema, context.withOption(property, value));
  }

  @Override
  public ThisT project(Schema projectedSchema) {
    return newRefinedScan(table, schema, context.project(projectedSchema));
  }

  @Override
  public ThisT caseSensitive(boolean caseSensitive) {
    return newRefinedScan(table, schema, context.setCaseSensitive(caseSensitive));
  }

  @Override
  public boolean isCaseSensitive() {
    return context().caseSensitive();
  }

  @Override
  public ThisT includeColumnStats() {
    return newRefinedScan(table, schema, context.shouldReturnColumnStats(true));
  }

  @Override
  public ThisT includeColumnStats(Collection<String> requestedColumns) {
    return newRefinedScan(
        table,
        schema,
        context
            .shouldReturnColumnStats(true)
            .columnsToKeepStats(
                requestedColumns.stream()
                    .map(c -> schema.findField(c).fieldId())
                    .collect(Collectors.toSet())));
  }

  @Override
  public ThisT select(Collection<String> columns) {
    return newRefinedScan(table, schema, context.selectColumns(columns));
  }

  @Override
  public ThisT filter(Expression expr) {
    return newRefinedScan(
        table, schema, context.filterRows(Expressions.and(context.rowFilter(), expr)));
  }

  @Override
  public Expression filter() {
    return context().rowFilter();
  }

  @Override
  public ThisT ignoreResiduals() {
    return newRefinedScan(table, schema, context.ignoreResiduals(true));
  }

  @Override
  public ThisT planWith(ExecutorService executorService) {
    return newRefinedScan(table, schema, context.planWith(executorService));
  }

  @Override
  public Schema schema() {
    return lazyColumnProjection(context, schema);
  }

  @Override
  public long targetSplitSize() {
    long tableValue =
        PropertyUtil.propertyAsLong(
            table().properties(), TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);
    return PropertyUtil.propertyAsLong(context.options(), TableProperties.SPLIT_SIZE, tableValue);
  }

  @Override
  public int splitLookback() {
    int tableValue =
        PropertyUtil.propertyAsInt(
            table().properties(),
            TableProperties.SPLIT_LOOKBACK,
            TableProperties.SPLIT_LOOKBACK_DEFAULT);
    return PropertyUtil.propertyAsInt(
        context.options(), TableProperties.SPLIT_LOOKBACK, tableValue);
  }

  @Override
  public long splitOpenFileCost() {
    long tableValue =
        PropertyUtil.propertyAsLong(
            table().properties(),
            TableProperties.SPLIT_OPEN_FILE_COST,
            TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
    return PropertyUtil.propertyAsLong(
        context.options(), TableProperties.SPLIT_OPEN_FILE_COST, tableValue);
  }

  /**
   * Resolve the schema to be projected lazily.
   *
   * <p>If there are selected columns from scan context, selected columns are projected to the table
   * schema. Otherwise, projected schema from scan context shall be returned.
   *
   * @param context scan context
   * @param schema table schema
   * @return the Schema to project
   */
  private static Schema lazyColumnProjection(TableScanContext context, Schema schema) {
    Collection<String> selectedColumns = context.selectedColumns();
    if (selectedColumns != null) {
      Set<Integer> requiredFieldIds = Sets.newHashSet();

      // all of the filter columns are required
      requiredFieldIds.addAll(
          Binder.boundReferences(
              schema.asStruct(),
              Collections.singletonList(context.rowFilter()),
              context.caseSensitive()));

      // all of the projection columns are required
      Set<Integer> selectedIds;
      if (context.caseSensitive()) {
        selectedIds = TypeUtil.getProjectedIds(schema.select(selectedColumns));
      } else {
        selectedIds = TypeUtil.getProjectedIds(schema.caseInsensitiveSelect(selectedColumns));
      }
      requiredFieldIds.addAll(selectedIds);

      return TypeUtil.project(schema, requiredFieldIds);

    } else if (context.projectedSchema() != null) {
      return context.projectedSchema();
    }

    return schema;
  }

  @Override
  public ThisT metricsReporter(MetricsReporter reporter) {
    return newRefinedScan(table, schema, context.reportWith(reporter));
  }

  /**
   * Retrieves a list of column names based on the type of manifest content provided.
   *
   * @param content the manifest content type to scan.
   * @return a list of column names corresponding to the specified manifest content type.
   */
  static List<String> scanColumns(ManifestContent content) {
    switch (content) {
      case DATA:
        return BaseScan.SCAN_COLUMNS;
      case DELETES:
        return BaseScan.DELETE_SCAN_COLUMNS;
      default:
        throw new UnsupportedOperationException("Cannot read unknown manifest type: " + content);
    }
  }
}
