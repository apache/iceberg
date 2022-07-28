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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;

abstract class BaseScan<ThisT, T extends ScanTask, G extends ScanTaskGroup<T>>
    implements Scan<ThisT, T, G> {
  private final TableOperations ops;
  private final Table table;
  private final Schema schema;
  private final TableScanContext context;

  protected BaseScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    this.ops = ops;
    this.table = table;
    this.schema = schema;
    this.context = context;
  }

  protected TableOperations tableOps() {
    return ops;
  }

  protected Table table() {
    return table;
  }

  protected Schema tableSchema() {
    return schema;
  }

  protected TableScanContext context() {
    return context;
  }

  protected abstract ThisT newRefinedScan(
      TableOperations newOps, Table newTable, Schema newSchema, TableScanContext newContext);

  @Override
  public ThisT option(String property, String value) {
    return newRefinedScan(ops, table, schema, context.withOption(property, value));
  }

  @Override
  public ThisT project(Schema projectedSchema) {
    return newRefinedScan(ops, table, schema, context.project(projectedSchema));
  }

  @Override
  public ThisT caseSensitive(boolean caseSensitive) {
    return newRefinedScan(ops, table, schema, context.setCaseSensitive(caseSensitive));
  }

  @Override
  public ThisT includeColumnStats() {
    return newRefinedScan(ops, table, schema, context.shouldReturnColumnStats(true));
  }

  @Override
  public ThisT select(Collection<String> columns) {
    return newRefinedScan(ops, table, schema, context.selectColumns(columns));
  }

  @Override
  public ThisT filter(Expression expr) {
    return newRefinedScan(
        ops, table, schema, context.filterRows(Expressions.and(context.rowFilter(), expr)));
  }

  @Override
  public ThisT ignoreResiduals() {
    return newRefinedScan(ops, table, schema, context.ignoreResiduals(true));
  }

  @Override
  public ThisT planWith(ExecutorService executorService) {
    return newRefinedScan(ops, table, schema, context.planWith(executorService));
  }

  @Override
  public Schema schema() {
    return lazyColumnProjection(context, schema);
  }

  @Override
  public long targetSplitSize() {
    long tableValue =
        ops.current()
            .propertyAsLong(TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);
    return PropertyUtil.propertyAsLong(context.options(), TableProperties.SPLIT_SIZE, tableValue);
  }

  @Override
  public int splitLookback() {
    int tableValue =
        ops.current()
            .propertyAsInt(TableProperties.SPLIT_LOOKBACK, TableProperties.SPLIT_LOOKBACK_DEFAULT);
    return PropertyUtil.propertyAsInt(
        context.options(), TableProperties.SPLIT_LOOKBACK, tableValue);
  }

  @Override
  public long splitOpenFileCost() {
    long tableValue =
        ops.current()
            .propertyAsLong(
                TableProperties.SPLIT_OPEN_FILE_COST, TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
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
}
