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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@link TableScan} implementations.
 */
abstract class BaseTableScan implements TableScan {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTableScan.class);

  private final TableOperations ops;
  private final Table table;
  private final Schema schema;
  private final TableScanContext context;

  protected BaseTableScan(TableOperations ops, Table table, Schema schema) {
    this(ops, table, schema, new TableScanContext());
  }

  protected BaseTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    this.ops = ops;
    this.table = table;
    this.schema = schema;
    this.context = context;
  }

  protected TableOperations tableOps() {
    return ops;
  }

  protected Schema tableSchema() {
    return schema;
  }

  protected Long snapshotId() {
    return context.snapshotId();
  }

  protected boolean colStats() {
    return context.returnColumnStats();
  }

  protected boolean shouldIgnoreResiduals() {
    return context.ignoreResiduals();
  }

  protected Collection<String> selectedColumns() {
    return context.selectedColumns();
  }

  protected ExecutorService planExecutor() {
    return context.planExecutor();
  }

  protected Map<String, String> options() {
    return context.options();
  }

  protected TableScanContext context() {
    return context;
  }

  @SuppressWarnings("checkstyle:HiddenField")
  protected abstract TableScan newRefinedScan(TableOperations ops, Table table, Schema schema,
                                              TableScanContext context);

  protected abstract CloseableIterable<FileScanTask> doPlanFiles();

  @Override
  public Table table() {
    return table;
  }

  @Override
  public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    throw new UnsupportedOperationException("Incremental scan is not supported");
  }

  @Override
  public TableScan appendsAfter(long fromSnapshotId) {
    throw new UnsupportedOperationException("Incremental scan is not supported");
  }

  @Override
  public TableScan planWith(ExecutorService executorService) {
    return newRefinedScan(ops, table, schema, context.planWith(executorService));
  }

  @Override
  public TableScan useSnapshot(long scanSnapshotId) {
    Preconditions.checkArgument(snapshotId() == null,
        "Cannot override snapshot, already set to id=%s", snapshotId());
    Preconditions.checkArgument(ops.current().snapshot(scanSnapshotId) != null,
        "Cannot find snapshot with ID %s", scanSnapshotId);
    return newRefinedScan(ops, table, schema, context.useSnapshotId(scanSnapshotId));
  }

  @Override
  public TableScan asOfTime(long timestampMillis) {
    Preconditions.checkArgument(snapshotId() == null,
        "Cannot override snapshot, already set to id=%s", snapshotId());

    return useSnapshot(SnapshotUtil.snapshotIdAsOfTime(table(), timestampMillis));
  }

  @Override
  public TableScan option(String property, String value) {
    return newRefinedScan(ops, table, schema, context.withOption(property, value));
  }

  @Override
  public TableScan project(Schema projectedSchema) {
    return newRefinedScan(ops, table, schema, context.project(projectedSchema));
  }

  @Override
  public TableScan caseSensitive(boolean scanCaseSensitive) {
    return newRefinedScan(ops, table, schema, context.setCaseSensitive(scanCaseSensitive));
  }

  @Override
  public TableScan includeColumnStats() {
    return newRefinedScan(ops, table, schema, context.shouldReturnColumnStats(true));
  }

  @Override
  public TableScan select(Collection<String> columns) {
    return newRefinedScan(ops, table, schema, context.selectColumns(columns));
  }

  @Override
  public TableScan filter(Expression expr) {
    return newRefinedScan(ops, table, schema, context.filterRows(Expressions.and(filter(), expr)));
  }

  @Override
  public Expression filter() {
    return context.rowFilter();
  }

  @Override
  public TableScan ignoreResiduals() {
    return newRefinedScan(ops, table, schema, context.ignoreResiduals(true));
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    Snapshot snapshot = snapshot();
    if (snapshot != null) {
      LOG.info("Scanning table {} snapshot {} created at {} with filter {}", table,
          snapshot.snapshotId(), DateTimeUtil.formatTimestampMillis(snapshot.timestampMillis()),
          ExpressionUtil.toSanitizedString(filter()));

      Listeners.notifyAll(new ScanEvent(table.name(), snapshot.snapshotId(), filter(), schema()));

      return doPlanFiles();

    } else {
      LOG.info("Scanning empty table {}", table);
      return CloseableIterable.empty();
    }
  }

  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    CloseableIterable<FileScanTask> fileScanTasks = planFiles();
    CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(fileScanTasks, targetSplitSize());
    return TableScanUtil.planTasks(splitFiles, targetSplitSize(), splitLookback(), splitOpenFileCost());
  }

  @Override
  public int splitLookback() {
    int tableValue = tableOps().current().propertyAsInt(
        TableProperties.SPLIT_LOOKBACK,
        TableProperties.SPLIT_LOOKBACK_DEFAULT);
    return PropertyUtil.propertyAsInt(options(), TableProperties.SPLIT_LOOKBACK, tableValue);
  }

  @Override
  public long splitOpenFileCost() {
    long tableValue = tableOps().current().propertyAsLong(
        TableProperties.SPLIT_OPEN_FILE_COST,
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
    return PropertyUtil.propertyAsLong(options(), TableProperties.SPLIT_OPEN_FILE_COST, tableValue);
  }

  @Override
  public Schema schema() {
    return lazyColumnProjection();
  }

  @Override
  public Snapshot snapshot() {
    return snapshotId() != null ?
        ops.current().snapshot(snapshotId()) :
        ops.current().currentSnapshot();
  }

  @Override
  public boolean isCaseSensitive() {
    return context.caseSensitive();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table", table)
        .add("projection", schema().asStruct())
        .add("filter", filter())
        .add("ignoreResiduals", shouldIgnoreResiduals())
        .add("caseSensitive", isCaseSensitive())
        .toString();
  }

  /**
   * To be able to make refinements {@link #select(Collection)} and {@link #caseSensitive(boolean)} in any order,
   * we resolve the schema to be projected lazily here.
   *
   * @return the Schema to project
   */
  private Schema lazyColumnProjection() {
    if (selectedColumns() != null) {
      Set<Integer> requiredFieldIds = Sets.newHashSet();

      // all of the filter columns are required
      requiredFieldIds.addAll(
          Binder.boundReferences(schema.asStruct(),
              Collections.singletonList(filter()), isCaseSensitive()));

      // all of the projection columns are required
      Set<Integer> selectedIds;
      if (isCaseSensitive()) {
        selectedIds = TypeUtil.getProjectedIds(schema.select(selectedColumns()));
      } else {
        selectedIds = TypeUtil.getProjectedIds(schema.caseInsensitiveSelect(selectedColumns()));
      }
      requiredFieldIds.addAll(selectedIds);

      return TypeUtil.project(schema, requiredFieldIds);

    } else if (context.projectedSchema() != null) {
      return context.projectedSchema();
    }

    return schema;
  }
}
