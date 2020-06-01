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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@link TableScan} implementations.
 */
@SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
abstract class BaseTableScan implements TableScan {
  private static final Logger LOG = LoggerFactory.getLogger(TableScan.class);

  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  private final TableOperations ops;
  private final Table table;
  private final Long snapshotId;
  private final Schema schema;
  private final Expression rowFilter;
  private final boolean ignoreResiduals;
  private final boolean caseSensitive;
  private final boolean colStats;
  private final Collection<String> selectedColumns;
  private final ImmutableMap<String, String> options;

  protected BaseTableScan(TableOperations ops, Table table, Schema schema) {
    this(ops, table, null, schema, Expressions.alwaysTrue(), false, true, false, null, ImmutableMap.of());
  }

  protected BaseTableScan(TableOperations ops, Table table, Long snapshotId, Schema schema,
                          Expression rowFilter, boolean ignoreResiduals, boolean caseSensitive, boolean colStats,
                          Collection<String> selectedColumns, ImmutableMap<String, String> options) {
    this.ops = ops;
    this.table = table;
    this.snapshotId = snapshotId;
    this.schema = schema;
    this.rowFilter = rowFilter;
    this.ignoreResiduals = ignoreResiduals;
    this.caseSensitive = caseSensitive;
    this.colStats = colStats;
    this.selectedColumns = selectedColumns;
    this.options = options != null ? options : ImmutableMap.of();
  }

  protected TableOperations tableOps() {
    return ops;
  }

  protected Long snapshotId() {
    return snapshotId;
  }

  protected boolean colStats() {
    return colStats;
  }

  protected boolean shouldIgnoreResiduals() {
    return ignoreResiduals;
  }

  protected Collection<String> selectedColumns() {
    return selectedColumns;
  }

  protected ImmutableMap<String, String> options() {
    return options;
  }

  @SuppressWarnings("checkstyle:HiddenField")
  protected abstract long targetSplitSize(TableOperations ops);

  @SuppressWarnings("checkstyle:HiddenField")
  protected abstract TableScan newRefinedScan(
      TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
      boolean ignoreResiduals, boolean caseSensitive, boolean colStats, Collection<String> selectedColumns,
      ImmutableMap<String, String> options);

  @SuppressWarnings("checkstyle:HiddenField")
  protected abstract CloseableIterable<FileScanTask> planFiles(
      TableOperations ops, Snapshot snapshot, Expression rowFilter,
      boolean ignoreResiduals, boolean caseSensitive, boolean colStats);

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
  public TableScan useSnapshot(long scanSnapshotId) {
    Preconditions.checkArgument(this.snapshotId == null,
        "Cannot override snapshot, already set to id=%s", snapshotId);
    Preconditions.checkArgument(ops.current().snapshot(scanSnapshotId) != null,
        "Cannot find snapshot with ID %s", scanSnapshotId);
    return newRefinedScan(
        ops, table, scanSnapshotId, schema, rowFilter, ignoreResiduals,
        caseSensitive, colStats, selectedColumns, options);
  }

  @Override
  public TableScan asOfTime(long timestampMillis) {
    Preconditions.checkArgument(this.snapshotId == null,
        "Cannot override snapshot, already set to id=%s", snapshotId);

    Long lastSnapshotId = null;
    for (HistoryEntry logEntry : ops.current().snapshotLog()) {
      if (logEntry.timestampMillis() <= timestampMillis) {
        lastSnapshotId = logEntry.snapshotId();
      }
    }

    // the snapshot ID could be null if no entries were older than the requested time. in that case,
    // there is no valid snapshot to read.
    Preconditions.checkArgument(lastSnapshotId != null,
        "Cannot find a snapshot older than %s", formatTimestampMillis(timestampMillis));

    return useSnapshot(lastSnapshotId);
  }

  @Override
  public TableScan option(String property, String value) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(options);
    builder.put(property, value);

    return newRefinedScan(
        ops, table, snapshotId, schema, rowFilter, ignoreResiduals,
        caseSensitive, colStats, selectedColumns, builder.build());
  }

  @Override
  public TableScan project(Schema projectedSchema) {
    return newRefinedScan(
        ops, table, snapshotId, projectedSchema, rowFilter, ignoreResiduals,
        caseSensitive, colStats, selectedColumns, options);
  }

  @Override
  public TableScan caseSensitive(boolean scanCaseSensitive) {
    return newRefinedScan(
        ops, table, snapshotId, schema, rowFilter, ignoreResiduals,
        scanCaseSensitive, colStats, selectedColumns, options);
  }

  @Override
  public TableScan includeColumnStats() {
    return newRefinedScan(
        ops, table, snapshotId, schema, rowFilter, ignoreResiduals,
        caseSensitive, true, selectedColumns, options);
  }

  @Override
  public TableScan select(Collection<String> columns) {
    return newRefinedScan(
        ops, table, snapshotId, schema, rowFilter, ignoreResiduals,
        caseSensitive, colStats, columns, options);
  }

  @Override
  public TableScan filter(Expression expr) {
    return newRefinedScan(
        ops, table, snapshotId, schema, Expressions.and(rowFilter, expr),
        ignoreResiduals, caseSensitive, colStats, selectedColumns, options);
  }

  @Override
  public Expression filter() {
    return rowFilter;
  }

  @Override
  public TableScan ignoreResiduals() {
    return newRefinedScan(
        ops, table, snapshotId, schema, rowFilter, true,
        caseSensitive, colStats, selectedColumns, options);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    Snapshot snapshot = snapshot();
    if (snapshot != null) {
      LOG.info("Scanning table {} snapshot {} created at {} with filter {}", table,
          snapshot.snapshotId(), formatTimestampMillis(snapshot.timestampMillis()),
          rowFilter);

      Listeners.notifyAll(
          new ScanEvent(table.toString(), snapshot.snapshotId(), rowFilter, schema()));

      return planFiles(ops, snapshot, rowFilter, ignoreResiduals, caseSensitive, colStats);

    } else {
      LOG.info("Scanning empty table {}", table);
      return CloseableIterable.empty();
    }
  }

  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    long splitSize;
    if (options.containsKey(TableProperties.SPLIT_SIZE)) {
      splitSize = Long.parseLong(options.get(TableProperties.SPLIT_SIZE));
    } else {
      splitSize = targetSplitSize(ops);
    }
    int lookback;
    if (options.containsKey(TableProperties.SPLIT_LOOKBACK)) {
      lookback = Integer.parseInt(options.get(TableProperties.SPLIT_LOOKBACK));
    } else {
      lookback = ops.current().propertyAsInt(
          TableProperties.SPLIT_LOOKBACK, TableProperties.SPLIT_LOOKBACK_DEFAULT);
    }
    long openFileCost;
    if (options.containsKey(TableProperties.SPLIT_OPEN_FILE_COST)) {
      openFileCost = Long.parseLong(options.get(TableProperties.SPLIT_OPEN_FILE_COST));
    } else {
      openFileCost = ops.current().propertyAsLong(
          TableProperties.SPLIT_OPEN_FILE_COST, TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
    }

    CloseableIterable<FileScanTask> fileScanTasks = planFiles();
    CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(fileScanTasks, splitSize);
    return TableScanUtil.planTasks(splitFiles, splitSize, lookback, openFileCost);
  }

  @Override
  public Schema schema() {
    return lazyColumnProjection();
  }

  @Override
  public Snapshot snapshot() {
    return snapshotId != null ?
        ops.current().snapshot(snapshotId) :
        ops.current().currentSnapshot();
  }

  @Override
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table", table)
        .add("projection", schema().asStruct())
        .add("filter", rowFilter)
        .add("ignoreResiduals", ignoreResiduals)
        .add("caseSensitive", caseSensitive)
        .toString();
  }

  /**
   * To be able to make refinements {@link #select(Collection)} and {@link #caseSensitive(boolean)} in any order,
   * we resolve the schema to be projected lazily here.
   *
   * @return the Schema to project
   */
  private Schema lazyColumnProjection() {
    if (selectedColumns != null) {
      Set<Integer> requiredFieldIds = Sets.newHashSet();

      // all of the filter columns are required
      requiredFieldIds.addAll(
          Binder.boundReferences(table.schema().asStruct(), Collections.singletonList(rowFilter), caseSensitive));

      // all of the projection columns are required
      Set<Integer> selectedIds;
      if (caseSensitive) {
        selectedIds = TypeUtil.getProjectedIds(table.schema().select(selectedColumns));
      } else {
        selectedIds = TypeUtil.getProjectedIds(table.schema().caseInsensitiveSelect(selectedColumns));
      }
      requiredFieldIds.addAll(selectedIds);

      return TypeUtil.select(table.schema(), requiredFieldIds);
    }

    return schema;
  }

  private static String formatTimestampMillis(long millis) {
    return DATE_FORMAT.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault()));
  }
}
