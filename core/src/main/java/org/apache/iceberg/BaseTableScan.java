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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for {@link TableScan} implementations. */
abstract class BaseTableScan extends BaseScan<TableScan, FileScanTask, CombinedScanTask>
    implements TableScan {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTableScan.class);

  protected BaseTableScan(TableOperations ops, Table table, Schema schema) {
    this(ops, table, schema, new TableScanContext());
  }

  protected BaseTableScan(
      TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  protected Long snapshotId() {
    return context().snapshotId();
  }

  protected boolean colStats() {
    return context().returnColumnStats();
  }

  protected boolean shouldIgnoreResiduals() {
    return context().ignoreResiduals();
  }

  protected ExecutorService planExecutor() {
    return context().planExecutor();
  }

  protected Map<String, String> options() {
    return context().options();
  }

  protected abstract CloseableIterable<FileScanTask> doPlanFiles();

  @Override
  public Table table() {
    return super.table();
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
    Preconditions.checkArgument(
        snapshotId() == null, "Cannot override snapshot, already set to id=%s", snapshotId());
    Preconditions.checkArgument(
        tableOps().current().snapshot(scanSnapshotId) != null,
        "Cannot find snapshot with ID %s",
        scanSnapshotId);
    return newRefinedScan(
        tableOps(), table(), tableSchema(), context().useSnapshotId(scanSnapshotId));
  }

  @Override
  public TableScan asOfTime(long timestampMillis) {
    Preconditions.checkArgument(
        snapshotId() == null, "Cannot override snapshot, already set to id=%s", snapshotId());

    return useSnapshot(SnapshotUtil.snapshotIdAsOfTime(table(), timestampMillis));
  }

  @Override
  public Expression filter() {
    return context().rowFilter();
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    Snapshot snapshot = snapshot();
    if (snapshot != null) {
      LOG.info(
          "Scanning table {} snapshot {} created at {} with filter {}",
          table(),
          snapshot.snapshotId(),
          DateTimeUtil.formatTimestampMillis(snapshot.timestampMillis()),
          ExpressionUtil.toSanitizedString(filter()));

      Listeners.notifyAll(new ScanEvent(table().name(), snapshot.snapshotId(), filter(), schema()));

      return doPlanFiles();

    } else {
      LOG.info("Scanning empty table {}", table());
      return CloseableIterable.empty();
    }
  }

  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    CloseableIterable<FileScanTask> fileScanTasks = planFiles();
    CloseableIterable<FileScanTask> splitFiles =
        TableScanUtil.splitFiles(fileScanTasks, targetSplitSize());
    return TableScanUtil.planTasks(
        splitFiles, targetSplitSize(), splitLookback(), splitOpenFileCost());
  }

  @Override
  public Snapshot snapshot() {
    return snapshotId() != null
        ? tableOps().current().snapshot(snapshotId())
        : tableOps().current().currentSnapshot();
  }

  @Override
  public boolean isCaseSensitive() {
    return context().caseSensitive();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table", table())
        .add("projection", schema().asStruct())
        .add("filter", filter())
        .add("ignoreResiduals", shouldIgnoreResiduals())
        .add("caseSensitive", isCaseSensitive())
        .toString();
  }
}
