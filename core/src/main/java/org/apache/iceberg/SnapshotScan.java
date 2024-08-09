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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.Timer;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a common base class to share code between different BaseScan implementations that handle
 * scans of a particular snapshot.
 *
 * @param <ThisT> actual BaseScan implementation class type
 * @param <T> type of ScanTask returned
 * @param <G> type of ScanTaskGroup returned
 */
public abstract class SnapshotScan<
        ThisT extends Scan, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends BaseScan<ThisT, T, G> {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotScan.class);

  private ScanMetrics scanMetrics;

  protected SnapshotScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  protected Long snapshotId() {
    return context().snapshotId();
  }

  protected abstract CloseableIterable<T> doPlanFiles();

  // controls whether to use the snapshot schema while time travelling
  protected boolean useSnapshotSchema() {
    return false;
  }

  protected ScanMetrics scanMetrics() {
    if (scanMetrics == null) {
      this.scanMetrics = ScanMetrics.of(new DefaultMetricsContext());
    }

    return scanMetrics;
  }

  public ThisT useSnapshot(long scanSnapshotId) {
    Preconditions.checkArgument(
        snapshotId() == null, "Cannot override snapshot, already set snapshot id=%s", snapshotId());
    Preconditions.checkArgument(
        table().snapshot(scanSnapshotId) != null,
        "Cannot find snapshot with ID %s",
        scanSnapshotId);
    Schema newSchema =
        useSnapshotSchema() ? SnapshotUtil.schemaFor(table(), scanSnapshotId) : tableSchema();
    TableScanContext newContext = context().useSnapshotId(scanSnapshotId);
    return newRefinedScan(table(), newSchema, newContext);
  }

  public ThisT useRef(String name) {
    if (SnapshotRef.MAIN_BRANCH.equals(name)) {
      return newRefinedScan(table(), tableSchema(), context());
    }

    Preconditions.checkArgument(
        snapshotId() == null, "Cannot override ref, already set snapshot id=%s", snapshotId());
    Snapshot snapshot = table().snapshot(name);
    Preconditions.checkArgument(snapshot != null, "Cannot find ref %s", name);
    TableScanContext newContext = context().useSnapshotId(snapshot.snapshotId());
    return newRefinedScan(table(), SnapshotUtil.schemaFor(table(), name), newContext);
  }

  public ThisT asOfTime(long timestampMillis) {
    Preconditions.checkArgument(
        snapshotId() == null, "Cannot override snapshot, already set snapshot id=%s", snapshotId());

    return useSnapshot(SnapshotUtil.snapshotIdAsOfTime(table(), timestampMillis));
  }

  @Override
  public CloseableIterable<T> planFiles() {
    Snapshot snapshot = snapshot();

    if (snapshot == null) {
      LOG.info("Scanning empty table {}", table());
      return CloseableIterable.empty();
    }

    LOG.info(
        "Scanning table {} snapshot {} created at {} with filter {}",
        table(),
        snapshot.snapshotId(),
        DateTimeUtil.formatTimestampMillis(snapshot.timestampMillis()),
        ExpressionUtil.toSanitizedString(filter()));

    Listeners.notifyAll(new ScanEvent(table().name(), snapshot.snapshotId(), filter(), schema()));
    List<Integer> projectedFieldIds = Lists.newArrayList(TypeUtil.getProjectedIds(schema()));
    List<String> projectedFieldNames =
        projectedFieldIds.stream().map(schema()::findColumnName).collect(Collectors.toList());

    Timer.Timed planningDuration = scanMetrics().totalPlanningDuration().start();

    return CloseableIterable.whenComplete(
        doPlanFiles(),
        () -> {
          planningDuration.stop();
          Map<String, String> metadata = Maps.newHashMap(context().options());
          metadata.putAll(EnvironmentContext.get());
          ScanReport scanReport =
              ImmutableScanReport.builder()
                  .schemaId(schema().schemaId())
                  .projectedFieldIds(projectedFieldIds)
                  .projectedFieldNames(projectedFieldNames)
                  .tableName(table().name())
                  .snapshotId(snapshot.snapshotId())
                  .filter(
                      ExpressionUtil.sanitize(
                          schema().asStruct(), filter(), context().caseSensitive()))
                  .scanMetrics(ScanMetricsResult.fromScanMetrics(scanMetrics()))
                  .metadata(metadata)
                  .build();
          context().metricsReporter().report(scanReport);
        });
  }

  public Snapshot snapshot() {
    return snapshotId() != null ? table().snapshot(snapshotId()) : table().currentSnapshot();
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
