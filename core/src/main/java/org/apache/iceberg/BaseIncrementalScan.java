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
import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ImmutableIncrementalScanReport;
import org.apache.iceberg.metrics.IncrementalScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.Timer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.SnapshotUtil;

abstract class BaseIncrementalScan<ThisT, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends BaseScan<ThisT, T, G> implements IncrementalScan<ThisT, T, G> {

  private ScanMetrics scanMetrics;

  protected BaseIncrementalScan(
      TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  protected abstract CloseableIterable<T> doPlanFiles(
      Long fromSnapshotIdExclusive, long toSnapshotIdInclusive);

  protected ScanMetrics scanMetrics() {
    if (scanMetrics == null) {
      this.scanMetrics = ScanMetrics.of(new DefaultMetricsContext());
    }

    return scanMetrics;
  }

  @Override
  public ThisT fromSnapshotInclusive(long fromSnapshotId) {
    Preconditions.checkArgument(
        table().snapshot(fromSnapshotId) != null,
        "Cannot find the starting snapshot: %s",
        fromSnapshotId);
    TableScanContext newContext = context().fromSnapshotIdInclusive(fromSnapshotId);
    return newRefinedScan(tableOps(), table(), schema(), newContext);
  }

  @Override
  public ThisT fromSnapshotExclusive(long fromSnapshotId) {
    // for exclusive behavior, table().snapshot(fromSnapshotId) check can't be applied
    // as fromSnapshotId could be matched to a parent snapshot that is already expired
    TableScanContext newContext = context().fromSnapshotIdExclusive(fromSnapshotId);
    return newRefinedScan(tableOps(), table(), schema(), newContext);
  }

  @Override
  public ThisT toSnapshot(long toSnapshotId) {
    Preconditions.checkArgument(
        table().snapshot(toSnapshotId) != null, "Cannot find the end snapshot: %s", toSnapshotId);
    TableScanContext newContext = context().toSnapshotId(toSnapshotId);
    return newRefinedScan(tableOps(), table(), schema(), newContext);
  }

  @Override
  public CloseableIterable<T> planFiles() {
    if (scanCurrentLineage() && table().currentSnapshot() == null) {
      // If the table is empty (no current snapshot) and both from and to snapshots aren't set,
      // simply return an empty iterable. In this case, the listener notification is also skipped.
      return CloseableIterable.empty();
    }

    long toSnapshotIdInclusive = toSnapshotIdInclusive();
    Long fromSnapshotIdExclusive = fromSnapshotIdExclusive(toSnapshotIdInclusive);

    long fromSnapshotId;
    if (fromSnapshotIdExclusive != null) {
      fromSnapshotId = fromSnapshotIdExclusive;
      Listeners.notifyAll(
          new IncrementalScanEvent(
              table().name(),
              fromSnapshotIdExclusive,
              toSnapshotIdInclusive,
              filter(),
              schema(),
              false /* from snapshot ID inclusive */));
    } else {
      fromSnapshotId = SnapshotUtil.oldestAncestorOf(table(), toSnapshotIdInclusive).snapshotId();
      Listeners.notifyAll(
          new IncrementalScanEvent(
              table().name(),
              fromSnapshotId,
              toSnapshotIdInclusive,
              filter(),
              schema(),
              true /* from snapshot ID inclusive */));
    }

    List<Integer> projectedFieldIds = Lists.newArrayList(TypeUtil.getProjectedIds(schema()));
    List<String> projectedFieldNames = TypeUtil.getProjectedFieldNames(schema(), projectedFieldIds);

    Timer.Timed planningDuration = scanMetrics().totalPlanningDuration().start();

    return CloseableIterable.whenComplete(
        doPlanFiles(fromSnapshotIdExclusive, toSnapshotIdInclusive),
        () -> {
          planningDuration.stop();
          Map<String, String> metadata = Maps.newHashMap(context().options());
          metadata.putAll(EnvironmentContext.get());
          IncrementalScanReport scanReport =
              ImmutableIncrementalScanReport.builder()
                  .schemaId(schema().schemaId())
                  .projectedFieldIds(projectedFieldIds)
                  .projectedFieldNames(projectedFieldNames)
                  .tableName(table().name())
                  .fromSnapshotId(fromSnapshotId)
                  .toSnapshotId(toSnapshotIdInclusive)
                  .filter(ExpressionUtil.sanitize(filter()))
                  .scanMetrics(ScanMetricsResult.fromScanMetrics(scanMetrics()))
                  .metadata(metadata)
                  .build();
          context().metricsReporter().report(scanReport);
        });
  }

  private boolean scanCurrentLineage() {
    return context().fromSnapshotId() == null && context().toSnapshotId() == null;
  }

  private long toSnapshotIdInclusive() {
    if (context().toSnapshotId() != null) {
      return context().toSnapshotId();
    } else {
      Snapshot currentSnapshot = table().currentSnapshot();
      Preconditions.checkArgument(
          currentSnapshot != null, "End snapshot is not set and table has no current snapshot");
      return currentSnapshot.snapshotId();
    }
  }

  private Long fromSnapshotIdExclusive(long toSnapshotIdInclusive) {
    Long fromSnapshotId = context().fromSnapshotId();
    boolean fromSnapshotInclusive = context().fromSnapshotInclusive();

    if (fromSnapshotId == null) {
      return null;
    } else {
      if (fromSnapshotInclusive) {
        // validate fromSnapshotId is an ancestor of toSnapshotIdInclusive
        Preconditions.checkArgument(
            SnapshotUtil.isAncestorOf(table(), toSnapshotIdInclusive, fromSnapshotId),
            "Starting snapshot (inclusive) %s is not an ancestor of end snapshot %s",
            fromSnapshotId,
            toSnapshotIdInclusive);
        // for inclusive behavior fromSnapshotIdExclusive is set to the parent snapshot ID,
        // which can be null
        return table().snapshot(fromSnapshotId).parentId();

      } else {
        // validate there is an ancestor of toSnapshotIdInclusive where parent is fromSnapshotId
        Preconditions.checkArgument(
            SnapshotUtil.isParentAncestorOf(table(), toSnapshotIdInclusive, fromSnapshotId),
            "Starting snapshot (exclusive) %s is not a parent ancestor of end snapshot %s",
            fromSnapshotId,
            toSnapshotIdInclusive);
        return fromSnapshotId;
      }
    }
  }
}
