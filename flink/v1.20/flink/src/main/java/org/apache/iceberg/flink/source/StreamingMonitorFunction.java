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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the single (non-parallel) monitoring task which takes a {@link FlinkInputFormat}, it is
 * responsible for:
 *
 * <ol>
 *   <li>Monitoring snapshots of the Iceberg table.
 *   <li>Creating the {@link FlinkInputSplit splits} corresponding to the incremental files
 *   <li>Assigning them to downstream tasks for further processing.
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link StreamingReaderOperator} which
 * can have parallelism greater than one.
 */
public class StreamingMonitorFunction extends RichSourceFunction<FlinkInputSplit>
    implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingMonitorFunction.class);

  private static final long INIT_LAST_SNAPSHOT_ID = -1L;

  private final TableLoader tableLoader;
  private final ScanContext scanContext;

  private volatile boolean isRunning = true;

  // The checkpoint thread is not the same thread that running the function for SourceStreamTask
  // now. It's necessary to
  // mark this as volatile.
  private volatile long lastSnapshotId = INIT_LAST_SNAPSHOT_ID;

  private transient SourceContext<FlinkInputSplit> sourceContext;
  private transient Table table;
  private transient ListState<Long> lastSnapshotIdState;
  private transient ExecutorService workerPool;

  public StreamingMonitorFunction(TableLoader tableLoader, ScanContext scanContext) {
    Preconditions.checkArgument(
        scanContext.snapshotId() == null, "Cannot set snapshot-id option for streaming reader");
    Preconditions.checkArgument(
        scanContext.asOfTimestamp() == null,
        "Cannot set as-of-timestamp option for streaming reader");
    Preconditions.checkArgument(
        scanContext.endSnapshotId() == null,
        "Cannot set end-snapshot-id option for streaming reader");
    Preconditions.checkArgument(
        scanContext.endTag() == null, "Cannot set end-tag option for streaming reader");
    Preconditions.checkArgument(
        scanContext.maxPlanningSnapshotCount() > 0,
        "The max-planning-snapshot-count must be greater than zero");
    this.tableLoader = tableLoader;
    this.scanContext = scanContext;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    final RuntimeContext runtimeContext = getRuntimeContext();
    ValidationException.check(
        runtimeContext instanceof StreamingRuntimeContext,
        "context should be instance of StreamingRuntimeContext");
    final String operatorID = ((StreamingRuntimeContext) runtimeContext).getOperatorUniqueID();
    this.workerPool =
        ThreadPools.newFixedThreadPool(
            "iceberg-worker-pool-" + operatorID, scanContext.planParallelism());
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    // Load iceberg table from table loader.
    tableLoader.open();
    table = tableLoader.loadTable();

    // Initialize the flink state for last snapshot id.
    lastSnapshotIdState =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("snapshot-id-state", LongSerializer.INSTANCE));

    // Restore the last-snapshot-id from flink's state if possible.
    if (context.isRestored()) {
      LOG.info("Restoring state for the {}.", getClass().getSimpleName());
      lastSnapshotId = lastSnapshotIdState.get().iterator().next();
    } else if (scanContext.startTag() != null || scanContext.startSnapshotId() != null) {
      Preconditions.checkArgument(
          !(scanContext.startTag() != null && scanContext.startSnapshotId() != null),
          "START_SNAPSHOT_ID and START_TAG cannot both be set.");
      Preconditions.checkNotNull(
          table.currentSnapshot(), "Don't have any available snapshot in table.");

      long startSnapshotId;
      if (scanContext.startTag() != null) {
        Preconditions.checkArgument(
            table.snapshot(scanContext.startTag()) != null,
            "Cannot find snapshot with tag %s in table.",
            scanContext.startTag());
        startSnapshotId = table.snapshot(scanContext.startTag()).snapshotId();
      } else {
        startSnapshotId = scanContext.startSnapshotId();
      }

      long currentSnapshotId = table.currentSnapshot().snapshotId();
      Preconditions.checkState(
          SnapshotUtil.isAncestorOf(table, currentSnapshotId, startSnapshotId),
          "The option start-snapshot-id %s is not an ancestor of the current snapshot.",
          startSnapshotId);

      lastSnapshotId = startSnapshotId;
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    lastSnapshotIdState.clear();
    lastSnapshotIdState.add(lastSnapshotId);
  }

  @Override
  public void run(SourceContext<FlinkInputSplit> ctx) throws Exception {
    this.sourceContext = ctx;
    while (isRunning) {
      monitorAndForwardSplits();
      Thread.sleep(scanContext.monitorInterval().toMillis());
    }
  }

  private long toSnapshotIdInclusive(
      long lastConsumedSnapshotId, long currentSnapshotId, int maxPlanningSnapshotCount) {
    List<Long> snapshotIds =
        SnapshotUtil.snapshotIdsBetween(table, lastConsumedSnapshotId, currentSnapshotId);
    if (snapshotIds.size() <= maxPlanningSnapshotCount) {
      return currentSnapshotId;
    } else {
      // It uses reverted index since snapshotIdsBetween returns Ids that are ordered by committed
      // time descending.
      return snapshotIds.get(snapshotIds.size() - maxPlanningSnapshotCount);
    }
  }

  @VisibleForTesting
  void sourceContext(SourceContext<FlinkInputSplit> ctx) {
    this.sourceContext = ctx;
  }

  @VisibleForTesting
  void monitorAndForwardSplits() {
    // Refresh the table to get the latest committed snapshot.
    table.refresh();

    Snapshot snapshot =
        scanContext.branch() != null
            ? table.snapshot(scanContext.branch())
            : table.currentSnapshot();
    if (snapshot != null && snapshot.snapshotId() != lastSnapshotId) {
      long snapshotId = snapshot.snapshotId();

      ScanContext newScanContext;
      if (lastSnapshotId == INIT_LAST_SNAPSHOT_ID) {
        newScanContext = scanContext.copyWithSnapshotId(snapshotId);
      } else {
        snapshotId =
            toSnapshotIdInclusive(
                lastSnapshotId, snapshotId, scanContext.maxPlanningSnapshotCount());
        newScanContext = scanContext.copyWithAppendsBetween(lastSnapshotId, snapshotId);
      }

      LOG.debug(
          "Start discovering splits from {} (exclusive) to {} (inclusive)",
          lastSnapshotId,
          snapshotId);
      long start = System.currentTimeMillis();
      FlinkInputSplit[] splits =
          FlinkSplitPlanner.planInputSplits(table, newScanContext, workerPool);
      LOG.debug(
          "Discovered {} splits, time elapsed {}ms",
          splits.length,
          System.currentTimeMillis() - start);

      // only need to hold the checkpoint lock when emitting the splits and updating lastSnapshotId
      start = System.currentTimeMillis();
      synchronized (sourceContext.getCheckpointLock()) {
        for (FlinkInputSplit split : splits) {
          sourceContext.collect(split);
        }

        lastSnapshotId = snapshotId;
      }
      LOG.debug(
          "Forwarded {} splits, time elapsed {}ms",
          splits.length,
          System.currentTimeMillis() - start);
    }
  }

  @Override
  public void cancel() {
    // this is to cover the case where cancel() is called before the run()
    if (sourceContext != null) {
      synchronized (sourceContext.getCheckpointLock()) {
        isRunning = false;
      }
    } else {
      isRunning = false;
    }

    // Release all the resources here.
    if (tableLoader != null) {
      try {
        tableLoader.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Override
  public void close() {
    cancel();

    if (workerPool != null) {
      workerPool.shutdown();
    }
  }
}
