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
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the single (non-parallel) monitoring task which takes a {@link FlinkInputFormat},
 * it is responsible for:
 *
 * <ol>
 *     <li>Monitoring snapshots of the Iceberg table.</li>
 *     <li>Creating the {@link FlinkInputSplit splits} corresponding to the incremental files</li>
 *     <li>Assigning them to downstream tasks for further processing.</li>
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link StreamingReaderOperator}
 * which can have parallelism greater than one.
 */
public class StreamingMonitorFunction extends RichSourceFunction<FlinkInputSplit> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingMonitorFunction.class);

  private static final long INIT_LAST_SNAPSHOT_ID = -1L;

  private final TableLoader tableLoader;
  private final ScanContext scanContext;

  private volatile boolean isRunning = true;

  // The checkpoint thread is not the same thread that running the function for SourceStreamTask now. It's necessary to
  // mark this as volatile.
  private volatile long lastSnapshotId = INIT_LAST_SNAPSHOT_ID;

  private transient SourceContext<FlinkInputSplit> sourceContext;
  private transient Table table;
  private transient ListState<Long> lastSnapshotIdState;

  public StreamingMonitorFunction(TableLoader tableLoader, ScanContext scanContext) {
    Preconditions.checkArgument(scanContext.snapshotId() == null,
        "Cannot set snapshot-id option for streaming reader");
    Preconditions.checkArgument(scanContext.asOfTimestamp() == null,
        "Cannot set as-of-timestamp option for streaming reader");
    Preconditions.checkArgument(scanContext.endSnapshotId() == null,
        "Cannot set end-snapshot-id option for streaming reader");
    this.tableLoader = tableLoader;
    this.scanContext = scanContext;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    // Load iceberg table from table loader.
    tableLoader.open();
    table = tableLoader.loadTable();

    // Initialize the flink state for last snapshot id.
    lastSnapshotIdState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>(
            "snapshot-id-state",
            LongSerializer.INSTANCE));

    // Restore the last-snapshot-id from flink's state if possible.
    if (context.isRestored()) {
      LOG.info("Restoring state for the {}.", getClass().getSimpleName());
      lastSnapshotId = lastSnapshotIdState.get().iterator().next();
    } else if (scanContext.startSnapshotId() != null) {
      Preconditions.checkNotNull(table.currentSnapshot(), "Don't have any available snapshot in table.");

      long currentSnapshotId = table.currentSnapshot().snapshotId();
      Preconditions.checkState(SnapshotUtil.isAncestorOf(table, currentSnapshotId, scanContext.startSnapshotId()),
          "The option start-snapshot-id %s is not an ancestor of the current snapshot.", scanContext.startSnapshotId());

      lastSnapshotId = scanContext.startSnapshotId();
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
      synchronized (sourceContext.getCheckpointLock()) {
        if (isRunning) {
          monitorAndForwardSplits();
        }
      }
      Thread.sleep(scanContext.monitorInterval().toMillis());
    }
  }

  private void monitorAndForwardSplits() {
    // Refresh the table to get the latest committed snapshot.
    table.refresh();

    Snapshot snapshot = table.currentSnapshot();
    if (snapshot != null && snapshot.snapshotId() != lastSnapshotId) {
      long snapshotId = snapshot.snapshotId();
      if (lastSnapshotId == INIT_LAST_SNAPSHOT_ID) {
        Iterator<Snapshot> historySnapshots = table.snapshots().iterator();
        Snapshot dealSnapshot = historySnapshots.next();
        dealSnapshot(dealSnapshot);
        lastSnapshotId = dealSnapshot.snapshotId();

      } else {
        List<Snapshot> snapshots = SnapshotUtil.snapshotsBetween(table, lastSnapshotId, snapshotId);
        Snapshot dealSnapshot = snapshots.get(snapshots.size() - 1);
        dealSnapshot(dealSnapshot);
        lastSnapshotId = dealSnapshot.snapshotId();
      }
    }
  }

  private void dealSnapshot(Snapshot dealSnapshot) {
    ScanContext newScanContext;
    if (dealSnapshot.operation().equals(DataOperations.APPEND) ||
            dealSnapshot.operation().equals(DataOperations.OVERWRITE)) {
      newScanContext = scanContext.copyWithSnapshotId(dealSnapshot.snapshotId());
    } else {
      return;
    }
    FlinkInputSplit[] splits = FlinkSplitPlanner.planInputSplits(table, newScanContext);
    for (FlinkInputSplit split : splits) {
      sourceContext.collect(split);
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
  }
}
