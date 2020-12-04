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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
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

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(StreamingMonitorFunction.class);

  private static final long DUMMY_START_SNAPSHOT_ID = -1;

  private final TableLoader tableLoader;
  private final ScanContext ctxt;

  private volatile boolean isRunning = true;
  private transient Object checkpointLock;
  private transient Table table;
  private transient ListState<Long> snapshotIdState;
  private transient long startSnapshotId;

  public StreamingMonitorFunction(TableLoader tableLoader, ScanContext ctxt) {
    Preconditions.checkArgument(ctxt.snapshotId() == null && ctxt.asOfTimestamp() == null,
        "The streaming reader does not support using snapshot-id or as-of-timestamp to select the table snapshot.");
    Preconditions.checkArgument(ctxt.endSnapshotId() == null,
        "The streaming reader does not support using end snapshot id.");
    this.tableLoader = tableLoader;
    this.ctxt = ctxt;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    snapshotIdState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>(
            "snapshot-id-state",
            LongSerializer.INSTANCE));
    tableLoader.open();
    table = tableLoader.loadTable();
    if (context.isRestored()) {
      LOG.info("Restoring state for the {}.", getClass().getSimpleName());
      startSnapshotId = snapshotIdState.get().iterator().next();
    } else {
      Long optionStartSnapshot = ctxt.startSnapshotId();
      if (optionStartSnapshot != null) {
        if (!SnapshotUtil.ancestorOf(table, table.currentSnapshot().snapshotId(), optionStartSnapshot)) {
          throw new IllegalStateException("The option start-snapshot-id " + optionStartSnapshot +
              " is not an ancestor of the current snapshot");
        }

        startSnapshotId = optionStartSnapshot;
      } else {
        startSnapshotId = DUMMY_START_SNAPSHOT_ID;
      }
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    snapshotIdState.clear();
    snapshotIdState.add(startSnapshotId);
  }

  @Override
  public void run(SourceContext<FlinkInputSplit> ctx) throws Exception {
    checkpointLock = ctx.getCheckpointLock();
    while (isRunning) {
      synchronized (checkpointLock) {
        monitorAndForwardSplits(ctx);
      }
      Thread.sleep(ctxt.monitorInterval().toMillis());
    }
  }

  private void monitorAndForwardSplits(SourceContext<FlinkInputSplit> ctx) {
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    if (snapshot != null && snapshot.snapshotId() != startSnapshotId) {
      long snapshotId = snapshot.snapshotId();
      // Read current static table if startSnapshotId not set.
      ScanContext newScanContext = startSnapshotId == DUMMY_START_SNAPSHOT_ID ?
          ctxt.copyWithSnapshotId(snapshotId) :
          ctxt.copyWithAppendsBetween(startSnapshotId, snapshotId);

      FlinkInputSplit[] splits = FlinkSplitGenerator.createInputSplits(table, newScanContext);
      for (FlinkInputSplit split : splits) {
        ctx.collect(split);
      }
      startSnapshotId = snapshotId;
    }
  }

  @Override
  public void cancel() {
    // this is to cover the case where cancel() is called before the run()
    if (checkpointLock != null) {
      synchronized (checkpointLock) {
        isRunning = false;
      }
    } else {
      isRunning = false;
    }

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
