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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.concurrent.ExecutorService;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.flink.source.FlinkSplitPlanner;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Plans the splits to read a metadata table content. */
@Internal
public class MetadataTablePlanner extends ProcessFunction<Trigger, MetadataTablePlanner.SplitInfo> {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataTablePlanner.class);

  private final String taskName;
  private final int taskIndex;
  private final TableLoader tableLoader;
  private final int workerPoolSize;
  private final ScanContext scanContext;
  private transient ExecutorService workerPool;
  private transient Counter errorCounter;
  private transient Table table;
  private transient IcebergSourceSplitSerializer splitSerializer;
  private final MetadataTableType metadataTableType;

  public MetadataTablePlanner(
      String taskName,
      int taskIndex,
      TableLoader tableLoader,
      ScanContext scanContext,
      MetadataTableType metadataTableType,
      int workerPoolSize) {
    Preconditions.checkNotNull(taskName, "Task name should no be null");
    Preconditions.checkNotNull(tableLoader, "Table should no be null");
    Preconditions.checkArgument(scanContext.isStreaming(), "Streaming should be set to true");

    this.taskName = taskName;
    this.taskIndex = taskIndex;
    this.tableLoader = tableLoader;
    this.scanContext = scanContext;
    this.workerPoolSize = workerPoolSize;
    this.metadataTableType = metadataTableType;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    tableLoader.open();
    Table originalTable = tableLoader.loadTable();
    this.table = MetadataTableUtils.createMetadataTableInstance(originalTable, metadataTableType);
    this.workerPool =
        ThreadPools.newFixedThreadPool(table.name() + "-table-planner", workerPoolSize);
    this.splitSerializer = new IcebergSourceSplitSerializer(scanContext.caseSensitive());
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(
                getRuntimeContext(), originalTable.name(), taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);
  }

  @Override
  public void processElement(Trigger trigger, Context ctx, Collector<SplitInfo> out)
      throws Exception {
    try {
      table.refresh();
      for (IcebergSourceSplit split :
          FlinkSplitPlanner.planIcebergSourceSplits(table, scanContext, workerPool)) {
        out.collect(new SplitInfo(splitSerializer.getVersion(), splitSerializer.serialize(split)));
      }
    } catch (Exception e) {
      LOG.warn("Exception planning scan for {} at {}", table, ctx.timestamp(), e);
      ctx.output(DeleteOrphanFiles.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
    if (workerPool != null) {
      workerPool.shutdown();
    }
  }

  public static class SplitInfo {
    private final int version;
    private final byte[] split;

    public SplitInfo(int version, byte[] split) {
      this.version = version;
      this.split = split;
    }

    public int version() {
      return version;
    }

    public byte[] split() {
      return split;
    }
  }
}
