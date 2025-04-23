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

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calls the {@link ExpireSnapshots} to remove the old snapshots and emits the filenames which could
 * be removed in the {@link #DELETE_STREAM} side output.
 */
@Internal
public class ExpireSnapshotsProcessor extends ProcessFunction<Trigger, TaskResult> {
  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsProcessor.class);
  public static final OutputTag<String> DELETE_STREAM =
      new OutputTag<>("expire-snapshots-file-deletes-stream", Types.STRING);

  private final TableLoader tableLoader;
  private final Long maxSnapshotAgeMs;
  private final Integer numSnapshots;
  private final Integer plannerPoolSize;
  private transient ExecutorService plannerPool;
  private transient Table table;

  public ExpireSnapshotsProcessor(
      TableLoader tableLoader,
      Long maxSnapshotAgeMs,
      Integer numSnapshots,
      Integer plannerPoolSize) {
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");

    this.tableLoader = tableLoader;
    this.maxSnapshotAgeMs = maxSnapshotAgeMs;
    this.numSnapshots = numSnapshots;
    this.plannerPoolSize = plannerPoolSize;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    tableLoader.open();
    this.table = tableLoader.loadTable();
    this.plannerPool =
        plannerPoolSize != null
            ? ThreadPools.newWorkerPool(table.name() + "-table--planner", plannerPoolSize)
            : ThreadPools.getWorkerPool();
  }

  @Override
  public void processElement(Trigger trigger, Context ctx, Collector<TaskResult> out)
      throws Exception {
    try {
      table.refresh();
      ExpireSnapshots expireSnapshots = table.expireSnapshots();
      if (maxSnapshotAgeMs != null) {
        expireSnapshots = expireSnapshots.expireOlderThan(ctx.timestamp() - maxSnapshotAgeMs);
      }

      if (numSnapshots != null) {
        expireSnapshots = expireSnapshots.retainLast(numSnapshots);
      }

      AtomicLong deleteFileCounter = new AtomicLong(0L);
      expireSnapshots
          .planWith(plannerPool)
          .deleteWith(
              file -> {
                ctx.output(DELETE_STREAM, file);
                deleteFileCounter.incrementAndGet();
              })
          .cleanExpiredFiles(true)
          .commit();

      LOG.info(
          "Successfully finished expiring snapshots for {} at {}. Scheduled {} files for delete.",
          table,
          ctx.timestamp(),
          deleteFileCounter.get());
      out.collect(
          new TaskResult(trigger.taskId(), trigger.timestamp(), true, Collections.emptyList()));
    } catch (Exception e) {
      LOG.error("Failed to expiring snapshots for {} at {}", table, ctx.timestamp(), e);
      out.collect(
          new TaskResult(trigger.taskId(), trigger.timestamp(), false, Lists.newArrayList(e)));
    }
  }

  @Override
  public void close() throws Exception {
    super.close();

    tableLoader.close();
    if (plannerPoolSize != null) {
      plannerPool.shutdown();
    }
  }
}
