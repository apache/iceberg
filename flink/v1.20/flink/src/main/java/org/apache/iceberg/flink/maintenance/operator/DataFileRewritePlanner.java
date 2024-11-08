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

import java.math.RoundingMode;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.actions.RewriteFileGroupPlanner;
import org.apache.iceberg.actions.RewriteFileGroupPlanner.RewritePlanResult;
import org.apache.iceberg.actions.SizeBasedDataRewriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans the rewrite groups using the {@link RewriteFileGroupPlanner}. The input is the {@link
 * Trigger}, the output is zero, or some {@link PlannedGroup}s.
 */
@Internal
public class DataFileRewritePlanner
    extends ProcessFunction<Trigger, DataFileRewritePlanner.PlannedGroup> {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileRewritePlanner.class);

  private final String tableName;
  private final String taskName;
  private final int taskIndex;
  private final TableLoader tableLoader;
  private final int partialProgressMaxCommits;
  private final long maxRewriteBytes;
  private final Map<String, String> rewriterOptions;
  private transient SizeBasedDataRewriter rewriter;
  private transient RewriteFileGroupPlanner planner;
  private transient Counter errorCounter;

  public DataFileRewritePlanner(
      String tableName,
      String taskName,
      int taskIndex,
      TableLoader tableLoader,
      int newPartialProgressMaxCommits,
      long maxRewriteBytes,
      Map<String, String> rewriterOptions) {
    Preconditions.checkNotNull(tableName, "Table name should no be null");
    Preconditions.checkNotNull(taskName, "Task name should no be null");
    Preconditions.checkNotNull(tableLoader, "Table loader should no be null");
    Preconditions.checkNotNull(rewriterOptions, "Options map should no be null");

    this.tableName = tableName;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
    this.tableLoader = tableLoader;
    this.partialProgressMaxCommits = newPartialProgressMaxCommits;
    this.maxRewriteBytes = maxRewriteBytes;
    this.rewriterOptions = rewriterOptions;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    tableLoader.open();
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), tableName, taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);

    this.rewriter =
        new SizeBasedDataRewriter(tableLoader.loadTable()) {
          @Override
          public Set<DataFile> rewrite(List<FileScanTask> group) {
            // We use the rewriter only for bin-packing the file groups to compact
            throw new UnsupportedOperationException("Should not be called");
          }
        };

    rewriter.init(rewriterOptions);
    this.planner = new RewriteFileGroupPlanner(rewriter, RewriteJobOrder.NONE);
  }

  @Override
  public void processElement(Trigger value, Context ctx, Collector<PlannedGroup> out)
      throws Exception {
    LOG.debug(
        "Creating rewrite plan for table {} with {}[{}] at {}",
        tableName,
        taskName,
        taskIndex,
        ctx.timestamp());
    try {
      SerializableTable table =
          (SerializableTable) SerializableTable.copyOf(tableLoader.loadTable());
      if (table.currentSnapshot() == null) {
        LOG.info(
            "Nothing to plan for in an empty table {} with {}[{}] at {}",
            tableName,
            taskName,
            taskIndex,
            ctx.timestamp());
        return;
      }

      RewritePlanResult plan =
          planner.plan(
              table, Expressions.alwaysTrue(), table.currentSnapshot().snapshotId(), false);

      long rewriteBytes = 0;
      List<RewriteFileGroup> groups = plan.fileGroups().collect(Collectors.toList());
      ListIterator<RewriteFileGroup> iter = groups.listIterator();
      while (iter.hasNext()) {
        RewriteFileGroup group = iter.next();
        if (rewriteBytes + group.sizeInBytes() > maxRewriteBytes) {
          // Keep going, maybe some other group might fit in
          LOG.info(
              "Skipping group {} as max rewrite size reached for table {} with {}[{}] at {}",
              group,
              tableName,
              taskName,
              taskIndex,
              ctx.timestamp());
          iter.remove();
        } else {
          rewriteBytes += group.sizeInBytes();
        }
      }

      int groupsPerCommit =
          IntMath.divide(
              plan.context().totalGroupCount(), partialProgressMaxCommits, RoundingMode.CEILING);

      LOG.info(
          "Rewrite plan created {} for table {} with {}[{}] at {}",
          groups,
          tableName,
          taskName,
          taskIndex,
          ctx.timestamp());

      for (RewriteFileGroup group : groups) {
        LOG.debug(
            "Emitting {} with for table {} with {}[{}] at {}",
            group,
            tableName,
            taskName,
            taskIndex,
            ctx.timestamp());
        out.collect(
            new PlannedGroup(
                table, groupsPerCommit, rewriter.splitSize(group.sizeInBytes()), group));
      }
    } catch (Exception e) {
      LOG.info(
          "Exception planning data file rewrite groups for table {} with {}[{}] at {}",
          tableName,
          taskName,
          taskIndex,
          ctx.timestamp(),
          e);
      ctx.output(TaskResultAggregator.ERROR_STREAM, e);
      errorCounter.inc();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }

  public static class PlannedGroup {
    private final SerializableTable table;
    private final int groupsPerCommit;
    private final long splitSize;
    private final RewriteFileGroup group;

    private PlannedGroup(
        SerializableTable table, int groupsPerCommit, long splitSize, RewriteFileGroup group) {
      this.table = table;
      this.groupsPerCommit = groupsPerCommit;
      this.splitSize = splitSize;
      this.group = group;
    }

    SerializableTable table() {
      return table;
    }

    int groupsPerCommit() {
      return groupsPerCommit;
    }

    long splitSize() {
      return splitSize;
    }

    RewriteFileGroup group() {
      return group;
    }
  }
}
