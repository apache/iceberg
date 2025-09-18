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
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.FileRewritePlan;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans the rewrite groups using the {@link BinPackRewriteFilePlanner}. The input is the {@link
 * Trigger}, the output is zero, or some {@link PlannedGroup}s.
 */
@Internal
public class DataFileRewritePlanner
    extends ProcessFunction<Trigger, DataFileRewritePlanner.PlannedGroup> {
  static final String MESSAGE_PREFIX = "[For table {} with {}[{}] at {}]: ";
  private static final Logger LOG = LoggerFactory.getLogger(DataFileRewritePlanner.class);

  private final String tableName;
  private final String taskName;
  private final int taskIndex;
  private final TableLoader tableLoader;
  private final int partialProgressMaxCommits;
  private final long maxRewriteBytes;
  private final Map<String, String> rewriterOptions;
  private transient Counter errorCounter;
  private final Expression filter;

  public DataFileRewritePlanner(
      String tableName,
      String taskName,
      int taskIndex,
      TableLoader tableLoader,
      int newPartialProgressMaxCommits,
      long maxRewriteBytes,
      Map<String, String> rewriterOptions,
      Expression filter) {

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
    this.filter = filter;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    tableLoader.open();
    Table table = tableLoader.loadTable();
    Preconditions.checkArgument(
        !TableUtil.supportsRowLineage(table),
        "Flink does not support compaction on row lineage enabled tables (V3+)");
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), tableName, taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);
  }

  @Override
  public void processElement(Trigger value, Context ctx, Collector<PlannedGroup> out)
      throws Exception {
    LOG.info(
        DataFileRewritePlanner.MESSAGE_PREFIX + "Creating rewrite plan",
        tableName,
        taskName,
        taskIndex,
        ctx.timestamp());
    try {
      SerializableTable table =
          (SerializableTable) SerializableTable.copyOf(tableLoader.loadTable());
      if (table.currentSnapshot() == null) {
        LOG.info(
            DataFileRewritePlanner.MESSAGE_PREFIX + "Nothing to plan for in an empty table",
            tableName,
            taskName,
            taskIndex,
            ctx.timestamp());
        return;
      }

      BinPackRewriteFilePlanner planner = new BinPackRewriteFilePlanner(table, filter);
      planner.init(rewriterOptions);

      FileRewritePlan<RewriteDataFiles.FileGroupInfo, FileScanTask, DataFile, RewriteFileGroup>
          plan = planner.plan();

      long rewriteBytes = 0;
      List<RewriteFileGroup> groups = Lists.newArrayList();
      for (CloseableIterator<RewriteFileGroup> groupIterator = plan.groups().iterator();
          groupIterator.hasNext(); ) {
        RewriteFileGroup group = groupIterator.next();
        if (rewriteBytes + group.inputFilesSizeInBytes() > maxRewriteBytes) {
          // Keep going, maybe some other group might fit in
          LOG.info(
              DataFileRewritePlanner.MESSAGE_PREFIX
                  + "Skipping group as max rewrite size reached {}",
              tableName,
              taskName,
              taskIndex,
              ctx.timestamp(),
              group);
        } else {
          rewriteBytes += group.inputFilesSizeInBytes();
          groups.add(group);
        }
      }

      int groupsPerCommit =
          IntMath.divide(groups.size(), partialProgressMaxCommits, RoundingMode.CEILING);

      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Rewrite plan created {}",
          tableName,
          taskName,
          taskIndex,
          ctx.timestamp(),
          groups);

      for (RewriteFileGroup group : groups) {
        LOG.info(
            DataFileRewritePlanner.MESSAGE_PREFIX + "Emitting {}",
            tableName,
            taskName,
            taskIndex,
            ctx.timestamp(),
            group);
        out.collect(new PlannedGroup(table, groupsPerCommit, group));
      }
    } catch (Exception e) {
      LOG.warn(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Failed to plan data file rewrite groups",
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
    private final RewriteFileGroup group;

    private PlannedGroup(SerializableTable table, int groupsPerCommit, RewriteFileGroup group) {
      this.table = table;
      this.groupsPerCommit = groupsPerCommit;
      this.group = group;
    }

    SerializableTable table() {
      return table;
    }

    int groupsPerCommit() {
      return groupsPerCommit;
    }

    RewriteFileGroup group() {
      return group;
    }
  }
}
