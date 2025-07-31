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

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewritePlanner.PlannedGroup;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.FileScanTaskReader;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a rewrite for a single {@link PlannedGroup}. Reads the files with the standard {@link
 * FileScanTaskReader}, so the delete files are considered, and writes using the {@link
 * TaskWriterFactory}. The output is an {@link ExecutedGroup}.
 */
@Internal
public class DataFileRewriteRunner
    extends ProcessFunction<PlannedGroup, DataFileRewriteRunner.ExecutedGroup> {
  private static final Logger LOG = LoggerFactory.getLogger(DataFileRewriteRunner.class);

  private final String tableName;
  private final String taskName;
  private final int taskIndex;

  private transient int subTaskId;
  private transient int attemptId;
  private transient Counter errorCounter;

  public DataFileRewriteRunner(String tableName, String taskName, int taskIndex) {
    Preconditions.checkNotNull(tableName, "Table name should no be null");
    Preconditions.checkNotNull(taskName, "Task name should no be null");
    this.tableName = tableName;
    this.taskName = taskName;
    this.taskIndex = taskIndex;
  }

  @Override
  public void open(OpenContext context) {
    this.errorCounter =
        TableMaintenanceMetrics.groupFor(getRuntimeContext(), tableName, taskName, taskIndex)
            .counter(TableMaintenanceMetrics.ERROR_COUNTER);

    this.subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getTaskInfo().getAttemptNumber();
  }

  @Override
  public void processElement(PlannedGroup value, Context ctx, Collector<ExecutedGroup> out)
      throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Rewriting files for group {} with files: {}",
          tableName,
          taskName,
          taskIndex,
          ctx.timestamp(),
          value.group().info(),
          value.group().rewrittenFiles());
    } else {
      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX
              + "Rewriting files for group {} with {} number of files",
          tableName,
          taskName,
          taskIndex,
          ctx.timestamp(),
          value.group().info(),
          value.group().rewrittenFiles().size());
    }

    try (TaskWriter<RowData> writer = writerFor(value)) {
      try (DataIterator<RowData> iterator = readerFor(value)) {
        while (iterator.hasNext()) {
          writer.write(iterator.next());
        }

        Set<DataFile> dataFiles = Sets.newHashSet(writer.dataFiles());
        value.group().setOutputFiles(dataFiles);
        out.collect(
            new ExecutedGroup(
                value.table().currentSnapshot().snapshotId(),
                value.groupsPerCommit(),
                value.group()));
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              DataFileRewritePlanner.MESSAGE_PREFIX + "Rewritten files {} from {} to {}",
              tableName,
              taskName,
              taskIndex,
              ctx.timestamp(),
              value.group().info(),
              value.group().rewrittenFiles(),
              value.group().addedFiles());
        } else {
          LOG.info(
              DataFileRewritePlanner.MESSAGE_PREFIX + "Rewritten {} files to {} files",
              tableName,
              taskName,
              taskIndex,
              ctx.timestamp(),
              value.group().rewrittenFiles().size(),
              value.group().addedFiles().size());
        }
      } catch (Exception ex) {
        LOG.info(
            DataFileRewritePlanner.MESSAGE_PREFIX + "Exception rewriting datafile group {}",
            tableName,
            taskName,
            taskIndex,
            ctx.timestamp(),
            value.group(),
            ex);
        ctx.output(TaskResultAggregator.ERROR_STREAM, ex);
        errorCounter.inc();
        abort(writer, ctx.timestamp());
      }
    } catch (Exception ex) {
      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX
              + "Exception creating compaction writer for group {}",
          tableName,
          taskName,
          taskIndex,
          ctx.timestamp(),
          value.group(),
          ex);
      ctx.output(TaskResultAggregator.ERROR_STREAM, ex);
      errorCounter.inc();
    }
  }

  private TaskWriter<RowData> writerFor(PlannedGroup value) {
    String formatString =
        PropertyUtil.propertyAsString(
            value.table().properties(),
            TableProperties.DEFAULT_FILE_FORMAT,
            TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    RowDataTaskWriterFactory factory =
        new RowDataTaskWriterFactory(
            value.table(),
            FlinkSchemaUtil.convert(value.table().schema()),
            value.group().inputSplitSize(),
            FileFormat.fromString(formatString),
            value.table().properties(),
            null,
            false);
    factory.initialize(subTaskId, attemptId);
    return factory.create();
  }

  private DataIterator<RowData> readerFor(PlannedGroup value) {
    RowDataFileScanTaskReader reader =
        new RowDataFileScanTaskReader(
            value.table().schema(),
            value.table().schema(),
            PropertyUtil.propertyAsString(value.table().properties(), DEFAULT_NAME_MAPPING, null),
            false,
            Collections.emptyList());
    return new DataIterator<>(
        reader,
        new BaseCombinedScanTask(value.group().fileScanTasks()),
        value.table().io(),
        value.table().encryption());
  }

  private void abort(TaskWriter<RowData> writer, long timestamp) {
    try {
      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX
              + "Aborting rewrite for (subTaskId {}, attemptId {})",
          tableName,
          taskName,
          taskIndex,
          timestamp,
          subTaskId,
          attemptId);
      writer.abort();
    } catch (Exception e) {
      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Exception in abort",
          tableName,
          taskName,
          taskIndex,
          timestamp,
          e);
    }
  }

  public static class ExecutedGroup {
    private final long snapshotId;
    private final int groupsPerCommit;
    private final RewriteFileGroup group;

    @VisibleForTesting
    ExecutedGroup(long snapshotId, int groupsPerCommit, RewriteFileGroup group) {
      this.snapshotId = snapshotId;
      this.groupsPerCommit = groupsPerCommit;
      this.group = group;
    }

    long snapshotId() {
      return snapshotId;
    }

    int groupsPerCommit() {
      return groupsPerCommit;
    }

    RewriteFileGroup group() {
      return group;
    }
  }
}
