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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.flink.maintenance.operator.DataFileRewritePlanner.PlannedGroup;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.FileScanTaskReader;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.parquet.ParquetFileMerger;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a rewrite for a single {@link PlannedGroup}. Reads the files with the standard {@link
 * FileScanTaskReader}, so the delete files are considered, and writes using the {@link
 * TaskWriterFactory}. The output is an {@link DataFileRewriteRunner.ExecutedGroup}.
 */
@Internal
public class ParquetMergeDataFileRewriteRunner extends DataFileRewriteRunner {
  private static final Logger LOG =
      LoggerFactory.getLogger(ParquetMergeDataFileRewriteRunner.class);
  private final long parquetMergeThresholdByteSize;

  public ParquetMergeDataFileRewriteRunner(
      String tableName, String taskName, int taskIndex, long parquetMergeThresholdByteSize) {
    super(tableName, taskName, taskIndex);
    this.parquetMergeThresholdByteSize = parquetMergeThresholdByteSize;
  }

  @Override
  public void open(OpenContext context) {
    super.open(context);
  }

  @Override
  public void processElement(
      PlannedGroup value, Context ctx, Collector<DataFileRewriteRunner.ExecutedGroup> out)
      throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          DataFileRewritePlanner.MESSAGE_PREFIX + "Rewriting files for group {} with files: {}",
          tableName(),
          taskName(),
          taskIndex(),
          ctx.timestamp(),
          value.group().info(),
          value.group().rewrittenFiles());
    } else {
      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX
              + "Rewriting files for group {} with {} number of files",
          tableName(),
          taskName(),
          taskIndex(),
          ctx.timestamp(),
          value.group().info(),
          value.group().rewrittenFiles().size());
    }

    MessageType messageType = null;
    try {
      messageType = canMergeAndGetSchema(value.group(), value.table());
    } catch (Exception ex) {
      LOG.warn(
          DataFileRewritePlanner.MESSAGE_PREFIX
              + "Exception checking if Parquet merge can be used for group {}",
          tableName(),
          taskName(),
          taskIndex(),
          ctx.timestamp(),
          value.group(),
          ex);
    }

    if (messageType != null) {
      try {
        // schema is lazy init, so we need to get it here,or it will be exception in kryoSerializes
        value.group().fileScanTasks().forEach(FileScanTask::schema);

        List<FileScanTask> smallFileScanTasks =
            value.group().fileScanTasks().stream()
                .filter(item -> item.file().fileSizeInBytes() < parquetMergeThresholdByteSize)
                .collect(Collectors.toList());
        List<DataFile> bigFiles =
            value.group().rewrittenFiles().stream()
                .filter(item -> item.fileSizeInBytes() >= parquetMergeThresholdByteSize)
                .collect(Collectors.toList());

        // common merge small files
        if (bigFiles.isEmpty()) {
          super.processElement(value, ctx, out);
          return;
        }

        Set<DataFile> dataFiles = commonRewriteDataFiles(value, ctx, smallFileScanTasks);
        if (dataFiles != null) {
          bigFiles.addAll(dataFiles);
        } else {
          // fall back to the default way
          super.processElement(value, ctx, out);
          return;
        }

        DataFile resultFile =
            rewriteDataFilesUseParquetMerge(value.group(), value.table(), messageType, bigFiles);
        value.group().setOutputFiles(Sets.newHashSet(resultFile));
        DataFileRewriteRunner.ExecutedGroup executedGroup =
            new DataFileRewriteRunner.ExecutedGroup(
                value.table().currentSnapshot().snapshotId(),
                value.groupsPerCommit(),
                value.group());
        out.collect(executedGroup);
      } catch (Exception ex) {
        LOG.info(
            DataFileRewritePlanner.MESSAGE_PREFIX
                + "Exception creating compaction writer for group {}",
            tableName(),
            taskName(),
            taskIndex(),
            ctx.timestamp(),
            value.group(),
            ex);
        ctx.output(TaskResultAggregator.ERROR_STREAM, ex);
        errorCounter().inc();
      }

      return;
    }

    // fall back to the default way
    super.processElement(value, ctx, out);
  }

  private Set<DataFile> commonRewriteDataFiles(
      PlannedGroup group, Context ctx, List<FileScanTask> smallFileScanTasks) {
    boolean preserveRowId = TableUtil.supportsRowLineage(group.table());
    try (TaskWriter<RowData> writer = writerFor(group, preserveRowId)) {
      try (DataIterator<RowData> iterator = readerFor(group, preserveRowId, smallFileScanTasks)) {
        while (iterator.hasNext()) {
          writer.write(iterator.next());
        }

        return Sets.newHashSet(writer.dataFiles());
      } catch (Exception ex) {
        LOG.info(
            DataFileRewritePlanner.MESSAGE_PREFIX + "Exception rewriting datafile group {}",
            tableName(),
            taskName(),
            taskIndex(),
            ctx.timestamp(),
            group.group(),
            ex);
        ctx.output(TaskResultAggregator.ERROR_STREAM, ex);
        errorCounter().inc();
        abort(writer, ctx.timestamp());
      }
    } catch (Exception ex) {
      LOG.info(
          DataFileRewritePlanner.MESSAGE_PREFIX
              + "Exception creating compaction writer for group {}",
          tableName(),
          taskName(),
          taskIndex(),
          ctx.timestamp(),
          group.group(),
          ex);
      ctx.output(TaskResultAggregator.ERROR_STREAM, ex);
      errorCounter().inc();
    }

    return null;
  }

  private DataFile rewriteDataFilesUseParquetMerge(
      RewriteFileGroup group,
      Table table,
      MessageType messageType,
      List<DataFile> rewriteBigDataFiles)
      throws IOException {

    PartitionSpec spec = table.specs().get(group.outputSpecId());

    long rowGroupSize =
        PropertyUtil.propertyAsLong(
            table.properties(),
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT);

    OutputFileFactory outputFileFactory =
        OutputFileFactory.builderFor(table, taskIndex(), attemptId())
            .format(FileFormat.PARQUET)
            .ioSupplier(table::io)
            .defaultSpec(spec)
            .build();

    OutputFile outputFile =
        outputFileFactory.newOutputFile(group.info().partition()).encryptingOutputFile();

    return ParquetFileMerger.mergeFiles(
        rewriteBigDataFiles,
        table.io(),
        outputFile,
        messageType,
        rowGroupSize,
        table.spec(),
        group.info().partition());
  }

  private MessageType canMergeAndGetSchema(RewriteFileGroup group, Table table) {
    // Check if group expects exactly one output file
    if (group.expectedOutputFiles() != 1) {
      return null;
    }

    // Check if table has a sort order
    if (table.sortOrder().isSorted()) {
      return null;
    }

    // Check for delete files
    boolean hasDeletes = group.fileScanTasks().stream().anyMatch(task -> !task.deletes().isEmpty());
    if (hasDeletes) {
      return null;
    }

    // Check that all files match the output spec (binary merge cannot transform partition specs)
    int outputSpecId = group.outputSpecId();
    boolean specMismatch =
        group.rewrittenFiles().stream().anyMatch(file -> file.specId() != outputSpecId);
    if (specMismatch) {
      return null;
    }

    // Validate Parquet-specific requirements and get schema
    return ParquetFileMerger.canMergeAndGetSchema(
        Lists.newArrayList(group.rewrittenFiles()), table.io(), group.maxOutputFileSize());
  }
}
