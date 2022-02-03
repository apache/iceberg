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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergStreamRewriter extends AbstractStreamOperator<RewriteResult>
    implements OneInputStreamOperator<RewriteTask, RewriteResult> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergStreamRewriter.class);

  private final TableLoader tableLoader;

  private transient Table table;
  private transient RowDataFileScanTaskReader rowDataReader;
  private transient TaskWriterFactory<RowData> taskWriterFactory;

  IcebergStreamRewriter(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void open() throws Exception {
    super.open();

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();

    // init dependence
    String nameMapping = PropertyUtil.propertyAsString(table.properties(),
        TableProperties.DEFAULT_NAME_MAPPING, null);
    boolean caseSensitive = PropertyUtil.propertyAsBoolean(table.properties(),
        FlinkSinkOptions.STREAMING_REWRITE_CASE_SENSITIVE, FlinkSinkOptions.STREAMING_REWRITE_CASE_SENSITIVE_DEFAULT);
    this.rowDataReader = new RowDataFileScanTaskReader(table.schema(), table.schema(), nameMapping, caseSensitive);

    int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    int attemptId = getRuntimeContext().getAttemptNumber();
    String formatString = PropertyUtil.propertyAsString(table.properties(),
        TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
    RowType flinkSchema = FlinkSchemaUtil.convert(table.schema());
    this.taskWriterFactory = new RowDataTaskWriterFactory(
        SerializableTable.copyOf(table), flinkSchema, Long.MAX_VALUE, format, null, false);
    taskWriterFactory.initialize(subTaskId, attemptId);
  }

  @Override
  public void processElement(StreamRecord<RewriteTask> record) throws Exception {
    RewriteTask rewriteTask = record.getValue();

    LOG.info("Rewriting task {}.", rewriteTask);
    long start = System.currentTimeMillis();
    RewriteResult rewriteResult = rewrite(rewriteTask.snapshotId(), rewriteTask.partition(), rewriteTask.task());
    LOG.info("Rewritten task {} in {} ms.", rewriteTask, System.currentTimeMillis() - start);

    emit(rewriteResult);
  }

  private RewriteResult rewrite(long snapshotId, StructLike partition, CombinedScanTask task) throws IOException {
    TaskWriter<RowData> writer = taskWriterFactory.create();
    try (DataIterator<RowData> iterator = new DataIterator<>(rowDataReader, task, table.io(), table.encryption())) {
      while (iterator.hasNext()) {
        RowData rowData = iterator.next();
        writer.write(rowData);
      }
    } catch (Throwable originalThrowable) {
      try {
        writer.abort();
      } catch (Throwable inner) {
        if (originalThrowable != inner) {
          originalThrowable.addSuppressed(inner);
          LOG.warn("Suppressing exception in catch: {}", inner.getMessage(), inner);
        }
      }

      if (originalThrowable instanceof Exception) {
        throw originalThrowable;
      } else {
        throw new RuntimeException(originalThrowable);
      }
    }

    List<DataFile> addedDataFiles = Lists.newArrayList(writer.dataFiles());
    List<DataFile> currentDataFiles = task.files().stream().map(FileScanTask::file) .collect(Collectors.toList());

    return RewriteResult.builder(snapshotId, table.spec().partitionType())
        .partition(partition)
        .addAddedDataFiles(addedDataFiles)
        .addRewrittenDataFiles(currentDataFiles)
        .build();
  }

  private void emit(RewriteResult result) {
    output.collect(new StreamRecord<>(result));
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (tableLoader != null) {
      tableLoader.close();
    }
  }
}
