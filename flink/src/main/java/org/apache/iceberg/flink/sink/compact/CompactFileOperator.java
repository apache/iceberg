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

package org.apache.iceberg.flink.sink.compact;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.CommonControllerMessage;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.CompactCommitInfo;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.CompactionUnit;
import org.apache.iceberg.flink.sink.compact.SmallFilesMessage.EndCompaction;
import org.apache.iceberg.flink.source.RowDataRewriter;
import org.apache.iceberg.flink.source.RowDataRewriter.RewriteMap;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

public class CompactFileOperator extends AbstractStreamOperator<CommonControllerMessage>
    implements OneInputStreamOperator<CommonControllerMessage, CommonControllerMessage>, BoundedOneInput {
  private static final Logger LOG = LoggerFactory.getLogger(CompactFileOperator.class);

  private final TableLoader tableLoader;
  private transient Table table;
  private transient int subTaskId;
  private transient int attemptId;

  private Schema schema;
  private FileFormat format;
  private String nameMapping;
  private FileIO io;
  private boolean caseSensitive;
  private EncryptionManager encryptionManager;
  private TaskWriterFactory<RowData> taskWriterFactory;
  private final List<Integer> equalityFieldIds;

  //  cache compact files data
  private final List<DataFile> dataFiles = Lists.newArrayList();
  private final List<DataFile> currentDataFiles = Lists.newArrayList();
  private final List<DataFile> deleteFiles = Lists.newArrayList();

  public CompactFileOperator(TableLoader tableLoader, List<Integer> equalityFieldIds) {
    this.tableLoader = tableLoader;
    this.equalityFieldIds = equalityFieldIds;
  }

  @Override
  public void open() {
    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();

    // Initialize the task writer factory.
    this.taskWriterFactory.initialize(subTaskId, attemptId);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    this.tableLoader.open();
    this.table = this.tableLoader.loadTable();

    this.schema = table.schema();
    this.caseSensitive = false;
    this.io = table.io();
    this.encryptionManager = table.encryption();
    this.nameMapping = PropertyUtil.propertyAsString(table.properties(), DEFAULT_NAME_MAPPING, null);

    String formatString = PropertyUtil.propertyAsString(table.properties(), TableProperties.DEFAULT_FILE_FORMAT,
        TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    this.format = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
    RowType flinkSchema = FlinkSchemaUtil.convert(table.schema());

    this.taskWriterFactory = new RowDataTaskWriterFactory(
        SerializableTable.copyOf(table),
        flinkSchema,
        Long.MAX_VALUE,
        format,
        null,
        false);
  }

  @Override
  public void processElement(StreamRecord<CommonControllerMessage> element) throws Exception {
    LOG.debug("Received primary keys：{}", this.equalityFieldIds);
    CommonControllerMessage value = element.getValue();
    if (value instanceof CompactionUnit) {
      CompactionUnit unit = (CompactionUnit) value;

      if (unit.isTaskMessage(
              getRuntimeContext().getNumberOfParallelSubtasks(),
              getRuntimeContext().getIndexOfThisSubtask())) {
        RowDataRewriter.RewriteMap rewriteMap = new RewriteMap(
                schema,
                nameMapping,
                io,
                caseSensitive,
                encryptionManager,
                taskWriterFactory);

        List<DataFile> addedDataFiles = rewriteMap.map(unit.getCombinedScanTask());
        dataFiles.addAll(addedDataFiles);

        List<DataFile> existDataFiles = unit.getCombinedScanTask()
                .files()
                .stream()
                .map(FileScanTask::file)
                .collect(Collectors.toList());
        currentDataFiles.addAll(existDataFiles);

        LOG.info("Rewrite CompactionUnit (id: {}, to delete {} files: {}, to add {} files: {})",
                unit.getUnitId(),
                existDataFiles.size(),
                Joiner.on(", ").join(existDataFiles.stream()
                        .map(s -> s.content() + "=>" + s.fileSizeInBytes() / 1024 / 1024 + "M")
                        .collect(Collectors.toList())),
                addedDataFiles.size(),
                Joiner.on(",").join(addedDataFiles.stream()
                        .map(s -> s.content() + "=>" + s.fileSizeInBytes() / 1024 / 1024 + "M")
                        .collect(Collectors.toList())));
      }
    } else if (value instanceof EndCompaction) {
      EndCompaction endTaskCompact = (EndCompaction) value;
      long endCheckpointId = endTaskCompact.getCheckpointId();
      long startingSnapshotId = endTaskCompact.getStartingSnapshotId();
      emit(new CompactCommitInfo(endCheckpointId,
              getRuntimeContext().getIndexOfThisSubtask(),
              getRuntimeContext().getNumberOfParallelSubtasks(),
              startingSnapshotId,
              new ArrayList<>(this.dataFiles),
              new ArrayList<>(this.currentDataFiles)));

      LOG.info("Summary: checkpoint {}, emit CompactCommitInfo (total delete {} files: {}, total add {} files: {})",
              endCheckpointId,
              currentDataFiles.size(),
              Joiner.on(", ").join(currentDataFiles.stream()
                      .map(s -> s.content() + "=>" + s.fileSizeInBytes() / 1024 / 1024 + "M")
                      .collect(Collectors.toList())),
              dataFiles.size(),
              Joiner.on(",").join(dataFiles.stream()
                      .map(s -> s.content() + "=>" + s.fileSizeInBytes() / 1024 / 1024 + "M")
                      .collect(Collectors.toList())));

      this.dataFiles.clear();
      this.currentDataFiles.clear();
    }
  }

  /**
   * It is notified that no more data will arrive on the input.
   */
  @Override
  public void endInput() throws Exception {
  }

  @Override
  public void dispose() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

  private void emit(CommonControllerMessage result) {
    output.collect(new StreamRecord<>(result));
  }
}
