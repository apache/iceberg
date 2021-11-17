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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StaticDataTableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE_DEFAULT;

class IcebergStreamRewriter extends AbstractStreamOperator<RewriteResult>
    implements OneInputStreamOperator<CommitResult, RewriteResult>, BoundedOneInput {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergStreamRewriter.class);

  public static final String MAX_FILES_COUNT = "flink.rewrite.max-files-count";
  public static final int MAX_FILES_COUNT_DEFAULT = Integer.MAX_VALUE;
  public static final String TARGET_FILE_SIZE = "flink.rewrite.target-file-size-bytes";
  public static final long TARGET_FILE_SIZE_DEFAULT = TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

  private final TableLoader tableLoader;
  private transient Table table;
  private transient TableOperations ops;
  private transient ManifestOutputFileFactory manifestOutputFileFactory;
  private transient RowDataFileScanTaskReader rowDataReader;
  private transient TaskWriterFactory<RowData> taskWriterFactory;

  private transient int maxFilesCount;
  private transient long targetSizeInBytes;
  private transient long maxFileSizeInBytes;
  private transient Integer splitLookback;
  private transient Long splitOpenFileCost;

  private final Map<StructLike, DataFileGroup> dataFileGroupByPartition = Maps.newHashMap();

  private static final ListStateDescriptor<Map<StructLike, DataFileGroup>> STATE_DESCRIPTOR = buildStateDescriptor();
  private transient ListState<Map<StructLike, DataFileGroup>> dataFileGroupsState;

  IcebergStreamRewriter(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();
    this.ops = ((HasTableOperations) table).operations();

    validateAndInitOptions(table.properties());

    // init dependence
    String flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    int attemptId = getRuntimeContext().getAttemptNumber();

    this.rowDataReader = new RowDataFileScanTaskReader(table.schema(), table.schema(), null, false);

    String formatString = PropertyUtil.propertyAsString(table.properties(), TableProperties.DEFAULT_FILE_FORMAT,
        TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
    RowType flinkSchema = FlinkSchemaUtil.convert(table.schema());
    this.taskWriterFactory = new RowDataTaskWriterFactory(
        SerializableTable.copyOf(table), flinkSchema, Long.MAX_VALUE, format, null, false);
    taskWriterFactory.initialize(subTaskId, attemptId);

    this.manifestOutputFileFactory = FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, subTaskId, attemptId);

    // restore state
    this.dataFileGroupsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    if (context.isRestored()) {
      dataFileGroupByPartition.putAll(dataFileGroupsState.get().iterator().next());
    }
  }

  @Override
  public void open() throws Exception {
    super.open();
  }

  @Override
  public void processElement(StreamRecord<CommitResult> record) throws Exception {
    CommitResult result = record.getValue();
    DataFileGroup fileGroup = dataFileGroupByPartition.getOrDefault(result.partition(), new DataFileGroup());

    int dataFilesCount = result.writeResult().dataFiles().length;
    long dataFilesSize = Arrays.stream(result.writeResult().dataFiles())
        .map(ContentFile::fileSizeInBytes)
        .reduce(0L, (acc, size) -> acc += size);
    DeltaManifests deltaManifests = FlinkManifestUtil.writeExistingFiles(
        result.snapshotId(), result.sequenceNumber(), result.writeResult(),
        () -> manifestOutputFileFactory.createTmp(), table.specs().get(result.specId())
    );
    fileGroup.append(dataFilesCount, dataFilesSize, result.snapshotId(), result.sequenceNumber(), deltaManifests);

    dataFileGroupByPartition.put(result.partition(), fileGroup);

    rewriteFileGroup(result.partition(), fileGroup);
  }

  private void emit(RewriteResult result) {
    output.collect(new StreamRecord<>(result));
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);

    dataFileGroupsState.clear();
    dataFileGroupsState.add(dataFileGroupByPartition);
  }

  private void rewriteFileGroup(StructLike partition, DataFileGroup fileGroup) {
    if (fileGroup.filesSize() < targetSizeInBytes && fileGroup.filesCount() < maxFilesCount) {
      return;
    }
    String description = MoreObjects.toStringHelper(DataFileGroup.class)
        .add("partition", partition)
        .add("latestSequenceNumber", fileGroup.latestSequenceNumber())
        .add("latestSnapshotId", fileGroup.latestSnapshotId())
        .add("filesCount", fileGroup.filesCount())
        .add("filesSize", fileGroup.filesSize())
        .toString();
    LOG.info("Rewriting file group of table {}: {}.", table, description);

    long start = System.currentTimeMillis();
    try {
      RewriteResult rewriteResult = rewrite(partition, fileGroup);
      emit(rewriteResult);
    } catch (Exception e) {
      LOG.error("Rewrite file group {} is fail and will retry in next record.", description, e);
      return;
    }
    long duration = System.currentTimeMillis() - start;
    LOG.info("Rewritten file group {} in {} ms.", description, duration);

    dataFileGroupByPartition.remove(partition);
    for (ManifestFile file : fileGroup.manifestFiles()) {
      try {
        table.io().deleteFile(file.path());
      } catch (Exception e) {
        LOG.warn("The file group {} has been rewritten, but we failed to clean the temporary manifests: {}",
            description, file.path(), e);
      }
    }
  }

  private RewriteResult rewrite(StructLike partition, DataFileGroup fileGroup) throws IOException {
    CloseableIterable<FileScanTask> fileScanTasks = StaticDataTableScan.of(table, ops)
        .scan(fileGroup.manifestFiles())
        .planFiles();

    CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(fileScanTasks, targetSizeInBytes);
    CloseableIterable<CombinedScanTask> scanTasks = TableScanUtil.planTasks(
        splitFiles, maxFileSizeInBytes, splitLookback, splitOpenFileCost);

    List<DataFile> addedDataFiles = Lists.newArrayList();
    for (CombinedScanTask task : scanTasks) {
      TaskWriter<RowData> writer = taskWriterFactory.create();
      try (DataIterator<RowData> iterator = new DataIterator<>(rowDataReader, task, table.io(), table.encryption())) {
        while (iterator.hasNext()) {
          RowData rowData = iterator.next();
          writer.write(rowData);
        }
        Collections.addAll(addedDataFiles, writer.dataFiles());
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
    }

    List<DataFile> currentDataFiles = Lists.newArrayList();
    scanTasks.iterator().forEachRemaining(tasks -> tasks.files().forEach(task -> currentDataFiles.add(task.file())));

    return RewriteResult.builder()
        .partition(partition)
        .startingSnapshotSeqNum(fileGroup.latestSequenceNumber())
        .startingSnapshotId(fileGroup.latestSnapshotId())
        .addAddedDataFiles(addedDataFiles)
        .addDeletedDataFiles(currentDataFiles)
        .build();
  }

  @Override
  public void endInput() throws Exception {
    dataFileGroupByPartition.forEach(this::rewriteFileGroup);
  }

  @Override
  public void dispose() throws Exception {
    super.dispose();
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

  private static ListStateDescriptor<Map<StructLike, DataFileGroup>> buildStateDescriptor() {
    TypeInformation<Map<StructLike, DataFileGroup>> info = TypeInformation.of(
        new TypeHint<Map<StructLike, DataFileGroup>>() {}
    );
    return new ListStateDescriptor<>("iceberg-stream-rewriter-state", info);
  }

  private void validateAndInitOptions(Map<String, String> properties) {
    maxFilesCount = PropertyUtil.propertyAsInt(properties, MAX_FILES_COUNT, MAX_FILES_COUNT_DEFAULT);
    Preconditions.checkArgument(maxFilesCount > 0,
        "Cannot set %s to a negative number, %d < 0", MAX_FILES_COUNT, maxFilesCount);

    long splitSize = PropertyUtil.propertyAsLong(properties, SPLIT_SIZE, SPLIT_SIZE_DEFAULT);
    Preconditions.checkArgument(splitSize > 0,
        "Cannot set %s to a negative number or zero, %d <= 0", SPLIT_SIZE, splitSize);

    long targetFileSize = PropertyUtil.propertyAsLong(properties, TARGET_FILE_SIZE, TARGET_FILE_SIZE_DEFAULT);
    Preconditions.checkArgument(targetFileSize > 0,
        "Cannot set %s to a negative number, %d < 0", TARGET_FILE_SIZE, targetFileSize);

    targetSizeInBytes = Math.min(splitSize, targetFileSize);

    // Use a larger max target file size than target size to avoid creating tiny remainder files.
    maxFileSizeInBytes = (long) (targetFileSize * 1.5);

    splitLookback = PropertyUtil.propertyAsInt(properties, SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT);
    Preconditions.checkArgument(splitLookback > 0,
        "Cannot set %s to a negative number or zero, %d <= 0", SPLIT_LOOKBACK, splitLookback);

    splitOpenFileCost = PropertyUtil.propertyAsLong(properties, SPLIT_OPEN_FILE_COST, SPLIT_OPEN_FILE_COST_DEFAULT);
    Preconditions.checkArgument(splitOpenFileCost >= 0,
        "Cannot set %s to a negative number, %d < 0", SPLIT_OPEN_FILE_COST, splitOpenFileCost);
  }
}
