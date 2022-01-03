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
import org.apache.flink.core.io.SimpleVersionedSerialization;
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
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergStreamRewriter extends AbstractStreamOperator<RewriteResult>
    implements OneInputStreamOperator<PartitionFileGroup, RewriteResult>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  public static final double MAX_FILE_SIZE_DEFAULT_RATIO = 1.80d;

  private static final Logger LOG = LoggerFactory.getLogger(IcebergStreamRewriter.class);

  private final TableLoader tableLoader;

  private transient Table table;
  private transient String nameMapping;
  private transient boolean caseSensitive;
  private transient RowDataFileScanTaskReader rowDataReader;
  private transient TaskWriterFactory<RowData> taskWriterFactory;
  private transient ManifestOutputFileFactory manifestOutputFileFactory;

  private transient int maxFilesCount;
  private transient long targetSizeInBytes;
  private transient long maxFileSizeInBytes;
  private transient Integer splitLookback;
  private transient Long splitOpenFileCost;

  private transient Map<StructLikeWrapper, RewriteFileGroup> pendingFileGroupByPartition;
  private transient List<RewriteFileGroup> rewrittenFileGroups;

  private static final ListStateDescriptor<byte[]> STATE_DESCRIPTOR = buildStateDescriptor();
  private transient ListState<byte[]> pendingRewriteFileGroupsState;

  IcebergStreamRewriter(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    // Open the table loader and load the table.
    this.tableLoader.open();
    this.table = tableLoader.loadTable();
    this.pendingFileGroupByPartition = Maps.newHashMap();
    this.rewrittenFileGroups = Lists.newArrayList();

    validateAndInitOptions(table.properties());

    // init dependence
    String flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    int attemptId = getRuntimeContext().getAttemptNumber();

    this.rowDataReader = new RowDataFileScanTaskReader(table.schema(), table.schema(), nameMapping, caseSensitive);

    String formatString = PropertyUtil.propertyAsString(table.properties(),
        TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.valueOf(formatString.toUpperCase(Locale.ENGLISH));
    RowType flinkSchema = FlinkSchemaUtil.convert(table.schema());
    this.taskWriterFactory = new RowDataTaskWriterFactory(
        SerializableTable.copyOf(table), flinkSchema, Long.MAX_VALUE, format, null, false);
    taskWriterFactory.initialize(subTaskId, attemptId);

    this.manifestOutputFileFactory = FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, subTaskId, attemptId);

    this.pendingRewriteFileGroupsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    if (context.isRestored()) {
      for (byte[] bytes : pendingRewriteFileGroupsState.get()) {
        RewriteFileGroup rewriteFileGroup = SimpleVersionedSerialization.readVersionAndDeSerialize(
            RewriteFileGroup.Serializer.INSTANCE, bytes);
        pendingFileGroupByPartition.put(wrap(rewriteFileGroup.partition()), rewriteFileGroup);
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    long checkpointId = context.getCheckpointId();
    LOG.info("Start to flush rewrite state to state backend, table: {}, checkpointId: {}", table, checkpointId);

    List<byte[]> pendingRewriteFileGroups = Lists.newArrayListWithCapacity(pendingFileGroupByPartition.size());
    for (RewriteFileGroup rewriteFileGroup : pendingFileGroupByPartition.values()) {
      pendingRewriteFileGroups.add(SimpleVersionedSerialization.writeVersionAndSerialize(
          RewriteFileGroup.Serializer.INSTANCE, rewriteFileGroup));
    }
    pendingRewriteFileGroupsState.clear();
    pendingRewriteFileGroupsState.addAll(pendingRewriteFileGroups);

    // keep rewritten file groups manifests until checkpoint to prevent lost manifest when restore from checkpoint.
    for (RewriteFileGroup fileGroup : rewrittenFileGroups) {
      for (ManifestFile file : fileGroup.manifestFiles()) {
        try {
          table.io().deleteFile(file.path());
        } catch (Exception e) {
          LOG.warn("The file group {} has been rewritten, but we failed to clean the temporary manifests: {}",
              fileGroup, file.path(), e);
        }
      }
    }
    rewrittenFileGroups.clear();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    // TODO 使用latestSnapshotId检测多少个ckpt没有新数据加入，可合并
  }

  @Override
  public void processElement(StreamRecord<PartitionFileGroup> record) throws Exception {
    PartitionFileGroup partitionFileGroup = record.getValue();

    StructLikeWrapper partition = wrap(partitionFileGroup.partition());
    RewriteFileGroup rewriteFileGroup = pendingFileGroupByPartition.getOrDefault(partition,
        new RewriteFileGroup(partitionFileGroup.partition()));

    int dataFilesCount = partitionFileGroup.dataFiles().length;
    long dataFilesSize = Arrays.stream(partitionFileGroup.dataFiles()).mapToLong(ContentFile::fileSizeInBytes).sum();
    DeltaManifests deltaManifests = FlinkManifestUtil.writeExistingFiles(
        partitionFileGroup.sequenceNumber(), partitionFileGroup.snapshotId(),
        partitionFileGroup.dataFiles(), partitionFileGroup.deleteFiles(),
        () -> manifestOutputFileFactory.createTmp(), table.spec()
    );
    rewriteFileGroup.add(dataFilesCount, dataFilesSize,
        partitionFileGroup.sequenceNumber(), partitionFileGroup.snapshotId(), deltaManifests);

    pendingFileGroupByPartition.putIfAbsent(partition, rewriteFileGroup);

    rewriteFiles(partition, rewriteFileGroup);
  }

  private void rewriteFiles(StructLikeWrapper partition, RewriteFileGroup rewriteFileGroup) throws IOException {
    if (rewriteFileGroup.filesSize() < targetSizeInBytes && rewriteFileGroup.filesCount() < maxFilesCount) {
      return;
    }

    // TODO rewrite性能优化
    LOG.info("Rewriting file group of table {}: {}.", table, rewriteFileGroup);

    long start = System.currentTimeMillis();
    RewriteResult rewriteResult = rewrite(rewriteFileGroup);
    long duration = System.currentTimeMillis() - start;
    LOG.info("Rewritten file group {} in {} ms.", rewriteFileGroup, duration);

    emit(rewriteResult);

    rewrittenFileGroups.add(pendingFileGroupByPartition.remove(partition));
  }

  private RewriteResult rewrite(RewriteFileGroup rewriteFileGroup) throws IOException {
    CloseableIterable<FileScanTask> fileScanTasks = table.newScan()
        .useManifests(rewriteFileGroup.manifestFiles())
        .caseSensitive(caseSensitive)
        .ignoreResiduals()
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

    return RewriteResult.builder(rewriteFileGroup.latestSnapshotId())
        .partition(rewriteFileGroup.partition())
        .addAddedDataFiles(addedDataFiles)
        .addRewrittenDataFiles(currentDataFiles)
        .build();
  }

  private void emit(RewriteResult result) {
    output.collect(new StreamRecord<>(result));
  }

  @Override
  public void endInput() throws Exception {
    LOG.info("Flush remain rewrite file groups for table '{}'", table);
    for (Map.Entry<StructLikeWrapper, RewriteFileGroup> entry : pendingFileGroupByPartition.entrySet()) {
      rewriteFiles(entry.getKey(), entry.getValue());
    }
  }

  private StructLikeWrapper wrap(StructLike partition) {
    return StructLikeWrapper.forType(table.spec().partitionType()).set(partition);
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

  private static ListStateDescriptor<byte[]> buildStateDescriptor() {
    TypeInformation<byte[]> info = TypeInformation.of(new TypeHint<byte[]>() {});
    return new ListStateDescriptor<>("iceberg-streaming-rewriter-state", info);
  }

  private void validateAndInitOptions(Map<String, String> properties) {
    nameMapping = PropertyUtil.propertyAsString(properties, TableProperties.DEFAULT_NAME_MAPPING, null);
    caseSensitive = PropertyUtil.propertyAsBoolean(properties, FlinkSinkOptions.STREAMING_REWRITE_CASE_SENSITIVE,
        FlinkSinkOptions.STREAMING_REWRITE_CASE_SENSITIVE_DEFAULT);

    maxFilesCount = PropertyUtil.propertyAsInt(properties, FlinkSinkOptions.STREAMING_REWRITE_MAX_FILES_COUNT,
        FlinkSinkOptions.STREAMING_REWRITE_MAX_FILES_COUNT_DEFAULT);
    Preconditions.checkArgument(maxFilesCount > 0, "Cannot set %s to a negative number, %d < 0",
        FlinkSinkOptions.STREAMING_REWRITE_MAX_FILES_COUNT, maxFilesCount);

    targetSizeInBytes = PropertyUtil.propertyAsLong(properties, FlinkSinkOptions.STREAMING_REWRITE_TARGET_FILE_SIZE,
        FlinkSinkOptions.STREAMING_REWRITE_TARGET_FILE_SIZE_DEFAULT);
    Preconditions.checkArgument(targetSizeInBytes > 0, "Cannot set %s to a negative number, %d < 0",
        FlinkSinkOptions.STREAMING_REWRITE_TARGET_FILE_SIZE, targetSizeInBytes);

    // Use a larger max target file size than target size to avoid creating tiny remainder files.
    maxFileSizeInBytes = (long) (targetSizeInBytes * MAX_FILE_SIZE_DEFAULT_RATIO);

    splitLookback = PropertyUtil.propertyAsInt(properties,
        TableProperties.SPLIT_LOOKBACK, TableProperties.SPLIT_LOOKBACK_DEFAULT);
    Preconditions.checkArgument(splitLookback > 0, "Cannot set %s to a negative number or zero, %d <= 0",
        TableProperties.SPLIT_LOOKBACK, splitLookback);

    splitOpenFileCost = PropertyUtil.propertyAsLong(properties,
        TableProperties.SPLIT_OPEN_FILE_COST, TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
    Preconditions.checkArgument(splitOpenFileCost >= 0, "Cannot set %s to a negative number, %d < 0",
        TableProperties.SPLIT_OPEN_FILE_COST, splitOpenFileCost);
  }
}
