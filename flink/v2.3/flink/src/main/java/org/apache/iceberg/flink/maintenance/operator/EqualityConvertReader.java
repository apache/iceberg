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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parallel reader that processes {@link ReadCommand}s from the planner and emits {@link
 * IndexCommand}s. Dispatches on the wrapped {@link ContentScanTask} subtype: {@link FileScanTask}
 * (data file) emits {@code ADD_DATA_ROW} per row, our equality-delete task emits {@code
 * RESOLVE_DELETE} per row.
 */
@Internal
public class EqualityConvertReader extends ProcessFunction<ReadCommand, IndexCommand> {

  private static final Logger LOG = LoggerFactory.getLogger(EqualityConvertReader.class);

  public static final OutputTag<DVPosition> READER_ABORT_STREAM =
      new OutputTag<>("reader-abort-stream") {};

  private final TableLoader tableLoader;
  private final Set<Integer> eqFieldIds;
  private final boolean stagingOnTargetBranch;

  private transient Table table;
  private transient EqualityKeyReader keyReader;
  private transient DeletionVectorHelper deletionVectorHelper;

  public EqualityConvertReader(
      TableLoader tableLoader, Set<Integer> eqFieldIds, boolean stagingOnTargetBranch) {
    Preconditions.checkArgument(
        eqFieldIds != null && !eqFieldIds.isEmpty(), "eqFieldIds must not be null or empty");
    this.tableLoader = tableLoader;
    this.eqFieldIds = ImmutableSet.copyOf(eqFieldIds);
    this.stagingOnTargetBranch = stagingOnTargetBranch;
  }

  @Override
  public void open(OpenContext openContext) throws Exception {
    super.open(openContext);
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    table = tableLoader.loadTable();
    // Equality fields resolve against the current schema at build time, so it covers all ids.
    keyReader = new EqualityKeyReader(table.io(), table.schema(), eqFieldIds);
    deletionVectorHelper = new DeletionVectorHelper(table);
  }

  @Override
  public void processElement(ReadCommand cmd, Context ctx, Collector<IndexCommand> out)
      throws Exception {
    ContentScanTask<?> task = cmd.task();
    ContentFile<?> file = task.file();
    try {
      if (task instanceof FileScanTask dataTask) {
        processDataFile(
            dataTask,
            cmd.mainSnapshotId(),
            cmd.indexGeneration(),
            cmd.dataSequenceNumber(),
            cmd.staging(),
            out);
      } else if (task instanceof EqualityDeleteFileScanTask deleteTask) {
        processDeleteFile(
            deleteTask, cmd.mainSnapshotId(), cmd.indexGeneration(), cmd.dataSequenceNumber(), out);
      } else {
        throw new IllegalStateException(
            "Unexpected ContentScanTask type: " + task.getClass().getName());
      }
    } catch (Exception e) {
      LOG.error("Reader failed to process command for file={}", file.location(), e);
      ctx.output(TaskResultAggregator.ERROR_STREAM, e);
      ctx.output(READER_ABORT_STREAM, DVPosition.ABORT);
    }
  }

  private void processDataFile(
      FileScanTask task,
      Long mainSnapshotId,
      Long indexGeneration,
      long dataSequenceNumber,
      boolean staging,
      Collector<IndexCommand> out)
      throws IOException {
    ContentFile<?> file = task.file();
    PositionDeleteIndex existingDeletes = loadExistingDVs(task, file.location());

    int specId = file.specId();
    // Use the planner-serialized task spec, not the reader's table, which is loaded once and may
    // lack specs added after startup.
    Types.StructType partitionType = task.spec().partitionType();
    byte[] partitionBytes = keyReader.encodePartition(file.partition(), partitionType);

    keyReader.readLiveRows(
        file,
        existingDeletes,
        (key, position) ->
            out.collect(
                IndexCommand.addDataRow(
                    mainSnapshotId,
                    indexGeneration,
                    key,
                    file.location(),
                    position,
                    specId,
                    partitionBytes,
                    dataSequenceNumber,
                    staging)));
  }

  private void processDeleteFile(
      EqualityDeleteFileScanTask task,
      Long mainSnapshotId,
      Long indexGeneration,
      long dataSequenceNumber,
      Collector<IndexCommand> out)
      throws IOException {
    ContentFile<?> file = task.file();

    int specId = file.specId();
    // An unpartitioned equality delete applies globally; a partitioned one applies only within its
    // own spec. Use the planner-serialized task spec, not the reader's table, which is loaded once
    // and may lack specs added after startup.
    int deleteSpecId = task.spec().isUnpartitioned() ? IndexCommand.GLOBAL_DELETE_SPEC_ID : specId;

    keyReader.readKeys(
        file,
        key ->
            out.collect(
                IndexCommand.resolveDelete(
                    mainSnapshotId, indexGeneration, key, dataSequenceNumber, deleteSpecId)));
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }

  private PositionDeleteIndex loadExistingDVs(FileScanTask task, String dataFilePath) {
    List<DeleteFile> dvs = Lists.newArrayList();
    for (DeleteFile deleteFile : task.deletes()) {
      if (ContentFileUtil.isDV(deleteFile)) {
        dvs.add(deleteFile);
      } else if (deleteFile.content() == FileContent.POSITION_DELETES) {
        throw new IllegalStateException(
            String.format(
                "V2 positional delete file %s attached to main data file %s; "
                    + "the converter expects a V3 target with deletion vectors only.",
                deleteFile.location(), dataFilePath));
      } else if (deleteFile.content() == FileContent.EQUALITY_DELETES && !stagingOnTargetBranch) {
        // When stagingBranch == targetBranch the target carries unconverted equality deletes; they
        // are indexed as rows here and converted via the planner's RESOLVE_DELETE commands. On a
        // separate target branch an attached equality delete means an unconverted delete leaked
        // onto the target, which the converter cannot reason about.
        throw new IllegalStateException(
            String.format(
                "Equality delete file %s attached to main data file %s; the converter expects "
                    + "equality deletes only on the staging branch, converted to DVs on the target.",
                deleteFile.location(), dataFilePath));
      }
    }

    return deletionVectorHelper.load(dataFilePath, dvs);
  }
}
