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

import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.DeletionVectorHelper;
import org.apache.iceberg.flink.maintenance.operator.DeletionVectorHelper.FilePositions;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns the positions resolved during a checkpoint into deletion vectors. Keyed by the path of the
 * data file the positions belong to, so that a data file is handled by a single subtask and ends up
 * with a single deletion vector per commit.
 *
 * <p>A data file may carry at most one deletion vector, so the vector a file carries when the
 * barrier arrives is merged into the new one and replaced by it in the same commit. That vector may
 * no longer be current by the time the commit happens, for instance when the commit of the previous
 * checkpoint lands in between. The commit detects this and {@link IcebergCommitter} merges the
 * positions again against the table as it is then, so this operator keeps no state.
 *
 * <p>Positions are buffered in memory and written when the checkpoint barrier arrives or the input
 * ends, which is what makes the resulting files part of the same commit as the deletes that
 * produced them. The buffer is empty whenever a snapshot is taken, so a restore replays it from
 * upstream.
 */
@Internal
class DvOnlyDeletionVectorWriterOperator
    extends AbstractStreamOperator<CommittableMessage<SinkWriteResult>>
    implements OneInputStreamOperator<DVPosition, CommittableMessage<SinkWriteResult>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DvOnlyDeletionVectorWriterOperator.class);

  private final TableLoader tableLoader;
  private final String branch;

  private transient Table table;
  private transient DeletionVectorHelper deletionVectorHelper;
  private transient OutputFileFactory fileFactory;
  private transient Map<String, FilePositions> buffered;
  private transient int subtaskId;

  DvOnlyDeletionVectorWriterOperator(TableLoader tableLoader, String branch) {
    this.tableLoader = tableLoader;
    this.branch = branch;
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    table = tableLoader.loadTable();
    deletionVectorHelper = new DeletionVectorHelper(table);
    subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    int attemptId = getRuntimeContext().getTaskInfo().getAttemptNumber();
    fileFactory =
        OutputFileFactory.builderFor(table, subtaskId, attemptId).format(FileFormat.PUFFIN).build();
    buffered = Maps.newLinkedHashMap();
  }

  @Override
  public void processElement(StreamRecord<DVPosition> element) {
    DVPosition position = element.getValue();
    buffered
        .computeIfAbsent(
            position.dataFilePath(),
            path -> new FilePositions(position.specId(), position.partition()))
        .add(position.position());
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush(checkpointId);
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void finish() throws Exception {
    flush(DvOnlyExecution.END_OF_INPUT);
    super.finish();
  }

  private void flush(long checkpointId) {
    if (!buffered.isEmpty()) {
      writeDeletionVectors(checkpointId);
      buffered.clear();
    }
  }

  private void writeDeletionVectors(long checkpointId) {
    table.refresh();
    Map<String, DeleteFile> attachedVectors =
        deletionVectorHelper.collectExistingDVs(table.snapshot(branch), buffered);
    DeleteWriteResult result = deletionVectorHelper.write(fileFactory, buffered, attachedVectors);
    LOG.info(
        "Wrote {} deletion vector(s), superseding {}, for checkpoint {}",
        result.deleteFiles().size(),
        result.rewrittenDeleteFiles().size(),
        checkpointId);

    WriteResult writeResult =
        WriteResult.builder()
            .addDeleteFiles(result.deleteFiles())
            .addRewrittenDeleteFiles(result.rewrittenDeleteFiles())
            .addReferencedDataFiles(result.referencedDataFiles())
            .build();
    output.collect(
        new StreamRecord<>(
            new CommittableWithLineage<>(
                new SinkWriteResult(writeResult), checkpointId, subtaskId)));
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }
}
