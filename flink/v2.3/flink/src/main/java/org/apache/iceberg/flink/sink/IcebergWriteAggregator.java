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
import java.util.Collection;
import java.util.Map;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator which aggregates the individual {@link SinkWriteResult} objects) to a single {@link
 * IcebergCommittable} per checkpoint (storing the serialized {@link
 * org.apache.iceberg.flink.sink.DeltaManifests}, jobId, operatorId, checkpointId)
 */
class IcebergWriteAggregator extends AbstractStreamOperator<CommittableMessage<IcebergCommittable>>
    implements OneInputStreamOperator<
        CommittableMessage<SinkWriteResult>, CommittableMessage<IcebergCommittable>> {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriteAggregator.class);
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

  private final Collection<WriteResult> results;
  private final TableLoader tableLoader;
  private final boolean dvOnlyMode;

  private long lastCheckpointId = CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1;

  // Only reported when the sink resolves equality deletes itself, see SinkWriteResult#baseline.
  private Long baselineSnapshotId = null;

  private transient ManifestOutputFileFactory icebergManifestOutputFileFactory;
  private transient Table table;

  IcebergWriteAggregator(TableLoader tableLoader) {
    this(tableLoader, false);
  }

  /**
   * @param dvOnlyMode whether the sink resolves equality deletes to deletion vectors itself, whose
   *     manifests need a newer serialization version, see {@link DeltaManifestsSerializer}
   */
  IcebergWriteAggregator(TableLoader tableLoader, boolean dvOnlyMode) {
    this.results = Sets.newHashSet();
    this.tableLoader = tableLoader;
    this.dvOnlyMode = dvOnlyMode;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    context
        .getRestoredCheckpointId()
        .ifPresent(checkpointId -> this.lastCheckpointId = checkpointId);
  }

  @Override
  public void open() throws Exception {
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }

    String flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    String operatorId = getOperatorID().toString();
    int subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    Preconditions.checkArgument(
        subTaskId == 0, "The subTaskId must be zero in the IcebergWriteAggregator");
    int attemptId = getRuntimeContext().getTaskInfo().getAttemptNumber();
    this.table = tableLoader.loadTable();

    this.icebergManifestOutputFileFactory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), flinkJobId, operatorId, subTaskId, attemptId);
  }

  @Override
  public void finish() throws IOException {
    prepareSnapshotPreBarrier(lastCheckpointId + 1);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws IOException {
    if (checkpointId == lastCheckpointId) {
      // Already flushed. This can happen when finish() above triggers flushing prior creating the
      // final checkpoint. The calls are mutually exclusive, but we need to ensure we don't flush
      // twice.
      LOG.info("Aggregated writes for checkpoint id {} already flushed.", checkpointId);
      return;
    }

    this.lastCheckpointId = checkpointId;

    IcebergCommittable committable =
        new IcebergCommittable(
            writeToManifest(results, checkpointId),
            getContainingTask().getEnvironment().getJobID().toString(),
            getRuntimeContext().getOperatorUniqueID(),
            checkpointId);
    CommittableMessage<IcebergCommittable> summary =
        new CommittableSummary<>(0, 1, checkpointId, 1, 1, 0);
    output.collect(new StreamRecord<>(summary));
    CommittableMessage<IcebergCommittable> message =
        new CommittableWithLineage<>(committable, checkpointId, 0);
    output.collect(new StreamRecord<>(message));
    LOG.info("Emitted commit message to downstream committer operator");
    results.clear();
    baselineSnapshotId = null;
  }

  /**
   * Write all the completed data files to a newly created manifest file and return the manifest's
   * avro serialized bytes.
   */
  public byte[] writeToManifest(Collection<WriteResult> writeResults, long checkpointId)
      throws IOException {
    if (writeResults.isEmpty()) {
      return EMPTY_MANIFEST_DATA;
    }

    WriteResult result = WriteResult.builder().addAll(writeResults).build();
    if (!dvOnlyMode) {
      DeltaManifests deltaManifests =
          FlinkManifestUtil.writeCompletedFiles(
              result,
              () -> icebergManifestOutputFileFactory.create(checkpointId),
              table.spec(),
              TableUtil.formatVersion(table));
      return SimpleVersionedSerialization.writeVersionAndSerialize(
          DeltaManifestsSerializer.INSTANCE, deltaManifests);
    }

    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            result,
            () -> icebergManifestOutputFileFactory.create(checkpointId),
            table.spec(),
            specsOf(result),
            TableUtil.formatVersion(table),
            baselineSnapshotId);
    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.DV_ONLY, deltaManifests);
  }

  private Map<Integer, PartitionSpec> specsOf(WriteResult result) {
    for (DeleteFile[] deleteFiles :
        ImmutableList.of(result.deleteFiles(), result.rewrittenDeleteFiles())) {
      for (DeleteFile deleteFile : deleteFiles) {
        if (!table.specs().containsKey(deleteFile.specId())) {
          table.refresh();
          return table.specs();
        }
      }
    }

    return table.specs();
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<SinkWriteResult>> element)
      throws Exception {

    if (element.isRecord() && element.getValue() instanceof CommittableWithLineage) {
      SinkWriteResult result =
          ((CommittableWithLineage<SinkWriteResult>) element.getValue()).getCommittable();
      if (result.isBaseline()) {
        // Carries no files, only the snapshot the deletes of this checkpoint were resolved against.
        baselineSnapshotId = result.baselineSnapshotId();
        return;
      }

      results.add(result.writeResult());
    }
  }
}
