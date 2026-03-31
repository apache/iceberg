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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator which aggregates the individual {@link WriteResult} objects) to a single {@link
 * IcebergCommittable} per checkpoint (storing the serialized {@link
 * org.apache.iceberg.flink.sink.DeltaManifests}, jobId, operatorId, checkpointId)
 */
class IcebergWriteAggregator extends AbstractStreamOperator<CommittableMessage<IcebergCommittable>>
    implements OneInputStreamOperator<
        CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>> {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriteAggregator.class);
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];
  private static final IcebergCommittableSerializer COMMITTABLE_SERIALIZER =
      new IcebergCommittableSerializer();

  private final Collection<WriteResult> results;
  private final TableLoader tableLoader;
  @Nullable private final CommitGate commitGate;

  private long lastCheckpointId = CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1;

  private transient ManifestOutputFileFactory icebergManifestOutputFileFactory;
  private transient Table table;
  private transient ListState<byte[]> pendingCommittablesState;
  private transient boolean wasPausedPreviously = false;
  private transient boolean forceFlush = false;

  IcebergWriteAggregator(TableLoader tableLoader) {
    this(tableLoader, null);
  }

  IcebergWriteAggregator(TableLoader tableLoader, @Nullable CommitGate commitGate) {
    this.results = Sets.newHashSet();
    this.tableLoader = tableLoader;
    this.commitGate = commitGate;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    context
        .getRestoredCheckpointId()
        .ifPresent(checkpointId -> this.lastCheckpointId = checkpointId);

    if (commitGate != null) {
      ListStateDescriptor<byte[]> descriptor =
          new ListStateDescriptor<>(
              "iceberg-aggregator-pending-committables", BytePrimitiveArraySerializer.INSTANCE);
      pendingCommittablesState = context.getOperatorStateStore().getListState(descriptor);
      wasPausedPreviously = pendingCommittablesState.get().iterator().hasNext();
    }
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
    forceFlush = true;
    try {
      prepareSnapshotPreBarrier(lastCheckpointId + 1);
    } finally {
      forceFlush = false;
    }
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

    boolean isGated =
        commitGate != null && !forceFlush && !commitGate.isCommitAllowed(checkpointId);

    if (wasPausedPreviously && !isGated && pendingCommittablesState != null) {
      flushBufferedCommittables();
    }

    IcebergCommittable committable =
        new IcebergCommittable(
            writeToManifest(results, checkpointId),
            getContainingTask().getEnvironment().getJobID().toString(),
            getRuntimeContext().getOperatorUniqueID(),
            checkpointId);

    if (isGated) {
      try {
        byte[] serialized =
            SimpleVersionedSerialization.writeVersionAndSerialize(
                COMMITTABLE_SERIALIZER, committable);
        pendingCommittablesState.add(serialized);
      } catch (Exception e) {
        throw new IOException("Failed to serialize committable for buffering", e);
      }
      LOG.info("Commit gate closed for checkpoint {}. Buffering committable.", checkpointId);
      wasPausedPreviously = true;
    } else {
      emitCommittable(committable, checkpointId);
      LOG.info("Emitted commit message to downstream committer operator");
      wasPausedPreviously = false;
    }

    results.clear();
  }

  private void flushBufferedCommittables() throws IOException {
    List<IcebergCommittable> buffered = Lists.newArrayList();
    try {
      for (byte[] serialized : pendingCommittablesState.get()) {
        buffered.add(
            SimpleVersionedSerialization.readVersionAndDeSerialize(
                COMMITTABLE_SERIALIZER, serialized));
      }
    } catch (Exception e) {
      throw new IOException("Failed to deserialize buffered committable", e);
    }

    if (!buffered.isEmpty()) {
      buffered.sort((c1, c2) -> Long.compare(c1.checkpointId(), c2.checkpointId()));
      for (IcebergCommittable c : buffered) {
        emitCommittable(c, c.checkpointId());
      }
      pendingCommittablesState.clear();
      LOG.info("Commit gate opened. Flushed {} buffered committables", buffered.size());
    }
  }

  private void emitCommittable(IcebergCommittable committable, long checkpointId) {
    output.collect(new StreamRecord<>(new CommittableSummary<>(0, 1, checkpointId, 1, 1, 0)));
    output.collect(new StreamRecord<>(new CommittableWithLineage<>(committable, checkpointId, 0)));
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
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            result,
            () -> icebergManifestOutputFileFactory.create(checkpointId),
            table.spec(),
            TableUtil.formatVersion(table));

    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<WriteResult>> element)
      throws Exception {

    if (element.isRecord() && element.getValue() instanceof CommittableWithLineage) {
      results.add(((CommittableWithLineage<WriteResult>) element.getValue()).getCommittable());
    }
  }
}
