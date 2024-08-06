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
package org.apache.iceberg.flink.sink.committer;

import java.io.IOException;
import java.util.Collection;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.flink.sink.DeltaManifestsSerializer;
import org.apache.iceberg.flink.sink.FlinkManifestUtil;
import org.apache.iceberg.flink.sink.ManifestOutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator which aggregates the individual {@link WriteResult} objects) to a single {@link
 * IcebergCommittable} per checkpoint (storing the serialized {@link DeltaManifests}, jobId,
 * operatorId, checkpointId)
 */
@Internal
public class IcebergWriteAggregator
    extends AbstractStreamOperator<CommittableMessage<IcebergCommittable>>
    implements OneInputStreamOperator<
        CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergWriteAggregator.class);
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];
  private final Collection<WriteResult> results;
  private transient ManifestOutputFileFactory icebergManifestOutputFileFactory;
  private transient Table table;
  private final TableLoader tableLoader;

  public IcebergWriteAggregator(TableLoader tableLoader) {
    this.results = Sets.newHashSet();
    this.tableLoader = tableLoader;
  }

  @Override
  public void open() throws Exception {
    if (!tableLoader.isOpen()) {
      tableLoader.open();
    }
    String flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    String operatorId = getOperatorID().toString();
    int subTaskId = 0;
    int attemptId = getRuntimeContext().getTaskInfo().getAttemptNumber();
    this.table = tableLoader.loadTable();

    this.icebergManifestOutputFileFactory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), flinkJobId, operatorId, subTaskId, attemptId);
  }

  @Override
  public void finish() throws IOException {
    prepareSnapshotPreBarrier(Long.MAX_VALUE);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws IOException {
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
            result, () -> icebergManifestOutputFileFactory.create(checkpointId), table.spec());

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
