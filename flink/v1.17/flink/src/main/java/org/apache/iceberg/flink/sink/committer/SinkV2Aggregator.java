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
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator which aggregates the individual {@link SinkV2Committable} results from the writer
 * (storing only {@link WriteResult} objects) to a single {@link SinkV2Committable} per checkpoint
 * (storing the serialized {@link DeltaManifests}, jobId, operatorId, checkpointId)
 */
public class SinkV2Aggregator extends AbstractStreamOperator<CommittableMessage<SinkV2Committable>>
    implements OneInputStreamOperator<
        CommittableMessage<SinkV2Committable>, CommittableMessage<SinkV2Committable>> {
  private static final Logger LOG = LoggerFactory.getLogger(SinkV2Aggregator.class);

  private final Collection<WriteResult> results;
  private final CommonCommitter commonCommitter;

  public SinkV2Aggregator(CommonCommitter commonCommitter) {
    this.results = Sets.newHashSet();
    this.commonCommitter = commonCommitter;
  }

  @Override
  public void open() throws Exception {
    super.open();
    commonCommitter.init(null);
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<SinkV2Committable>> element)
      throws Exception {
    if (element.isRecord() && element.getValue() instanceof CommittableWithLineage) {
      results.add(
          ((CommittableWithLineage<SinkV2Committable>) element.getValue())
              .getCommittable()
              .writeResult());
    }
  }

  @Override
  public void finish() throws IOException {
    prepareSnapshotPreBarrier(Long.MAX_VALUE);
  };

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws IOException {
    SinkV2Committable committable =
        new SinkV2Committable(
            commonCommitter.writeToManifest(results, checkpointId),
            getContainingTask().getEnvironment().getJobID().toString(),
            getRuntimeContext().getOperatorUniqueID(),
            checkpointId);
    CommittableMessage<SinkV2Committable> message =
        new CommittableWithLineage<>(committable, checkpointId, 0);
    CommittableMessage<SinkV2Committable> summary =
        new CommittableSummary<>(0, 1, checkpointId, 1, 1, 0);
    output.collect(new StreamRecord<>(summary));
    output.collect(new StreamRecord<>(message));
    LOG.info("Aggregated commit message emitted {}", message);
    results.clear();
  }
}
