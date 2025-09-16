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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator which aggregates the individual {@link WriteResult} objects to a single {@link
 * DynamicCommittable} per checkpoint.
 */
class DynamicWriteResultAggregator
    extends AbstractStreamOperator<CommittableMessage<DynamicCommittable>>
    implements OneInputStreamOperator<
        CommittableMessage<DynamicWriteResult>, CommittableMessage<DynamicCommittable>> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicWriteResultAggregator.class);

  private transient Map<TableKey, List<WriteResult>> results;
  private transient String flinkJobId;
  private transient String operatorId;
  private transient int subTaskId;

  @Override
  public void open() throws Exception {
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    this.operatorId = getOperatorID().toString();
    this.subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    this.results = Maps.newHashMap();
  }

  @Override
  public void finish() {
    prepareSnapshotPreBarrier(Long.MAX_VALUE);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) {
    Collection<CommittableWithLineage<DynamicCommittable>> committables =
        Sets.newHashSetWithExpectedSize(results.size());
    int count = 0;
    for (Map.Entry<TableKey, List<WriteResult>> entries : results.entrySet()) {
      committables.add(
          new CommittableWithLineage<>(
              new DynamicCommittable(
                  entries.getKey(),
                  aggregate(entries.getValue()),
                  flinkJobId,
                  operatorId,
                  checkpointId),
              checkpointId,
              count));
      ++count;
    }

    output.collect(
        new StreamRecord<>(
            new CommittableSummary<>(subTaskId, count, checkpointId, count, count, 0)));
    committables.forEach(
        c ->
            output.collect(
                new StreamRecord<>(
                    new CommittableWithLineage<>(c.getCommittable(), checkpointId, subTaskId))));
    LOG.info("Emitted {} commit message to downstream committer operator", count);
    results.clear();
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<DynamicWriteResult>> element)
      throws Exception {

    if (element.isRecord() && element.getValue() instanceof CommittableWithLineage) {
      DynamicWriteResult result =
          ((CommittableWithLineage<DynamicWriteResult>) element.getValue()).getCommittable();
      Collection<WriteResult> resultsPerTableKey =
          results.computeIfAbsent(result.key(), unused -> Lists.newArrayList());
      resultsPerTableKey.add(result.writeResult());
      LOG.debug("Added {}, totalResults={}", result, resultsPerTableKey.size());
    }
  }

  private static WriteResult aggregate(List<WriteResult> writeResults) {
    WriteResult.Builder builder = WriteResult.builder();
    writeResults.forEach(builder::add);
    return builder.build();
  }
}
