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
package org.apache.iceberg.connect.channel;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

class Worker extends Channel {

  private final IcebergSinkConfig config;
  private final SinkTaskContext context;
  private final SinkWriter sinkWriter;

  Worker(
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkWriter sinkWriter,
      SinkTaskContext context) {
    // pass transient consumer group ID to which we never commit offsets
    super(
        "worker",
        config.controlGroupIdPrefix() + UUID.randomUUID(),
        config,
        clientFactory,
        context);

    this.config = config;
    this.context = context;
    this.sinkWriter = sinkWriter;
  }

  void process() {
    consumeAvailable(Duration.ZERO);
  }

  @Override
  protected void subscribeToControlTopic() {
    // this consumer's group id is a fresh, single-member, never-reused id (see constructor), so
    // it gets no benefit from consumer-group management -- there is never more than one member
    // to balance partitions against. Using assign() instead of subscribe() avoids the
    // JoinGroup/SyncGroup protocol entirely, which otherwise exposes this reader to broker-side
    // member-fencing failure modes for no reason, since group membership was never needed here.
    assignControlTopicPartitions(awaitControlTopicPartitions());
  }

  // unlike subscribe(), a one-shot partitionsFor() lookup doesn't tolerate the control topic
  // not existing yet at the moment this is called -- retry with a bounded backoff rather than
  // silently assigning zero partitions, which would poll forever and never receive anything.
  private List<TopicPartition> awaitControlTopicPartitions() {
    AtomicReference<List<PartitionInfo>> partitionInfos = new AtomicReference<>();
    // rely solely on exponentialBackoff's time cap (30s) to bound retries -- a large retry()
    // count here would just be a second, redundant bound that has to be kept in sync with it.
    Tasks.range(1)
        .retry(Integer.MAX_VALUE - 1) // retry() adds 1 for maxAttempts; avoid overflow
        .exponentialBackoff(200, 200, Duration.ofSeconds(30).toMillis(), 1)
        .onlyRetryOn(ControlTopicNotReadyException.class)
        .run(
            i -> {
              List<PartitionInfo> infos = controlTopicPartitions();
              if (infos == null || infos.isEmpty()) {
                throw new ControlTopicNotReadyException(controlTopic());
              }
              partitionInfos.set(infos);
            });

    return partitionInfos.get().stream()
        .map(info -> new TopicPartition(info.topic(), info.partition()))
        .collect(Collectors.toList());
  }

  private static class ControlTopicNotReadyException extends ConnectException {
    ControlTopicNotReadyException(String controlTopic) {
      super("Control topic " + controlTopic + " does not exist yet");
    }
  }

  @Override
  protected boolean receive(Envelope envelope) {
    Event event = envelope.event();
    if (event.payload().type() != PayloadType.START_COMMIT) {
      return false;
    }

    SinkWriterResult results = sinkWriter.completeWrite();

    // include all assigned topic partitions even if no messages were read
    // from a partition, as the coordinator will use that to determine
    // when all data for a commit has been received
    List<TopicPartitionOffset> assignments =
        context.assignment().stream()
            .map(
                tp -> {
                  Offset offset = results.sourceOffsets().get(tp);
                  if (offset == null) {
                    offset = Offset.NULL_OFFSET;
                  }
                  return new TopicPartitionOffset(
                      tp.topic(), tp.partition(), offset.offset(), offset.timestamp());
                })
            .collect(Collectors.toList());

    UUID commitId = ((StartCommit) event.payload()).commitId();

    List<Event> events =
        results.writerResults().stream()
            .map(
                writeResult ->
                    new Event(
                        config.connectGroupId(),
                        new DataWritten(
                            writeResult.partitionStruct(),
                            commitId,
                            writeResult.tableReference(),
                            writeResult.dataFiles(),
                            writeResult.deleteFiles())))
            .collect(Collectors.toList());

    Event readyEvent = new Event(config.connectGroupId(), new DataComplete(commitId, assignments));
    events.add(readyEvent);

    send(events, results.sourceOffsets());

    return true;
  }

  @Override
  void stop() {
    super.stop();
    sinkWriter.close();
  }

  void save(Collection<SinkRecord> sinkRecords) {
    sinkWriter.save(sinkRecords);
  }
}
