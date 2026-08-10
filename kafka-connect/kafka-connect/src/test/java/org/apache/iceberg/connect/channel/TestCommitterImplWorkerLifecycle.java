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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * The worker is not only the writer for the records handed to a task: it also answers the
 * coordinator's commit requests for every partition the task is assigned, reporting a null offset
 * for the partitions that received no records. A task therefore needs a worker for as long as it
 * holds an assignment, whether or not records are flowing.
 *
 * <p>These tests cover a task that holds an assignment but receives no records, both from the start
 * and after a rebalance has stopped its worker.
 */
@SuppressWarnings("deprecation")
class TestCommitterImplWorkerLifecycle extends ChannelTestBase {

  private static final TopicPartition LEADER_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 0);
  private static final TopicPartition TP_1 = new TopicPartition(SRC_TOPIC_NAME, 1);
  private static final TopicPartition TP_2 = new TopicPartition(SRC_TOPIC_NAME, 2);
  private static final TopicPartition CTL_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0);

  private final AtomicReference<Set<TopicPartition>> assigned = new AtomicReference<>();
  private SinkTaskContext context;
  private MockedStatic<KafkaUtils> mockKafkaUtils;
  private MockProducer<String, byte[]> ctlProducer;
  private MockConsumer<String, byte[]> ctlConsumer;

  @BeforeEach
  void stubFrameworkAndClients() {
    assigned.set(ImmutableSet.of(TP_1, TP_2));
    context = mock(SinkTaskContext.class);
    when(context.assignment()).thenAnswer(invocation -> assigned.get());

    // KafkaUtils talks to a real broker and to the framework's own consumer; only the group
    // lookup needs an answer here, the rest becomes a no-op
    mockKafkaUtils = mockStatic(KafkaUtils.class);
    ConsumerGroupDescription groupDesc = mock(ConsumerGroupDescription.class);
    when(groupDesc.members())
        .thenReturn(
            ImmutableList.of(
                member(ImmutableSet.of(LEADER_PARTITION)), member(ImmutableSet.of(TP_1, TP_2))));
    mockKafkaUtils
        .when(() -> KafkaUtils.consumerGroupDescription(any(), any()))
        .thenReturn(groupDesc);

    when(clientFactory.createProducer(any())).thenAnswer(invocation -> installProducer());
    when(clientFactory.createConsumer(any())).thenAnswer(invocation -> installConsumer());
  }

  @AfterEach
  void releaseStaticMock() {
    mockKafkaUtils.close();
  }

  @Test
  void idleTaskAnswersCommitRequestForAllAssignedPartitions() throws Exception {
    CommitterImpl committer = openedCommitter();

    // the empty put() the framework issues on every poll iteration is all this task ever gets
    committer.save(ImmutableList.of());
    assertThat(worker(committer)).isNotNull();

    UUID commitId = requestCommit();
    committer.save(ImmutableList.of());

    assertNullOffsetsReported(commitId, TP_1.partition(), TP_2.partition());
  }

  @Test
  void idleTaskAnswersCommitRequestAfterRebalance() throws Exception {
    CommitterImpl committer = openedCommitter();
    committer.save(ImmutableList.of());
    Worker workerBeforeRebalance = worker(committer);
    assertThat(workerBeforeRebalance).isNotNull();

    // A cooperative rebalance revokes partition 1 only, which stops the worker. The framework does
    // not call open() afterwards, since no partitions were added, so put() is the next callback
    // this task sees even though it still owns partition 2.
    committer.close(ImmutableList.of(TP_1));
    assigned.set(ImmutableSet.of(TP_2));
    assertThat(worker(committer)).isNull();

    committer.save(ImmutableList.of());
    assertThat(worker(committer)).isNotNull().isNotSameAs(workerBeforeRebalance);

    UUID commitId = requestCommit();
    committer.save(ImmutableList.of());

    assertNullOffsetsReported(commitId, TP_2.partition());
  }

  @Test
  void fullyRevokedTaskDoesNotRestartWorker() throws Exception {
    CommitterImpl committer = openedCommitter();
    committer.save(ImmutableList.of());
    assertThat(worker(committer)).isNotNull();

    committer.close(ImmutableList.of(TP_1, TP_2));
    assigned.set(ImmutableSet.of());

    committer.save(ImmutableList.of());
    assertThat(worker(committer)).isNull();
  }

  /** Publishes a commit request on the control topic the newest worker is subscribed to. */
  private UUID requestCommit() {
    // assign after the worker has subscribed, since subscribe() resets the assignment
    ctlConsumer.rebalance(ImmutableList.of(CTL_PARTITION));
    ctlConsumer.updateBeginningOffsets(ImmutableMap.of(CTL_PARTITION, 0L));

    UUID commitId = UUID.randomUUID();
    byte[] bytes = AvroUtil.encode(new Event(config.connectGroupId(), new StartCommit(commitId)));
    ctlConsumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0, "key", bytes));
    return commitId;
  }

  /** Asserts the newest worker answered the request for exactly these partitions, with no data. */
  private void assertNullOffsetsReported(UUID commitId, Integer... partitions) {
    assertThat(ctlProducer.history()).hasSize(1);
    Event event = AvroUtil.decode(ctlProducer.history().get(0).value());
    assertThat(event.payload().type()).isEqualTo(PayloadType.DATA_COMPLETE);

    DataComplete dataComplete = (DataComplete) event.payload();
    assertThat(dataComplete.commitId()).isEqualTo(commitId);
    assertThat(dataComplete.assignments())
        .extracting(TopicPartitionOffset::partition)
        .containsExactlyInAnyOrder(partitions);
    assertThat(dataComplete.assignments())
        .allSatisfy(assignment -> assertThat(assignment.offset()).isNull());
  }

  private MemberDescription member(Collection<TopicPartition> partitions) {
    return new MemberDescription(
        null, Optional.empty(), null, null, new MemberAssignment(ImmutableSet.copyOf(partitions)));
  }

  private MockProducer<String, byte[]> installProducer() {
    ctlProducer = new MockProducer<>(false, new StringSerializer(), new ByteArraySerializer());
    ctlProducer.initTransactions();
    return ctlProducer;
  }

  private MockConsumer<String, byte[]> installConsumer() {
    ctlConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    return ctlConsumer;
  }

  /** A committer that has been handed its partitions, with the mock clients wired in. */
  private CommitterImpl openedCommitter() throws Exception {
    CommitterImpl committer = new CommitterImpl();
    // bypass initialize() so the mock client factory is used instead of a real one
    setField(committer, "catalog", catalog);
    setField(committer, "config", config);
    setField(committer, "context", context);
    setField(committer, "clientFactory", clientFactory);
    setField(committer, "taskId", "connector-0");
    ((AtomicBoolean) field("isInitialized").get(committer)).set(true);

    committer.open(catalog, config, context, ImmutableList.of(TP_1, TP_2));
    // a coordinator would run on its own thread, where the static KafkaUtils mock does not apply,
    // and would reach for a real broker rather than fail an assertion
    assertThat(field("coordinatorThread").get(committer)).isNull();
    return committer;
  }

  private Worker worker(CommitterImpl committer) throws Exception {
    return (Worker) field("worker").get(committer);
  }

  private void setField(CommitterImpl committer, String name, Object value) throws Exception {
    field(name).set(committer, value);
  }

  private Field field(String name) throws Exception {
    Field field = CommitterImpl.class.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }
}
