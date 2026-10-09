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
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

class TestCoordinatorPartitionExpansion extends ChannelTestBase {
  private static final TopicPartition FIRST_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 0);
  private static final TopicPartition SECOND_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 1);
  private static final TopicPartition ADDED_PARTITION = new TopicPartition(SRC_TOPIC_NAME, 2);
  private static final TopicPartition CONTROL_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0);
  private static final OffsetDateTime LATER_TIMESTAMP = OffsetDateTime.parse("2026-09-29T12:00Z");
  private static final OffsetDateTime EARLIER_TIMESTAMP = LATER_TIMESTAMP.minusHours(1);
  private Coordinator coordinator;
  private CoordinatorThread retainedThread;

  enum UnverifiedMetadata {
    UNAVAILABLE,
    REBALANCING,
    EMPTY_GROUP,
    EMPTY_ASSIGNMENTS
  }

  enum TopologyChange {
    EXPANSION,
    PARTITION_REPLACEMENT,
    OWNER_CHANGE
  }

  enum AssignmentCallback {
    OPEN_ADDED_ONLY,
    OPEN_FULL,
    CLOSE_NON_LEADER
  }

  @AfterEach
  void stopCoordinator() {
    if (coordinator != null) {
      coordinator.terminate();
      coordinator.stop();
    }
  }

  @Test
  void expansionBeforeCommitWaitsForNewPartition() {
    startCoordinator(initialMembers());
    describeConsumerGroup(ConsumerGroupState.STABLE, expandedMembers());
    UUID commitId = startCommit();
    DataFile firstFile = dataFile("first");
    writeFile(commitId, firstFile, 1L);
    ready(commitId, LATER_TIMESTAMP, 2L, FIRST_PARTITION, SECOND_PARTITION);
    coordinator.process();
    assertPending();

    DataFile addedFile = dataFile("added");
    writeFile(commitId, addedFile, 3L);
    ready(commitId, EARLIER_TIMESTAMP, 4L, ADDED_PARTITION);
    coordinator.process();
    assertCompleted(commitId, EARLIER_TIMESTAMP, 5L, 1, firstFile, addedFile);
  }

  @ParameterizedTest
  @EnumSource(UnverifiedMetadata.class)
  void unverifiedRefreshAfterSuccessfulCycleRequiresPartialCommit(UnverifiedMetadata metadata) {
    startCoordinator(initialMembers());
    UUID firstCommitId = startCommit();
    DataFile firstFile = dataFile("first");
    writeFile(firstCommitId, firstFile, 1L);
    ready(firstCommitId, LATER_TIMESTAMP, 2L, FIRST_PARTITION, SECOND_PARTITION);
    coordinator.process();
    assertCompleted(firstCommitId, LATER_TIMESTAMP, 3L, 1, firstFile);
    producer.clear();

    unverifiedMetadata(metadata);
    UUID partialCommitId = startCommit();
    describeConsumerGroup(ConsumerGroupState.STABLE, initialMembers());
    DataFile partialFile = dataFile("partial");
    writeFile(partialCommitId, partialFile, 3L);
    ready(partialCommitId, LATER_TIMESTAMP, 4L, FIRST_PARTITION, SECOND_PARTITION);
    coordinator.process();
    assertPending(1);

    when(config.commitTimeoutMs()).thenReturn(-1);
    coordinator.process();
    assertCompleted(partialCommitId, null, 5L, 2, partialFile);

    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    UUID recoveredCommitId = startCommit();
    DataFile recoveredFile = dataFile("recovered");
    writeFile(recoveredCommitId, recoveredFile, 5L);
    ready(recoveredCommitId, EARLIER_TIMESTAMP, 6L, FIRST_PARTITION, SECOND_PARTITION);
    coordinator.process();
    assertCompleted(recoveredCommitId, EARLIER_TIMESTAMP, 7L, 3, recoveredFile);
  }

  @ParameterizedTest
  @EnumSource(UnverifiedMetadata.class)
  void failedFullVerificationCannotRegainEligibilityInTheSameCycle(UnverifiedMetadata metadata) {
    startCoordinator(initialMembers());
    UUID commitId = startCommit();
    DataFile file = dataFile("pending");
    writeFile(commitId, file, 1L);
    unverifiedMetadata(metadata);
    ready(commitId, LATER_TIMESTAMP, 2L, FIRST_PARTITION, SECOND_PARTITION);
    coordinator.process();
    assertPending();

    describeConsumerGroup(ConsumerGroupState.STABLE, initialMembers());
    ready(commitId, LATER_TIMESTAMP, 3L, FIRST_PARTITION, SECOND_PARTITION);
    coordinator.process();
    assertPending();
    when(config.commitTimeoutMs()).thenReturn(-1);
    coordinator.process();
    assertCompleted(commitId, null, 4L, 1, file);
  }

  @ParameterizedTest
  @EnumSource(TopologyChange.class)
  void changedTopologyBeforeFullCommitRequiresPartialCommit(TopologyChange change) {
    startCoordinator(initialMembers());
    UUID commitId = startCommit();
    List<MemberDescription> changedMembers =
        switch (change) {
          case EXPANSION -> expandedMembers();
          case PARTITION_REPLACEMENT ->
              List.of(
                  member("leader", FIRST_PARTITION),
                  member("other", new TopicPartition("other-topic", SECOND_PARTITION.partition())));
          case OWNER_CHANGE ->
              List.of(member("leader", SECOND_PARTITION), member("other", FIRST_PARTITION));
        };
    describeConsumerGroup(ConsumerGroupState.STABLE, changedMembers);
    DataFile file = dataFile("pending");
    writeFile(commitId, file, 1L);
    ready(commitId, LATER_TIMESTAMP, 2L, FIRST_PARTITION, SECOND_PARTITION);
    coordinator.process();
    assertPending();
    when(config.commitTimeoutMs()).thenReturn(-1);
    coordinator.process();
    assertCompleted(commitId, null, 3L, 1, file);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void assignmentChangeDuringMetadataReadCannotRestoreEligibility(boolean duringFullVerification) {
    startCoordinator(initialMembers());
    UUID commitId = duringFullVerification ? startCommit() : null;
    DescribeConsumerGroupsResult result =
        describeConsumerGroup(ConsumerGroupState.STABLE, initialMembers());
    CoordinatorThread notificationTarget = new CoordinatorThread(coordinator);
    doAnswer(
            invocation -> {
              notificationTarget.assignmentChanged();
              return result;
            })
        .when(admin)
        .describeConsumerGroups(anyCollection());
    if (!duringFullVerification) {
      commitId = startCommit();
      describeConsumerGroup(ConsumerGroupState.STABLE, initialMembers());
    }

    DataFile file = dataFile("pending");
    writeFile(commitId, file, 1L);
    ready(commitId, LATER_TIMESTAMP, 2L, FIRST_PARTITION, SECOND_PARTITION);
    coordinator.process();
    assertPending();
    when(config.commitTimeoutMs()).thenReturn(-1);
    coordinator.process();
    assertCompleted(commitId, null, 3L, 1, file);
  }

  private void unverifiedMetadata(UnverifiedMetadata metadata) {
    switch (metadata) {
      case UNAVAILABLE ->
          doThrow(new KafkaException("Injected describe failure"))
              .when(admin)
              .describeConsumerGroups(anyCollection());
      case REBALANCING ->
          describeConsumerGroup(ConsumerGroupState.PREPARING_REBALANCE, initialMembers());
      case EMPTY_GROUP -> describeConsumerGroup(ConsumerGroupState.STABLE, List.of());
      case EMPTY_ASSIGNMENTS ->
          describeConsumerGroup(
              ConsumerGroupState.STABLE, List.of(member("leader"), member("other")));
    }
  }

  @ParameterizedTest
  @EnumSource(AssignmentCallback.class)
  void retainedCoordinatorCallbackInvalidatesCurrentCycle(AssignmentCallback callback) {
    SinkTaskContext context = mock(SinkTaskContext.class);
    Set<TopicPartition> leaderAssignment =
        callback == AssignmentCallback.CLOSE_NON_LEADER
            ? Set.of(FIRST_PARTITION, ADDED_PARTITION)
            : Set.of(FIRST_PARTITION);
    List<MemberDescription> members =
        List.of(
            member("leader", leaderAssignment.toArray(TopicPartition[]::new)),
            member("other", SECOND_PARTITION));
    CommitterImpl committer = startRetainedCoordinator(context, members, leaderAssignment);
    UUID commitId = startCommit();
    DataFile file = dataFile("pending");
    writeFile(commitId, file, 1L);

    try (MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class)) {
      kafkaUtils
          .when(() -> KafkaUtils.consumerGroupDescription(CONNECT_CONSUMER_GROUP_ID, admin))
          .thenCallRealMethod();
      switch (callback) {
        case OPEN_ADDED_ONLY -> committer.open(catalog, config, context, Set.of(ADDED_PARTITION));
        case OPEN_FULL ->
            committer.open(catalog, config, context, Set.of(FIRST_PARTITION, ADDED_PARTITION));
        case CLOSE_NON_LEADER -> committer.close(Set.of(ADDED_PARTITION));
      }
    }

    verify(retainedThread).assignmentChanged();
    verify(retainedThread).start();
    verify(retainedThread, never()).terminate();
    TopicPartition[] reportingPartitions =
        members.stream()
            .flatMap(member -> member.assignment().topicPartitions().stream())
            .toArray(TopicPartition[]::new);
    ready(commitId, LATER_TIMESTAMP, 2L, reportingPartitions);
    coordinator.process();
    assertPending();
    when(config.commitTimeoutMs()).thenReturn(-1);
    coordinator.process();
    assertCompleted(commitId, null, 3L, 1, file);
  }

  private CommitterImpl startRetainedCoordinator(
      SinkTaskContext context,
      List<MemberDescription> members,
      Set<TopicPartition> leaderAssignment) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    describeConsumerGroup(ConsumerGroupState.STABLE, members);
    CommitterImpl committer = new CommitterImpl();
    try (MockedConstruction<KafkaClientFactory> factories =
            mockConstruction(
                KafkaClientFactory.class,
                (factory, construction) -> {
                  when(factory.createProducer(any())).thenReturn(producer);
                  when(factory.createConsumer(any())).thenReturn(consumer);
                  when(factory.createAdmin()).thenReturn(admin);
                });
        MockedConstruction<CoordinatorThread> threads =
            mockConstruction(
                CoordinatorThread.class,
                (thread, construction) ->
                    this.coordinator = (Coordinator) construction.arguments().get(0))) {
      committer.open(catalog, config, context, leaderAssignment);
      assertThat(factories.constructed()).hasSize(1);
      assertThat(threads.constructed()).hasSize(1);
      this.retainedThread = threads.constructed().get(0);
    }

    CoordinatorThread notificationTarget = new CoordinatorThread(coordinator);
    doAnswer(
            invocation -> {
              notificationTarget.assignmentChanged();
              return null;
            })
        .when(retainedThread)
        .assignmentChanged();
    coordinator.start();
    initConsumer();
    return committer;
  }

  private void startCoordinator(List<MemberDescription> members) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    describeConsumerGroup(ConsumerGroupState.STABLE, members);
    this.coordinator =
        new Coordinator(catalog, config, members, clientFactory, mock(SinkTaskContext.class));
    coordinator.start();
    initConsumer();
  }

  private UUID startCommit() {
    coordinator.process();
    Event event = AvroUtil.decode(producer.history().get(producer.history().size() - 1).value());
    return ((StartCommit) event.payload()).commitId();
  }

  private static List<MemberDescription> initialMembers() {
    return List.of(member("leader", FIRST_PARTITION), member("other", SECOND_PARTITION));
  }

  private static List<MemberDescription> expandedMembers() {
    return List.of(
        member("leader", FIRST_PARTITION), member("other", SECOND_PARTITION, ADDED_PARTITION));
  }

  private static MemberDescription member(String memberId, TopicPartition... partitions) {
    return new MemberDescription(
        memberId,
        Optional.empty(),
        "client-" + memberId,
        "localhost",
        new MemberAssignment(Set.of(partitions)));
  }

  private DataFile dataFile(String name) {
    return DataFiles.builder(table.spec())
        .withPath(name + ".parquet")
        .withFileSizeInBytes(100L)
        .withRecordCount(1L)
        .build();
  }

  private void writeFile(UUID commitId, DataFile file, long offset) {
    addRecord(
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
                List.of(file),
                List.of())),
        offset);
  }

  private void ready(
      UUID commitId, OffsetDateTime timestamp, long offset, TopicPartition... partitions) {
    List<TopicPartitionOffset> assignments =
        Arrays.stream(partitions)
            .map(
                partition ->
                    new TopicPartitionOffset(
                        partition.topic(), partition.partition(), 1L, timestamp))
            .toList();
    addRecord(new Event(config.connectGroupId(), new DataComplete(commitId, assignments)), offset);
  }

  private void addRecord(Event event, long offset) {
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset, "key", AvroUtil.encode(event)));
  }

  private void assertPending() {
    assertPending(0);
  }

  private void assertPending(int snapshotCount) {
    table.refresh();
    assertThat(table.snapshots()).hasSize(snapshotCount);
    assertThat(producer.history())
        .extracting(record -> AvroUtil.decode(record.value()).type())
        .containsExactly(PayloadType.START_COMMIT);
    assertThat(committedControlOffset()).isNull();
  }

  private void assertCompleted(
      UUID commitId,
      OffsetDateTime timestamp,
      long committedOffset,
      int snapshotCount,
      DataFile... files) {
    table.refresh();
    assertThat(table.snapshots()).hasSize(snapshotCount);
    assertThat(table.currentSnapshot().addedDataFiles(table.io()))
        .extracting(DataFile::location)
        .containsExactlyInAnyOrderElementsOf(Arrays.stream(files).map(DataFile::location).toList());
    assertThat(table.currentSnapshot().summary())
        .containsEntry(COMMIT_ID_SNAPSHOT_PROP, commitId.toString())
        .containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":" + committedOffset + "}");
    if (timestamp == null) {
      assertThat(table.currentSnapshot().summary())
          .doesNotContainKey(VALID_THROUGH_TS_SNAPSHOT_PROP);
    } else {
      assertThat(table.currentSnapshot().summary())
          .containsEntry(VALID_THROUGH_TS_SNAPSHOT_PROP, timestamp.toString());
    }

    int eventCount = producer.history().size();
    assertThat(AvroUtil.decode(producer.history().get(eventCount - 2).value()).payload())
        .isInstanceOfSatisfying(
            CommitToTable.class,
            complete -> {
              assertThat(complete.commitId()).isEqualTo(commitId);
              assertThat(complete.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
              assertThat(complete.validThroughTs()).isEqualTo(timestamp);
            });
    assertThat(AvroUtil.decode(producer.history().get(eventCount - 1).value()).payload())
        .isInstanceOfSatisfying(
            CommitComplete.class,
            complete -> {
              assertThat(complete.commitId()).isEqualTo(commitId);
              assertThat(complete.validThroughTs()).isEqualTo(timestamp);
            });
    assertThat(committedControlOffset()).isEqualTo(committedOffset);
  }

  private Long committedControlOffset() {
    OffsetAndMetadata committed =
        committedGroupOffsets(consumer.groupMetadata().groupId()).get(CONTROL_PARTITION);
    return committed != null ? committed.offset() : null;
  }
}
