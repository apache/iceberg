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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

public class TestCommitterImpl {

  private static final TopicPartition LEADER_PARTITION = new TopicPartition("topic1", 0);
  private static final TopicPartition OTHER_PARTITION = new TopicPartition("topic1", 1);

  @Test
  public void testIsLeader() {
    MemberAssignment assignment1 =
        new MemberAssignment(
            ImmutableSet.of(new TopicPartition("topic1", 0), new TopicPartition("topic2", 1)));
    MemberDescription member1 =
        new MemberDescription(null, Optional.empty(), null, null, assignment1);

    MemberAssignment assignment2 =
        new MemberAssignment(
            ImmutableSet.of(new TopicPartition("topic2", 0), new TopicPartition("topic1", 1)));
    MemberDescription member2 =
        new MemberDescription(null, Optional.empty(), null, null, assignment2);

    List<MemberDescription> members = ImmutableList.of(member1, member2);

    List<TopicPartition> leaderAssignments =
        ImmutableList.of(new TopicPartition("topic2", 1), new TopicPartition("topic1", 0));
    List<TopicPartition> nonLeaderAssignments =
        ImmutableList.of(new TopicPartition("topic2", 0), new TopicPartition("topic1", 1));

    CommitterImpl committer = new CommitterImpl();
    assertThat(committer.containsFirstPartition(members, leaderAssignments)).isTrue();
    assertThat(committer.containsFirstPartition(members, nonLeaderAssignments)).isFalse();
  }

  @Test
  public void testHasLeaderPartition() throws NoSuchFieldException, IllegalAccessException {
    MemberAssignment assignment1 =
        new MemberAssignment(
            ImmutableSet.of(new TopicPartition("topic1", 0), new TopicPartition("topic2", 1)));
    MemberDescription member1 =
        new MemberDescription(null, Optional.empty(), null, null, assignment1);

    MemberAssignment assignment2 =
        new MemberAssignment(
            ImmutableSet.of(new TopicPartition("topic2", 0), new TopicPartition("topic1", 1)));
    MemberDescription member2 =
        new MemberDescription(null, Optional.empty(), null, null, assignment2);

    List<MemberDescription> members = ImmutableList.of(member1, member2);

    List<TopicPartition> leaderAssignments =
        ImmutableList.of(new TopicPartition("topic2", 1), new TopicPartition("topic1", 0));
    List<TopicPartition> nonLeaderAssignments =
        ImmutableList.of(new TopicPartition("topic2", 0), new TopicPartition("topic1", 1));

    CommitterImpl committer = new CommitterImpl();
    Field configField = CommitterImpl.class.getDeclaredField("config");
    Field clientFactoryField = CommitterImpl.class.getDeclaredField("clientFactory");
    configField.setAccessible(true);
    clientFactoryField.setAccessible(true);

    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.connectGroupId()).thenReturn("test-group");
    configField.set(committer, config);

    KafkaClientFactory clientFactory = mock(KafkaClientFactory.class);
    Admin admin = mock(Admin.class);
    when(clientFactory.createAdmin()).thenReturn(admin);
    clientFactoryField.set(committer, clientFactory);

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupDescription consumerGroupDescription = mock(ConsumerGroupDescription.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupDescription(any(), any()))
          .thenReturn(consumerGroupDescription);

      when(consumerGroupDescription.members()).thenReturn(members);

      assertThat(committer.hasLeaderPartition(leaderAssignments)).isTrue();
      assertThat(committer.hasLeaderPartition(nonLeaderAssignments)).isFalse();
    }
  }

  @Test
  public void testCommitFailurePropagatesAsNotRunningException()
      throws NoSuchFieldException, IllegalAccessException {
    RuntimeException cause = new RuntimeException("commit failed");
    Coordinator coordinator = mock(Coordinator.class);
    doThrow(cause).when(coordinator).process();

    CoordinatorThread coordinatorThread = new CoordinatorThread(coordinator);
    coordinatorThread.start();

    // wait for the thread to catch the exception, set terminated, and call stop
    verify(coordinator, timeout(1000)).stop();
    assertThat(coordinatorThread.isTerminated()).isTrue();
    assertThat(coordinatorThread.isFenced()).isFalse();

    CommitterImpl committer = new CommitterImpl();
    Field field = CommitterImpl.class.getDeclaredField("coordinatorThread");
    field.setAccessible(true);
    field.set(committer, coordinatorThread);

    assertThatThrownBy(() -> committer.save(Collections.emptyList()))
        .isInstanceOf(NotRunningException.class)
        .hasMessageContaining("Coordinator unexpectedly terminated")
        .cause()
        .isSameAs(cause);
  }

  @Test
  public void testStartFailurePropagatesAsNotRunningException()
      throws NoSuchFieldException, IllegalAccessException {
    Coordinator coordinator = mock(Coordinator.class);
    doThrow(new RuntimeException("start failed")).when(coordinator).start();

    CoordinatorThread coordinatorThread = new CoordinatorThread(coordinator);
    coordinatorThread.start();

    // wait for the thread to catch the exception, set terminated, and call stop
    verify(coordinator, timeout(1000)).stop();
    assertThat(coordinatorThread.isTerminated()).isTrue();

    CommitterImpl committer = new CommitterImpl();
    Field field = CommitterImpl.class.getDeclaredField("coordinatorThread");
    field.setAccessible(true);
    field.set(committer, coordinatorThread);

    assertThatThrownBy(() -> committer.save(Collections.emptyList()))
        .isInstanceOf(NotRunningException.class)
        .hasMessageContaining("Coordinator unexpectedly terminated");
  }

  @ParameterizedTest
  @ValueSource(strings = {"ProducerFenced", "InvalidProducerEpoch"})
  public void testFencedCoordinatorIsClearedWithoutFailingTask(String exceptionType)
      throws NoSuchFieldException, IllegalAccessException {
    RuntimeException fenceException =
        "ProducerFenced".equals(exceptionType)
            ? new ProducerFencedException("fenced by a newer coordinator")
            : new InvalidProducerEpochException("producer epoch bumped by a newer coordinator");

    Coordinator coordinator = mock(Coordinator.class);
    doThrow(fenceException).when(coordinator).process();

    CoordinatorThread coordinatorThread = new CoordinatorThread(coordinator);
    coordinatorThread.start();

    verify(coordinator, timeout(1000)).stop();
    assertThat(coordinatorThread.isTerminated()).isTrue();
    assertThat(coordinatorThread.isFenced()).isTrue();

    CommitterImpl committer = new CommitterImpl();
    Field field = CommitterImpl.class.getDeclaredField("coordinatorThread");
    field.setAccessible(true);
    field.set(committer, coordinatorThread);

    committer.save(Collections.emptyList());
    assertThat(field.get(committer)).isNull();
  }

  @ParameterizedTest
  @MethodSource("rebalancingGroups")
  void closeStopsCoordinatorWhenLeaderPartitionIsRevoked(
      List<MemberDescription> rebalancingMembers, List<TopicPartition> revokedPartitions) {
    ConsumerGroupDescription groupDescription = mock(ConsumerGroupDescription.class);
    try (MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class);
        MockedConstruction<KafkaClientFactory> clientFactories =
            mockConstruction(KafkaClientFactory.class);
        MockedConstruction<Coordinator> coordinators =
            mockConstruction(Coordinator.class, TestCommitterImpl::slowProcess)) {
      CommitterImpl committer = openAsLeader(kafkaUtils, groupDescription);

      when(groupDescription.members()).thenReturn(rebalancingMembers);
      committer.close(revokedPartitions);

      verify(coordinators.constructed().get(0)).terminate();
    }
  }

  @Test
  void closeKeepsCoordinatorWhenLeaderPartitionIsRetained() {
    try (MockedStatic<KafkaUtils> kafkaUtils = mockStatic(KafkaUtils.class);
        MockedConstruction<KafkaClientFactory> clientFactories =
            mockConstruction(KafkaClientFactory.class);
        MockedConstruction<Coordinator> coordinators =
            mockConstruction(Coordinator.class, TestCommitterImpl::slowProcess)) {
      CommitterImpl committer = openAsLeader(kafkaUtils, mock(ConsumerGroupDescription.class));
      committer.close(ImmutableList.of(OTHER_PARTITION));

      verify(coordinators.constructed().get(0), never()).terminate();
      committer.close(ImmutableList.of());
    }
  }

  static Stream<Arguments> rebalancingGroups() {
    return Stream.of(
        Arguments.of(
            Named.of("no assignments", ImmutableList.of(member())),
            ImmutableList.of(LEADER_PARTITION, OTHER_PARTITION)),
        Arguments.of(
            Named.of("leader partition unassigned", ImmutableList.of(member(OTHER_PARTITION))),
            ImmutableList.of(LEADER_PARTITION)));
  }

  private static CommitterImpl openAsLeader(
      MockedStatic<KafkaUtils> kafkaUtils, ConsumerGroupDescription groupDescription) {
    when(groupDescription.members())
        .thenReturn(ImmutableList.of(member(LEADER_PARTITION, OTHER_PARTITION)));
    kafkaUtils
        .when(() -> KafkaUtils.consumerGroupDescription(any(), any()))
        .thenReturn(groupDescription);

    CommitterImpl committer = new CommitterImpl();
    committer.open(
        null,
        mock(IcebergSinkConfig.class),
        null,
        ImmutableList.of(LEADER_PARTITION, OTHER_PARTITION));
    return committer;
  }

  private static MemberDescription member(TopicPartition... partitions) {
    return new MemberDescription(
        null, Optional.empty(), null, null, new MemberAssignment(ImmutableSet.copyOf(partitions)));
  }

  private static void slowProcess(Coordinator coordinator, MockedConstruction.Context context) {
    doAnswer(
            invocation -> {
              Thread.sleep(10);
              return null;
            })
        .when(coordinator)
        .process();
  }
}
