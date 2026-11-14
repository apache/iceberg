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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

public class TestCommitterImpl {

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

  @Test
  public void testCloseRewindsPartitionsWhoseBufferedRecordsWereDiscarded()
      throws NoSuchFieldException, IllegalAccessException {
    TopicPartition p0 = new TopicPartition("topic1", 0);
    TopicPartition p1 = new TopicPartition("topic1", 1);
    MemberDescription member =
        new MemberDescription(
            null, Optional.empty(), null, null, new MemberAssignment(ImmutableSet.of(p0, p1)));

    CommitterImpl committer = new CommitterImpl();
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.connectGroupId()).thenReturn("test-group");
    KafkaClientFactory clientFactory = mock(KafkaClientFactory.class);
    when(clientFactory.createAdmin()).thenReturn(mock(Admin.class));
    SinkTaskContext context = mock(SinkTaskContext.class);
    Worker worker = mock(Worker.class);
    setField(committer, "config", config);
    setField(committer, "clientFactory", clientFactory);
    setField(committer, "context", context);
    setField(committer, "worker", worker);
    ((AtomicBoolean) getField(committer, "isInitialized")).set(true);

    committer.save(
        ImmutableList.of(record(p0, 5L), record(p0, 6L), record(p1, 7L), record(p1, 8L)));

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupDescription groupDescription = mock(ConsumerGroupDescription.class);
      when(groupDescription.members()).thenReturn(ImmutableList.of(member));
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupDescription(any(), any()))
          .thenReturn(groupDescription);

      // p1 is revoked and p0 stays assigned: the worker's records for both are discarded
      committer.close(ImmutableList.of(p1));

      verify(worker).stop();
      mockKafkaUtils.verify(
          () -> KafkaUtils.seekToLastCommittedOffsets(context, ImmutableMap.of(p0, 5L, p1, 7L)));
    }
  }

  private static SinkRecord record(TopicPartition topicPartition, long offset) {
    return new SinkRecord(
        topicPartition.topic(), topicPartition.partition(), null, null, null, "value", offset);
  }

  private static void setField(CommitterImpl committer, String name, Object value)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = CommitterImpl.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(committer, value);
  }

  private static Object getField(CommitterImpl committer, String name)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = CommitterImpl.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(committer);
  }
}
