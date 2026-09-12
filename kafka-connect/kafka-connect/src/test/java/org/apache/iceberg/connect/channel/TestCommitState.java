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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public class TestCommitState {
  private static TopicPartitionOffset partition(int partition) {
    return partition("src-topic", partition);
  }

  private static TopicPartitionOffset partition(String topic, int partition) {
    TopicPartitionOffset tp = mock(TopicPartitionOffset.class);
    when(tp.topic()).thenReturn(topic);
    when(tp.partition()).thenReturn(partition);
    return tp;
  }

  private static Set<TopicPartition> topicPartitions(TopicPartitionOffset... assignments) {
    return Arrays.stream(assignments)
        .map(assignment -> new TopicPartition(assignment.topic(), assignment.partition()))
        .collect(Collectors.toSet());
  }

  @Test
  void readinessRequiresActiveCommit() {
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));

    assertThat(commitState.isCommitReady(topicPartitions())).isFalse();

    commitState.startNewCommit();
    commitState.endCurrentCommit();

    assertThat(commitState.isCommitReady(topicPartitions())).isFalse();
  }

  @Test
  void emptyAssignmentIsReadyDuringCommit() {
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    assertThat(commitState.isCommitReady(topicPartitions())).isTrue();
  }

  @Test
  public void testIsCommitReady() {
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    DataComplete payload1 = mock(DataComplete.class);
    when(payload1.commitId()).thenReturn(commitState.currentCommitId());
    TopicPartitionOffset tp0 = partition(0);
    TopicPartitionOffset tp1 = partition(1);
    when(payload1.assignments()).thenReturn(ImmutableList.of(tp0, tp1));

    DataComplete payload2 = mock(DataComplete.class);
    when(payload2.commitId()).thenReturn(commitState.currentCommitId());
    TopicPartitionOffset tp2 = partition(2);
    when(payload2.assignments()).thenReturn(ImmutableList.of(tp2));

    DataComplete payload3 = mock(DataComplete.class);
    when(payload3.commitId()).thenReturn(UUID.randomUUID());
    TopicPartitionOffset tp3 = partition(3);
    when(payload3.assignments()).thenReturn(ImmutableList.of(tp3));

    commitState.addReady(wrapInEnvelope(payload1));
    commitState.addReady(wrapInEnvelope(payload2));
    commitState.addReady(wrapInEnvelope(payload3));

    assertThat(commitState.isCommitReady(topicPartitions(tp0, tp1, tp2))).isTrue();
    assertThat(commitState.isCommitReady(topicPartitions(tp0, tp1, tp2, tp3))).isFalse();
  }

  @Test
  public void testReplayedReadyDoesNotSatisfyQuorumTwice() {
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    // one worker owning source partition 0 reports; a control-topic replay redelivers it
    TopicPartitionOffset tp0 = partition(0);
    TopicPartitionOffset tp1 = partition(1);
    Set<TopicPartition> expectedPartitions = topicPartitions(tp0, tp1);
    DataComplete payload = mock(DataComplete.class);
    when(payload.commitId()).thenReturn(commitState.currentCommitId());
    when(payload.assignments()).thenReturn(ImmutableList.of(tp0));

    commitState.addReady(wrapInEnvelope(payload));
    commitState.addReady(wrapInEnvelope(payload));

    assertThat(commitState.isCommitReady(expectedPartitions))
        .as("a redelivered response must not stand in for a partition that never reported")
        .isFalse();

    // the partition that was actually missing reports
    DataComplete second = mock(DataComplete.class);
    when(second.commitId()).thenReturn(commitState.currentCommitId());
    when(second.assignments()).thenReturn(ImmutableList.of(tp1));
    commitState.addReady(wrapInEnvelope(second));

    assertThat(commitState.isCommitReady(expectedPartitions)).isTrue();
  }

  @Test
  public void testOverlappingAssignmentsDoNotSatisfyQuorumTwice() {
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    // during a rebalance two workers can transiently claim the same source partition
    TopicPartitionOffset tp0 = partition(0);
    TopicPartitionOffset alsoTp0 = partition(0);
    TopicPartitionOffset otherTopicPartition = partition("other-topic", 0);
    TopicPartitionOffset missing = partition(1);
    Set<TopicPartition> expectedPartitions = topicPartitions(tp0, otherTopicPartition);

    DataComplete leaving = mock(DataComplete.class);
    when(leaving.commitId()).thenReturn(commitState.currentCommitId());
    when(leaving.assignments()).thenReturn(ImmutableList.of(tp0));

    DataComplete arriving = mock(DataComplete.class);
    when(arriving.commitId()).thenReturn(commitState.currentCommitId());
    when(arriving.assignments()).thenReturn(ImmutableList.of(alsoTp0));

    commitState.addReady(wrapInEnvelope(leaving));
    commitState.addReady(wrapInEnvelope(arriving));

    assertThat(commitState.isCommitReady(expectedPartitions))
        .as("two claims on one partition cover one partition, not two")
        .isFalse();

    DataComplete otherTopic = mock(DataComplete.class);
    when(otherTopic.commitId()).thenReturn(commitState.currentCommitId());
    when(otherTopic.assignments()).thenReturn(ImmutableList.of(otherTopicPartition));
    commitState.addReady(wrapInEnvelope(otherTopic));

    assertThat(commitState.isCommitReady(expectedPartitions)).isTrue();
    assertThat(commitState.isCommitReady(topicPartitions(tp0, otherTopicPartition, missing)))
        .isFalse();
  }

  @Test
  void unexpectedPartitionsDoNotSatisfyReadiness() {
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    TopicPartitionOffset first = partition(0);
    TopicPartitionOffset unexpected = partition("other-topic", 0);
    TopicPartitionOffset missing = partition(1);
    Set<TopicPartition> expectedPartitions = topicPartitions(first, missing);
    DataComplete payload = mock(DataComplete.class);
    when(payload.commitId()).thenReturn(commitState.currentCommitId());
    when(payload.assignments()).thenReturn(ImmutableList.of(first, unexpected));
    commitState.addReady(wrapInEnvelope(payload));

    assertThat(commitState.isCommitReady(expectedPartitions)).isFalse();

    DataComplete missingPayload = mock(DataComplete.class);
    when(missingPayload.commitId()).thenReturn(commitState.currentCommitId());
    when(missingPayload.assignments()).thenReturn(ImmutableList.of(missing));
    commitState.addReady(wrapInEnvelope(missingPayload));

    assertThat(commitState.isCommitReady(expectedPartitions)).isTrue();
  }

  @Test
  public void testIsCommitReadyResetsBetweenCommits() {
    TopicPartitionOffset tp = partition(0);
    TopicPartitionOffset other = partition(1);

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    DataComplete firstPayload = mock(DataComplete.class);
    when(firstPayload.commitId()).thenReturn(commitState.currentCommitId());
    when(firstPayload.assignments()).thenReturn(ImmutableList.of(tp, other));
    commitState.addReady(wrapInEnvelope(firstPayload));
    assertThat(commitState.isCommitReady(topicPartitions(tp, other))).isTrue();

    commitState.endCurrentCommit();
    commitState.startNewCommit();

    assertThat(commitState.isCommitReady(topicPartitions(tp))).isFalse();

    DataComplete secondPayload = mock(DataComplete.class);
    when(secondPayload.commitId()).thenReturn(commitState.currentCommitId());
    when(secondPayload.assignments()).thenReturn(ImmutableList.of(tp));
    commitState.addReady(wrapInEnvelope(secondPayload));

    assertThat(commitState.isCommitReady(topicPartitions(tp))).isTrue();
    assertThat(commitState.isCommitReady(topicPartitions(tp, other))).isFalse();
  }

  @Test
  public void testIsCommitReadyIgnoresZombieCoordinatorPayloads() {
    TopicPartitionOffset tp = partition(0);
    TopicPartitionOffset missing = partition(1);

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    // Stale DataComplete from a zombie Coordinator that started a different commit.
    DataComplete zombiePayload = mock(DataComplete.class);
    when(zombiePayload.commitId()).thenReturn(UUID.randomUUID());
    when(zombiePayload.assignments()).thenReturn(ImmutableList.of(missing));

    DataComplete currentPayload = mock(DataComplete.class);
    when(currentPayload.commitId()).thenReturn(commitState.currentCommitId());
    when(currentPayload.assignments()).thenReturn(ImmutableList.of(tp));

    commitState.addReady(wrapInEnvelope(zombiePayload));
    assertThat(commitState.isCommitReady(topicPartitions(missing))).isFalse();

    commitState.addReady(wrapInEnvelope(currentPayload));

    // Only the current commit's payload counts toward readiness.
    assertThat(commitState.isCommitReady(topicPartitions(tp))).isTrue();
    assertThat(commitState.isCommitReady(topicPartitions(tp, missing))).isFalse();

    DataComplete missingPayload = mock(DataComplete.class);
    when(missingPayload.commitId()).thenReturn(commitState.currentCommitId());
    when(missingPayload.assignments()).thenReturn(ImmutableList.of(missing));
    commitState.addReady(wrapInEnvelope(missingPayload));

    assertThat(commitState.isCommitReady(topicPartitions(tp, missing))).isTrue();
  }

  @Test
  public void testGetValidThroughTs() {
    DataComplete payload1 = mock(DataComplete.class);
    TopicPartitionOffset tp1 = partition(0);
    OffsetDateTime ts1 = EventTestUtil.now();
    when(tp1.timestamp()).thenReturn(ts1);

    TopicPartitionOffset tp2 = partition(1);
    OffsetDateTime ts2 = ts1.plusSeconds(1);
    when(tp2.timestamp()).thenReturn(ts2);
    when(payload1.assignments()).thenReturn(ImmutableList.of(tp1, tp2));

    DataComplete payload2 = mock(DataComplete.class);
    TopicPartitionOffset tp3 = partition(2);
    OffsetDateTime ts3 = ts1.plusSeconds(2);
    when(tp3.timestamp()).thenReturn(ts3);
    when(payload2.assignments()).thenReturn(ImmutableList.of(tp3));

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    commitState.addReady(wrapInEnvelope(payload1));
    commitState.addReady(wrapInEnvelope(payload2));

    assertThat(commitState.validThroughTs(false)).isEqualTo(ts1);
    assertThat(commitState.validThroughTs(true)).isNull();

    // null timestamp for one, so should not set a valid-through timestamp
    DataComplete payload3 = mock(DataComplete.class);
    TopicPartitionOffset tp4 = partition(3);
    when(tp4.timestamp()).thenReturn(null);
    when(payload3.assignments()).thenReturn(ImmutableList.of(tp4));

    commitState.addReady(wrapInEnvelope(payload3));

    assertThat(commitState.validThroughTs(false)).isNull();
    assertThat(commitState.validThroughTs(true)).isNull();
  }

  private Envelope wrapInEnvelope(Payload payload) {
    Event event = mock(Event.class);
    when(event.payload()).thenReturn(payload);
    return new Envelope(event, 0, 0);
  }
}
