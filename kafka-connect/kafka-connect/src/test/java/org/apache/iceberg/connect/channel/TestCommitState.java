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
import java.util.UUID;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.Payload;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestCommitState {
  private static TopicPartitionOffset partition(int partition) {
    TopicPartitionOffset tp = mock(TopicPartitionOffset.class);
    when(tp.topic()).thenReturn("src-topic");
    when(tp.partition()).thenReturn(partition);
    return tp;
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

    assertThat(commitState.isCommitReady(3)).isTrue();
    assertThat(commitState.isCommitReady(4)).isFalse();
  }

  @Test
  public void testReplayedReadyDoesNotSatisfyQuorumTwice() {
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    // one worker owning source partition 0 reports; a control-topic replay redelivers it
    TopicPartitionOffset tp0 = partition(0);
    DataComplete payload = mock(DataComplete.class);
    when(payload.commitId()).thenReturn(commitState.currentCommitId());
    when(payload.assignments()).thenReturn(ImmutableList.of(tp0));

    commitState.addReady(wrapInEnvelope(payload));
    commitState.addReady(wrapInEnvelope(payload));

    assertThat(commitState.isCommitReady(2))
        .as("a redelivered response must not stand in for a partition that never reported")
        .isFalse();

    // the partition that was actually missing reports
    TopicPartitionOffset tp1 = partition(1);
    DataComplete second = mock(DataComplete.class);
    when(second.commitId()).thenReturn(commitState.currentCommitId());
    when(second.assignments()).thenReturn(ImmutableList.of(tp1));
    commitState.addReady(wrapInEnvelope(second));

    assertThat(commitState.isCommitReady(2)).isTrue();
  }

  @Test
  public void testOverlappingAssignmentsDoNotSatisfyQuorumTwice() {
    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    // during a rebalance two workers can transiently claim the same source partition
    TopicPartitionOffset tp0 = partition(0);
    TopicPartitionOffset alsoTp0 = partition(0);

    DataComplete leaving = mock(DataComplete.class);
    when(leaving.commitId()).thenReturn(commitState.currentCommitId());
    when(leaving.assignments()).thenReturn(ImmutableList.of(tp0));

    DataComplete arriving = mock(DataComplete.class);
    when(arriving.commitId()).thenReturn(commitState.currentCommitId());
    when(arriving.assignments()).thenReturn(ImmutableList.of(alsoTp0));

    commitState.addReady(wrapInEnvelope(leaving));
    commitState.addReady(wrapInEnvelope(arriving));

    assertThat(commitState.isCommitReady(2))
        .as("two claims on one partition cover one partition, not two")
        .isFalse();
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
    assertThat(commitState.isCommitReady(2)).isTrue();

    commitState.endCurrentCommit();
    commitState.startNewCommit();

    assertThat(commitState.isCommitReady(1)).isFalse();

    DataComplete secondPayload = mock(DataComplete.class);
    when(secondPayload.commitId()).thenReturn(commitState.currentCommitId());
    when(secondPayload.assignments()).thenReturn(ImmutableList.of(tp));
    commitState.addReady(wrapInEnvelope(secondPayload));

    assertThat(commitState.isCommitReady(1)).isTrue();
    assertThat(commitState.isCommitReady(2)).isFalse();
  }

  @Test
  public void testIsCommitReadyIgnoresZombieCoordinatorPayloads() {
    TopicPartitionOffset tp = partition(0);

    CommitState commitState = new CommitState(mock(IcebergSinkConfig.class));
    commitState.startNewCommit();

    // Stale DataComplete from a zombie Coordinator that started a different commit.
    DataComplete zombiePayload = mock(DataComplete.class);
    when(zombiePayload.commitId()).thenReturn(UUID.randomUUID());
    when(zombiePayload.assignments()).thenReturn(ImmutableList.of(tp, tp));

    DataComplete currentPayload = mock(DataComplete.class);
    when(currentPayload.commitId()).thenReturn(commitState.currentCommitId());
    when(currentPayload.assignments()).thenReturn(ImmutableList.of(tp));

    commitState.addReady(wrapInEnvelope(zombiePayload));
    commitState.addReady(wrapInEnvelope(currentPayload));

    // Only the current commit's payload counts toward readiness.
    assertThat(commitState.isCommitReady(1)).isTrue();
    assertThat(commitState.isCommitReady(2)).isFalse();
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
