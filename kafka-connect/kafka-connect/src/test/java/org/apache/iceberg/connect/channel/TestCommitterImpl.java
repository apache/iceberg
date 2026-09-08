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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public class TestCommitterImpl {

  @Test
  public void testLeaderPartitionIsPartitionZeroOfSmallestTopic() {
    // no subscription yet → no leader partition
    assertThat(CommitterImpl.leaderPartition(List.of())).isNull();

    // single topic → partition 0 of that topic
    assertThat(CommitterImpl.leaderPartition(List.of("only")))
        .isEqualTo(new TopicPartition("only", 0));

    // multiple topics → partition 0 of the lexicographically-smallest topic
    assertThat(CommitterImpl.leaderPartition(List.of("topicB", "topicA", "topicC")))
        .isEqualTo(new TopicPartition("topicA", 0));
  }

  @Test
  public void testCommitFailurePropagatesAsNotRunningException()
      throws NoSuchFieldException, IllegalAccessException {
    Coordinator coordinator = mock(Coordinator.class);
    doThrow(new RuntimeException("commit failed")).when(coordinator).process();

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
}
