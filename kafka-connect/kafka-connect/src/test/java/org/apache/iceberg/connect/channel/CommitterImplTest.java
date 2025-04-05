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
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CommitterImplTest {

  @Test
  public void testIsLeader() {
    CommitterImpl committer = new CommitterImpl();

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

    List<TopicPartition> assignments =
        ImmutableList.of(new TopicPartition("topic2", 1), new TopicPartition("topic1", 0));
    assertThat(committer.containsFirstPartition(members, assignments)).isTrue();

    assignments =
        ImmutableList.of(new TopicPartition("topic2", 0), new TopicPartition("topic1", 1));
    assertThat(committer.containsFirstPartition(members, assignments)).isFalse();
  }

  @Test
  public void testCoordinatorElectionWhenTwoJobsHaveSameConsumerGroupButSubscribedToMutuallyExclusiveTopics() {

    IcebergSinkConfig icebergSinkConfig = Mockito.mock(IcebergSinkConfig.class);

    CommitterImpl committer = new CommitterImpl(icebergSinkConfig);
    when(icebergSinkConfig.sourceTopics()).thenReturn(Sets.newHashSet("topic1"));

    MemberAssignment assignment1 =
            new MemberAssignment(
                    ImmutableSet.of(new TopicPartition("topic1", 0)));
    MemberDescription member1 =
            new MemberDescription("connector1-consumer-0", Optional.empty(), "connector1-consumer-0", null, assignment1);

    MemberAssignment assignment2 =
            new MemberAssignment(
                    ImmutableSet.of(new TopicPartition("topic2", 0)));
    MemberDescription member2 =
            new MemberDescription("connector2-consumer-0", Optional.empty(), "connector1-consumer-0", null, assignment2);

    List<MemberDescription> members = ImmutableList.of(member1, member2);

    List<TopicPartition> assignments =
            ImmutableList.of(new TopicPartition("topic1", 0));
    assertThat(committer.containsFirstPartition(members, assignments)).isTrue();
    List<TopicPartition> assignmentsNotContaining0thPartition =
            ImmutableList.of(new TopicPartition("topic1", 1));
    assertThat(committer.containsFirstPartition(members, assignments)).isFalse();
  }

  @Test
  public void testCoordinatorElectionWhenTwoJobsHaveSameConsumerGroupButSubscribedToMutuallyInclusiveTopics() {
    IcebergSinkConfig icebergSinkConfig = Mockito.mock(IcebergSinkConfig.class);
    when(icebergSinkConfig.sourceTopics()).thenReturn(Sets.newHashSet("topic1"));

    CommitterImpl committer = new CommitterImpl(icebergSinkConfig);

    MemberAssignment assignment1 =
            new MemberAssignment(
                    ImmutableSet.of(new TopicPartition("topic1", 0), new TopicPartition("topic3", 0)));
    MemberDescription member1 =
            new MemberDescription("connector1-consumer-0", Optional.empty(), "connector1-consumer-0", null, assignment1);

    MemberAssignment assignment2 =
            new MemberAssignment(
                    ImmutableSet.of(new TopicPartition("topic2", 0), new TopicPartition("topic1", 1)));
    MemberDescription member2 =
            new MemberDescription("connector2-consumer-0", Optional.empty(), "connector1-consumer-0", null, assignment2); // same client ID

    List<MemberDescription> members = ImmutableList.of(member1, member2);

    List<TopicPartition> assignments =
            ImmutableList.of(new TopicPartition("topic1", 0), new TopicPartition("topic3", 0));

    assertThatThrownBy(() -> committer.containsFirstPartition(members, assignments))
            .isInstanceOf(ConnectException.class)
            .hasMessageContaining("Possibly more than one jobs are sharing the same consumer group.");
  }

}
