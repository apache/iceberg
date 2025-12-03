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
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
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
}
