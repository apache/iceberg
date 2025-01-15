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

import java.util.List;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

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
    assertThat(committer.isLeader(members, assignments)).isTrue();

    assignments =
        ImmutableList.of(new TopicPartition("topic2", 0), new TopicPartition("topic1", 1));
    assertThat(committer.isLeader(members, assignments)).isFalse();
  }
}
