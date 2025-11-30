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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.TestCustomCommitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Comprehensive test suite for Committer functionality.
 *
 * <p>Tests both the default CommitterImpl and custom committer implementations, along with the
 * CommitterFactory configuration and instantiation logic.
 */
public class TestCommitter {

  private static final Map<String, String> BASE_CONFIG =
      ImmutableMap.of(
          "name",
          "test-connector",
          "iceberg.tables",
          "test_table",
          "iceberg.catalog.type",
          "hadoop",
          "iceberg.catalog.warehouse",
          "file:///tmp/test-warehouse");

  @Nested
  class CommitterImplTests {

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

  @Nested
  class CustomCommitterTests {

    private TestCustomCommitter committer;
    private IcebergSinkConfig config;
    private SinkTaskContext context;

    @BeforeEach
    public void setUp() {
      committer = new TestCustomCommitter();
      config = new IcebergSinkConfig(BASE_CONFIG);
      context = mock(SinkTaskContext.class);
    }

    @AfterEach
    public void tearDown() {
      if (committer != null && committer.isStarted()) {
        committer.onTaskStopped();
      }
    }

    @Test
    public void testCommitterInstantiation() {
      assertThat(committer).isNotNull();
      assertThat(committer.isStarted()).isFalse();
    }

    @Test
    public void testOnTaskStarted() {
      committer.onTaskStarted(config, context);

      assertThat(committer.isStarted()).isTrue();
      assertThat(committer.getCatalog()).isNotNull();
      assertThat(committer.getConfig()).isEqualTo(config);
      assertThat(committer.getContext()).isEqualTo(context);
    }

    @Test
    public void testOnPartitionsAdded() {
      committer.onTaskStarted(config, context);

      Collection<TopicPartition> partitions = new ArrayList<>();
      partitions.add(new TopicPartition("test_topic", 0));
      partitions.add(new TopicPartition("test_topic", 1));

      committer.onPartitionsAdded(partitions);

      assertThat(committer.isStarted()).isTrue();
    }

    @Test
    public void testOnPartitionsRemoved() {
      committer.onTaskStarted(config, context);

      Collection<TopicPartition> partitions = new ArrayList<>();
      partitions.add(new TopicPartition("test_topic", 0));

      committer.onPartitionsRemoved(partitions);

      assertThat(committer.isStarted()).isTrue();
    }

    @Test
    public void testOnTaskStopped() {
      committer.onTaskStarted(config, context);

      assertThat(committer.isStarted()).isTrue();

      committer.onTaskStopped();

      assertThat(committer.isStarted()).isFalse();
      assertThat(committer.getCatalog()).isNull();
    }

    @Test
    public void testSaveRecords() {
      committer.onTaskStarted(config, context);

      List<SinkRecord> records = createTestRecords(10);

      committer.save(records);
    }

    @Test
    public void testSaveRecordsBeforeStart() {
      List<SinkRecord> records = createTestRecords(5);

      committer.save(records);

      assertThat(committer.isStarted()).isFalse();
    }

    @Test
    public void testSaveEmptyRecords() {
      committer.onTaskStarted(config, context);

      List<SinkRecord> records = Collections.emptyList();

      committer.save(records);
    }

    @Test
    public void testLifecycleFlow() {
      assertThat(committer.isStarted()).isFalse();

      committer.onTaskStarted(config, context);
      assertThat(committer.isStarted()).isTrue();

      Collection<TopicPartition> addedPartitions = new ArrayList<>();
      addedPartitions.add(new TopicPartition("test_topic", 0));
      addedPartitions.add(new TopicPartition("test_topic", 1));
      committer.onPartitionsAdded(addedPartitions);

      List<SinkRecord> records1 = createTestRecords(10);
      committer.save(records1);

      List<SinkRecord> records2 = createTestRecords(5);
      committer.save(records2);

      Collection<TopicPartition> removedPartitions = new ArrayList<>();
      removedPartitions.add(new TopicPartition("test_topic", 1));
      committer.onPartitionsRemoved(removedPartitions);

      committer.onTaskStopped();
      assertThat(committer.isStarted()).isFalse();
    }

    @Test
    public void testMultipleStartStopCycles() {
      for (int i = 0; i < 3; i++) {
        committer.onTaskStarted(config, context);
        assertThat(committer.isStarted()).isTrue();

        List<SinkRecord> records = createTestRecords(5);
        committer.save(records);

        committer.onTaskStopped();
        assertThat(committer.isStarted()).isFalse();
      }
    }

    @Test
    public void testCatalogClosedOnStop() {
      committer.onTaskStarted(config, context);

      assertThat(committer.getCatalog()).isNotNull();

      committer.onTaskStopped();

      assertThat(committer.getCatalog()).isNull();
    }

    private List<SinkRecord> createTestRecords(int count) {
      List<SinkRecord> records = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        SinkRecord record = new SinkRecord("test_topic", 0, null, "key" + i, null, "value" + i, i);
        records.add(record);
      }
      return records;
    }
  }
}
