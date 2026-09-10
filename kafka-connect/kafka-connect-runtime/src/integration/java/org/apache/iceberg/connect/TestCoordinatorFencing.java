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
package org.apache.iceberg.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that a stale coordinator cannot overwrite offsets committed by a newer coordinator, even
 * when the stale coordinator commits an offset ahead of its own previous one but still behind the
 * newer coordinator's.
 */
public class TestCoordinatorFencing {

  private static final long STALE_INITIAL_OFFSET = 100L;
  private static final long NEW_COORDINATOR_OFFSET = 200L;
  private static final long NEXT_STALE_OFFSET = 150L;

  private final TestContext context = TestContext.instance();

  private String topicName;
  private String groupId;
  private TopicPartition topicPartition;
  private Admin admin;

  @BeforeEach
  public void before() {
    topicName = "coord-fencing-topic-" + UUID.randomUUID();
    groupId = "coord-fencing-group-" + UUID.randomUUID();
    topicPartition = new TopicPartition(topicName, 0);
    admin = context.initLocalAdmin();
    createTopic(topicName);
  }

  @AfterEach
  public void after() {
    deleteTopic(topicName);
    admin.close();
  }

  @Test
  public void newCoordinatorFencesStaleCoordinatorOffsetCommits() throws Exception {
    Map<String, String> connectorProps = connectorProps();
    String staleCoordinatorId = new IcebergSinkConfig(connectorProps).coordinatorTransactionalId();
    String newCoordinatorId = new IcebergSinkConfig(connectorProps).coordinatorTransactionalId();
    KafkaProducer<String, String> staleCoordinator =
        context.initLocalTransactionalProducer(staleCoordinatorId);
    KafkaProducer<String, String> newCoordinator =
        context.initLocalTransactionalProducer(newCoordinatorId);

    try {
      staleCoordinator.initTransactions();
      commitOffset(staleCoordinator, STALE_INITIAL_OFFSET);
      awaitCommittedOffset(STALE_INITIAL_OFFSET);

      newCoordinator.initTransactions();
      commitOffset(newCoordinator, NEW_COORDINATOR_OFFSET);
      awaitCommittedOffset(NEW_COORDINATOR_OFFSET);

      assertThatThrownBy(() -> commitOffset(staleCoordinator, NEXT_STALE_OFFSET))
          .isInstanceOf(ProducerFencedException.class)
          .hasMessageContaining("fence");

      assertThat(committedOffset())
          .as("fenced coordinator must not clobber the new coordinator's committed offset")
          .isEqualTo(NEW_COORDINATOR_OFFSET);
    } finally {
      staleCoordinator.close();
      newCoordinator.close();
    }
  }

  private Map<String, String> connectorProps() {
    return ImmutableMap.of(
        "iceberg.catalog.type", "rest",
        "iceberg.tables", "db.tbl",
        "name", "coord-fencing-connector-" + UUID.randomUUID());
  }

  private void commitOffset(KafkaProducer<String, String> producer, long offset) {
    producer.beginTransaction();
    producer.sendOffsetsToTransaction(
        ImmutableMap.of(topicPartition, new OffsetAndMetadata(offset)),
        new ConsumerGroupMetadata(groupId));
    producer.commitTransaction();
  }

  private void awaitCommittedOffset(long expected) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(() -> assertThat(committedOffset()).isEqualTo(expected));
  }

  private long committedOffset() throws InterruptedException, ExecutionException, TimeoutException {
    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin
            .listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata()
            .get(10, TimeUnit.SECONDS);
    OffsetAndMetadata metadata = offsets.get(topicPartition);
    return metadata == null ? -1L : metadata.offset();
  }

  private void createTopic(String topic) {
    try {
      admin
          .createTopics(ImmutableList.of(new NewTopic(topic, 1, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private void deleteTopic(String topic) {
    try {
      admin.deleteTopics(ImmutableList.of(topic)).all().get(10, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
