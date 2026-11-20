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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestKafkaUtils {

  private static final TopicPartition P0 = new TopicPartition("src-topic", 0);
  private static final TopicPartition P1 = new TopicPartition("src-topic", 1);
  private static final TopicPartition P2 = new TopicPartition("src-topic", 2);

  private MockConsumer<byte[], byte[]> consumer;

  @BeforeEach
  public void before() {
    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.assign(ImmutableList.of(P0, P1, P2));
    consumer.updateBeginningOffsets(ImmutableMap.of(P0, 0L, P1, 0L, P2, 0L));
    // the task has consumed past the buffered records on every partition
    consumer.seek(P0, 10L);
    consumer.seek(P1, 10L);
    consumer.seek(P2, 10L);
  }

  @Test
  public void testSeekToCommittedOffsets() {
    consumer.commitSync(ImmutableMap.of(P0, new OffsetAndMetadata(4L)));

    KafkaUtils.seekToLastCommittedOffsets(consumer, ImmutableMap.of(P0, 2L, P1, 3L));

    assertThat(consumer.position(P0)).as("committed offset is preferred").isEqualTo(4L);
    assertThat(consumer.position(P1)).as("no commit: first discarded record").isEqualTo(3L);
    assertThat(consumer.position(P2)).as("no commit and nothing discarded").isEqualTo(10L);
  }

  @Test
  public void testSeekWhenNoPartitionHasCommitted() {
    KafkaUtils.seekToLastCommittedOffsets(consumer, ImmutableMap.of(P0, 2L, P1, 3L));

    assertThat(consumer.position(P0)).isEqualTo(2L);
    assertThat(consumer.position(P1)).isEqualTo(3L);
    assertThat(consumer.position(P2)).isEqualTo(10L);
  }
}
