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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);

  private static final String CONTEXT_CLASS_NAME =
      "org.apache.kafka.connect.runtime.WorkerSinkTaskContext";

  static ConsumerGroupDescription consumerGroupDescription(String consumerGroupId, Admin admin) {
    try {
      DescribeConsumerGroupsResult result =
          admin.describeConsumerGroups(ImmutableList.of(consumerGroupId));
      return result.describedGroups().get(consumerGroupId).get();

    } catch (InterruptedException | ExecutionException e) {
      throw new ConnectException(
          "Cannot retrieve members for consumer group: " + consumerGroupId, e);
    }
  }

  static ConsumerGroupMetadata consumerGroupMetadata(SinkTaskContext context) {
    return kafkaConsumer(context).groupMetadata();
  }

  /**
   * Seeks each assigned partition to its last committed offset. A partition without a committed
   * offset is sought to its offset in {@code firstDiscardedOffsets}, the first offset of the
   * records that were buffered for it and discarded, so that those records are consumed again.
   */
  static void seekToLastCommittedOffsets(
      SinkTaskContext context, Map<TopicPartition, Long> firstDiscardedOffsets) {
    Consumer<byte[], byte[]> consumer = kafkaConsumer(context);
    if (consumer == null) {
      return;
    }

    seekToLastCommittedOffsets(consumer, firstDiscardedOffsets);
  }

  @VisibleForTesting
  static void seekToLastCommittedOffsets(
      Consumer<?, ?> consumer, Map<TopicPartition, Long> firstDiscardedOffsets) {
    Set<TopicPartition> assignment = consumer.assignment();
    Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(assignment);

    for (TopicPartition topicPartition : assignment) {
      OffsetAndMetadata committed =
          committedOffsets != null ? committedOffsets.get(topicPartition) : null;
      Long offset;
      if (committed != null) {
        offset = committed.offset();
      } else {
        offset = firstDiscardedOffsets.get(topicPartition);
      }

      if (offset != null) {
        try {
          consumer.seek(topicPartition, offset);
        } catch (IllegalStateException e) {
          LOG.warn(
              "Rebalance may have occurred, partition {} lost before seeking", topicPartition, e);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Consumer<byte[], byte[]> kafkaConsumer(SinkTaskContext context) {
    String contextClassName = context.getClass().getName();
    try {
      return ((Consumer<byte[], byte[]>)
          DynFields.builder().hiddenImpl(CONTEXT_CLASS_NAME, "consumer").build(context).get());
    } catch (Exception e) {
      throw new ConnectException(
          "Unable to retrieve consumer from context: " + contextClassName, e);
    }
  }

  private KafkaUtils() {}
}
