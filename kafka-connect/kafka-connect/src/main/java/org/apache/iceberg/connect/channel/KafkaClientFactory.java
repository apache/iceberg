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
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

class KafkaClientFactory {
  private final Map<String, String> kafkaProps;

  KafkaClientFactory(Map<String, String> kafkaProps) {
    this.kafkaProps = kafkaProps;
  }

  Producer<String, byte[]> createProducer(String transactionalId) {
    Map<String, Object> producerProps = Maps.newHashMap(kafkaProps);
    producerProps.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    KafkaProducer<String, byte[]> result =
        new KafkaProducer<>(producerProps, new StringSerializer(), new ByteArraySerializer());
    result.initTransactions();
    return result;
  }

  Consumer<String, byte[]> createConsumer(String consumerGroupId) {
    return createConsumer(consumerGroupId, "latest");
  }

  /**
   * Creates a control-topic consumer.
   *
   * @param autoOffsetReset default reset policy for partitions with no committed offset or an
   *     offset that is out of range; an explicitly configured reset policy takes precedence
   */
  Consumer<String, byte[]> createConsumer(String consumerGroupId, String autoOffsetReset) {
    return new KafkaConsumer<>(
        consumerProps(consumerGroupId, autoOffsetReset),
        new StringDeserializer(),
        new ByteArrayDeserializer());
  }

  @VisibleForTesting
  Map<String, Object> consumerProps(String consumerGroupId, String autoOffsetReset) {
    Map<String, Object> consumerProps = Maps.newHashMap(kafkaProps);
    consumerProps.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
    consumerProps.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    return consumerProps;
  }

  Admin createAdmin() {
    Map<String, Object> adminProps = Maps.newHashMap(kafkaProps);
    return Admin.create(adminProps);
  }
}
