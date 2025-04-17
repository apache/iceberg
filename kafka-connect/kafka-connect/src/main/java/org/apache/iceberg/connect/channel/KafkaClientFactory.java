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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

class KafkaClientFactory {
  private final Map<String, String> kafkaProps;

  KafkaClientFactory(Map<String, String> kafkaProps) {
    this.kafkaProps = kafkaProps;
  }

  @SuppressWarnings("unchecked")
  Producer<String, byte[]> createProducer(String transactionalId) {
    Map<String, Object> producerProps = Maps.newHashMap(kafkaProps);
    producerProps.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

    try {
      // Resolve key and value serializer classes safely
      Object keySerializerProp =
          producerProps.getOrDefault(
              ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      Object valueSerializerProp =
          producerProps.getOrDefault(
              ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

      // Convert String to Class if necessary
      Class<? extends Serializer<String>> keySerializerClass =
          (keySerializerProp instanceof String)
              ? (Class<? extends Serializer<String>>) Class.forName((String) keySerializerProp)
              : (Class<? extends Serializer<String>>) keySerializerProp;

      Class<? extends Serializer<byte[]>> valueSerializerClass =
          (valueSerializerProp instanceof String)
              ? (Class<? extends Serializer<byte[]>>) Class.forName((String) valueSerializerProp)
              : (Class<? extends Serializer<byte[]>>) valueSerializerProp;

      Serializer<String> keySerializer = keySerializerClass.getDeclaredConstructor().newInstance();
      keySerializer.configure(producerProps, true);
      Serializer<byte[]> valueSerializer =
          valueSerializerClass.getDeclaredConstructor().newInstance();
      valueSerializer.configure(producerProps, false);
      KafkaProducer<String, byte[]> result =
          new KafkaProducer<>(producerProps, keySerializer, valueSerializer);
      result.initTransactions();
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate serializers", e);
    }
  }

  @SuppressWarnings("unchecked")
  Consumer<String, byte[]> createConsumer(String consumerGroupId) {
    Map<String, Object> consumerProps = Maps.newHashMap(kafkaProps);
    consumerProps.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

    try {
      // Resolve key and value deserializer classes safely
      Object keyDeserializerProp =
          consumerProps.getOrDefault(
              ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      Object valueDeserializerProp =
          consumerProps.getOrDefault(
              ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

      // Convert String to Class if necessary
      Class<? extends Deserializer<String>> keyDeserializerClass =
          (keyDeserializerProp instanceof String)
              ? (Class<? extends Deserializer<String>>) Class.forName((String) keyDeserializerProp)
              : (Class<? extends Deserializer<String>>) keyDeserializerProp;

      Class<? extends Deserializer<byte[]>> valueDeserializerClass =
          (valueDeserializerProp instanceof String)
              ? (Class<? extends Deserializer<byte[]>>)
                  Class.forName((String) valueDeserializerProp)
              : (Class<? extends Deserializer<byte[]>>) valueDeserializerProp;

      // Instantiate deserializers
      Deserializer<String> keyDeserializer =
          keyDeserializerClass.getDeclaredConstructor().newInstance();
      keyDeserializer.configure(consumerProps, true);
      Deserializer<byte[]> valueDeserializer =
          valueDeserializerClass.getDeclaredConstructor().newInstance();
      valueDeserializer.configure(consumerProps, false);

      return new KafkaConsumer<>(consumerProps, keyDeserializer, valueDeserializer);
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate deserializers", e);
    }
  }

  Admin createAdmin() {
    Map<String, Object> adminProps = Maps.newHashMap(kafkaProps);
    return Admin.create(adminProps);
  }
}
