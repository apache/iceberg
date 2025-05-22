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
package org.apache.iceberg.connect.handler;

import java.util.Properties;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DlqReporter implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DlqReporter.class);

  private final KafkaProducer<String, String> producer;
  private final String dlqTopic;

  public DlqReporter(IcebergSinkConfig config, String dlqTopic) {
    this.producer = new KafkaProducer<>(this.initiateProperties(config));
    this.dlqTopic = dlqTopic;
  }

  public Properties initiateProperties(IcebergSinkConfig config) {
    Properties producerProps = new Properties();
    producerProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        config.kafkaProps().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    producerProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    return producerProps;
  }

  public void send(String key, String value) {
    try {
      ProducerRecord<String, String> record = new ProducerRecord<>(dlqTopic, key, value);
      producer.send(record);
    } catch (Exception ex) {
      LOG.error("Error writing to dead letter queue topic: {}", this.dlqTopic, ex);
    }
  }

  @Override
  public void close() {
    producer.close();
  }
}
