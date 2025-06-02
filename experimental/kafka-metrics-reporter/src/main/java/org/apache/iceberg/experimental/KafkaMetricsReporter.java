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
package org.apache.iceberg.experimental;

import java.util.Map;
import java.util.Properties;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CommitReportParser;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReportParser;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A custom {@link MetricsReporter} implementation that can be used to report metrics to Kafka. */
public class KafkaMetricsReporter implements MetricsReporter {
  public static final String ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY = "metrics-reporter.kafka.";
  public static final String ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_KEY =
      ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY + "topic";
  public static final String ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_DEFAULT = "iceberg-metrics";

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsReporter.class);
  private Producer<String, String> producer;
  private String topic;

  public KafkaMetricsReporter() {}

  @VisibleForTesting
  KafkaMetricsReporter(Producer<String, String> producer) {
    this(producer, ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_DEFAULT);
  }

  @VisibleForTesting
  KafkaMetricsReporter(Producer<String, String> producer, String topic) {
    this.producer = producer;
    this.topic = topic;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.topic =
        properties.getOrDefault(
            ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_KEY, ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_DEFAULT);

    this.producer = new KafkaProducer<String, String>(toProducerProperties(properties));
    LOG.info("Initialized IcebergKafkaMetricsReporter with properties");
  }

  @VisibleForTesting
  Properties toProducerProperties(Map<String, String> properties) {
    Properties producerProps = new Properties();

    // Set default properties for Kafka producer
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Fill the properties from the provided map
    properties.forEach(
        (key, value) -> {
          if (key.startsWith(ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY)) {
            producerProps.putIfAbsent(
                key.substring(ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY.length()), value);
          }
        });

    producerProps.remove(ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_KEY);

    return producerProps;
  }

  /**
   * Converts the provided {@link MetricsReport} into a Kafka {@link ProducerRecord}.
   *
   * @param report the metrics report to convert
   * @param reportTopic the Kafka topic to which the record will be sent
   * @return a {@link ProducerRecord} containing the JSON representation of the report
   */
  @VisibleForTesting
  ProducerRecord<String, String> toProducerRecord(MetricsReport report, String reportTopic) {
    ProducerRecord<String, String> record;

    if (report instanceof ScanReport) {
      record =
          new ProducerRecord<>(
              reportTopic,
              report.getClass().getName(),
              ScanReportParser.toJson((ScanReport) report, false));
    } else if (report instanceof CommitReport) {
      record =
          new ProducerRecord<>(
              reportTopic,
              report.getClass().getName(),
              CommitReportParser.toJson((CommitReport) report, false));
    } else {
      throw new IllegalArgumentException(
          "Unsupported MetricsReport type: " + report.getClass().getName());
    }

    return record;
  }

  @Override
  public void report(MetricsReport report) {
    if (report == null) {
      LOG.warn("Received invalid metrics report: null");
      return;
    }
    try {
      ProducerRecord<String, String> record = toProducerRecord(report, topic);
      producer.send(record).get();
      LOG.info("Reported metrics to Kafka topic: {}", topic);
    } catch (Exception e) {
      LOG.warn("Failed to report metrics to Kafka topic {}", topic, e);
    }
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
      LOG.info("Closed IcebergKafkaMetricsReporter");
    }
  }
}
