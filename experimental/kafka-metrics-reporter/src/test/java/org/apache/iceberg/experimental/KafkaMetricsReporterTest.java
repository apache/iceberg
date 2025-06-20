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

import static org.apache.iceberg.experimental.KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

public class KafkaMetricsReporterTest {

  @Test
  public void testReport() throws Exception {
    // Set up mock Kafka producer
    MockProducer<String, String> mockProducer =
        new MockProducer<>(true, new StringSerializer(), new StringSerializer());

    KafkaMetricsReporter reporter = new KafkaMetricsReporter(mockProducer);

    MetricsReport scanReport =
        ImmutableScanReport.builder()
            .tableName("test-table")
            .schemaId(4)
            .addProjectedFieldIds(1, 2, 3)
            .addProjectedFieldNames("c1", "c2", "c3")
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();

    String scanReportExpectedJson =
        "{\"table-name\":\"test-table\",\"snapshot-id\":23,\"filter\":true,\"schema-id\":4,"
            + "\"projected-field-ids\":[1,2,3],\"projected-field-names\":[\"c1\",\"c2\",\"c3\"],"
            + "\"metrics\":{}}";

    // Send the report
    reporter.report(scanReport);

    // Verify that one record was sent to Kafka
    List<ProducerRecord<String, String>> history = mockProducer.history();
    assertThat(history.size()).isEqualTo(1);
    ProducerRecord<String, String> scanReportRecord = history.get(0);
    assertThat(scanReportRecord.topic())
        .isEqualTo(KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_DEFAULT);
    assertThat(scanReportRecord.key()).isEqualTo(ImmutableScanReport.class.getName());
    assertThat(scanReportRecord.value()).isEqualTo(scanReportExpectedJson);

    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("roundTripTableName")
            .snapshotId(23L)
            .operation("DELETE")
            .sequenceNumber(4L)
            .commitMetrics(CommitMetricsResult.from(CommitMetrics.noop(), Map.of()))
            .build();

    String commitReportExpectedJson =
        "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"sequence-number\":4,"
            + "\"operation\":\"DELETE\",\"metrics\":{}}";

    reporter.report(commitReport);

    history = mockProducer.history();
    assertThat(history.size()).isEqualTo(2);
    ProducerRecord<String, String> commitReportRecord = history.get(1);
    assertThat(commitReportRecord.topic())
        .isEqualTo(KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_DEFAULT);
    assertThat(commitReportRecord.key()).isEqualTo(ImmutableCommitReport.class.getName());
    assertThat(commitReportRecord.value()).isEqualTo(commitReportExpectedJson);

    // Report a null report
    reporter.report(null);

    // Verify that no records were sent to Kafka
    history = mockProducer.history();
    assertThat(history.size()).isEqualTo(2);
  }

  @Test
  public void testInitialize() {
    // Arrange
    KafkaMetricsReporter reporter = new KafkaMetricsReporter();
    Map<String, String> properties =
        Map.of(
            KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_KEY,
            "test-topic",
            KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY
                + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092");

    // Act
    reporter.initialize(properties);

    // Assert
    assertThat(reporter).extracting("topic").isEqualTo("test-topic");

    Properties producerProps = reporter.toProducerProperties(properties);
    assertThat(producerProps.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
        .isEqualTo("localhost:9092");
    assertThat(producerProps.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
        .isEqualTo(StringSerializer.class.getName());
    assertThat(producerProps.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
        .isEqualTo(StringSerializer.class.getName());
  }

  @Test
  public void testToProducerProperties() {
    AtomicReference<Properties> capturedProps = new AtomicReference<>();

    String topic = "iceberg-metrics-topic";
    Map<String, String> props =
        Map.of(
            ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092",
            ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            BytesSerializer.class.getName(),
            ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            BytesSerializer.class.getName(),
            ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY
                + KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_KEY,
            topic,
            "iceberg.x.y.test",
            "test-value");

    Map<String, String> expectedProps =
        Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    KafkaMetricsReporter reporter = new KafkaMetricsReporter();
    Properties actualProps = reporter.toProducerProperties(props);

    assertThat(actualProps).isEqualTo(expectedProps);
  }

  @Test
  public void testToProducerRecord() throws InstantiationException, IllegalAccessException {
    // Set up mock Kafka producer
    MockProducer<String, String> mockProducer =
        new MockProducer<>(true, new StringSerializer(), new StringSerializer());

    KafkaMetricsReporter reporter = new KafkaMetricsReporter(mockProducer);

    MetricsReport scanReport =
        ImmutableScanReport.builder()
            .tableName("test-table")
            .schemaId(4)
            .addProjectedFieldIds(1, 2, 3)
            .addProjectedFieldNames("c1", "c2", "c3")
            .snapshotId(23L)
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();

    ProducerRecord<String, String> scanReportRecord =
        reporter.toProducerRecord(
            scanReport, KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_DEFAULT);

    String scanReportExpectedJson =
        "{\"table-name\":\"test-table\",\"snapshot-id\":23,\"filter\":true,\"schema-id\":4,"
            + "\"projected-field-ids\":[1,2,3],"
            + "\"projected-field-names\":[\"c1\",\"c2\",\"c3\"],\"metrics\":{}}";

    assertThat(scanReportRecord.topic())
        .isEqualTo(KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_DEFAULT);
    assertThat(scanReportRecord.key()).isEqualTo(ImmutableScanReport.class.getName());
    assertThat(scanReportRecord.value()).isEqualTo(scanReportExpectedJson);

    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName("roundTripTableName")
            .snapshotId(23L)
            .operation("DELETE")
            .sequenceNumber(4L)
            .commitMetrics(CommitMetricsResult.from(CommitMetrics.noop(), Map.of()))
            .build();

    ProducerRecord<String, String> commitReportRecord =
        reporter.toProducerRecord(
            commitReport, KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_DEFAULT);

    String commitReportExpectedJson =
        "{\"table-name\":\"roundTripTableName\",\"snapshot-id\":23,\"sequence-number\":4,"
            + "\"operation\":\"DELETE\",\"metrics\":{}}";

    assertThat(commitReportRecord.topic())
        .isEqualTo(KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_DEFAULT);
    assertThat(commitReportRecord.key()).isEqualTo(ImmutableCommitReport.class.getName());
    assertThat(commitReportRecord.value()).isEqualTo(commitReportExpectedJson);

    MetricsReport report = new MetricsReport() {};
    assertThatThrownBy(
            () ->
                reporter.toProducerRecord(
                    report, KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_TOPIC_DEFAULT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported MetricsReport type: " + report.getClass().getName());
  }

  @Test
  void testClose() throws InstantiationException, IllegalAccessException {
    // Set up mock Kafka producer
    MockProducer<String, String> mockProducer =
        new MockProducer<>(true, new StringSerializer(), new StringSerializer());

    KafkaMetricsReporter reporter = new KafkaMetricsReporter(mockProducer);

    // Close the reporter
    reporter.close();

    // Verify that the producer was closed
    assertThat(mockProducer.closed()).isTrue();

    // test non-initialized reporter
    KafkaMetricsReporter nullReporter = new KafkaMetricsReporter();

    reporter.close();
  }

  @Test
  void testLoadMetricsReporter() throws NoSuchMethodException {
    Map<String, String> properties =
        Map.of(
            CatalogProperties.METRICS_REPORTER_IMPL,
            KafkaMetricsReporter.class.getName(),
            KafkaMetricsReporter.ICEBERG_EXPERIMENTAL_KAFKA_PREFIX_KEY
                + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092");

    MetricsReporter metricsReporter = CatalogUtil.loadMetricsReporter(properties);
    assertThat(metricsReporter).isInstanceOf(KafkaMetricsReporter.class);
  }
}
