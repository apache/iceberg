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

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.IcebergWriterResult;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestWorker extends ChannelTestBase {

  @Test
  public void testSave() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      TopicPartition topicPartition = new TopicPartition(SRC_TOPIC_NAME, 0);
      when(context.assignment()).thenReturn(ImmutableSet.of(topicPartition));

      IcebergWriterResult writeResult =
          new IcebergWriterResult(
              TableIdentifier.parse(TABLE_NAME),
              ImmutableList.of(EventTestUtil.createDataFile()),
              ImmutableList.of(),
              StructType.of());

      Map<TopicPartition, Offset> offsets =
          ImmutableMap.of(topicPartition, new Offset(1L, EventTestUtil.now()));

      SinkWriterResult sinkWriterResult =
          new SinkWriterResult(ImmutableList.of(writeResult), offsets);
      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite()).thenReturn(sinkWriterResult);

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();

      // save a record
      Map<String, Object> value = ImmutableMap.of();
      SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
      worker.save(ImmutableList.of(rec));

      UUID commitId = UUID.randomUUID();
      Event commitRequest = new Event(config.connectGroupId(), new StartCommit(commitId));
      byte[] bytes = AvroUtil.encode(commitRequest);
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .pollInterval(Duration.ofMillis(10))
          .until(() -> worker.pendingEventCount() > 0);

      worker.process();

      assertThat(producer.history()).hasSize(2);

      Event event = AvroUtil.decode(producer.history().get(0).value());
      assertThat(event.payload().type()).isEqualTo(PayloadType.DATA_WRITTEN);
      DataWritten dataWritten = (DataWritten) event.payload();
      assertThat(dataWritten.commitId()).isEqualTo(commitId);

      event = AvroUtil.decode(producer.history().get(1).value());
      assertThat(event.type()).isEqualTo(PayloadType.DATA_COMPLETE);
      DataComplete dataComplete = (DataComplete) event.payload();
      assertThat(dataComplete.commitId()).isEqualTo(commitId);
      assertThat(dataComplete.assignments()).hasSize(1);
      assertThat(dataComplete.assignments().get(0).offset()).isEqualTo(1L);

      worker.stop();
    }
  }

  @Test
  public void testBackgroundPollingBuffersEvents() {
    when(config.catalogName()).thenReturn("catalog");
    when(config.controlPollIntervalMs()).thenReturn(50);

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      TopicPartition topicPartition = new TopicPartition(SRC_TOPIC_NAME, 0);
      when(context.assignment()).thenReturn(ImmutableSet.of(topicPartition));

      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite())
          .thenReturn(new SinkWriterResult(ImmutableList.of(), ImmutableMap.of()));

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();

      // Add multiple events to consumer
      UUID commitId1 = UUID.randomUUID();
      Event event1 = new Event(config.connectGroupId(), new StartCommit(commitId1));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(event1)));

      UUID commitId2 = UUID.randomUUID();
      Event event2 = new Event(config.connectGroupId(), new StartCommit(commitId2));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", AvroUtil.encode(event2)));

      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .pollInterval(Duration.ofMillis(10))
          .until(() -> worker.pendingEventCount() >= 2);

      // Process should handle both buffered events
      worker.process();

      // Should have 2 DATA_COMPLETE events (one per commit)
      assertThat(producer.history().size()).isGreaterThanOrEqualTo(2);

      worker.stop();
    }
  }

  @Test
  public void testWorkerIgnoresNonRelevantEvents() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      when(context.assignment()).thenReturn(ImmutableSet.of());

      SinkWriter sinkWriter = mock(SinkWriter.class);

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();

      // Add events with different group IDs (should be ignored by Channel's group filter)
      UUID commitId = UUID.randomUUID();
      Event event = new Event("different-group-id", new StartCommit(commitId));
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(event)));

      // Also add a non-START_COMMIT event with correct group (should be ignored by Worker)
      Event commitComplete =
          new Event(config.connectGroupId(), new CommitComplete(commitId, EventTestUtil.now()));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", AvroUtil.encode(commitComplete)));

      // Wait a bit for the background thread to process the records
      Awaitility.await()
          .pollDelay(Duration.ofMillis(200))
          .atMost(Duration.ofSeconds(5))
          .until(() -> true);

      worker.process();

      // Should not produce any events since no matching START_COMMIT was received
      assertThat(producer.history()).isEmpty();
      assertThat(worker.pendingEventCount()).isEqualTo(0);

      worker.stop();
    }
  }

  @Test
  public void testWorkerGracefulShutdown() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      when(context.assignment()).thenReturn(ImmutableSet.of());

      SinkWriter sinkWriter = mock(SinkWriter.class);

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();

      // Stop worker immediately — should complete without exceptions
      worker.stop();

      assertThat(producer.history()).isEmpty();
    }
  }

  @Test
  public void testWorkerHandlesEmptyQueue() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      when(context.assignment()).thenReturn(ImmutableSet.of());

      SinkWriter sinkWriter = mock(SinkWriter.class);

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();

      // Call process multiple times with no events
      worker.process();
      worker.process();
      worker.process();

      assertThat(producer.history()).isEmpty();

      worker.stop();
    }
  }

  @Test
  public void testWorkerMultipleStartCommits() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      TopicPartition topicPartition = new TopicPartition(SRC_TOPIC_NAME, 0);
      when(context.assignment()).thenReturn(ImmutableSet.of(topicPartition));

      IcebergWriterResult writeResult1 =
          new IcebergWriterResult(
              TableIdentifier.parse(TABLE_NAME),
              ImmutableList.of(EventTestUtil.createDataFile()),
              ImmutableList.of(),
              StructType.of());

      IcebergWriterResult writeResult2 =
          new IcebergWriterResult(
              TableIdentifier.parse(TABLE_NAME),
              ImmutableList.of(EventTestUtil.createDataFile()),
              ImmutableList.of(),
              StructType.of());

      Map<TopicPartition, Offset> offsets =
          ImmutableMap.of(topicPartition, new Offset(1L, EventTestUtil.now()));

      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite())
          .thenReturn(new SinkWriterResult(ImmutableList.of(writeResult1), offsets))
          .thenReturn(new SinkWriterResult(ImmutableList.of(writeResult2), offsets));

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();

      // Add multiple START_COMMIT events
      UUID commitId1 = UUID.randomUUID();
      Event event1 = new Event(config.connectGroupId(), new StartCommit(commitId1));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(event1)));

      UUID commitId2 = UUID.randomUUID();
      Event event2 = new Event(config.connectGroupId(), new StartCommit(commitId2));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", AvroUtil.encode(event2)));

      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .pollInterval(Duration.ofMillis(10))
          .until(() -> worker.pendingEventCount() >= 2);

      // Process both commits
      worker.process();

      // Should have events for both commits (2 data written + 2 data complete)
      assertThat(producer.history()).hasSize(4);

      worker.stop();
    }
  }

  @Test
  public void testBackgroundPollingErrorPropagation() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
      mockKafkaUtils
          .when(() -> KafkaUtils.consumerGroupMetadata(any()))
          .thenReturn(consumerGroupMetadata);

      SinkTaskContext context = mock(SinkTaskContext.class);
      when(context.assignment()).thenReturn(ImmutableSet.of());

      SinkWriter sinkWriter = mock(SinkWriter.class);

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();

      // Add a record with invalid payload to cause deserialization error in backgroundPoll
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", new byte[] {0x00, 0x01}));

      // Wait for the background thread to encounter the error
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .pollInterval(Duration.ofMillis(10))
          .until(
              () -> {
                try {
                  worker.process();
                  return false;
                } catch (ConnectException e) {
                  return true;
                }
              });
    }
  }
}
