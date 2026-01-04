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

import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.data.IcebergWriterResult;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.data.SinkWriterResult;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestWorker extends ChannelTestBase {

  @Test
  public void testSave() throws InterruptedException {
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

      // init consumer after subscribe()
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

      // Give background thread time to poll and buffer the event
      Thread.sleep(500);
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
  public void testBackgroundPollingBuffersEvents() throws InterruptedException {
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

      // Wait for background polling to buffer events
      Thread.sleep(300);

      // Process should handle both buffered events
      worker.process();

      // Should have 2 DATA_COMPLETE events (one per commit)
      assertThat(producer.history().size()).isGreaterThanOrEqualTo(2);

      worker.stop();
    }
  }

  @Test
  public void testWorkerIgnoresNonRelevantEvents() throws InterruptedException {
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

      // Add events with different group IDs (should be ignored)
      UUID commitId = UUID.randomUUID();
      Event event = new Event("different-group-id", new StartCommit(commitId));
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(event)));

      Thread.sleep(200);
      worker.process();

      // Should not produce any events since the group ID doesn't match
      assertThat(producer.history()).isEmpty();

      worker.stop();
    }
  }

  @Test
  public void testWorkerGracefulShutdown() throws InterruptedException {
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

      // Stop worker immediately
      worker.stop();

      // Should complete without exceptions
      assertThat(producer.history()).isEmpty();
    }
  }

  @Test
  public void testWorkerProcessesMultipleEventTypes() throws InterruptedException {
    when(config.catalogName()).thenReturn("catalog");

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

      UUID commitId = UUID.randomUUID();

      // Add START_COMMIT event
      Event startCommit = new Event(config.connectGroupId(), new StartCommit(commitId));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(startCommit)));

      // Add COMMIT_COMPLETE event
      Event commitComplete =
          new Event(config.connectGroupId(), new CommitComplete(commitId, EventTestUtil.now()));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", AvroUtil.encode(commitComplete)));

      // Add COMMIT_TO_TABLE event
      Event commitToTable =
          new Event(
              config.connectGroupId(),
              new CommitToTable(
                  commitId,
                  TableReference.of("catalog", TableIdentifier.parse(TABLE_NAME)),
                  1L,
                  EventTestUtil.now()));
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 3, "key", AvroUtil.encode(commitToTable)));

      Thread.sleep(300);

      // All events should be buffered
      worker.process();

      // Should have processed the START_COMMIT event
      assertThat(producer.history()).isNotEmpty();

      worker.stop();
    }
  }

  @Test
  public void testWorkerHandlesEmptyQueue() throws InterruptedException {
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
  public void testWorkerWithCustomPollInterval() throws InterruptedException {
    when(config.catalogName()).thenReturn("catalog");
    when(config.controlPollIntervalMs()).thenReturn(1000); // 1 second

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

      UUID commitId = UUID.randomUUID();
      Event event = new Event(config.connectGroupId(), new StartCommit(commitId));
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(event)));

      // Wait for longer than poll interval to ensure event is buffered
      Thread.sleep(1500);

      worker.process();

      assertThat(producer.history()).isNotEmpty();

      worker.stop();
    }
  }

  @Test
  public void testWorkerMultipleStartCommits() throws InterruptedException {
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

      Thread.sleep(300);

      // Process both commits
      worker.process();

      // Should have events for both commits
      assertThat(producer.history().size())
          .isGreaterThanOrEqualTo(4); // 2 data written + 2 data complete

      worker.stop();
    }
  }
}
