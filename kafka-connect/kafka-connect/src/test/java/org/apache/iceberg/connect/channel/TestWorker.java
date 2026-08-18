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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
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
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestWorker extends ChannelTestBase {

  private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);

  /**
   * Waits for the polling thread to make observable progress, rather than sleeping for a fixed
   * duration and hoping it got there.
   */
  private static void awaitPoller(Callable<Boolean> condition) {
    Awaitility.await().atMost(AWAIT_TIMEOUT).pollInterval(Duration.ofMillis(10)).until(condition);
  }

  private void mockConsumerGroupMetadata(MockedStatic<KafkaUtils> mockKafkaUtils) {
    ConsumerGroupMetadata consumerGroupMetadata = mock(ConsumerGroupMetadata.class);
    mockKafkaUtils
        .when(() -> KafkaUtils.consumerGroupMetadata(any()))
        .thenReturn(consumerGroupMetadata);
  }

  private SinkTaskContext contextWithAssignment(TopicPartition... assignment) {
    SinkTaskContext context = mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(ImmutableSet.copyOf(assignment));
    return context;
  }

  private void addStartCommit(UUID commitId, long offset) {
    Event event = new Event(config.connectGroupId(), new StartCommit(commitId));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset, "key", AvroUtil.encode(event)));
  }

  @Test
  public void testSave() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      TopicPartition topicPartition = new TopicPartition(SRC_TOPIC_NAME, 0);
      SinkTaskContext context = contextWithAssignment(topicPartition);

      IcebergWriterResult writeResult =
          new IcebergWriterResult(
              TableReference.of("unknown", TableIdentifier.parse(TABLE_NAME), null),
              ImmutableList.of(EventTestUtil.createDataFile()),
              ImmutableList.of(),
              StructType.of());

      Map<TopicPartition, Offset> offsets =
          ImmutableMap.of(topicPartition, new Offset(1L, EventTestUtil.now()));

      SinkWriterResult sinkWriterResult =
          new SinkWriterResult(ImmutableList.of(writeResult), offsets);
      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite()).thenReturn(sinkWriterResult);

      // all mock consumer setup happens before the polling thread can touch the consumer
      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      // start() returns only once the polling thread has subscribed
      worker.start();
      try {
        // save a record
        Map<String, Object> value = ImmutableMap.of();
        SinkRecord rec = new SinkRecord(SRC_TOPIC_NAME, 0, null, "key", null, value, 0L);
        worker.save(ImmutableList.of(rec));

        UUID commitId = UUID.randomUUID();
        addStartCommit(commitId, 1L);
        awaitPoller(() -> worker.pendingEventCount() > 0);

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
      } finally {
        worker.stop();
      }
    }
  }

  @Test
  public void testBackgroundPollingBuffersEvents() {
    when(config.catalogName()).thenReturn("catalog");
    when(config.controlPollIntervalMs()).thenReturn(50);

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      SinkTaskContext context = contextWithAssignment(new TopicPartition(SRC_TOPIC_NAME, 0));

      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite())
          .thenReturn(new SinkWriterResult(ImmutableList.of(), ImmutableMap.of()));

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
      try {
        addStartCommit(UUID.randomUUID(), 1L);
        addStartCommit(UUID.randomUUID(), 2L);

        awaitPoller(() -> worker.pendingEventCount() == 2);

        // process should handle both buffered events
        worker.process();

        // exactly one DATA_COMPLETE per commit, and no DATA_WRITTEN since nothing was written
        assertThat(producer.history()).hasSize(2);
        assertThat(worker.pendingEventCount()).isZero();
      } finally {
        worker.stop();
      }
    }
  }

  @Test
  public void testWorkerIgnoresNonRelevantEvents() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      SinkTaskContext context = contextWithAssignment();
      SinkWriter sinkWriter = mock(SinkWriter.class);

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
      try {
        // a START_COMMIT for a different connector, filtered out by the group id check
        UUID commitId = UUID.randomUUID();
        Event otherGroup = new Event("different-group-id", new StartCommit(commitId));
        consumer.addRecord(
            new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", AvroUtil.encode(otherGroup)));

        // an event for this connector that the worker does not handle
        Event commitComplete =
            new Event(config.connectGroupId(), new CommitComplete(commitId, EventTestUtil.now()));
        consumer.addRecord(
            new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", AvroUtil.encode(commitComplete)));

        // wait until the polling thread has actually consumed both records, so that an empty queue
        // means "consumed and ignored" rather than "not read yet"
        awaitPoller(() -> worker.consumedRecordCount() >= 2);

        worker.process();

        assertThat(worker.pendingEventCount()).isZero();
        assertThat(worker.skippedRecordCount()).isZero();
        assertThat(producer.history()).isEmpty();
        assertThat(worker.isPolling()).isTrue();
      } finally {
        worker.stop();
      }
    }
  }

  @Test
  public void testWorkerSkipsUndecodableControlRecord() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      SinkTaskContext context = contextWithAssignment();
      SinkWriter sinkWriter = mock(SinkWriter.class);
      when(sinkWriter.completeWrite())
          .thenReturn(new SinkWriterResult(ImmutableList.of(), ImmutableMap.of()));

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
      try {
        // a single poison-pill record must not take down the polling thread
        consumer.addRecord(
            new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", new byte[] {0x00, 0x01}));
        UUID commitId = UUID.randomUUID();
        addStartCommit(commitId, 2L);

        awaitPoller(() -> worker.pendingEventCount() > 0);

        assertThat(worker.skippedRecordCount()).isEqualTo(1);
        assertThat(worker.isPolling()).isTrue();

        // the START_COMMIT that follows the bad record is still answered
        worker.process();

        assertThat(producer.history()).hasSize(1);
        Event event = AvroUtil.decode(producer.history().get(0).value());
        assertThat(event.type()).isEqualTo(PayloadType.DATA_COMPLETE);
        assertThat(((DataComplete) event.payload()).commitId()).isEqualTo(commitId);
      } finally {
        worker.stop();
      }
    }
  }

  @Test
  public void testWorkerGracefulShutdown() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      SinkTaskContext context = contextWithAssignment();
      SinkWriter sinkWriter = mock(SinkWriter.class);

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();

      // stop immediately -- must complete without exceptions and release every resource
      assertThatCode(worker::stop).doesNotThrowAnyException();

      assertThat(worker.isPolling()).isFalse();
      assertThat(producer.history()).isEmpty();
      assertThat(producer.closed()).isTrue();
      assertThat(consumer.closed()).isTrue();
      verify(sinkWriter).close();
      verify(admin).close();
    }
  }

  @Test
  public void testWorkerStopIsIdempotent() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      SinkTaskContext context = contextWithAssignment();
      SinkWriter sinkWriter = mock(SinkWriter.class);

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
      worker.stop();

      assertThatCode(worker::stop).doesNotThrowAnyException();
      verify(sinkWriter, times(1)).close();
    }
  }

  @Test
  public void testStopClosesClientsWhenSinkWriterCloseFails() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      SinkTaskContext context = contextWithAssignment();
      SinkWriter sinkWriter = mock(SinkWriter.class);
      doThrow(new RuntimeException("writer close failed")).when(sinkWriter).close();

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();

      // a failing sink writer must not leak the transactional producer
      assertThatCode(worker::stop).doesNotThrowAnyException();

      assertThat(producer.closed()).isTrue();
      assertThat(consumer.closed()).isTrue();
      verify(admin).close();
    }
  }

  @Test
  public void testWorkerHandlesEmptyQueue() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      SinkTaskContext context = contextWithAssignment();
      SinkWriter sinkWriter = mock(SinkWriter.class);

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
      try {
        // call process multiple times with no events
        worker.process();
        worker.process();
        worker.process();

        assertThat(producer.history()).isEmpty();
        verify(sinkWriter, never()).completeWrite();
      } finally {
        worker.stop();
      }
    }
  }

  @Test
  public void testWorkerMultipleStartCommits() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      TopicPartition topicPartition = new TopicPartition(SRC_TOPIC_NAME, 0);
      SinkTaskContext context = contextWithAssignment(topicPartition);

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
      try {
        UUID commitId1 = UUID.randomUUID();
        addStartCommit(commitId1, 1L);
        UUID commitId2 = UUID.randomUUID();
        addStartCommit(commitId2, 2L);

        awaitPoller(() -> worker.pendingEventCount() == 2);

        // process both commits
        worker.process();

        // one DATA_WRITTEN and one DATA_COMPLETE per commit
        assertThat(producer.history()).hasSize(4);
        assertThat(worker.pendingEventCount()).isZero();
      } finally {
        worker.stop();
      }
    }
  }

  @Test
  public void testBackgroundPollingErrorIsStickyAcrossProcessCalls() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      SinkTaskContext context = contextWithAssignment();
      SinkWriter sinkWriter = mock(SinkWriter.class);

      initConsumer();

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      worker.start();
      try {
        // fail the polling thread after startup
        consumer.setPollException(new KafkaException("polling failed"));
        awaitPoller(() -> !worker.isPolling());

        // The failure must be reported on every call, not consumed by the first one: otherwise a
        // second put() would silently keep draining a queue that a dead poller can never refill.
        for (int i = 0; i < 3; i++) {
          assertThatThrownBy(worker::process)
              .isInstanceOf(ConnectException.class)
              .hasMessageContaining("failed while polling the control topic")
              .hasRootCauseMessage("polling failed");
        }

        verify(sinkWriter, never()).completeWrite();
        assertThat(producer.history()).isEmpty();
      } finally {
        worker.stop();
      }
    }
  }

  @Test
  public void testStartFailsWhenSubscriptionFails() {
    when(config.catalogName()).thenReturn("catalog");

    try (MockedStatic<KafkaUtils> mockKafkaUtils = mockStatic(KafkaUtils.class)) {
      mockConsumerGroupMetadata(mockKafkaUtils);

      SinkTaskContext context = contextWithAssignment();
      SinkWriter sinkWriter = mock(SinkWriter.class);

      initConsumer();
      // fail the initial poll performed while subscribing
      consumer.setPollException(new KafkaException("subscribe failed"));

      Worker worker = new Worker(config, clientFactory, sinkWriter, context);
      try {
        assertThatThrownBy(worker::start)
            .isInstanceOf(ConnectException.class)
            .hasMessageContaining("failed to subscribe to the control topic")
            .hasRootCauseMessage("subscribe failed");
      } finally {
        worker.stop();
      }
    }
  }
}
