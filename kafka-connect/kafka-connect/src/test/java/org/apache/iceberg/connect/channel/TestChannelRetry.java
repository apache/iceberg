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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Unit tests for the re-balance-aware error translation in {@link Channel#send(java.util.List,
 * java.util.Map)}. These tests use mocked {@link Producer} instances to exercise re-balance-induced
 * commit failures that {@link org.apache.kafka.clients.producer.MockProducer} cannot easily
 * reproduce.
 *
 * <p>Behavior contract: when {@code commitTransaction} or {@code sendOffsetsToTransaction} fails
 * with one of the configured retriable exception classes (default: {@link CommitFailedException},
 * {@link InvalidProducerEpochException}, {@link ProducerFencedException}), {@code send()} aborts
 * the transaction and throws {@link RetriableException} so the Connect framework pauses the
 * consumer, lets the worker close + recreate (re-running {@code initTransactions()} for a fresh
 * producer epoch), and re-delivers the same batch. Exceptions outside the configured list remain
 * fatal.
 */
public class TestChannelRetry {

  private static final String CONTROL_TOPIC = "ctl-topic";
  private static final String CONNECT_GROUP = "cg-connect";
  private static final String CONSUMER_GROUP = "worker-cg";

  private static final List<Class<? extends Throwable>> DEFAULT_RETRIABLE_EXCEPTIONS =
      ImmutableList.of(
          CommitFailedException.class,
          InvalidProducerEpochException.class,
          ProducerFencedException.class);

  private Producer<String, byte[]> producer;
  private Consumer<String, byte[]> consumer;
  private Admin admin;
  private KafkaClientFactory clientFactory;
  private IcebergSinkConfig config;
  private SinkTaskContext context;
  private MockedStatic<KafkaUtils> mockedKafkaUtils;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void before() {
    producer = mock(Producer.class);
    consumer = mock(Consumer.class);
    admin = mock(Admin.class);

    clientFactory = mock(KafkaClientFactory.class);
    when(clientFactory.createProducer(any())).thenReturn(producer);
    when(clientFactory.createConsumer(any())).thenReturn(consumer);
    when(clientFactory.createAdmin()).thenReturn(admin);

    config = mock(IcebergSinkConfig.class);
    when(config.controlTopic()).thenReturn(CONTROL_TOPIC);
    when(config.connectGroupId()).thenReturn(CONNECT_GROUP);
    when(config.transactionalPrefix()).thenReturn("");
    when(config.transactionalSuffix()).thenReturn("");
    when(config.transactionalCommitRetriableExceptionClasses())
        .thenReturn(DEFAULT_RETRIABLE_EXCEPTIONS);

    context = mock(SinkTaskContext.class);

    ConsumerGroupMetadata groupMetadata = mock(ConsumerGroupMetadata.class);
    mockedKafkaUtils = mockStatic(KafkaUtils.class);
    mockedKafkaUtils
        .when(() -> KafkaUtils.consumerGroupMetadata(any(SinkTaskContext.class)))
        .thenReturn(groupMetadata);
  }

  @AfterEach
  void after() {
    if (mockedKafkaUtils != null) {
      mockedKafkaUtils.close();
    }
  }

  // ------------------------------------------------------------
  // Happy path
  // ------------------------------------------------------------

  @Test
  public void sendSucceedsOnFirstAttempt() {
    StubChannel channel = newChannel();

    channel.sendForTest(startCommitEvent(), offsetsForSrc(42L));

    verify(producer, times(1)).beginTransaction();
    verify(producer, times(1)).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
    verify(producer, times(1)).commitTransaction();
    verify(producer, never()).abortTransaction();
  }

  @Test
  public void sendWithEmptySourceOffsetsSkipsSendOffsetsToTransaction() {
    StubChannel channel = newChannel();

    channel.sendForTest(startCommitEvent(), ImmutableMap.of());

    verify(producer, times(1)).beginTransaction();
    verify(producer, never()).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
    verify(producer, times(1)).commitTransaction();
  }

  // ------------------------------------------------------------
  // Recoverable re-balance failures — translated to RetriableException
  // ------------------------------------------------------------

  @Test
  public void commitFailedExceptionIsTranslatedToRetriableException() {
    CommitFailedException rebalance = new CommitFailedException("generation id mismatch");
    doThrow(rebalance).when(producer).commitTransaction();

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(7L)))
        .isInstanceOf(RetriableException.class)
        .hasMessageContaining("consumer group re-balance")
        .hasCause(rebalance);

    verify(producer, times(1)).beginTransaction();
    verify(producer, times(1)).commitTransaction();
    verify(producer, times(1)).abortTransaction();
  }

  @Test
  public void invalidProducerEpochExceptionIsTranslatedToRetriableException() {
    InvalidProducerEpochException epochBump = new InvalidProducerEpochException("stale epoch");
    doThrow(epochBump).when(producer).commitTransaction();

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(11L)))
        .isInstanceOf(RetriableException.class)
        .hasMessageContaining("consumer group re-balance")
        .hasCause(epochBump);

    verify(producer, times(1)).beginTransaction();
    verify(producer, times(1)).commitTransaction();
    verify(producer, times(1)).abortTransaction();
  }

  @Test
  public void sendOffsetsToTransactionFailureIsAlsoTranslated() {
    CommitFailedException rebalance = new CommitFailedException("metadata mismatch");
    doThrow(rebalance)
        .when(producer)
        .sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(5L)))
        .isInstanceOf(RetriableException.class)
        .hasMessageContaining("consumer group re-balance")
        .hasCause(rebalance);

    verify(producer, times(1)).beginTransaction();
    verify(producer, times(1)).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
    verify(producer, never()).commitTransaction();
    verify(producer, times(1)).abortTransaction();
  }

  @Test
  public void wrappedCommitFailedExceptionIsAlsoTranslated() {
    CommitFailedException inner = new CommitFailedException("rebalanced");
    KafkaException wrapper = new KafkaException("wrapped", inner);
    doThrow(wrapper).when(producer).commitTransaction();

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(3L)))
        .isInstanceOf(RetriableException.class)
        .hasMessageContaining("consumer group re-balance")
        .hasCause(wrapper);

    verify(producer, times(1)).abortTransaction();
  }

  // ------------------------------------------------------------
  // ProducerFencedException is recoverable by default
  // ------------------------------------------------------------

  @Test
  public void producerFencedExceptionIsRetriableByDefault() {
    // Default config includes ProducerFencedException among retriable exceptions: when the
    // worker is closed and re-created on retry, initTransactions() obtains a fresh epoch.
    ProducerFencedException fenced = new ProducerFencedException("fenced by newer producer");
    doThrow(fenced).when(producer).commitTransaction();

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(1L)))
        .isInstanceOf(RetriableException.class)
        .hasMessageContaining("consumer group re-balance")
        .hasCause(fenced);

    verify(producer, times(1)).abortTransaction();
  }

  @Test
  public void producerFencedIsFatalWhenExcludedFromConfiguredList() {
    // User-supplied list omits ProducerFencedException → exception surfaces as-is.
    when(config.transactionalCommitRetriableExceptionClasses())
        .thenReturn(
            ImmutableList.of(CommitFailedException.class, InvalidProducerEpochException.class));

    ProducerFencedException fenced = new ProducerFencedException("fenced by newer producer");
    doThrow(fenced).when(producer).commitTransaction();

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(1L)))
        .isSameAs(fenced)
        .hasMessageContaining("fenced");

    verify(producer, times(1)).abortTransaction();
  }

  @Test
  public void emptyConfiguredListMakesEverythingFatal() {
    when(config.transactionalCommitRetriableExceptionClasses()).thenReturn(ImmutableList.of());

    CommitFailedException rebalance = new CommitFailedException("rebalanced");
    doThrow(rebalance).when(producer).commitTransaction();

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(1L)))
        .isSameAs(rebalance)
        .hasMessageContaining("rebalanced");
  }

  @Test
  public void customSubclassListMatchesViaIsInstance() {
    // Configuring KafkaException as retriable should match all of its subclasses
    // (e.g. CommitFailedException) via Class.isInstance.
    when(config.transactionalCommitRetriableExceptionClasses())
        .thenReturn(ImmutableList.of(KafkaException.class));

    CommitFailedException rebalance = new CommitFailedException("rebalanced");
    doThrow(rebalance).when(producer).commitTransaction();

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(1L)))
        .isInstanceOf(RetriableException.class)
        .hasMessageContaining("consumer group re-balance")
        .hasCause(rebalance);
  }

  @Test
  public void nonRebalanceKafkaExceptionIsRethrownAsIs() {
    KafkaException broken = new KafkaException("broker connection broken");
    doThrow(broken).when(producer).commitTransaction();

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(1L)))
        .isSameAs(broken)
        .hasMessageContaining("broker connection broken");

    verify(producer, times(1)).abortTransaction();
  }

  @Test
  public void beginTransactionFailureIsRethrownAndDoesNotAbort() {
    IllegalStateException beginFailure = new IllegalStateException("producer not initialized");
    doThrow(beginFailure).when(producer).beginTransaction();

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(1L)))
        .isSameAs(beginFailure)
        .hasMessageContaining("not initialized");

    verify(producer, times(1)).beginTransaction();
    verify(producer, never()).commitTransaction();
    verify(producer, never()).abortTransaction();
  }

  // ------------------------------------------------------------
  // Abort robustness
  // ------------------------------------------------------------

  @Test
  public void abortFailureIsSwallowedAndDoesNotMaskRetriableException() {
    doThrow(new CommitFailedException("rebalanced")).when(producer).commitTransaction();
    doThrow(new IllegalStateException("abort failed")).when(producer).abortTransaction();

    StubChannel channel = newChannel();

    assertThatThrownBy(() -> channel.sendForTest(startCommitEvent(), offsetsForSrc(1L)))
        .isInstanceOf(RetriableException.class)
        .hasMessageContaining("consumer group re-balance")
        .hasCauseInstanceOf(CommitFailedException.class);

    verify(producer, times(1)).abortTransaction();
  }

  // ------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------

  private StubChannel newChannel() {
    return new StubChannel("worker", CONSUMER_GROUP, config, clientFactory, context);
  }

  private Event startCommitEvent() {
    return new Event(CONNECT_GROUP, new StartCommit(java.util.UUID.randomUUID()));
  }

  private Map<TopicPartition, Offset> offsetsForSrc(long offset) {
    return ImmutableMap.of(new TopicPartition("src-topic", 0), new Offset(offset, null));
  }

  /** Concrete subclass exposing the protected {@code send()} method for tests. */
  private static class StubChannel extends Channel {

    StubChannel(
        String name,
        String consumerGroupId,
        IcebergSinkConfig config,
        KafkaClientFactory clientFactory,
        SinkTaskContext context) {
      super(name, consumerGroupId, config, clientFactory, context);
    }

    @Override
    protected boolean receive(Envelope envelope) {
      return false;
    }

    void sendForTest(Event event, Map<TopicPartition, Offset> sourceOffsets) {
      send(ImmutableList.of(event), sourceOffsets);
    }
  }

  @Test
  public void startCommitPayloadTypeIsStable() {
    assertThat(startCommitEvent().payload().type()).isEqualTo(PayloadType.START_COMMIT);
  }
}
