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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class Channel {

  private static final Logger LOG = LoggerFactory.getLogger(Channel.class);

  private final String controlTopic;
  private final String connectGroupId;
  private final Producer<String, byte[]> producer;
  private final Consumer<String, byte[]> consumer;
  private final SinkTaskContext context;
  private final Admin admin;
  private final Map<Integer, Long> controlTopicOffsets = Maps.newHashMap();
  private final String producerId;
  private final List<Class<? extends Throwable>> retriableCommitExceptions;

  Channel(
      String name,
      String consumerGroupId,
      IcebergSinkConfig config,
      KafkaClientFactory clientFactory,
      SinkTaskContext context) {
    this.controlTopic = config.controlTopic();
    this.connectGroupId = config.connectGroupId();
    this.context = context;

    String transactionalId = config.transactionalPrefix() + name + config.transactionalSuffix();
    this.producer = clientFactory.createProducer(transactionalId);
    this.consumer = clientFactory.createConsumer(consumerGroupId);
    this.admin = clientFactory.createAdmin();

    this.producerId = UUID.randomUUID().toString();
    this.retriableCommitExceptions = config.transactionalCommitRetriableExceptionClasses();
  }

  protected void send(Event event) {
    send(ImmutableList.of(event), ImmutableMap.of());
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  protected void send(List<Event> events, Map<TopicPartition, Offset> sourceOffsets) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
    sourceOffsets.forEach((k, v) -> offsetsToCommit.put(k, new OffsetAndMetadata(v.offset())));

    List<ProducerRecord<String, byte[]>> recordList =
        events.stream()
            .map(
                event -> {
                  LOG.info("Sending event of type: {}", event.type().name());
                  byte[] data = AvroUtil.encode(event);
                  // key by producer ID to keep event order
                  return new ProducerRecord<>(controlTopic, producerId, data);
                })
            .toList();

    synchronized (producer) {
      boolean transactionStarted = false;
      try {
        producer.beginTransaction();
        transactionStarted = true;
        // NOTE: we shouldn't call get() on the future in a transactional context,
        // see docs for org.apache.kafka.clients.producer.KafkaProducer
        recordList.forEach(producer::send);
        if (!sourceOffsets.isEmpty()) {
          producer.sendOffsetsToTransaction(
              offsetsToCommit, KafkaUtils.consumerGroupMetadata(context));
        }
        producer.commitTransaction();
      } catch (Exception e) {
        safeAbortTransaction(transactionStarted);
        if (isRecoverableCommitError(e)) {
          // The transactional commit hit a configured-recoverable failure (default: a
          // consumer-group re-balance — CommitFailedException — or a producer-epoch bump —
          // InvalidProducerEpochException / ProducerFencedException). The transaction was
          // aborted, so source offsets did not advance — when Connect re-delivers the same
          // batch (after the worker is closed and re-created with a fresh producer epoch via
          // initTransactions(), and the affected partitions are reassigned) processing resumes
          // from the last committed offsets with no data loss.
          LOG.warn(
              "Transactional offset commit failed with recoverable exception; "
                  + "aborted transaction and signalling Connect to retry",
              e);
          throw new RetriableException(
              "Transactional offset commit failed due to consumer group re-balance", e);
        }
        throw e;
      }
    }
  }

  private void safeAbortTransaction(boolean transactionStarted) {
    if (!transactionStarted) {
      return;
    }
    try {
      producer.abortTransaction();
    } catch (Exception ex) {
      LOG.warn("Error aborting producer transaction", ex);
    }
  }

  /**
   * Returns true when the throwable (or any cause in its chain) matches one of the configured
   * {@code iceberg.kafka.transactional-commit-retriable-exceptions} classes — i.e. a transient
   * commit-time failure that should be translated to {@link RetriableException} so Connect pauses
   * the consumer, lets the worker close + recreate (which obtains a fresh producer epoch via {@code
   * initTransactions()}), and re-delivers the same batch.
   */
  private boolean isRecoverableCommitError(Throwable throwable) {
    if (retriableCommitExceptions.isEmpty()) {
      return false;
    }
    Throwable cause = throwable;
    while (cause != null) {
      for (Class<? extends Throwable> klass : retriableCommitExceptions) {
        if (klass.isInstance(cause)) {
          return true;
        }
      }
      cause = cause.getCause();
    }
    return false;
  }

  protected abstract boolean receive(Envelope envelope);

  protected void consumeAvailable(Duration pollDuration) {
    ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
    while (!records.isEmpty()) {
      records.forEach(
          record -> {
            // the consumer stores the offsets that corresponds to the next record to consume,
            // so increment the record offset by one
            controlTopicOffsets.put(record.partition(), record.offset() + 1);

            Event event = AvroUtil.decode(record.value());

            if (event.groupId().equals(connectGroupId)) {
              LOG.debug("Received event of type: {}", event.type().name());
              if (receive(new Envelope(event, record.partition(), record.offset()))) {
                LOG.info("Handled event of type: {}", event.type().name());
              }
            }
          });
      records = consumer.poll(pollDuration);
    }
  }

  protected Map<Integer, Long> controlTopicOffsets() {
    return controlTopicOffsets;
  }

  protected void commitConsumerOffsets() {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = Maps.newHashMap();
    controlTopicOffsets()
        .forEach(
            (k, v) ->
                offsetsToCommit.put(new TopicPartition(controlTopic, k), new OffsetAndMetadata(v)));
    consumer.commitSync(offsetsToCommit);
  }

  void start() {
    consumer.subscribe(ImmutableList.of(controlTopic));

    // initial poll with longer duration so the consumer will initialize...
    consumeAvailable(Duration.ofSeconds(1));
  }

  void stop() {
    LOG.info("Channel stopping");
    producer.close();
    consumer.close();
    admin.close();
  }
}
