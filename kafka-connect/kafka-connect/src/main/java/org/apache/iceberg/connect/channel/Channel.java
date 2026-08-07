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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.Offset;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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
  // only touched from the thread that owns the consumer, i.e. wherever consumeAvailable() runs
  private final Map<Integer, Long> controlTopicOffsets = Maps.newHashMap();
  private final String producerId;
  private final AtomicLong consumedRecordCount = new AtomicLong();
  private final AtomicLong skippedRecordCount = new AtomicLong();

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
            .collect(Collectors.toList());

    synchronized (producer) {
      producer.beginTransaction();
      try {
        // NOTE: we shouldn't call get() on the future in a transactional context,
        // see docs for org.apache.kafka.clients.producer.KafkaProducer
        recordList.forEach(producer::send);
        if (!sourceOffsets.isEmpty()) {
          producer.sendOffsetsToTransaction(
              offsetsToCommit, KafkaUtils.consumerGroupMetadata(context));
        }
        producer.commitTransaction();
      } catch (Exception e) {
        try {
          producer.abortTransaction();
        } catch (Exception ex) {
          LOG.warn("Error aborting producer transaction", ex);
        }
        throw e;
      }
    }
  }

  protected abstract boolean receive(Envelope envelope);

  /**
   * Drains the control topic. Must always be called from the thread that owns the consumer, since
   * {@link Consumer} is not thread safe and this also mutates {@link #controlTopicOffsets}.
   */
  protected void consumeAvailable(Duration pollDuration) {
    ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
    while (!records.isEmpty()) {
      records.forEach(this::handleRecord);
      records = consumer.poll(pollDuration);
    }
  }

  private void handleRecord(ConsumerRecord<String, byte[]> record) {
    // the consumer stores the offsets that corresponds to the next record to consume,
    // so increment the record offset by one
    controlTopicOffsets.put(record.partition(), record.offset() + 1);
    consumedRecordCount.incrementAndGet();

    Event event;
    try {
      event = AvroUtil.decode(record.value());
    } catch (Exception e) {
      // A single malformed record (truncated, written by an incompatible version, produced by
      // something other than this connector) must not take down the channel. The offset above has
      // already been advanced, so skipping lets the channel keep making progress instead of
      // failing again on every restart. Missing a START_COMMIT this way only defers this task's
      // data to the next commit round, it cannot drop data.
      skippedRecordCount.incrementAndGet();
      LOG.error(
          "Skipping undecodable control topic record at {}-{}, {} record(s) skipped so far",
          record.partition(),
          record.offset(),
          skippedRecordCount.get(),
          e);
      return;
    }

    if (connectGroupId.equals(event.groupId())) {
      LOG.debug("Received event of type: {}", event.type().name());
      if (receive(new Envelope(event, record.partition(), record.offset()))) {
        LOG.info("Handled event of type: {}", event.type().name());
      }
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

  /**
   * Subscribes to the control topic and performs the initial poll that establishes the consumer
   * position. Must be called from the thread that owns the consumer, before any other consumer
   * access.
   */
  protected void initializeConsumer() {
    consumer.subscribe(ImmutableList.of(controlTopic));

    // initial poll with longer duration so the consumer will initialize...
    consumeAvailable(Duration.ofSeconds(1));
  }

  void start() {
    initializeConsumer();
  }

  void stop() {
    LOG.info("Channel stopping");
    closeQuietly("producer", producer);
    closeConsumer();
    closeQuietly("admin", admin);
  }

  /**
   * Closes the consumer. {@link Consumer#close()} is idempotent, so a subclass that hands the
   * consumer to another thread can close it there and still let {@link #stop()} run unchanged.
   */
  protected void closeConsumer() {
    closeQuietly("consumer", consumer);
  }

  /**
   * Closes a Kafka client, logging instead of propagating failures. One client failing to close
   * must not leak the others: the transactional producer in particular can block the next task
   * start if it is left open.
   */
  private void closeQuietly(String name, AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception e) {
      LOG.warn("Error closing {} on channel, ignoring...", name, e);
    }
  }

  /**
   * Wakeup the consumer. This is the only thread-safe method on KafkaConsumer and can be called
   * from any thread to interrupt a blocking poll().
   */
  protected void wakeupConsumer() {
    consumer.wakeup();
  }

  /** Total number of control topic records read by this channel, including skipped ones. */
  @VisibleForTesting
  long consumedRecordCount() {
    return consumedRecordCount.get();
  }

  /** Number of control topic records that could not be decoded and were skipped. */
  @VisibleForTesting
  long skippedRecordCount() {
    return skippedRecordCount.get();
  }
}
