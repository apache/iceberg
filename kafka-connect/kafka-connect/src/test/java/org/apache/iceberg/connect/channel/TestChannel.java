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
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

public class TestChannel extends ChannelTestBase {

  private static final TopicPartition CTL_TOPIC_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0);

  // not a valid Avro event, so a record carrying it fails if anything tries to decode it
  private static final byte[] UNDECODABLE = new byte[] {(byte) 0xff, 0x00, 0x7f};

  @Test
  public void controlTopicOffsetsTrackTheHighestPositionConsumed() {
    TestChannelImpl channel = startChannel();

    consume(channel, 0, 1, 2, 3, 4);
    assertThat(channel.controlTopicOffsets()).isEqualTo(ImmutableMap.of(0, 5L));

    // a re-read of the control topic, as happens when a rebalance resumes the consumer from the
    // last committed offsets, delivers records that were already handled. Only part of the replay
    // arrives in this batch, so the last record seen is behind the position reached above.
    consumer.seek(CTL_TOPIC_PARTITION, 1L);
    consume(channel, 1, 2);

    assertThat(channel.controlTopicOffsets()).isEqualTo(ImmutableMap.of(0, 5L));
    assertThat(channel.received()).hasSize(5);
  }

  @Test
  public void committedControlTopicOffsetsDoNotRegressOnReplay() {
    TestChannelImpl channel = startChannel();

    consume(channel, 0, 1, 2, 3, 4);
    consumer.seek(CTL_TOPIC_PARTITION, 1L);
    consume(channel, 1, 2);

    channel.commitConsumerOffsets();

    // the offset committed for the group is what a restarted channel resumes from, and what the
    // coordinator stamps on the snapshot, so a regression here is durable
    assertThat(committedGroupOffsets(consumer.groupMetadata().groupId()))
        .containsEntry(CTL_TOPIC_PARTITION, new OffsetAndMetadata(5L));
  }

  @Test
  public void controlTopicOffsetsAreTrackedPerPartition() {
    TestChannelImpl channel = startChannel();
    TopicPartition other = new TopicPartition(CTL_TOPIC_NAME, 1);
    consumer.rebalance(Lists.newArrayList(CTL_TOPIC_PARTITION, other));
    consumer.updateBeginningOffsets(ImmutableMap.of(CTL_TOPIC_PARTITION, 0L, other, 0L));

    addRecord(0, 0);
    addRecord(0, 1);
    addRecord(1, 0);
    channel.consumeAvailable(Duration.ZERO);

    assertThat(channel.controlTopicOffsets()).isEqualTo(ImmutableMap.of(0, 2L, 1, 1L));
  }

  @Test
  void sentEventsCarryTheirGroupAndPayloadTypeInHeaders() {
    TestChannelImpl channel = startChannel();
    Event event = new Event(CONNECT_CONSUMER_GROUP_ID, new StartCommit(UUID.randomUUID()));

    channel.send(event);

    assertThat(producer.history()).hasSize(1);
    ProducerRecord<String, byte[]> sent = producer.history().get(0);
    assertThat(header(sent.headers().lastHeader(Channel.GROUP_ID_HEADER)))
        .isEqualTo(CONNECT_CONSUMER_GROUP_ID);
    assertThat(header(sent.headers().lastHeader(Channel.PAYLOAD_TYPE_HEADER)))
        .isEqualTo("START_COMMIT");
    assertThat(AvroUtil.decode(sent.value()).type()).isEqualTo(PayloadType.START_COMMIT);
  }

  @Test
  void eventsForAnotherConnectorAreSkippedWithoutDecoding() {
    TestChannelImpl channel = startChannel();

    addRecord(0, 0, UNDECODABLE, headers("other-connector", "DATA_WRITTEN"));
    addRecord(0, 1);
    channel.consumeAvailable(Duration.ZERO);

    assertThat(channel.received()).hasSize(1);
    assertThat(channel.received().get(0).offset()).isEqualTo(1L);
    assertThat(channel.controlTopicOffsets()).isEqualTo(ImmutableMap.of(0, 2L));
  }

  @Test
  void eventTypesTheChannelDoesNotHandleAreSkippedWithoutDecoding() {
    TestChannelImpl channel = startChannel(EnumSet.of(PayloadType.START_COMMIT));

    addRecord(0, 0, UNDECODABLE, headers(CONNECT_CONSUMER_GROUP_ID, "DATA_WRITTEN"));
    addRecord(0, 1, UNDECODABLE, headers(CONNECT_CONSUMER_GROUP_ID, "DATA_COMPLETE"));
    addRecord(0, 2, startCommit(), headers(CONNECT_CONSUMER_GROUP_ID, "START_COMMIT"));
    channel.consumeAvailable(Duration.ZERO);

    assertThat(channel.received()).hasSize(1);
    assertThat(channel.received().get(0).offset()).isEqualTo(2L);
    assertThat(channel.received().get(0).event().type()).isEqualTo(PayloadType.START_COMMIT);
    // skipped records still advance the position the channel commits
    assertThat(channel.controlTopicOffsets()).isEqualTo(ImmutableMap.of(0, 3L));
  }

  @Test
  void eventsWithoutHeadersAreStillDecoded() {
    // records written before the headers existed, or by a producer that omits them
    TestChannelImpl channel = startChannel(EnumSet.noneOf(PayloadType.class));

    addRecord(0, 0);
    channel.consumeAvailable(Duration.ZERO);

    assertThat(channel.received()).hasSize(1);
  }

  @Test
  void anUnrecognizedPayloadTypeHeaderFallsBackToDecoding() {
    TestChannelImpl channel = startChannel(EnumSet.noneOf(PayloadType.class));

    addRecord(0, 0, startCommit(), headers(CONNECT_CONSUMER_GROUP_ID, "NOT_A_PAYLOAD_TYPE"));
    channel.consumeAvailable(Duration.ZERO);

    assertThat(channel.received()).hasSize(1);
    assertThat(channel.received().get(0).event().type()).isEqualTo(PayloadType.START_COMMIT);
  }

  @Test
  void aSentEventIsSkippedByAChannelThatDoesNotHandleItsType() {
    TestChannelImpl sender = startChannel();
    sender.send(new Event(CONNECT_CONSUMER_GROUP_ID, new StartCommit(UUID.randomUUID())));
    ProducerRecord<String, byte[]> sent = producer.history().get(0);

    TestChannelImpl reader =
        new TestChannelImpl(
            config,
            clientFactory,
            mock(SinkTaskContext.class),
            EnumSet.of(PayloadType.DATA_WRITTEN, PayloadType.DATA_COMPLETE));
    RecordHeaders headers = new RecordHeaders();
    sent.headers().forEach(headers::add);
    addRecord(0, 0, UNDECODABLE, headers);
    reader.consumeAvailable(Duration.ZERO);

    assertThat(reader.received()).isEmpty();
    assertThat(reader.controlTopicOffsets()).isEqualTo(ImmutableMap.of(0, 1L));
  }

  private static String header(Header header) {
    assertThat(header).isNotNull();
    return new String(header.value(), StandardCharsets.UTF_8);
  }

  private static RecordHeaders headers(String groupId, String payloadType) {
    RecordHeaders headers = new RecordHeaders();
    headers.add(
        new RecordHeader(Channel.GROUP_ID_HEADER, groupId.getBytes(StandardCharsets.UTF_8)));
    headers.add(
        new RecordHeader(
            Channel.PAYLOAD_TYPE_HEADER, payloadType.getBytes(StandardCharsets.UTF_8)));
    return headers;
  }

  private static byte[] startCommit() {
    return AvroUtil.encode(
        new Event(CONNECT_CONSUMER_GROUP_ID, new StartCommit(UUID.randomUUID())));
  }

  private void addRecord(int partition, long offset, byte[] value, RecordHeaders headers) {
    consumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            partition,
            offset,
            ConsumerRecord.NO_TIMESTAMP,
            TimestampType.NO_TIMESTAMP_TYPE,
            ConsumerRecord.NULL_SIZE,
            value.length,
            "key",
            value,
            headers,
            Optional.empty()));
  }

  private TestChannelImpl startChannel() {
    return startChannel(EnumSet.allOf(PayloadType.class));
  }

  private TestChannelImpl startChannel(Set<PayloadType> handled) {
    TestChannelImpl channel =
        new TestChannelImpl(config, clientFactory, mock(SinkTaskContext.class), handled);
    channel.start();

    // init consumer after subscribe()
    initConsumer();
    return channel;
  }

  private void consume(TestChannelImpl channel, int... offsets) {
    for (int offset : offsets) {
      addRecord(0, offset);
    }
    channel.consumeAvailable(Duration.ZERO);
  }

  private void addRecord(int partition, long offset) {
    Event event = new Event(CONNECT_CONSUMER_GROUP_ID, new StartCommit(UUID.randomUUID()));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, partition, offset, "key", AvroUtil.encode(event)));
  }

  private static class TestChannelImpl extends Channel {
    private final List<Envelope> received = Lists.newArrayList();
    private final Set<PayloadType> handled;

    TestChannelImpl(
        IcebergSinkConfig config,
        KafkaClientFactory clientFactory,
        SinkTaskContext context,
        Set<PayloadType> handled) {
      super("test", CONNECT_CONSUMER_GROUP_ID + "-test", config, clientFactory, context);
      this.handled = handled;
    }

    @Override
    protected boolean handles(PayloadType type) {
      return handled.contains(type);
    }

    @Override
    protected boolean receive(Envelope envelope) {
      received.add(envelope);
      return true;
    }

    List<Envelope> received() {
      return received;
    }
  }
}
