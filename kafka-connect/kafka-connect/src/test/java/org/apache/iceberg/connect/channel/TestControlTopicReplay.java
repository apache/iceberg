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
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

class TestControlTopicReplay extends ChannelTestBase {

  @Test
  void handlesReplayedControlRecordsOnce() {
    TrackingChannel channel =
        new TrackingChannel(config, clientFactory, mock(SinkTaskContext.class));
    channel.start();
    initConsumer();

    addStartCommit(0L);
    addStartCommit(1L);
    channel.process();

    assertThat(channel.receivedCount()).isEqualTo(2);
    assertThat(channel.nextOffset(0)).isEqualTo(2L);

    consumer.seek(new TopicPartition(CTL_TOPIC_NAME, 0), 0L);
    addStartCommit(0L);
    addStartCommit(1L);
    channel.process();

    assertThat(channel.receivedCount()).isEqualTo(2);
    assertThat(channel.nextOffset(0)).isEqualTo(2L);
  }

  @Test
  void doesNotCommitReplayedDataFilesTwice() throws IOException {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    MemberAssignment assignment =
        new MemberAssignment(
            ImmutableSet.of(
                new TopicPartition(SRC_TOPIC_NAME, 0),
                new TopicPartition(SRC_TOPIC_NAME, 1),
                new TopicPartition(SRC_TOPIC_NAME, 2)));
    MemberDescription member =
        new MemberDescription(null, Optional.empty(), null, null, assignment);
    Coordinator coordinator =
        new Coordinator(
            catalog, config, ImmutableList.of(member), clientFactory, mock(SinkTaskContext.class));
    coordinator.start();
    initConsumer();
    coordinator.process();

    UUID commitId =
        ((StartCommit) AvroUtil.decode(producer.history().get(0).value()).payload()).commitId();
    DataFile file1 = dataFile("path/to/file-1.parquet");
    DataFile file2 = dataFile("path/to/file-2.parquet");

    addDataWritten(0L, commitId, file1);
    addDataComplete(1L, commitId, 0);
    addDataWritten(2L, commitId, file2);
    addDataComplete(3L, commitId, 1);
    coordinator.process();

    consumer.seek(new TopicPartition(CTL_TOPIC_NAME, 0), 0L);
    addDataWritten(0L, commitId, file1);
    addDataComplete(1L, commitId, 0);
    addDataWritten(2L, commitId, file2);
    addDataComplete(3L, commitId, 1);
    coordinator.process();

    addDataComplete(4L, commitId, 2);
    coordinator.process();

    // Start another cycle and force a partial commit. Before the replay guard, the replayed tail
    // remains buffered and this cycle commits file2 a second time.
    when(config.commitTimeoutMs()).thenReturn(-1);
    coordinator.process();

    table.refresh();
    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    assertThat(snapshots).hasSize(1);
    assertThat(snapshots.get(0).summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":5}");

    List<String> locations = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> locations.add(task.file().location()));
    }

    assertThat(locations)
        .containsExactlyInAnyOrder(file1.location().toString(), file2.location().toString());
  }

  private void addStartCommit(long offset) {
    Event event = new Event(config.connectGroupId(), new StartCommit(UUID.randomUUID()));
    addControlRecord(offset, event);
  }

  private void addDataWritten(long offset, UUID commitId, DataFile file) {
    Event event =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
                ImmutableList.of(file),
                ImmutableList.of()));
    addControlRecord(offset, event);
  }

  private void addDataComplete(long offset, UUID commitId, int sourcePartition) {
    Event event =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId,
                ImmutableList.of(
                    new TopicPartitionOffset(
                        SRC_TOPIC_NAME, sourcePartition, 1L, EventTestUtil.now()))));
    addControlRecord(offset, event);
  }

  private void addControlRecord(long offset, Event event) {
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset, "key", AvroUtil.encode(event)));
  }

  private DataFile dataFile(String location) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(location)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L)
        .withRecordCount(1L)
        .build();
  }

  private static class TrackingChannel extends Channel {
    private int receivedCount = 0;

    private TrackingChannel(
        IcebergSinkConfig config, KafkaClientFactory clientFactory, SinkTaskContext context) {
      super("tracking", "tracking-group", config, clientFactory, context);
    }

    @Override
    protected boolean receive(Envelope envelope) {
      receivedCount += 1;
      return true;
    }

    private void process() {
      consumeAvailable(Duration.ZERO);
    }

    private int receivedCount() {
      return receivedCount;
    }

    private Long nextOffset(int partition) {
      return controlTopicOffsets().get(partition);
    }
  }
}
