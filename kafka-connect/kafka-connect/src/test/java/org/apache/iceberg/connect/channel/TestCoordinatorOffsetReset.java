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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.connect.data.SinkWriter;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

class TestCoordinatorOffsetReset extends ChannelTestBase {

  private static final TopicPartition CTL_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0);

  private Coordinator newCoordinator() {
    MemberDescription member =
        new MemberDescription(
            null,
            Optional.empty(),
            null,
            null,
            new MemberAssignment(ImmutableSet.of(new TopicPartition(SRC_TOPIC_NAME, 0))));
    return new Coordinator(
        catalog, config, ImmutableList.of(member), clientFactory, mock(SinkTaskContext.class));
  }

  private UUID startCommit(Coordinator coordinator) {
    coordinator.process();
    byte[] bytes = producer.history().get(producer.history().size() - 1).value();
    return ((StartCommit) AvroUtil.decode(bytes).payload()).commitId();
  }

  // The -coord group only gets a committed offset inside doCommit, so a coordinator that starts
  // before the first successful commit has none. It must read from the beginning.
  @Test
  void coordinatorConsumerReadsFromEarliest() {
    newCoordinator();
    verify(clientFactory).createConsumer(config.connectGroupId() + "-coord", "earliest");
  }

  // The worker's group is transient and never committed to, so it must not replay history.
  @Test
  void workerConsumerReadsFromLatest() {
    new Worker(config, clientFactory, mock(SinkWriter.class), mock(SinkTaskContext.class));
    verify(clientFactory).createConsumer(anyString(), eq("latest"));
  }

  // A coordinator replaced before its group's first successful commit must still deliver files
  // announced to its predecessor. Without an earliest reset it resumes at the log end and the
  // worker's source offsets are already committed, so those records never come back.
  @Test
  void replacementCoordinatorRecoversFilesBufferedByItsPredecessor() {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    // Build each consumer from the reset strategy production actually asked for, so the test
    // exercises the real wiring instead of hard-coding a strategy.
    List<MockConsumer<String, byte[]>> created = Lists.newArrayList();
    when(clientFactory.createConsumer(any(), any()))
        .thenAnswer(
            invocation -> {
              MockConsumer<String, byte[]> created2 =
                  new MockConsumer<>(
                      "earliest".equals(invocation.getArgument(1))
                          ? OffsetResetStrategy.EARLIEST
                          : OffsetResetStrategy.LATEST);
              created.add(created2);
              return created2;
            });

    Coordinator first = newCoordinator();
    MockConsumer<String, byte[]> firstConsumer = created.get(0);
    first.start();
    firstConsumer.rebalance(ImmutableList.of(CTL_PARTITION));
    firstConsumer.updateBeginningOffsets(ImmutableMap.of(CTL_PARTITION, 0L));
    firstConsumer.updateEndOffsets(ImmutableMap.of(CTL_PARTITION, 0L));

    UUID commitId = startCommit(first);
    DataFile file = EventTestUtil.createDataFile();
    Event announcement = dataWritten(commitId, file);
    firstConsumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0L, "key", AvroUtil.encode(announcement)));
    first.process(); // buffers the file; no DataComplete yet, so nothing is committed

    // The leader partition moves. The first coordinator and its buffer are discarded, and the
    // -coord group still has no committed offset because no commit ever succeeded. The record
    // announcing the file is still on the control topic at offset 0.
    Coordinator second = newCoordinator();
    MockConsumer<String, byte[]> secondConsumer = created.get(1);
    second.start();
    secondConsumer.rebalance(ImmutableList.of(CTL_PARTITION));
    secondConsumer.updateBeginningOffsets(ImmutableMap.of(CTL_PARTITION, 0L));
    secondConsumer.updateEndOffsets(ImmutableMap.of(CTL_PARTITION, 1L));

    UUID nextCommitId = startCommit(second);
    secondConsumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0L, "key", AvroUtil.encode(announcement)));
    secondConsumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME, 0, 1L, "key", AvroUtil.encode(dataComplete(nextCommitId))));
    second.process();

    table.refresh();
    assertThat(table.currentSnapshot())
        .as("file announced to the replaced coordinator must still reach the table")
        .isNotNull();
    assertThat(table.currentSnapshot().addedDataFiles(table.io()))
        .extracting(DataFile::location)
        .containsExactly(file.location());
  }

  @Test
  void replayedResponsesAlreadyCoveredBySnapshotOffsetsAreNotCommittedTwice() throws IOException {
    assertReplayRecovery(false);
  }

  @Test
  void recoversPendingFilesWhileSkippingCommittedFiles() throws IOException {
    assertReplayRecovery(true);
  }

  private void assertReplayRecovery(boolean hasPendingFiles) throws IOException {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    List<MockConsumer<String, byte[]>> created = Lists.newArrayList();
    when(clientFactory.createConsumer(any(), any()))
        .thenAnswer(
            invocation -> {
              MockConsumer<String, byte[]> consumer =
                  new MockConsumer<>(
                      "earliest".equals(invocation.getArgument(1))
                          ? OffsetResetStrategy.EARLIEST
                          : OffsetResetStrategy.LATEST);
              created.add(consumer);
              return consumer;
            });

    Coordinator first = newCoordinator();
    MockConsumer<String, byte[]> firstConsumer = created.get(0);
    first.start();
    firstConsumer.rebalance(ImmutableList.of(CTL_PARTITION));
    firstConsumer.updateBeginningOffsets(ImmutableMap.of(CTL_PARTITION, 0L));
    firstConsumer.updateEndOffsets(ImmutableMap.of(CTL_PARTITION, 0L));

    UUID commitId = startCommit(first);
    DataFile file = EventTestUtil.createDataFile();
    Event announcement = dataWritten(commitId, file);
    Event completion = dataComplete(commitId);
    firstConsumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0L, "key", AvroUtil.encode(announcement)));
    firstConsumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1L, "key", AvroUtil.encode(completion)));
    first.process();

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    Snapshot committed = table.currentSnapshot();
    List<String> expectedFiles = Lists.newArrayList(file.location());
    long nextOffset = 2L;
    DataFile pendingFile =
        DataFiles.builder(table.spec()).copy(file).withPath("pending.parquet").build();
    Event pendingAnnouncement = dataWritten(startCommit(first), pendingFile);

    if (hasPendingFiles) {
      firstConsumer.addRecord(
          new ConsumerRecord<>(
              CTL_TOPIC_NAME, 0, nextOffset, "key", AvroUtil.encode(pendingAnnouncement)));
      first.process();
      nextOffset++;
      expectedFiles.add(pendingFile.location());
    }

    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(committed.snapshotId());
    assertThat(firstConsumer.committed(ImmutableSet.of(CTL_PARTITION)).get(CTL_PARTITION).offset())
        .isEqualTo(2L);
    first.terminate();

    // a replacement reads the same history from the beginning
    Coordinator second = newCoordinator();
    MockConsumer<String, byte[]> secondConsumer = created.get(1);
    second.start();
    secondConsumer.rebalance(ImmutableList.of(CTL_PARTITION));
    secondConsumer.updateBeginningOffsets(ImmutableMap.of(CTL_PARTITION, 0L));
    secondConsumer.updateEndOffsets(ImmutableMap.of(CTL_PARTITION, nextOffset));

    UUID nextCommitId = startCommit(second);
    assertThat(secondConsumer.position(CTL_PARTITION)).isZero();
    assertThat(secondConsumer.committed(ImmutableSet.of(CTL_PARTITION)).get(CTL_PARTITION))
        .isNull();
    secondConsumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0L, "key", AvroUtil.encode(announcement)));
    secondConsumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1L, "key", AvroUtil.encode(completion)));
    if (hasPendingFiles) {
      secondConsumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2L, "key", AvroUtil.encode(pendingAnnouncement)));
    }

    secondConsumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME, 0, nextOffset, "key", AvroUtil.encode(dataComplete(nextCommitId))));
    second.process();

    table.refresh();
    try (CloseableIterable<FileScanTask> files = table.newScan().planFiles()) {
      assertThat(files)
          .extracting(task -> task.file().location())
          .containsExactlyInAnyOrderElementsOf(expectedFiles);
    }

    assertThat(table.snapshots())
        .as("a response already covered by the snapshot offsets must not be appended again")
        .hasSize(hasPendingFiles ? 2 : 1);
    assertThat(secondConsumer.committed(ImmutableSet.of(CTL_PARTITION)).get(CTL_PARTITION).offset())
        .isEqualTo(nextOffset + 1);
    if (hasPendingFiles) {
      assertThat(table.currentSnapshot().addedDataFiles(table.io()))
          .extracting(DataFile::location)
          .containsExactly(pendingFile.location());
    } else {
      assertThat(table.currentSnapshot().snapshotId()).isEqualTo(committed.snapshotId());
    }

    long snapshotIdAfterRecovery = table.currentSnapshot().snapshotId();
    UUID replayCommitId = startCommit(second);
    secondConsumer.seek(CTL_PARTITION, 0L);
    secondConsumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0L, "key", AvroUtil.encode(announcement)));
    secondConsumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1L, "key", AvroUtil.encode(completion)));
    if (hasPendingFiles) {
      secondConsumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2L, "key", AvroUtil.encode(pendingAnnouncement)));
    }

    secondConsumer.addRecord(
        new ConsumerRecord<>(
            CTL_TOPIC_NAME,
            0,
            nextOffset + 1,
            "key",
            AvroUtil.encode(dataComplete(replayCommitId))));
    second.process();

    table.refresh();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotIdAfterRecovery);
    assertThat(table.snapshots()).hasSize(hasPendingFiles ? 2 : 1);
    assertThat(secondConsumer.committed(ImmutableSet.of(CTL_PARTITION)).get(CTL_PARTITION).offset())
        .isEqualTo(nextOffset + 2);
    second.terminate();
  }

  private Event dataWritten(UUID commitId, DataFile file) {
    return new Event(
        config.connectGroupId(),
        new DataWritten(
            StructType.of(),
            commitId,
            TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
            ImmutableList.of(file),
            ImmutableList.of()));
  }

  private Event dataComplete(UUID commitId) {
    return new Event(
        config.connectGroupId(),
        new DataComplete(
            commitId, ImmutableList.of(new TopicPartitionOffset(SRC_TOPIC_NAME, 0, 1L, null))));
  }
}
