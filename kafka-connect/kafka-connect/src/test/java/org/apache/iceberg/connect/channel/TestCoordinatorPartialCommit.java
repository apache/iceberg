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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

/**
 * Reproduces the bug where stale DataWritten events from a partial commit (timeout) are merged into
 * the next commit cycle's RowDelta, causing equality deletes to not apply correctly due to
 * identical sequence numbers.
 *
 * <p>Scenario:
 *
 * <ol>
 *   <li>Commit cycle A: Worker 0 is still processing, Coordinator times out and performs a partial
 *       commit (no DataWritten from Worker 0)
 *   <li>Worker 0 sends DataWritten(A) + DataComplete(A) to control topic
 *   <li>Commit cycle B starts, Coordinator consumes Worker 0's stale DataWritten(A)
 *   <li>Without the fix: stale and current DataWritten merge into one RowDelta (same sequence
 *       number)
 *   <li>With the fix: stale DataWritten(A) commits first (seq=1), current DataWritten(B) commits
 *       second (seq=2), so equality deletes from cycle B correctly apply to cycle A data
 * </ol>
 */
public class TestCoordinatorPartialCommit extends ChannelTestBase {

  @Test
  public void testStaleDataWrittenFromPartialCommitSeparatedIntoDistinctRowDeltas() {
    when(config.commitIntervalMs()).thenReturn(0);
    // Timeout = 0 to trigger immediate partial commit
    when(config.commitTimeoutMs()).thenReturn(0);

    // 1 worker with 1 partition so totalPartitionCount = 1
    List<MemberDescription> members =
        ImmutableList.of(
            new MemberDescription(
                "member0",
                "client0",
                "host0",
                new MemberAssignment(
                    ImmutableSet.of(new TopicPartition("topic", 1)))));

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator = new Coordinator(catalog, config, members, clientFactory, context);
    coordinator.start();
    initConsumer();

    // === Commit cycle A: immediate timeout -> partial commit (no data) ===
    coordinator.process();

    Event startCommitA = AvroUtil.decode(producer.history().get(0).value());
    assertThat(startCommitA.type()).isEqualTo(PayloadType.START_COMMIT);
    UUID commitIdA = ((StartCommit) startCommitA.payload()).commitId();

    // Verify: no snapshots after partial commit (no DataWritten was available)
    table.refresh();
    assertThat(table.snapshots()).isEmpty();

    // === Commit cycle B: normal commit via isCommitReady ===
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    coordinator.process();

    // Find StartCommit(B)
    UUID commitIdB = null;
    for (int i = producer.history().size() - 1; i >= 0; i--) {
      Event evt = AvroUtil.decode(producer.history().get(i).value());
      if (evt.type() == PayloadType.START_COMMIT) {
        UUID id = ((StartCommit) evt.payload()).commitId();
        if (!id.equals(commitIdA)) {
          commitIdB = id;
          break;
        }
      }
    }
    assertThat(commitIdB).isNotNull();
    assertThat(commitIdB).isNotEqualTo(commitIdA);

    // Worker 0's late DataWritten(A) — was still processing during cycle A's timeout
    DataFile dataFileA = EventTestUtil.createDataFile("path/to/data-a.parquet");
    DeleteFile deleteFileA = EventTestUtil.createDeleteFile("path/to/delete-a.parquet");

    TableReference tableRef =
        TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null);
    Event staleDataWrittenA =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitIdA, // stale: cycle A's commitId
                tableRef,
                ImmutableList.of(dataFileA),
                ImmutableList.of(deleteFileA)));

    OffsetDateTime tsA =
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
    Event staleDataCompleteA =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitIdA,
                ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, tsA))));

    // Current cycle B's DataWritten and DataComplete
    DataFile dataFileB = EventTestUtil.createDataFile("path/to/data-b.parquet");
    DeleteFile deleteFileB = EventTestUtil.createDeleteFile("path/to/delete-b.parquet");

    Event dataWrittenB =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitIdB, // current: cycle B's commitId
                tableRef,
                ImmutableList.of(dataFileB),
                ImmutableList.of(deleteFileB)));

    OffsetDateTime tsB =
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(2000L), ZoneOffset.UTC);
    Event dataCompleteB =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitIdB,
                ImmutableList.of(new TopicPartitionOffset("topic", 1, 2L, tsB))));

    // Control topic order: A's events arrive before B's (single partition, producer ordering)
    int offset = 1;
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset++, "key", AvroUtil.encode(staleDataWrittenA)));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset++, "key", AvroUtil.encode(staleDataCompleteA)));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset++, "key", AvroUtil.encode(dataWrittenB)));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset++, "key", AvroUtil.encode(dataCompleteB)));

    // Process: DataComplete(B) triggers isCommitReady -> commit
    when(config.commitIntervalMs()).thenReturn(Integer.MAX_VALUE);
    coordinator.process();

    // === Verification: two separate snapshots (stale A first, current B second) ===
    table.refresh();
    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    assertThat(snapshots).hasSize(2);

    // Snapshot 1: cycle A's files (stale, committed first with lower sequence number)
    Snapshot snapshotA = snapshots.get(0);
    assertThat(snapshotA.addedDataFiles(table.io())).hasSize(1);
    assertThat(snapshotA.addedDeleteFiles(table.io())).hasSize(1);

    // Snapshot 2: cycle B's files (current, committed second with higher sequence number)
    Snapshot snapshotB = snapshots.get(1);
    assertThat(snapshotB.addedDataFiles(table.io())).hasSize(1);
    assertThat(snapshotB.addedDeleteFiles(table.io())).hasSize(1);

    // Key assertion: sequence number ordering ensures equality deletes work correctly
    // Cycle B's equality delete (seq=2) applies to cycle A's data (seq=1) because 1 < 2
    assertThat(snapshotA.sequenceNumber()).isLessThan(snapshotB.sequenceNumber());

    // Only the last batch (cycle B) should have the commit ID in snapshot properties
    assertThat(snapshotB.summary()).containsEntry(COMMIT_ID_SNAPSHOT_PROP, commitIdB.toString());
  }
}