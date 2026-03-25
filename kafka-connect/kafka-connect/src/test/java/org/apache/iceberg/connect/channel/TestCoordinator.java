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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.CommitComplete;
import org.apache.iceberg.connect.events.CommitToTable;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestCoordinator extends ChannelTestBase {

  @Test
  public void testCommitAppend() {
    assertThat(table.snapshots()).isEmpty();

    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId =
        coordinatorTest(ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), ts);
    table.refresh();

    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    assertThat(snapshots).hasSize(1);

    Snapshot snapshot = snapshots.get(0);
    assertThat(snapshot.operation()).isEqualTo(DataOperations.APPEND);
    SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
    assertThat(changes.addedDataFiles()).hasSize(1);
    assertThat(changes.addedDeleteFiles()).isEmpty();

    assertThat(snapshot.summary())
        .containsEntry(COMMIT_ID_SNAPSHOT_PROP, commitId.toString())
        .containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}")
        .containsEntry(VALID_THROUGH_TS_SNAPSHOT_PROP, ts.toString());
  }

  @Test
  public void testCommitDelta() {
    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId =
        coordinatorTest(
            ImmutableList.of(EventTestUtil.createDataFile()),
            ImmutableList.of(EventTestUtil.createDeleteFile()),
            ts);

    assertThat(producer.history()).hasSize(3);
    assertCommitTable(1, commitId, ts);
    assertCommitComplete(2, commitId, ts);

    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    assertThat(snapshots).hasSize(1);

    Snapshot snapshot = snapshots.get(0);
    assertThat(snapshot.operation()).isEqualTo(DataOperations.OVERWRITE);
    SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snapshot).build();
    assertThat(changes.addedDataFiles()).hasSize(1);
    assertThat(changes.addedDeleteFiles()).hasSize(1);

    assertThat(snapshot.summary())
        .containsEntry(COMMIT_ID_SNAPSHOT_PROP, commitId.toString())
        .containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}")
        .containsEntry(VALID_THROUGH_TS_SNAPSHOT_PROP, ts.toString());
  }

  @Test
  public void testCommitNoFiles() {
    OffsetDateTime ts = EventTestUtil.now();
    UUID commitId = coordinatorTest(ImmutableList.of(), ImmutableList.of(), ts);

    assertThat(producer.history()).hasSize(2);
    assertCommitComplete(1, commitId, ts);

    assertThat(table.snapshots()).isEmpty();
  }

  @Test
  public void testCommitError() {
    // this spec isn't registered with the table
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();

    assertThatThrownBy(
            () -> coordinatorTest(ImmutableList.of(badDataFile), ImmutableList.of(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find partition spec");

    assertThat(producer.history()).hasSize(1);
    assertThat(table.snapshots()).isEmpty();
  }

  @Test
  public void testCommitFailedExceptionPropagates() {
    Table spiedTable = spy(table);
    AppendFiles spiedAppend = spy(table.newAppend());
    doThrow(new CommitFailedException("Glue detected concurrent update"))
        .when(spiedAppend)
        .commit();
    when(spiedTable.newAppend()).thenReturn(spiedAppend);
    when(catalog.loadTable(TABLE_IDENTIFIER)).thenReturn(spiedTable);

    assertThatThrownBy(
            () ->
                coordinatorTest(
                    ImmutableList.of(EventTestUtil.createDataFile()),
                    ImmutableList.of(),
                    EventTestUtil.now()))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Glue detected concurrent update");
  }

  private void assertCommitTable(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitTable = AvroUtil.decode(bytes);
    assertThat(commitTable.type()).isEqualTo(PayloadType.COMMIT_TO_TABLE);
    CommitToTable commitToTablePayload = (CommitToTable) commitTable.payload();
    assertThat(commitToTablePayload.commitId()).isEqualTo(commitId);
    assertThat(commitToTablePayload.tableReference().identifier().toString())
        .isEqualTo(TABLE_IDENTIFIER.toString());
    assertThat(commitToTablePayload.validThroughTs()).isEqualTo(ts);
  }

  private void assertCommitComplete(int idx, UUID commitId, OffsetDateTime ts) {
    byte[] bytes = producer.history().get(idx).value();
    Event commitComplete = AvroUtil.decode(bytes);
    assertThat(commitComplete.type()).isEqualTo(PayloadType.COMMIT_COMPLETE);
    CommitComplete commitCompletePayload = (CommitComplete) commitComplete.payload();
    assertThat(commitCompletePayload.commitId()).isEqualTo(commitId);
    assertThat(commitCompletePayload.validThroughTs()).isEqualTo(ts);
  }

  private UUID coordinatorTest(
      List<DataFile> dataFiles, List<DeleteFile> deleteFiles, OffsetDateTime ts) {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), clientFactory, context);
    coordinator.start();

    // init consumer after subscribe()
    initConsumer();

    coordinator.process();

    assertThat(producer.transactionCommitted()).isTrue();
    assertThat(producer.history()).hasSize(1);

    byte[] bytes = producer.history().get(0).value();
    Event commitRequest = AvroUtil.decode(bytes);
    assertThat(commitRequest.type()).isEqualTo(PayloadType.START_COMMIT);

    UUID commitId = ((StartCommit) commitRequest.payload()).commitId();

    Event commitResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                dataFiles,
                deleteFiles));
    bytes = AvroUtil.encode(commitResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    Event commitReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, ts))));
    bytes = AvroUtil.encode(commitReady);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", bytes));

    when(config.commitIntervalMs()).thenReturn(0);

    coordinator.process();

    return commitId;
  }

  @Test
  public void testPartialCommitFailureMetric() {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(0);

    Table spiedTable = spy(table);
    AppendFiles spiedAppend = spy(table.newAppend());
    doThrow(new CommitFailedException("simulated failure")).when(spiedAppend).commit();
    when(spiedTable.newAppend()).thenReturn(spiedAppend);
    when(catalog.loadTable(TABLE_IDENTIFIER)).thenReturn(spiedTable);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), clientFactory, context);
    coordinator.start();

    initConsumer();

    coordinator.process();

    assertThat(coordinator.partialCommitFailureCount()).isEqualTo(0);

    UUID commitId =
        ((StartCommit) AvroUtil.decode(producer.history().get(0).value()).payload()).commitId();
    Event commitResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(EventTestUtil.createDataFile()),
                ImmutableList.of()));
    byte[] bytes = AvroUtil.encode(commitResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    coordinator.process();

    assertThat(coordinator.partialCommitFailureCount()).isEqualTo(1);
  }

  @Test
  public void testCoordinatorRunning() {
    TopicPartition tp0 = new TopicPartition(SRC_TOPIC_NAME, 0);
    TopicPartition tp1 = new TopicPartition(SRC_TOPIC_NAME, 1);
    TopicPartition tp2 = new TopicPartition(SRC_TOPIC_NAME, 2);

    // Assigning three topic partitions tp0, tp1, and tp2. This will be elected as leader as it has
    // tp0.
    sourceConsumer.rebalance(Lists.newArrayList(tp0, tp1, tp2));
    assertThat(mockIcebergSinkTask.isCoordinatorRunning()).isTrue();

    // Now revoking the partition 2, this should not close the coordinator as this task still has
    // the zeroth partition
    sourceConsumer.rebalance(Lists.newArrayList(tp0, tp1));
    assertThat(mockIcebergSinkTask.isCoordinatorRunning()).isTrue();

    // Now finally revoking partition zero and this should result in the closure of the coordinator
    sourceConsumer.rebalance(ImmutableList.of(tp1));
    assertThat(mockIcebergSinkTask.isCoordinatorRunning()).isFalse();
  }

  @Test
  public void testCoordinatorCommittedOffsetMerging() {
    // Set the initial offsets based on a message from partition 1
    table
        .newAppend()
        .appendFile(EventTestUtil.createDataFile())
        .set(OFFSETS_SNAPSHOT_PROP, "{\"1\":7}")
        .commit();

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"1\":7}");

    // Trigger commit to the table that will include partition 0 offsets
    coordinatorTest(
        ImmutableList.of(EventTestUtil.createDataFile()), ImmutableList.of(), EventTestUtil.now());

    // Assert that the table was not updated and both offsets are represented
    table.refresh();
    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.currentSnapshot().summary())
        .containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3,\"1\":7}");
  }

  @Test
  public void testCoordinatorCommittedOffsetValidation() {
    // This test demonstrates that the Coordinator's validateAndCommit method
    // prevents commits when another independent commit has updated the offsets
    // during the commit process

    // Set the initial offsets
    table
        .newAppend()
        .appendFile(EventTestUtil.createDataFile())
        .set(OFFSETS_SNAPSHOT_PROP, "{\"0\":1}")
        .commit();

    Table frozenTable = catalog.loadTable(TABLE_IDENTIFIER);

    // return the original table state on the first load, so that the update will happen
    // during the commit refresh
    when(catalog.loadTable(TABLE_IDENTIFIER)).thenReturn(frozenTable).thenCallRealMethod();

    // Independently update the offsets
    table
        .newAppend()
        .appendFile(EventTestUtil.createDataFile())
        .set(OFFSETS_SNAPSHOT_PROP, "{\"0\":7}")
        .commit();

    assertThat(table.snapshots()).hasSize(2);
    Snapshot firstSnapshot = table.currentSnapshot();
    assertThat(firstSnapshot.summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":7}");

    // Trigger commit to the table - should throw ValidationException
    assertThatThrownBy(
            () ->
                coordinatorTest(
                    ImmutableList.of(EventTestUtil.createDataFile()),
                    ImmutableList.of(),
                    EventTestUtil.now()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("stale offsets");

    table.refresh();
    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":7}");
  }

  @Test
  public void testStaleGroupFailureSkipsCurrentGroup() {
    // If a stale group fails to commit, remaining groups for that table must be
    // skipped to preserve sequence number ordering. No snapshots should be created.
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), clientFactory, context);
    coordinator.start();
    initConsumer();

    // Inject a stale DataWritten with a bad partition spec (will fail validation).
    UUID staleCommitId = UUID.randomUUID();
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();
    Event staleResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                staleCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(badDataFile),
                ImmutableList.of()));
    byte[] staleBytes = AvroUtil.encode(staleResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0, "key", staleBytes));

    // First process(): starts commit, consumes stale event.
    coordinator.process();

    byte[] bytes = producer.history().get(0).value();
    Event startEvent = AvroUtil.decode(bytes);
    UUID currentCommitId = ((StartCommit) startEvent.payload()).commitId();

    // Add valid current-cycle events.
    Event currentResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                currentCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(EventTestUtil.createDataFile()),
                ImmutableList.of()));
    bytes = AvroUtil.encode(currentResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    Event currentReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                currentCommitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, null))));
    bytes = AvroUtil.encode(currentReady);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", bytes));

    // Second process(): stale group fails → current group skipped for this table.
    coordinator.process();

    table.refresh();
    // No snapshots: both groups skipped due to stale failure.
    assertThat(table.snapshots()).isEmpty();

    // No CommitComplete sent (buffer not empty).
    boolean hasCommitComplete =
        producer.history().stream()
            .anyMatch(
                r -> {
                  Event event = AvroUtil.decode(r.value());
                  return event.type() == PayloadType.COMMIT_COMPLETE;
                });
    assertThat(hasCommitComplete).isFalse();
  }

  // =============================================================================
  // Stale Group Retry Exhaustion — ConnectException Must Propagate
  // =============================================================================

  // TDD: these tests define DESIRED behavior. With the current implementation,
  // commit() swallows ConnectException (Coordinator.java line ~174). The desired
  // behavior is that ConnectException from exhausted stale retries propagates out
  // of process() so Kafka Connect can stop the connector.

  @ParameterizedTest
  @ValueSource(ints = {0, 3})
  public void testStaleGroupBlocksThenThrowsConnectException(int maxRetries) {
    // For maxRetries=0: process() should throw ConnectException on the very first commit attempt.
    // For maxRetries=3: process() blocks (no exception) for 3 cycles, then throws on cycle 4.
    when(config.commitStaleMaxBlockingRetries()).thenReturn(maxRetries);
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), clientFactory, context);
    coordinator.start();
    initConsumer();

    // Inject a stale DataWritten with a bad partition spec (fails commitToTable).
    UUID staleCommitId = UUID.randomUUID();
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();
    Event staleResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                staleCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(badDataFile),
                ImmutableList.of()));
    byte[] staleBytes = AvroUtil.encode(staleResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0, "key", staleBytes));

    // First process(): starts commit (commitId1), consumes the stale event.
    // commitIntervalMs=0 guarantees a new commit is started each cycle.
    coordinator.process();

    // Extract the commitId from the StartCommit sent by the first process().
    byte[] bytes = producer.history().get(0).value();
    Event startEvent = AvroUtil.decode(bytes);
    UUID currentCommitId = ((StartCommit) startEvent.payload()).commitId();

    // Add valid current-cycle DataWritten and DataComplete so the first real commit triggers.
    Event currentResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                currentCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(EventTestUtil.createDataFile()),
                ImmutableList.of()));
    bytes = AvroUtil.encode(currentResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    Event currentReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                currentCommitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, null))));
    bytes = AvroUtil.encode(currentReady);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", bytes));

    // Run maxRetries blocking cycles. Each cycle: the stale group fails but stays within the
    // retry budget (count <= max), so no exception is thrown. On the (maxRetries + 1)-th failure,
    // isRetryAllowed returns false and ConnectException is thrown — asserted below.
    int offset = 3;
    for (int i = 0; i < maxRetries; i++) {
      // process() should NOT throw — the stale group is still within its retry budget.
      coordinator.process();

      // The coordinator started a new commit this cycle. Find the latest StartCommit.
      UUID nextCommitId =
          producer.history().stream()
              .map(r -> AvroUtil.decode(r.value()))
              .filter(e -> e.type() == PayloadType.START_COMMIT)
              .reduce((first, second) -> second) // last element
              .map(ev -> ((StartCommit) ev.payload()).commitId())
              .orElseThrow(() -> new AssertionError("No StartCommit found in producer history"));

      // Inject DataComplete so the next cycle's commit is ready to fire.
      Event nextReady =
          new Event(
              config.connectGroupId(),
              new DataComplete(
                  nextCommitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, null))));
      bytes = AvroUtil.encode(nextReady);
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset++, "key", bytes));
    }

    // On the next process() the stale group has exceeded its retry budget — must throw.
    assertThatThrownBy(coordinator::process)
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("Connector stopping");
  }

  @Test
  public void testStaleGroupFailureStopsConnector() {
    // With maxRetries=0 the very first commit attempt after a stale event must stop the connector.
    // The ConnectException must propagate out of process() with a message containing
    // "Connector stopping", and no CommitComplete must have been sent.
    when(config.commitStaleMaxBlockingRetries()).thenReturn(0);
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), clientFactory, context);
    coordinator.start();
    initConsumer();

    // Inject a stale DataWritten with a bad partition spec.
    UUID staleCommitId = UUID.randomUUID();
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();
    Event staleResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                staleCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(badDataFile),
                ImmutableList.of()));
    byte[] staleBytes = AvroUtil.encode(staleResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0, "key", staleBytes));

    // First process(): starts commit, consumes stale event.
    coordinator.process();

    byte[] bytes = producer.history().get(0).value();
    Event startEvent = AvroUtil.decode(bytes);
    UUID currentCommitId = ((StartCommit) startEvent.payload()).commitId();

    // Add valid current-cycle DataWritten + DataComplete so the commit triggers.
    Event currentResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                currentCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(EventTestUtil.createDataFile()),
                ImmutableList.of()));
    bytes = AvroUtil.encode(currentResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    Event currentReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                currentCommitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, null))));
    bytes = AvroUtil.encode(currentReady);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", bytes));

    // Second process(): stale group immediately exceeds retry budget (maxRetries=0).
    // ConnectException must propagate out of process().
    assertThatThrownBy(coordinator::process)
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("Connector stopping");

    // The connector must not have sent a CommitComplete — it is stopping.
    boolean hasCommitComplete =
        producer.history().stream()
            .anyMatch(
                r -> {
                  Event event = AvroUtil.decode(r.value());
                  return event.type() == PayloadType.COMMIT_COMPLETE;
                });
    assertThat(hasCommitComplete).isFalse();
  }

  @Test
  public void testZeroRetriesMultiTableCommitsGoodTableBeforeFailing() {
    // With maxRetries=0, when the stale event targets tbl2 and good data targets tbl,
    // tbl must receive its snapshot (committed before the exception) while tbl2 gets nothing.
    // The ConnectException must still propagate out of process().
    when(config.commitStaleMaxBlockingRetries()).thenReturn(0);
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    // Create a second table as the stale event's target.
    catalog.createTable(TableIdentifier.of("db", "tbl2"), SCHEMA);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), clientFactory, context);
    coordinator.start();
    initConsumer();

    // Inject a stale DataWritten targeting db.tbl2 with a bad partition spec.
    UUID staleCommitId = UUID.randomUUID();
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();
    Event staleResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                staleCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl2"), null),
                ImmutableList.of(badDataFile),
                ImmutableList.of()));
    byte[] staleBytes = AvroUtil.encode(staleResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0, "key", staleBytes));

    // First process(): starts commit, consumes stale event for tbl2.
    coordinator.process();

    byte[] bytes = producer.history().get(0).value();
    Event startEvent = AvroUtil.decode(bytes);
    UUID currentCommitId = ((StartCommit) startEvent.payload()).commitId();

    // Valid DataWritten for db.tbl (the good table).
    Event currentResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                currentCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(EventTestUtil.createDataFile()),
                ImmutableList.of()));
    bytes = AvroUtil.encode(currentResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    Event currentReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                currentCommitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, null))));
    bytes = AvroUtil.encode(currentReady);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", bytes));

    // Second process(): tbl's current group commits successfully, then tbl2's stale group
    // exhausts retries (maxRetries=0) and throws ConnectException.
    assertThatThrownBy(coordinator::process)
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("Connector stopping");

    // db.tbl must have 1 snapshot — committed before the exception was thrown.
    table.refresh();
    assertThat(table.snapshots()).hasSize(1);

    // db.tbl2 must have no snapshots — its stale group failed.
    Table table2 = catalog.loadTable(TableIdentifier.of("db", "tbl2"));
    assertThat(table2.snapshots()).isEmpty();
  }

  @Test
  public void testMultiTablePartialCommitWithStaleExhaustion() {
    // With maxRetries=3, the stale group for tbl2 blocks for 3 cycles while tbl accumulates
    // snapshots each cycle. On cycle 4 (retry exhaustion) process() throws ConnectException.
    // Final state: tbl has 3 snapshots, tbl2 has 0.
    when(config.commitStaleMaxBlockingRetries()).thenReturn(3);
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    catalog.createTable(TableIdentifier.of("db", "tbl2"), SCHEMA);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), clientFactory, context);
    coordinator.start();
    initConsumer();

    // Inject a stale DataWritten targeting db.tbl2 with a bad partition spec.
    UUID staleCommitId = UUID.randomUUID();
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();
    Event staleResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                staleCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl2"), null),
                ImmutableList.of(badDataFile),
                ImmutableList.of()));
    byte[] staleBytes = AvroUtil.encode(staleResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0, "key", staleBytes));

    // First process(): starts commit (commitId1), consumes the stale event.
    coordinator.process();

    // Run 3 blocking cycles (retries 1, 2, 3). On the 4th failure (count > maxRetries),
    // isRetryAllowed returns false and ConnectException is thrown — asserted after the loop.
    int offset = 1;
    for (int cycle = 1; cycle <= 3; cycle++) {
      UUID cycleCommitId =
          producer.history().stream()
              .map(r -> AvroUtil.decode(r.value()))
              .filter(e -> e.type() == PayloadType.START_COMMIT)
              .reduce((first, second) -> second) // last StartCommit
              .map(e -> ((StartCommit) e.payload()).commitId())
              .orElseThrow(() -> new AssertionError("No StartCommit found"));

      Event cycleResponse =
          new Event(
              config.connectGroupId(),
              new DataWritten(
                  StructType.of(),
                  cycleCommitId,
                  TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                  ImmutableList.of(EventTestUtil.createDataFile()),
                  ImmutableList.of()));
      byte[] cycleResponseBytes = AvroUtil.encode(cycleResponse);
      consumer.addRecord(
          new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset++, "key", cycleResponseBytes));

      Event cycleReady =
          new Event(
              config.connectGroupId(),
              new DataComplete(
                  cycleCommitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, null))));
      byte[] cycleReadyBytes = AvroUtil.encode(cycleReady);
      consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset++, "key", cycleReadyBytes));

      // Must not throw during blocking cycles.
      coordinator.process();

      // db.tbl gains one snapshot per successful blocking cycle.
      table.refresh();
      assertThat(ImmutableList.copyOf(table.snapshots())).hasSize(cycle);
    }

    // Cycle 4: stale group has now failed 3 times (blocking). This cycle is the 4th failure
    // (count > maxRetries), so isRetryAllowed returns false → throws ConnectException.
    // The good table (tbl) may still commit before the exception propagates.
    UUID finalCommitId =
        producer.history().stream()
            .map(r -> AvroUtil.decode(r.value()))
            .filter(e -> e.type() == PayloadType.START_COMMIT)
            .reduce((first, second) -> second)
            .map(e -> ((StartCommit) e.payload()).commitId())
            .orElseThrow(() -> new AssertionError("No StartCommit found for final cycle"));

    Event finalResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                finalCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(EventTestUtil.createDataFile()),
                ImmutableList.of()));
    byte[] finalResponseBytes = AvroUtil.encode(finalResponse);
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset++, "key", finalResponseBytes));

    Event finalReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                finalCommitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, null))));
    byte[] finalReadyBytes = AvroUtil.encode(finalReady);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset++, "key", finalReadyBytes));

    assertThatThrownBy(coordinator::process)
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("Connector stopping");

    // db.tbl has 4 snapshots: 3 from blocking cycles + 1 from the final cycle
    // (good table commits in parallel before the bad table's exception propagates).
    table.refresh();
    assertThat(ImmutableList.copyOf(table.snapshots())).hasSize(4);

    // db.tbl2 has no snapshots — its stale group never committed.
    Table table2 = catalog.loadTable(TableIdentifier.of("db", "tbl2"));
    assertThat(table2.snapshots()).isEmpty();
  }

  @Test
  public void testStaleSucceedsCurrentFails() {
    // When a stale group commits but the current group fails, the stale group's
    // envelopes should be removed and its snapshot should exist. The current group's
    // envelopes remain in the buffer for retry. No CommitComplete is sent.
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);

    SinkTaskContext context = mock(SinkTaskContext.class);
    Coordinator coordinator =
        new Coordinator(catalog, config, ImmutableList.of(), clientFactory, context);
    coordinator.start();
    initConsumer();

    // Inject a stale DataWritten with valid data.
    UUID staleCommitId = UUID.randomUUID();
    Event staleResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                staleCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(EventTestUtil.createDataFile()),
                ImmutableList.of()));
    byte[] staleBytes = AvroUtil.encode(staleResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 0, "key", staleBytes));

    // First process(): starts commit, consumes stale event.
    coordinator.process();

    byte[] bytes = producer.history().get(0).value();
    Event startEvent = AvroUtil.decode(bytes);
    UUID currentCommitId = ((StartCommit) startEvent.payload()).commitId();

    // Add current-cycle DataWritten with a bad partition spec (will fail).
    PartitionSpec badPartitionSpec =
        PartitionSpec.builderFor(SCHEMA).withSpecId(1).identity("id").build();
    DataFile badDataFile =
        DataFiles.builder(badPartitionSpec)
            .withPath(UUID.randomUUID() + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100L)
            .withRecordCount(5)
            .build();
    Event currentResponse =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                currentCommitId,
                TableReference.of("catalog", TableIdentifier.of("db", "tbl"), null),
                ImmutableList.of(badDataFile),
                ImmutableList.of()));
    bytes = AvroUtil.encode(currentResponse);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1, "key", bytes));

    Event currentReady =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                currentCommitId, ImmutableList.of(new TopicPartitionOffset("topic", 1, 1L, null))));
    bytes = AvroUtil.encode(currentReady);
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 2, "key", bytes));

    // Second process(): stale group succeeds, current group fails.
    coordinator.process();

    table.refresh();
    // Stale group committed: 1 snapshot with stale commitId.
    List<Snapshot> snapshots = ImmutableList.copyOf(table.snapshots());
    assertThat(snapshots).hasSize(1);
    assertThat(snapshots.get(0).summary())
        .containsEntry(COMMIT_ID_SNAPSHOT_PROP, staleCommitId.toString());

    // No CommitComplete sent (buffer not fully drained — current group's envelopes remain).
    boolean hasCommitComplete =
        producer.history().stream()
            .anyMatch(
                r -> {
                  Event event = AvroUtil.decode(r.value());
                  return event.type() == PayloadType.COMMIT_COMPLETE;
                });
    assertThat(hasCommitComplete).isFalse();
  }
}
