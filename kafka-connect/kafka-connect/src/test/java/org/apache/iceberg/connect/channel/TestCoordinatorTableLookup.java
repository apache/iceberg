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
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.connect.events.AvroUtil;
import org.apache.iceberg.connect.events.DataComplete;
import org.apache.iceberg.connect.events.DataWritten;
import org.apache.iceberg.connect.events.Event;
import org.apache.iceberg.connect.events.PayloadType;
import org.apache.iceberg.connect.events.StartCommit;
import org.apache.iceberg.connect.events.TableReference;
import org.apache.iceberg.connect.events.TopicPartitionOffset;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestCoordinatorTableLookup extends ChannelTestBase {
  private static final TopicPartition CONTROL_PARTITION = new TopicPartition(CTL_TOPIC_NAME, 0);
  private Coordinator activeCoordinator;

  @AfterEach
  void stopCoordinator() {
    if (activeCoordinator != null) {
      activeCoordinator.terminate();
      activeCoordinator.stop();
    }
  }

  @Test
  void availableTableRegistersFileBeforeCheckpointing() {
    Coordinator coordinator = startCoordinator();
    UUID commitId = currentCommitId();
    DataFile dataFile = EventTestUtil.createDataFile();
    bufferFile(coordinator, commitId, dataFile);
    completeCommit(coordinator, commitId, 2L);

    table.refresh();
    assertThat(table.snapshots()).hasSize(1);
    assertThat(
            SnapshotChanges.builderFor(table)
                .snapshot(table.currentSnapshot())
                .build()
                .addedDataFiles())
        .extracting(DataFile::location)
        .containsExactly(dataFile.location());
    assertThat(table.currentSnapshot().summary()).containsEntry(OFFSETS_SNAPSHOT_PROP, "{\"0\":3}");
    assertThat(consumer.committed(Set.of(CONTROL_PARTITION)))
        .containsEntry(CONTROL_PARTITION, new OffsetAndMetadata(3L));
    assertThat(producer.history())
        .extracting(record -> AvroUtil.decode(record.value()).type())
        .containsExactly(
            PayloadType.START_COMMIT, PayloadType.COMMIT_TO_TABLE, PayloadType.COMMIT_COMPLETE);
  }

  @Test
  void missingTableFailsAtDefaultRetryLimitWithoutCheckpointing() {
    Coordinator coordinator = startCoordinator();
    UUID commitId = currentCommitId();
    bufferFile(coordinator, commitId, EventTestUtil.createDataFile());
    catalog.renameTable(TABLE_IDENTIFIER, TableIdentifier.of(NAMESPACE, "unavailable"));

    assertThatThrownBy(() -> completeCommit(coordinator, commitId, 2L))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Cannot complete commit")
        .hasMessageContaining("unresolved tables");
    assertThat(consumer.committed(Set.of(CONTROL_PARTITION)))
        .containsEntry(CONTROL_PARTITION, new OffsetAndMetadata(1L));
    assertThat(producer.history())
        .extracting(record -> AvroUtil.decode(record.value()).type())
        .containsExactly(PayloadType.START_COMMIT);
  }

  @Test
  void missingTableRetainsResponsesAndCommitsThemWhenTheTableReturns() {
    when(config.commitMaxConsecutiveFailures()).thenReturn(2);
    Coordinator coordinator = startCoordinator();
    UUID commitId = currentCommitId();
    UUID originalUuid = table.uuid();
    DataFile dataFile = EventTestUtil.createDataFile();
    bufferFile(coordinator, commitId, dataFile);

    TableIdentifier movedIdentifier = TableIdentifier.of(NAMESPACE, "temporarily_moved");
    catalog.renameTable(TABLE_IDENTIFIER, movedIdentifier);
    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isFalse();
    completeCommit(coordinator, commitId, 2L);

    // nothing reached a table, so the control-topic offsets must not move past the response and
    // the commit must not be announced as complete
    assertThat(consumer.committed(Set.of(CONTROL_PARTITION)))
        .containsEntry(CONTROL_PARTITION, new OffsetAndMetadata(1L));
    assertThat(producer.history())
        .extracting(record -> AvroUtil.decode(record.value()).type())
        .containsExactly(PayloadType.START_COMMIT);
    assertThat(catalog.loadTable(movedIdentifier).snapshots()).isEmpty();

    catalog.renameTable(movedIdentifier, TABLE_IDENTIFIER);
    assertThat(catalog.loadTable(TABLE_IDENTIFIER).uuid()).isEqualTo(originalUuid);
    coordinator.process();
    completeCommit(coordinator, currentCommitId(), 3L);

    Table recovered = catalog.loadTable(TABLE_IDENTIFIER);
    assertThat(recovered.snapshots()).hasSize(1);
    assertThat(
            SnapshotChanges.builderFor(recovered)
                .snapshot(recovered.currentSnapshot())
                .build()
                .addedDataFiles())
        .extracting(DataFile::location)
        .containsExactly(dataFile.location());
    assertThat(consumer.committed(Set.of(CONTROL_PARTITION)))
        .containsEntry(CONTROL_PARTITION, new OffsetAndMetadata(4L));
    assertThat(producer.history())
        .extracting(record -> AvroUtil.decode(record.value()).type())
        .containsExactly(
            PayloadType.START_COMMIT,
            PayloadType.START_COMMIT,
            PayloadType.COMMIT_TO_TABLE,
            PayloadType.COMMIT_COMPLETE);
  }

  @Test
  void replacementTableIsProtectedAndTheOriginalFileIsCommittedWhenTheTableReturns() {
    when(config.commitMaxConsecutiveFailures()).thenReturn(2);
    Coordinator coordinator = startCoordinator();
    UUID commitId = currentCommitId();
    UUID originalUuid = table.uuid();
    DataFile dataFile = EventTestUtil.createDataFile();
    bufferFile(coordinator, commitId, dataFile);

    TableIdentifier movedIdentifier = TableIdentifier.of(NAMESPACE, "original_table");
    catalog.renameTable(TABLE_IDENTIFIER, movedIdentifier);
    Table replacement = catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    assertThat(replacement.uuid()).isNotEqualTo(originalUuid);
    completeCommit(coordinator, commitId, 2L);

    // the replacement must not receive the previous table's file, and the response must survive
    replacement.refresh();
    assertThat(replacement.snapshots()).isEmpty();
    assertThat(catalog.loadTable(movedIdentifier).snapshots()).isEmpty();
    assertThat(consumer.committed(Set.of(CONTROL_PARTITION)))
        .containsEntry(CONTROL_PARTITION, new OffsetAndMetadata(1L));
    assertThat(producer.history())
        .extracting(record -> AvroUtil.decode(record.value()).type())
        .containsExactly(PayloadType.START_COMMIT);

    assertThat(catalog.dropTable(TABLE_IDENTIFIER, false)).isTrue();
    catalog.renameTable(movedIdentifier, TABLE_IDENTIFIER);
    assertThat(catalog.loadTable(TABLE_IDENTIFIER).uuid()).isEqualTo(originalUuid);
    coordinator.process();
    completeCommit(coordinator, currentCommitId(), 3L);

    Table recovered = catalog.loadTable(TABLE_IDENTIFIER);
    assertThat(recovered.snapshots()).hasSize(1);
    assertThat(
            SnapshotChanges.builderFor(recovered)
                .snapshot(recovered.currentSnapshot())
                .build()
                .addedDataFiles())
        .extracting(DataFile::location)
        .containsExactly(dataFile.location());
    assertThat(consumer.committed(Set.of(CONTROL_PARTITION)))
        .containsEntry(CONTROL_PARTITION, new OffsetAndMetadata(4L));
  }

  @Test
  void missingTableFailsAfterConfiguredLimit() {
    when(config.commitMaxConsecutiveFailures()).thenReturn(2);
    Coordinator coordinator = startCoordinator();
    UUID commitId = currentCommitId();
    bufferFile(coordinator, commitId, EventTestUtil.createDataFile());
    catalog.renameTable(TABLE_IDENTIFIER, TableIdentifier.of(NAMESPACE, "unavailable"));

    completeCommit(coordinator, commitId, 2L);
    coordinator.process();
    assertThatThrownBy(() -> completeCommit(coordinator, currentCommitId(), 3L))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("unresolved tables");
    assertCheckpoint(1L);
    assertNoCommitComplete();
  }

  @Test
  void mixedTableUUIDsFailWithinLimitEvenWhenBothFilesHaveCommitted() {
    when(config.commitMaxConsecutiveFailures()).thenReturn(3);
    when(config.commitThreads()).thenReturn(2);
    Coordinator coordinator = startCoordinator();
    UUID originalUUID = table.uuid();
    DataFile originalFile = dataFile("original");
    bufferFile(coordinator, currentCommitId(), originalFile);

    TableIdentifier originalIdentifier = TableIdentifier.of(NAMESPACE, "original_table");
    TableIdentifier replacementIdentifier = TableIdentifier.of(NAMESPACE, "replacement_table");
    catalog.renameTable(TABLE_IDENTIFIER, originalIdentifier);
    Table replacement = catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    assertThat(replacement.uuid()).isNotEqualTo(originalUUID);
    completeCommit(coordinator, currentCommitId(), 2L);

    coordinator.process();
    DataFile replacementFile = dataFile("replacement");
    writeResponse(TABLE_IDENTIFIER, replacement.uuid(), replacementFile, 3L);
    completeCommit(coordinator, currentCommitId(), 4L);
    assertFiles(catalog.loadTable(TABLE_IDENTIFIER), replacementFile);
    assertCheckpoint(1L);

    catalog.renameTable(TABLE_IDENTIFIER, replacementIdentifier);
    catalog.renameTable(originalIdentifier, TABLE_IDENTIFIER);
    coordinator.process();
    assertThatThrownBy(() -> completeCommit(coordinator, currentCommitId(), 5L))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("unresolved tables");
    assertFiles(catalog.loadTable(TABLE_IDENTIFIER), originalFile);
    assertFiles(catalog.loadTable(replacementIdentifier), replacementFile);
    assertCheckpoint(1L);
    assertNoCommitComplete();
  }

  @Test
  void missingTablePartialCommitsCountFailuresAndRespectLimit() {
    when(config.commitMaxConsecutiveFailures()).thenReturn(2);
    Coordinator coordinator = startCoordinator();
    bufferFile(coordinator, currentCommitId(), EventTestUtil.createDataFile());
    catalog.renameTable(TABLE_IDENTIFIER, TableIdentifier.of(NAMESPACE, "unavailable"));
    when(config.commitTimeoutMs()).thenReturn(-1);

    coordinator.process();
    assertThat(coordinator.partialCommitFailureCount()).isEqualTo(1L);
    assertCheckpoint(1L);
    assertThatThrownBy(coordinator::process)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("unresolved tables");
    assertThat(coordinator.partialCommitFailureCount()).isEqualTo(2L);
    assertCheckpoint(1L);
    assertNoCommitComplete();
  }

  @Test
  void replacementTablePartialCommitFailsAtDefaultLimit() {
    Coordinator coordinator = startCoordinator();
    UUID originalUUID = table.uuid();
    bufferFile(coordinator, currentCommitId(), EventTestUtil.createDataFile());
    TableIdentifier originalIdentifier = TableIdentifier.of(NAMESPACE, "original_table");
    catalog.renameTable(TABLE_IDENTIFIER, originalIdentifier);
    Table replacement = catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    assertThat(replacement.uuid()).isNotEqualTo(originalUUID);
    when(config.commitTimeoutMs()).thenReturn(-1);

    assertThatThrownBy(coordinator::process)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("unresolved tables");
    assertThat(coordinator.partialCommitFailureCount()).isEqualTo(1L);
    assertThat(catalog.loadTable(originalIdentifier).snapshots()).isEmpty();
    assertThat(catalog.loadTable(TABLE_IDENTIFIER).snapshots()).isEmpty();
    assertCheckpoint(1L);
    assertNoCommitComplete();
  }

  @Test
  void fullAndPartialLookupFailuresShareRetryLimit() {
    when(config.commitMaxConsecutiveFailures()).thenReturn(2);
    Coordinator coordinator = startCoordinator();
    bufferFile(coordinator, currentCommitId(), EventTestUtil.createDataFile());
    catalog.renameTable(TABLE_IDENTIFIER, TableIdentifier.of(NAMESPACE, "unavailable"));
    completeCommit(coordinator, currentCommitId(), 2L);
    when(config.commitTimeoutMs()).thenReturn(-1);

    assertThatThrownBy(coordinator::process)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("unresolved tables");
    assertThat(coordinator.partialCommitFailureCount()).isEqualTo(1L);
    assertCheckpoint(1L);
    assertNoCommitComplete();
  }

  @Test
  void missingTableDoesNotResetEarlierCommitFailure() {
    when(config.commitMaxConsecutiveFailures()).thenReturn(3);
    Coordinator coordinator = startCoordinator();
    bufferFile(coordinator, currentCommitId(), EventTestUtil.createDataFile());
    doThrow(new CommitFailedException("Injected commit failure"))
        .when(catalog)
        .loadTable(TABLE_IDENTIFIER);
    completeCommit(coordinator, currentCommitId(), 2L);

    doCallRealMethod().when(catalog).loadTable(TABLE_IDENTIFIER);
    TableIdentifier movedIdentifier = TableIdentifier.of(NAMESPACE, "unavailable");
    catalog.renameTable(TABLE_IDENTIFIER, movedIdentifier);
    coordinator.process();
    completeCommit(coordinator, currentCommitId(), 3L);

    catalog.renameTable(movedIdentifier, TABLE_IDENTIFIER);
    doThrow(new CommitFailedException("Injected commit failure"))
        .when(catalog)
        .loadTable(TABLE_IDENTIFIER);
    coordinator.process();
    assertThatThrownBy(() -> completeCommit(coordinator, currentCommitId(), 4L))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected commit failure");
    assertCheckpoint(1L);
    assertNoCommitComplete();
  }

  @Test
  void successfulFullCommitResetsLookupFailureCount() {
    when(config.commitMaxConsecutiveFailures()).thenReturn(2);
    Coordinator coordinator = startCoordinator();
    DataFile firstFile = dataFile("first");
    bufferFile(coordinator, currentCommitId(), firstFile);
    TableIdentifier movedIdentifier = TableIdentifier.of(NAMESPACE, "unavailable");
    catalog.renameTable(TABLE_IDENTIFIER, movedIdentifier);
    completeCommit(coordinator, currentCommitId(), 2L);

    catalog.renameTable(movedIdentifier, TABLE_IDENTIFIER);
    coordinator.process();
    completeCommit(coordinator, currentCommitId(), 3L);
    assertFiles(catalog.loadTable(TABLE_IDENTIFIER), firstFile);
    assertCheckpoint(4L);
    producer.clear();

    coordinator.process();
    writeResponse(TABLE_IDENTIFIER, table.uuid(), dataFile("second"), 4L);
    catalog.renameTable(TABLE_IDENTIFIER, movedIdentifier);
    completeCommit(coordinator, currentCommitId(), 5L);
    assertCheckpoint(4L);
    coordinator.process();
    assertThatThrownBy(() -> completeCommit(coordinator, currentCommitId(), 6L))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("unresolved tables");
    assertCheckpoint(4L);
    assertNoCommitComplete();
  }

  @Test
  void missingTableRetryDoesNotDuplicateHealthyTableFiles() {
    when(config.commitMaxConsecutiveFailures()).thenReturn(2);
    when(config.commitThreads()).thenReturn(2);
    TableIdentifier healthyIdentifier = TableIdentifier.of(NAMESPACE, "healthy_table");
    Table healthy = catalog.createTable(healthyIdentifier, SCHEMA);
    Coordinator coordinator = startCoordinator();
    DataFile missingFile = dataFile("missing");
    DataFile healthyFile = dataFile("healthy");
    bufferFile(coordinator, currentCommitId(), missingFile);
    writeResponse(healthyIdentifier, healthy.uuid(), healthyFile, 2L);
    TableIdentifier movedIdentifier = TableIdentifier.of(NAMESPACE, "unavailable");
    catalog.renameTable(TABLE_IDENTIFIER, movedIdentifier);
    completeCommit(coordinator, currentCommitId(), 3L);
    assertCheckpoint(1L);
    assertFiles(catalog.loadTable(healthyIdentifier), healthyFile);
    assertNoCommitComplete();

    catalog.renameTable(movedIdentifier, TABLE_IDENTIFIER);
    coordinator.process();
    completeCommit(coordinator, currentCommitId(), 4L);
    assertCheckpoint(5L);
    assertFiles(catalog.loadTable(TABLE_IDENTIFIER), missingFile);
    assertFiles(catalog.loadTable(healthyIdentifier), healthyFile);
    assertThat(producer.history())
        .extracting(record -> AvroUtil.decode(record.value()).type())
        .containsExactly(
            PayloadType.START_COMMIT,
            PayloadType.COMMIT_TO_TABLE,
            PayloadType.START_COMMIT,
            PayloadType.COMMIT_TO_TABLE,
            PayloadType.COMMIT_COMPLETE);
  }

  private void writeResponse(
      TableIdentifier identifier, UUID tableUUID, DataFile file, long offset) {
    Event event =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                currentCommitId(),
                TableReference.of("catalog", identifier, tableUUID),
                List.of(file),
                List.of()));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset, "key", AvroUtil.encode(event)));
    activeCoordinator.process();
  }

  private void assertCheckpoint(long offset) {
    assertThat(consumer.committed(Set.of(CONTROL_PARTITION)))
        .containsEntry(CONTROL_PARTITION, new OffsetAndMetadata(offset));
  }

  private void assertNoCommitComplete() {
    assertThat(producer.history())
        .extracting(record -> AvroUtil.decode(record.value()).type())
        .doesNotContain(PayloadType.COMMIT_COMPLETE);
  }

  private static DataFile dataFile(String name) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(name + ".parquet")
        .withFileSizeInBytes(100)
        .withRecordCount(1)
        .build();
  }

  private static void assertFiles(Table target, DataFile expected) {
    assertThat(target.snapshots()).hasSize(1);
    assertThat(
            SnapshotChanges.builderFor(target)
                .snapshot(target.currentSnapshot())
                .build()
                .addedDataFiles())
        .extracting(DataFile::location)
        .containsExactly(expected.location());
  }

  private Coordinator startCoordinator() {
    when(config.commitIntervalMs()).thenReturn(0);
    when(config.commitTimeoutMs()).thenReturn(Integer.MAX_VALUE);
    MemberAssignment assignment =
        new MemberAssignment(Set.of(new TopicPartition(SRC_TOPIC_NAME, 0)));
    MemberDescription member =
        new MemberDescription("member", Optional.empty(), "client", "host", assignment);
    this.activeCoordinator =
        new Coordinator(
            catalog, config, List.of(member), clientFactory, mock(SinkTaskContext.class));
    activeCoordinator.start();
    initConsumer();
    consumer.commitSync(Map.of(CONTROL_PARTITION, new OffsetAndMetadata(1L)));
    activeCoordinator.process();
    return activeCoordinator;
  }

  private UUID currentCommitId() {
    return producer.history().stream()
        .map(record -> AvroUtil.decode(record.value()))
        .filter(event -> event.type() == PayloadType.START_COMMIT)
        .map(event -> ((StartCommit) event.payload()).commitId())
        .reduce((previous, current) -> current)
        .orElseThrow();
  }

  private void bufferFile(Coordinator coordinator, UUID commitId, DataFile dataFile) {
    Event event =
        new Event(
            config.connectGroupId(),
            new DataWritten(
                StructType.of(),
                commitId,
                TableReference.of("catalog", TABLE_IDENTIFIER, table.uuid()),
                List.of(dataFile),
                List.of()));
    consumer.addRecord(new ConsumerRecord<>(CTL_TOPIC_NAME, 0, 1L, "key", AvroUtil.encode(event)));
    coordinator.process();
    assertThat(consumer.committed(Set.of(CONTROL_PARTITION)))
        .containsEntry(CONTROL_PARTITION, new OffsetAndMetadata(1L));
    assertThat(table.snapshots()).isEmpty();
  }

  private void completeCommit(Coordinator coordinator, UUID commitId, long offset) {
    Event event =
        new Event(
            config.connectGroupId(),
            new DataComplete(
                commitId, List.of(new TopicPartitionOffset(SRC_TOPIC_NAME, 0, offset, null))));
    consumer.addRecord(
        new ConsumerRecord<>(CTL_TOPIC_NAME, 0, offset, "key", AvroUtil.encode(event)));
    coordinator.process();
  }
}
