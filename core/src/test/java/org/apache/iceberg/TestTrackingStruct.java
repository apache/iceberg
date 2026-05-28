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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestTrackingStruct {

  // Ordinals looked up from Tracking.schema() so tests don't hard-code positions.
  private static final List<Types.NestedField> TRACKING_FIELDS = Tracking.schema().fields();
  private static final int STATUS_ORDINAL = TRACKING_FIELDS.indexOf(Tracking.STATUS);
  private static final int SNAPSHOT_ID_ORDINAL = TRACKING_FIELDS.indexOf(Tracking.SNAPSHOT_ID);
  private static final int DATA_SEQUENCE_NUMBER_ORDINAL =
      TRACKING_FIELDS.indexOf(Tracking.SEQUENCE_NUMBER);
  private static final int FILE_SEQUENCE_NUMBER_ORDINAL =
      TRACKING_FIELDS.indexOf(Tracking.FILE_SEQUENCE_NUMBER);
  private static final int DV_SNAPSHOT_ID_ORDINAL =
      TRACKING_FIELDS.indexOf(Tracking.DV_SNAPSHOT_ID);
  private static final int FIRST_ROW_ID_ORDINAL = TRACKING_FIELDS.indexOf(Tracking.FIRST_ROW_ID);
  private static final int DELETED_POSITIONS_ORDINAL =
      TRACKING_FIELDS.indexOf(Tracking.DELETED_POSITIONS);
  private static final int REPLACED_POSITIONS_ORDINAL =
      TRACKING_FIELDS.indexOf(Tracking.REPLACED_POSITIONS);

  @Test
  void testFieldAccess() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());

    tracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());
    tracking.set(SNAPSHOT_ID_ORDINAL, 42L);
    tracking.set(DATA_SEQUENCE_NUMBER_ORDINAL, 10L);
    tracking.set(FILE_SEQUENCE_NUMBER_ORDINAL, 11L);
    tracking.set(DV_SNAPSHOT_ID_ORDINAL, 43L);
    tracking.set(FIRST_ROW_ID_ORDINAL, 1000L);

    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(42L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(10L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(11L);
    assertThat(tracking.dvSnapshotId()).isEqualTo(43L);
    assertThat(tracking.firstRowId()).isEqualTo(1000L);
    assertThat(tracking.deletedPositions()).isNull();
    assertThat(tracking.replacedPositions()).isNull();
  }

  @Test
  void testCopy() {
    Tracking tracking =
        TrackingStruct.builder(manifestSourceTracking(), 1L)
            .deletedPositions(ByteBuffer.wrap(new byte[] {1, 2}))
            .replacedPositions(ByteBuffer.wrap(new byte[] {3, 4}))
            .build();

    Tracking copy = tracking.copy();

    assertThat(copy.status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(copy.snapshotId()).isEqualTo(tracking.snapshotId());
    assertThat(copy.dataSequenceNumber()).isEqualTo(tracking.dataSequenceNumber());
    assertThat(copy.fileSequenceNumber()).isEqualTo(tracking.fileSequenceNumber());
    assertThat(copy.dvSnapshotId()).isNull();
    assertThat(copy.firstRowId()).isEqualTo(tracking.firstRowId());
    assertThat(copy.deletedPositions()).isEqualTo(tracking.deletedPositions());
    assertThat(copy.replacedPositions()).isEqualTo(tracking.replacedPositions());

    // verify deep copy of ByteBuffer backing arrays
    assertThat(copy.deletedPositions().array()).isNotSameAs(tracking.deletedPositions().array());
    assertThat(copy.replacedPositions().array()).isNotSameAs(tracking.replacedPositions().array());
  }

  @ParameterizedTest
  @EnumSource(EntryStatus.class)
  void testAllStatuses(EntryStatus status) {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(STATUS_ORDINAL, status.id());
    assertThat(tracking.status()).isEqualTo(status);
  }

  @Test
  void testIsLive() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());

    tracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());
    assertThat(tracking.isLive()).isTrue();

    tracking.set(STATUS_ORDINAL, EntryStatus.EXISTING.id());
    assertThat(tracking.isLive()).isTrue();

    tracking.set(STATUS_ORDINAL, EntryStatus.DELETED.id());
    assertThat(tracking.isLive()).isFalse();

    tracking.set(STATUS_ORDINAL, EntryStatus.REPLACED.id());
    assertThat(tracking.isLive()).isFalse();
  }

  @Test
  void testInheritSnapshotId() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());

    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // snapshotId is null, should inherit from manifest
    assertThat(tracking.snapshotId()).isEqualTo(100L);
  }

  @Test
  void testInheritSequenceNumberForAddedEntries() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());

    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // sequence numbers are null and status is ADDED, should inherit
    assertThat(tracking.dataSequenceNumber()).isEqualTo(60L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(60L);
  }

  @Test
  void testDoNotInheritSequenceNumberForExistingEntries() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(STATUS_ORDINAL, EntryStatus.EXISTING.id());
    tracking.set(DATA_SEQUENCE_NUMBER_ORDINAL, 5L);
    tracking.set(FILE_SEQUENCE_NUMBER_ORDINAL, 6L);

    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // sequence numbers are not inherited for EXISTING entries
    assertThat(tracking.dataSequenceNumber()).isEqualTo(5L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(6L);
  }

  @Test
  void testExplicitValuesOverrideInheritance() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());
    tracking.set(SNAPSHOT_ID_ORDINAL, 200L);
    tracking.set(DATA_SEQUENCE_NUMBER_ORDINAL, 75L);
    tracking.set(FILE_SEQUENCE_NUMBER_ORDINAL, 76L);

    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // explicit values should take precedence
    assertThat(tracking.snapshotId()).isEqualTo(200L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(75L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(76L);
  }

  @Test
  void testInheritFromRejectsUnequalSequenceNumbers() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());

    TrackingStruct manifestTracking = new TrackingStruct(Tracking.schema());
    manifestTracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());
    manifestTracking.set(SNAPSHOT_ID_ORDINAL, 100L);
    manifestTracking.set(DATA_SEQUENCE_NUMBER_ORDINAL, 50L);
    manifestTracking.set(FILE_SEQUENCE_NUMBER_ORDINAL, 60L);

    assertThatThrownBy(() -> tracking.inheritFrom(manifestTracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Manifest data and file sequence numbers must be equal, got 50 and 60");
  }

  @Test
  void testNoDefaultingWithoutInheritance() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());

    // no inheritance, nulls stay null
    assertThat(tracking.snapshotId()).isNull();
    assertThat(tracking.dataSequenceNumber()).isNull();
    assertThat(tracking.fileSequenceNumber()).isNull();
  }

  private static Tracking createManifestTracking(long snapshotId, long sequenceNumber) {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());
    tracking.set(SNAPSHOT_ID_ORDINAL, snapshotId);
    tracking.set(DATA_SEQUENCE_NUMBER_ORDINAL, sequenceNumber);
    tracking.set(FILE_SEQUENCE_NUMBER_ORDINAL, sequenceNumber);
    return tracking;
  }

  @Test
  void testAddedBuilder() {
    Tracking tracking = TrackingStruct.added(42L).dvUpdated().build();

    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(42L);
    assertThat(tracking.dvSnapshotId()).isEqualTo(42L);
    assertThat(tracking.deletedPositions()).isNull();
    assertThat(tracking.replacedPositions()).isNull();
    // sequence numbers and firstRowId remain null; populated by inheritance
    assertThat(tracking.dataSequenceNumber()).isNull();
    assertThat(tracking.fileSequenceNumber()).isNull();
    assertThat(tracking.firstRowId()).isNull();
  }

  @Test
  void testExistingBuilderPreservesSourceFields() {
    Tracking source = sourceTracking();

    Tracking existing = TrackingStruct.builder(source, 1L).build();

    assertThat(existing.status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(existing.snapshotId()).isEqualTo(source.snapshotId());
    assertThat(existing.dataSequenceNumber()).isEqualTo(source.dataSequenceNumber());
    assertThat(existing.fileSequenceNumber()).isEqualTo(source.fileSequenceNumber());
    assertThat(existing.dvSnapshotId()).isEqualTo(source.dvSnapshotId());
    assertThat(existing.firstRowId()).isEqualTo(source.firstRowId());
  }

  @Test
  void testDeleteUpdatesSnapshotIdAndPreservesRest() {
    Tracking source = sourceTracking();

    Tracking deleted = source.asDeleted(999L);

    assertThat(deleted.status()).isEqualTo(EntryStatus.DELETED);
    assertThat(deleted.snapshotId()).isEqualTo(999L);
    assertThat(deleted.dataSequenceNumber()).isEqualTo(source.dataSequenceNumber());
    assertThat(deleted.fileSequenceNumber()).isEqualTo(source.fileSequenceNumber());
    assertThat(deleted.dvSnapshotId()).isEqualTo(source.dvSnapshotId());
    assertThat(deleted.firstRowId()).isEqualTo(source.firstRowId());
  }

  @Test
  void testReplaceUpdatesSnapshotIdAndPreservesRest() {
    Tracking source = sourceTracking();

    Tracking replaced = source.asReplaced(999L);

    assertThat(replaced.status()).isEqualTo(EntryStatus.REPLACED);
    assertThat(replaced.snapshotId()).isEqualTo(999L);
    assertThat(replaced.dataSequenceNumber()).isEqualTo(source.dataSequenceNumber());
    assertThat(replaced.fileSequenceNumber()).isEqualTo(source.fileSequenceNumber());
    assertThat(replaced.dvSnapshotId()).isEqualTo(source.dvSnapshotId());
    assertThat(replaced.firstRowId()).isEqualTo(source.firstRowId());
  }

  @Test
  void testSourceDvPositionsAreNotCarriedForward() {
    TrackingStruct source = sourceTracking();
    source.set(DELETED_POSITIONS_ORDINAL, ByteBuffer.wrap(new byte[] {1, 2}));
    source.set(REPLACED_POSITIONS_ORDINAL, ByteBuffer.wrap(new byte[] {3, 4}));

    Tracking existing = TrackingStruct.builder(source, 1L).build();
    assertThat(existing.deletedPositions()).isNull();
    assertThat(existing.replacedPositions()).isNull();

    Tracking deleted = source.asDeleted(999L);
    assertThat(deleted.deletedPositions()).isNull();
    assertThat(deleted.replacedPositions()).isNull();

    Tracking replaced = source.asReplaced(999L);
    assertThat(replaced.deletedPositions()).isNull();
    assertThat(replaced.replacedPositions()).isNull();
  }

  @Test
  void testExistingBuilderAllowsDvMutation() {
    Tracking existing = TrackingStruct.builder(sourceTracking(), 999L).dvUpdated().build();
    assertThat(existing.dvSnapshotId()).isEqualTo(999L);
  }

  @Test
  void testMdvMutatorsRejectedOnAdded() {
    assertThatThrownBy(
            () -> TrackingStruct.added(42L).deletedPositions(ByteBuffer.wrap(new byte[] {1})))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set deleted positions on ADDED entry");

    assertThatThrownBy(
            () -> TrackingStruct.added(42L).replacedPositions(ByteBuffer.wrap(new byte[] {1})))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set replaced positions on ADDED entry");
  }

  @Test
  void testDvSnapshotIdAndMdvPositionsAreMutuallyExclusive() {
    // sourceTracking has dvSnapshotId=43, inherited by existing(source)
    assertThatThrownBy(
            () ->
                TrackingStruct.builder(sourceTracking(), 1L)
                    .deletedPositions(ByteBuffer.wrap(new byte[] {1})))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set deleted positions on a data file entry (DV snapshot ID is set)");

    assertThatThrownBy(
            () ->
                TrackingStruct.builder(sourceTracking(), 1L)
                    .replacedPositions(ByteBuffer.wrap(new byte[] {1})))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set replaced positions on a data file entry (DV snapshot ID is set)");

    // Setting MDV positions first then dvUpdated is also rejected
    assertThatThrownBy(
            () ->
                TrackingStruct.builder(manifestSourceTracking(), 1L)
                    .deletedPositions(ByteBuffer.wrap(new byte[] {1}))
                    .dvUpdated())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot mark DV updated on a manifest entry (deleted/replaced positions are set)");
  }

  @Test
  void testBuilderRejectsNullSource() {
    assertThatThrownBy(() -> TrackingStruct.builder(null, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid source tracking: null");
  }

  @Test
  void testSourceBuildersRejectSourceWithoutSequenceNumbers() {
    Tracking missingBoth = TrackingStruct.added(42L).build();

    assertThatThrownBy(() -> TrackingStruct.builder(missingBoth, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: data sequence number is null");

    assertThatThrownBy(() -> missingBoth.asDeleted(1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: data sequence number is null");

    assertThatThrownBy(() -> missingBoth.asReplaced(1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: data sequence number is null");

    TrackingStruct missingFileSeq = new TrackingStruct(Tracking.schema());
    missingFileSeq.set(STATUS_ORDINAL, EntryStatus.ADDED.id());
    missingFileSeq.set(SNAPSHOT_ID_ORDINAL, 42L);
    missingFileSeq.set(DATA_SEQUENCE_NUMBER_ORDINAL, 10L);

    assertThatThrownBy(() -> TrackingStruct.builder(missingFileSeq, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: file sequence number is null");

    assertThatThrownBy(() -> missingFileSeq.asDeleted(1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: file sequence number is null");

    assertThatThrownBy(() -> missingFileSeq.asReplaced(1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: file sequence number is null");
  }

  @Test
  void testRejectsTransitionsFromTerminalStatus() {
    Tracking deletedSource = sourceTrackingWithStatus(EntryStatus.DELETED);
    Tracking replacedSource = sourceTrackingWithStatus(EntryStatus.REPLACED);

    assertThatThrownBy(() -> TrackingStruct.builder(deletedSource, 1L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid status transition: DELETED -> EXISTING (DELETED is terminal)");

    assertThatThrownBy(() -> replacedSource.asDeleted(1L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid status transition: REPLACED -> DELETED (REPLACED is terminal)");
  }

  @Test
  void testExistingToExistingIsAllowed() {
    Tracking existingSource = sourceTrackingWithStatus(EntryStatus.EXISTING);

    Tracking existing = TrackingStruct.builder(existingSource, 1L).build();

    assertThat(existing.status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(existing.snapshotId()).isEqualTo(existingSource.snapshotId());
  }

  @Test
  void testProjectedStructLike() {
    // project only snapshot_id (field ID 1) and first_row_id (field ID 142)
    Types.StructType projection = Types.StructType.of(Tracking.SNAPSHOT_ID, Tracking.FIRST_ROW_ID);

    TrackingStruct tracking = new TrackingStruct(projection);
    assertThat(tracking.size()).isEqualTo(2);

    // projected position 0 maps to internal position 1 (snapshot_id)
    // projected position 1 maps to internal position 5 (first_row_id)
    tracking.set(0, 42L);
    tracking.set(1, 1000L);

    assertThat(tracking.snapshotId()).isEqualTo(42L);
    assertThat(tracking.firstRowId()).isEqualTo(1000L);
    assertThat(tracking.get(0, Long.class)).isEqualTo(42L);
    assertThat(tracking.get(1, Long.class)).isEqualTo(1000L);
  }

  @Test
  void testAddedWithDvSnapshotIdJavaSerializationRoundTrip()
      throws IOException, ClassNotFoundException {
    Tracking tracking = TrackingStruct.added(42L).dvUpdated().build();

    Tracking deserialized = TestHelpers.roundTripSerialize(tracking);

    assertThat(deserialized.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(deserialized.snapshotId()).isEqualTo(42L);
    assertThat(deserialized.dvSnapshotId()).isEqualTo(42L);
    assertThat(deserialized.deletedPositions()).isNull();
    assertThat(deserialized.replacedPositions()).isNull();
  }

  @Test
  void testExistingWithMdvPositionsJavaSerializationRoundTrip()
      throws IOException, ClassNotFoundException {
    Tracking tracking =
        TrackingStruct.builder(manifestSourceTracking(), 1L)
            .deletedPositions(ByteBuffer.wrap(new byte[] {1, 2}))
            .replacedPositions(ByteBuffer.wrap(new byte[] {3, 4}))
            .build();

    Tracking deserialized = TestHelpers.roundTripSerialize(tracking);

    assertThat(deserialized.status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(deserialized.dvSnapshotId()).isNull();
    assertThat(deserialized.deletedPositions()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2}));
    assertThat(deserialized.replacedPositions()).isEqualTo(ByteBuffer.wrap(new byte[] {3, 4}));
  }

  @Test
  void testAddedWithDvSnapshotIdKryoSerializationRoundTrip() throws IOException {
    Tracking tracking = TrackingStruct.added(42L).dvUpdated().build();

    Tracking deserialized = TestHelpers.KryoHelpers.roundTripSerialize(tracking);

    assertThat(deserialized.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(deserialized.snapshotId()).isEqualTo(42L);
    assertThat(deserialized.dvSnapshotId()).isEqualTo(42L);
    assertThat(deserialized.deletedPositions()).isNull();
    assertThat(deserialized.replacedPositions()).isNull();
  }

  @Test
  void testExistingWithMdvPositionsKryoSerializationRoundTrip() throws IOException {
    Tracking tracking =
        TrackingStruct.builder(manifestSourceTracking(), 1L)
            .deletedPositions(ByteBuffer.wrap(new byte[] {1, 2}))
            .replacedPositions(ByteBuffer.wrap(new byte[] {3, 4}))
            .build();

    Tracking deserialized = TestHelpers.KryoHelpers.roundTripSerialize(tracking);

    assertThat(deserialized.status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(deserialized.dvSnapshotId()).isNull();
    assertThat(deserialized.deletedPositions()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2}));
    assertThat(deserialized.replacedPositions()).isEqualTo(ByteBuffer.wrap(new byte[] {3, 4}));
  }

  private static TrackingStruct sourceTracking() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(STATUS_ORDINAL, EntryStatus.ADDED.id());
    tracking.set(SNAPSHOT_ID_ORDINAL, 42L);
    tracking.set(DATA_SEQUENCE_NUMBER_ORDINAL, 10L);
    tracking.set(FILE_SEQUENCE_NUMBER_ORDINAL, 10L);
    tracking.set(DV_SNAPSHOT_ID_ORDINAL, 43L);
    tracking.set(FIRST_ROW_ID_ORDINAL, 1000L);
    return tracking;
  }

  private static TrackingStruct sourceTrackingWithStatus(EntryStatus status) {
    TrackingStruct tracking = sourceTracking();
    tracking.set(STATUS_ORDINAL, status.id());
    return tracking;
  }

  private static TrackingStruct manifestSourceTracking() {
    TrackingStruct tracking = sourceTracking();
    tracking.set(DV_SNAPSHOT_ID_ORDINAL, null);
    return tracking;
  }
}
