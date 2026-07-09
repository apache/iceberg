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

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestTrackingBuilder {

  // Tests aren't expected to change these fields
  private static final Tracking SOURCE_TRACKING_ADDED =
      new TrackingStruct(EntryStatus.ADDED, 42L, 10L, 10L, 43L, 1000L, null, null);
  private static final Tracking SOURCE_TRACKING_EXISTING =
      new TrackingStruct(EntryStatus.EXISTING, 42L, 10L, 10L, 43L, 1000L, null, null);
  private static final Tracking SOURCE_TRACKING_MODIFIED =
      new TrackingStruct(EntryStatus.MODIFIED, 42L, 10L, 10L, 43L, 1000L, null, null);

  @Test
  void testAddedWithSameCommitDvStaysAdded() {
    Tracking tracking = TrackingBuilder.added(42L).dvUpdated().build();

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
    Tracking existing = TrackingBuilder.from(SOURCE_TRACKING_ADDED, 1L).build();

    assertThat(existing.status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(existing.snapshotId()).isEqualTo(SOURCE_TRACKING_ADDED.snapshotId());
    assertThat(existing.dataSequenceNumber()).isEqualTo(SOURCE_TRACKING_ADDED.dataSequenceNumber());
    assertThat(existing.fileSequenceNumber()).isEqualTo(SOURCE_TRACKING_ADDED.fileSequenceNumber());
    assertThat(existing.dvSnapshotId()).isEqualTo(SOURCE_TRACKING_ADDED.dvSnapshotId());
    assertThat(existing.firstRowId()).isEqualTo(SOURCE_TRACKING_ADDED.firstRowId());
  }

  @Test
  void testDeleteUpdatesSnapshotIdAndPreservesRest() {
    Tracking deleted = TrackingBuilder.deleted(SOURCE_TRACKING_ADDED, 999L);

    assertThat(deleted.status()).isEqualTo(EntryStatus.DELETED);
    assertThat(deleted.snapshotId()).isEqualTo(999L);
    assertThat(deleted.dataSequenceNumber()).isEqualTo(SOURCE_TRACKING_ADDED.dataSequenceNumber());
    assertThat(deleted.fileSequenceNumber()).isEqualTo(SOURCE_TRACKING_ADDED.fileSequenceNumber());
    assertThat(deleted.dvSnapshotId()).isEqualTo(SOURCE_TRACKING_ADDED.dvSnapshotId());
    assertThat(deleted.firstRowId()).isEqualTo(SOURCE_TRACKING_ADDED.firstRowId());
  }

  @Test
  void testReplaceUpdatesSnapshotIdAndPreservesRest() {
    Tracking replaced = TrackingBuilder.replaced(SOURCE_TRACKING_ADDED, 999L);

    assertThat(replaced.status()).isEqualTo(EntryStatus.REPLACED);
    assertThat(replaced.snapshotId()).isEqualTo(999L);
    assertThat(replaced.dataSequenceNumber()).isEqualTo(SOURCE_TRACKING_ADDED.dataSequenceNumber());
    assertThat(replaced.fileSequenceNumber()).isEqualTo(SOURCE_TRACKING_ADDED.fileSequenceNumber());
    assertThat(replaced.dvSnapshotId()).isEqualTo(SOURCE_TRACKING_ADDED.dvSnapshotId());
    assertThat(replaced.firstRowId()).isEqualTo(SOURCE_TRACKING_ADDED.firstRowId());
  }

  @Test
  void testSourceDvPositionsAreNotCarriedForward() {
    Tracking source =
        new TrackingStruct(
            EntryStatus.ADDED, 42L, 10L, 10L, 43L, 1000L, new byte[] {1, 2}, new byte[] {3, 4});

    Tracking existing = TrackingBuilder.from(source, 1L).build();
    assertThat(existing.deletedPositions()).isNull();
    assertThat(existing.replacedPositions()).isNull();

    Tracking deleted = TrackingBuilder.deleted(source, 999L);
    assertThat(deleted.deletedPositions()).isNull();
    assertThat(deleted.replacedPositions()).isNull();

    Tracking replaced = TrackingBuilder.replaced(source, 999L);
    assertThat(replaced.deletedPositions()).isNull();
    assertThat(replaced.replacedPositions()).isNull();
  }

  @Test
  void testDvUpdatedProducesModifiedAndAdvancesDvSnapshotId() {
    Tracking modified = TrackingBuilder.from(SOURCE_TRACKING_ADDED, 999L).dvUpdated().build();

    assertThat(modified.status()).isEqualTo(EntryStatus.MODIFIED);
    // the entry snapshot id is preserved so we still know when the base file was added
    assertThat(modified.snapshotId())
        .isEqualTo(SOURCE_TRACKING_ADDED.snapshotId())
        .isNotEqualTo(999L);
    // only the DV snapshot id advances to the commit snapshot
    assertThat(modified.dvSnapshotId()).isEqualTo(999L);
  }

  @Test
  void testManifestDVMutatorsRejectedOnAdded() {
    assertThatThrownBy(
            () -> TrackingBuilder.added(42L).deletedPositions(ByteBuffer.wrap(new byte[] {1})))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set deleted positions on ADDED entry");

    assertThatThrownBy(
            () -> TrackingBuilder.added(42L).replacedPositions(ByteBuffer.wrap(new byte[] {1})))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot set replaced positions on ADDED entry");
  }

  @Test
  void testDvUpdatedRejectedWhenManifestPositionsSet() {
    assertThatThrownBy(
            () ->
                TrackingBuilder.from(SOURCE_TRACKING_ADDED, 999L)
                    .deletedPositions(ByteBuffer.wrap(new byte[] {1}))
                    .dvUpdated())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot mark DV updated on a manifest entry (deleted/replaced positions are set)");

    assertThatThrownBy(
            () ->
                TrackingBuilder.from(SOURCE_TRACKING_ADDED, 999L)
                    .replacedPositions(ByteBuffer.wrap(new byte[] {1}))
                    .dvUpdated())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot mark DV updated on a manifest entry (deleted/replaced positions are set)");
  }

  @Test
  void testBuilderRejectsNullSource() {
    assertThatThrownBy(() -> TrackingBuilder.from(null, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid source tracking: null");
  }

  @Test
  void testSourceBuildersRejectSourceWithoutSequenceNumbers() {
    Tracking missingBoth = TrackingBuilder.added(42L).build();

    assertThatThrownBy(() -> TrackingBuilder.from(missingBoth, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: data sequence number is null");

    assertThatThrownBy(() -> TrackingBuilder.deleted(missingBoth, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: data sequence number is null");

    assertThatThrownBy(() -> TrackingBuilder.replaced(missingBoth, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: data sequence number is null");

    TrackingStruct missingFileSeq =
        new TrackingStruct(EntryStatus.ADDED, 42L, 10L, null, null, null, null, null);

    assertThatThrownBy(() -> TrackingBuilder.from(missingFileSeq, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: file sequence number is null");

    assertThatThrownBy(() -> TrackingBuilder.deleted(missingFileSeq, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: file sequence number is null");

    assertThatThrownBy(() -> TrackingBuilder.replaced(missingFileSeq, 1L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid tracking source: file sequence number is null");
  }

  private static Stream<Arguments> terminalTransitionCases() {
    Consumer<Tracking> builderCall = source -> TrackingBuilder.from(source, 1L);
    Consumer<Tracking> deletedCall = source -> TrackingBuilder.deleted(source, 1L);
    Consumer<Tracking> replacedCall = source -> TrackingBuilder.replaced(source, 1L);
    return Stream.of(
        Arguments.of(EntryStatus.DELETED, builderCall),
        Arguments.of(EntryStatus.DELETED, deletedCall),
        Arguments.of(EntryStatus.DELETED, replacedCall),
        Arguments.of(EntryStatus.REPLACED, builderCall),
        Arguments.of(EntryStatus.REPLACED, deletedCall),
        Arguments.of(EntryStatus.REPLACED, replacedCall));
  }

  @ParameterizedTest
  @MethodSource("terminalTransitionCases")
  void testRejectsTransitionsFromTerminalStatus(
      EntryStatus sourceStatus, Consumer<Tracking> factoryCall) {
    Tracking source = new TrackingStruct(sourceStatus, 42L, 10L, 10L, 43L, 1000L, null, null);

    assertThatThrownBy(() -> factoryCall.accept(source))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot revive non-live entry with status " + sourceStatus);
  }

  @Test
  void testExistingToExistingIsAllowed() {
    Tracking existing = TrackingBuilder.from(SOURCE_TRACKING_EXISTING, 1L).build();

    assertThat(existing.status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(existing.snapshotId()).isEqualTo(SOURCE_TRACKING_EXISTING.snapshotId());
  }

  @Test
  void testExistingToTerminalTransitions() {
    Tracking deleted = TrackingBuilder.deleted(SOURCE_TRACKING_EXISTING, 999L);
    assertThat(deleted.status()).isEqualTo(EntryStatus.DELETED);
    assertThat(deleted.snapshotId()).isEqualTo(999L);

    Tracking replaced = TrackingBuilder.replaced(SOURCE_TRACKING_EXISTING, 999L);
    assertThat(replaced.status()).isEqualTo(EntryStatus.REPLACED);
    assertThat(replaced.snapshotId()).isEqualTo(999L);
  }

  @Test
  void testExistingPreservesSourceSnapshotId() {
    Tracking existing = TrackingBuilder.from(SOURCE_TRACKING_ADDED, 999L).build();
    assertThat(existing.status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(existing.snapshotId())
        .isEqualTo(SOURCE_TRACKING_ADDED.snapshotId())
        .isNotEqualTo(999L);
  }

  @Test
  void testCarryForwardFromModifiedSourceChangesToExisting() {
    Tracking carried = TrackingBuilder.from(SOURCE_TRACKING_MODIFIED, 999L).build();
    assertThat(carried.status()).isEqualTo(EntryStatus.EXISTING);
    assertThat(carried.snapshotId())
        .isEqualTo(SOURCE_TRACKING_MODIFIED.snapshotId())
        .isNotEqualTo(999L);
    assertThat(carried.dvSnapshotId())
        .isEqualTo(SOURCE_TRACKING_MODIFIED.dvSnapshotId())
        .isNotEqualTo(999L);
    assertThat(carried.dataSequenceNumber())
        .isEqualTo(SOURCE_TRACKING_MODIFIED.dataSequenceNumber());
    assertThat(carried.fileSequenceNumber())
        .isEqualTo(SOURCE_TRACKING_MODIFIED.fileSequenceNumber());
    assertThat(carried.firstRowId()).isEqualTo(SOURCE_TRACKING_MODIFIED.firstRowId());
  }

  @Test
  void testManifestDVPositionsProduceModified() {
    ByteBuffer deletedBytes = ByteBuffer.wrap(new byte[] {1, 2});

    Tracking modified =
        TrackingBuilder.from(SOURCE_TRACKING_ADDED, 999L).deletedPositions(deletedBytes).build();
    assertThat(modified.status()).isEqualTo(EntryStatus.MODIFIED);
    // the entry snapshot id is preserved; only the DV snapshot id advances to the commit snapshot
    assertThat(modified.snapshotId())
        .isEqualTo(SOURCE_TRACKING_ADDED.snapshotId())
        .isNotEqualTo(999L);
    assertThat(modified.dvSnapshotId()).isEqualTo(999L);
    assertThat(modified.deletedPositions()).isEqualTo(deletedBytes);
  }
}
