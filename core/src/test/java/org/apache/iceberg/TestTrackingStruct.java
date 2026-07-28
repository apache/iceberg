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
import org.junit.jupiter.params.provider.MethodSource;

class TestTrackingStruct {
  private static final byte[] DELETED_POSITIONS = new byte[] {1, 2};
  private static final byte[] REPLACED_POSITIONS = new byte[] {3, 4};

  private static final int MANIFEST_POSITION_ORDINAL = Tracking.schema().fields().size();

  @Test
  void testFieldAccess() {
    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.ADDED, 42L, 10L, 11L, 43L, 1000L, DELETED_POSITIONS, REPLACED_POSITIONS);
    tracking.setManifestLocation("manifest-location");
    tracking.set(MANIFEST_POSITION_ORDINAL, 7L);

    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(42L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(10L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(11L);
    assertThat(tracking.dvSnapshotId()).isEqualTo(43L);
    assertThat(tracking.firstRowId()).isEqualTo(1000L);
    assertThat(tracking.deletedPositions()).isEqualTo(ByteBuffer.wrap(DELETED_POSITIONS));
    assertThat(tracking.replacedPositions()).isEqualTo(ByteBuffer.wrap(REPLACED_POSITIONS));
    assertThat(tracking.manifestLocation()).isEqualTo("manifest-location");
    assertThat(tracking.manifestPos()).isEqualTo(7L);
  }

  @Test
  void testSetByPosition() {
    TrackingStruct tracking = new TrackingStruct();

    tracking.set(pos("status"), EntryStatus.ADDED.id());
    tracking.set(pos("snapshot_id"), 42L);
    tracking.set(pos("sequence_number"), 10L);
    tracking.set(pos("file_sequence_number"), 11L);
    tracking.set(pos("dv_snapshot_id"), 43L);
    tracking.set(pos("first_row_id"), 1000L);
    tracking.set(pos("deleted_positions"), ByteBuffer.wrap(DELETED_POSITIONS));
    tracking.set(pos("replaced_positions"), ByteBuffer.wrap(REPLACED_POSITIONS));
    tracking.set(MANIFEST_POSITION_ORDINAL, 7L);

    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(42L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(10L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(11L);
    assertThat(tracking.dvSnapshotId()).isEqualTo(43L);
    assertThat(tracking.firstRowId()).isEqualTo(1000L);
    assertThat(tracking.deletedPositions()).isEqualTo(ByteBuffer.wrap(DELETED_POSITIONS));
    assertThat(tracking.replacedPositions()).isEqualTo(ByteBuffer.wrap(REPLACED_POSITIONS));
    assertThat(tracking.manifestPos()).isEqualTo(7L);
  }

  @Test
  void testGetByPosition() {
    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.ADDED, 42L, 10L, 11L, 43L, 1000L, DELETED_POSITIONS, REPLACED_POSITIONS);
    tracking.setManifestLocation("manifest-location");
    tracking.set(MANIFEST_POSITION_ORDINAL, 7L);

    assertThat(tracking.get(pos("status"), Integer.class)).isEqualTo(EntryStatus.ADDED.id());
    assertThat(tracking.get(pos("snapshot_id"), Long.class)).isEqualTo(42L);
    assertThat(tracking.get(pos("sequence_number"), Long.class)).isEqualTo(10L);
    assertThat(tracking.get(pos("file_sequence_number"), Long.class)).isEqualTo(11L);
    assertThat(tracking.get(pos("dv_snapshot_id"), Long.class)).isEqualTo(43L);
    assertThat(tracking.get(pos("first_row_id"), Long.class)).isEqualTo(1000L);
    assertThat(tracking.get(pos("deleted_positions"), ByteBuffer.class))
        .isEqualTo(ByteBuffer.wrap(DELETED_POSITIONS));
    assertThat(tracking.get(pos("replaced_positions"), ByteBuffer.class))
        .isEqualTo(ByteBuffer.wrap(REPLACED_POSITIONS));
    assertThat(tracking.get(MANIFEST_POSITION_ORDINAL, Long.class)).isEqualTo(7L);
  }

  @Test
  void testCopy() {
    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.MODIFIED, 42L, 10L, 11L, 43L, 1000L, DELETED_POSITIONS, REPLACED_POSITIONS);
    tracking.setManifestLocation("manifest-location");
    tracking.set(MANIFEST_POSITION_ORDINAL, 7L);

    Tracking copy = tracking.copy();

    assertThat(copy.status()).isEqualTo(EntryStatus.MODIFIED);
    assertThat(copy.snapshotId()).isEqualTo(tracking.snapshotId());
    assertThat(copy.dataSequenceNumber()).isEqualTo(tracking.dataSequenceNumber());
    assertThat(copy.fileSequenceNumber()).isEqualTo(tracking.fileSequenceNumber());
    assertThat(copy.dvSnapshotId()).isEqualTo(tracking.dvSnapshotId());
    assertThat(copy.firstRowId()).isEqualTo(tracking.firstRowId());
    assertThat(copy.deletedPositions()).isEqualTo(tracking.deletedPositions());
    assertThat(copy.replacedPositions()).isEqualTo(tracking.replacedPositions());
    assertThat(copy.manifestLocation()).isEqualTo(tracking.manifestLocation());
    assertThat(copy.manifestPos()).isEqualTo(tracking.manifestPos());

    // verify deep copy of ByteBuffer backing arrays
    assertThat(copy.deletedPositions().array()).isNotSameAs(tracking.deletedPositions().array());
    assertThat(copy.replacedPositions().array()).isNotSameAs(tracking.replacedPositions().array());
  }

  @Test
  void testInheritSnapshotId() {
    TrackingStruct tracking =
        new TrackingStruct(EntryStatus.ADDED, null, null, null, null, null, null, null);

    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // snapshotId is null, should inherit from manifest
    assertThat(tracking.snapshotId()).isEqualTo(100L);
  }

  @Test
  void testInheritSequenceNumberForAddedEntries() {
    TrackingStruct tracking =
        new TrackingStruct(EntryStatus.ADDED, 42L, null, null, null, null, null, null);

    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // sequence numbers are null and status is ADDED, should inherit
    assertThat(tracking.dataSequenceNumber()).isEqualTo(60L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(60L);
  }

  @Test
  void testDoNotInheritSequenceNumberForExistingEntries() {
    TrackingStruct tracking =
        new TrackingStruct(EntryStatus.EXISTING, 42L, 5L, 6L, null, null, null, null);

    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // sequence numbers are not inherited for EXISTING entries
    assertThat(tracking.dataSequenceNumber()).isEqualTo(5L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(6L);
  }

  @Test
  void testDoNotInheritSequenceNumberForModifiedEntries() {
    TrackingStruct tracking =
        new TrackingStruct(EntryStatus.MODIFIED, 42L, 5L, 6L, null, null, null, null);

    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // sequence numbers are not inherited for MODIFIED entries
    assertThat(tracking.dataSequenceNumber()).isEqualTo(5L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(6L);
  }

  @Test
  void testExplicitValuesOverrideInheritance() {
    TrackingStruct tracking =
        new TrackingStruct(EntryStatus.ADDED, 200L, 75L, 76L, null, null, null, null);

    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // explicit values should take precedence
    assertThat(tracking.snapshotId()).isEqualTo(200L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(75L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(76L);
  }

  @Test
  void testInheritFromRejectsUnequalSequenceNumbers() {
    TrackingStruct tracking =
        new TrackingStruct(EntryStatus.ADDED, 42L, null, null, null, null, null, null);

    TrackingStruct manifestTracking =
        new TrackingStruct(EntryStatus.ADDED, 100L, 50L, 60L, null, null, null, null);

    assertThatThrownBy(() -> tracking.inheritFrom(manifestTracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Manifest data and file sequence numbers must be equal, got 50 and 60");
  }

  @Test
  void testNoDefaultingWithoutInheritance() {
    TrackingStruct tracking =
        new TrackingStruct(EntryStatus.ADDED, null, null, null, null, null, null, null);

    // no inheritance, nulls stay null
    assertThat(tracking.snapshotId()).isNull();
    assertThat(tracking.dataSequenceNumber()).isNull();
    assertThat(tracking.fileSequenceNumber()).isNull();
  }

  @Test
  void testInheritFromNullIsNoOp() {
    TrackingStruct tracking =
        new TrackingStruct(EntryStatus.ADDED, null, null, null, null, null, null, null);

    tracking.inheritFrom(null);

    // null source is a no-op; all unset fields stay null
    assertThat(tracking.snapshotId()).isNull();
    assertThat(tracking.dataSequenceNumber()).isNull();
    assertThat(tracking.fileSequenceNumber()).isNull();
  }

  private static Tracking createManifestTracking(long snapshotId, long sequenceNumber) {
    return new TrackingStruct(
        EntryStatus.ADDED, snapshotId, sequenceNumber, sequenceNumber, null, null, null, null);
  }

  @ParameterizedTest
  @EnumSource(EntryStatus.class)
  void testIsLiveDelegatesToStatus(EntryStatus status) {
    TrackingStruct tracking = new TrackingStruct(status, null, null, null, null, null, null, null);

    assertThat(tracking.isLive()).isEqualTo(status.isLive());
  }

  @Test
  void testInternalSetIgnoresUnknownOrdinal() {
    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.ADDED, 42L, 10L, 11L, 43L, 1000L, DELETED_POSITIONS, REPLACED_POSITIONS);

    // unknown ordinals from a newer format version are silently ignored
    tracking.internalSet(99, "value from a newer format");

    // every field is unchanged
    assertThat(tracking.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(tracking.snapshotId()).isEqualTo(42L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(10L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(11L);
    assertThat(tracking.dvSnapshotId()).isEqualTo(43L);
    assertThat(tracking.firstRowId()).isEqualTo(1000L);
    assertThat(tracking.deletedPositions()).isEqualTo(ByteBuffer.wrap(DELETED_POSITIONS));
    assertThat(tracking.replacedPositions()).isEqualTo(ByteBuffer.wrap(REPLACED_POSITIONS));
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

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  void testSerializationRoundTrip(TestHelpers.RoundTripSerializer<Tracking> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    TrackingStruct tracking =
        new TrackingStruct(
            EntryStatus.MODIFIED, 42L, 10L, 11L, 43L, 1000L, DELETED_POSITIONS, REPLACED_POSITIONS);
    tracking.setManifestLocation("manifest-location");
    tracking.set(MANIFEST_POSITION_ORDINAL, 7L);

    Tracking deserialized = roundTripSerializer.apply(tracking);

    assertThat(deserialized.status()).isEqualTo(EntryStatus.MODIFIED);
    assertThat(deserialized.snapshotId()).isEqualTo(42L);
    assertThat(deserialized.dataSequenceNumber()).isEqualTo(10L);
    assertThat(deserialized.fileSequenceNumber()).isEqualTo(11L);
    assertThat(deserialized.dvSnapshotId()).isEqualTo(43L);
    assertThat(deserialized.firstRowId()).isEqualTo(1000L);
    assertThat(deserialized.deletedPositions()).isEqualTo(ByteBuffer.wrap(DELETED_POSITIONS));
    assertThat(deserialized.replacedPositions()).isEqualTo(ByteBuffer.wrap(REPLACED_POSITIONS));
    assertThat(deserialized.manifestLocation()).isEqualTo("manifest-location");
    assertThat(deserialized.manifestPos()).isEqualTo(7L);
  }

  // Returns the positions of fields within the Tracking schema by name. For internal fields like
  // '_pos' introduce a constant instead of using this function.
  private static int pos(String fieldName) {
    List<Types.NestedField> fields = Tracking.schema().fields();
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).name().equals(fieldName)) {
        return i;
      }
    }

    throw new IllegalArgumentException("Field not found in schema: " + fieldName);
  }
}
