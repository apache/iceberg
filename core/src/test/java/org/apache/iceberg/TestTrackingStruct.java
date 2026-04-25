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
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestTrackingStruct {

  @Test
  void testFieldAccess() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);
    tracking.set(2, 10L);
    tracking.set(3, 11L);
    tracking.set(4, 43L);
    tracking.set(5, 1000L);

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
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);
    tracking.set(2, 10L);
    tracking.set(6, ByteBuffer.wrap(new byte[] {1, 2}));

    TrackingStruct copy = tracking.copy();

    assertThat(copy.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(copy.snapshotId()).isEqualTo(42L);
    assertThat(copy.dataSequenceNumber()).isEqualTo(10L);
    assertThat(copy.deletedPositions()).isNotNull();

    // verify deep copy of ByteBuffer
    assertThat(copy.deletedPositions()).isNotSameAs(tracking.deletedPositions());
  }

  @ParameterizedTest
  @EnumSource(EntryStatus.class)
  void testAllStatuses(EntryStatus status) {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(0, status.id());
    assertThat(tracking.status()).isEqualTo(status);
  }

  @Test
  void testIsLive() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());

    tracking.set(0, EntryStatus.ADDED.id());
    assertThat(tracking.isLive()).isTrue();

    tracking.set(0, EntryStatus.EXISTING.id());
    assertThat(tracking.isLive()).isTrue();

    tracking.set(0, EntryStatus.DELETED.id());
    assertThat(tracking.isLive()).isFalse();

    tracking.set(0, EntryStatus.REPLACED.id());
    assertThat(tracking.isLive()).isFalse();
  }

  @Test
  void testInheritSnapshotId() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // snapshotId is null, should inherit from manifest
    assertThat(tracking.snapshotId()).isEqualTo(100L);
  }

  @Test
  void testInheritSequenceNumberForAddedEntries() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // sequence numbers are null and status is ADDED, should inherit
    assertThat(tracking.dataSequenceNumber()).isEqualTo(60L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(60L);
  }

  @Test
  void testDoNotInheritSequenceNumberForExistingEntries() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(0, EntryStatus.EXISTING.id());
    tracking.set(2, 5L);
    tracking.set(3, 6L);
    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // sequence numbers are not inherited for EXISTING entries
    assertThat(tracking.dataSequenceNumber()).isEqualTo(5L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(6L);
  }

  @Test
  void testExplicitValuesOverrideInheritance() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 200L);
    tracking.set(2, 75L);
    tracking.set(3, 76L);
    tracking.inheritFrom(createManifestTracking(100L, 60L));

    // explicit values should take precedence
    assertThat(tracking.snapshotId()).isEqualTo(200L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(75L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(76L);
  }

  @Test
  void testInheritFromRejectsUnequalSequenceNumbers() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(0, EntryStatus.ADDED.id());

    TrackingStruct manifestTracking = new TrackingStruct(Tracking.schema());
    manifestTracking.set(0, EntryStatus.ADDED.id());
    manifestTracking.set(1, 100L);
    manifestTracking.set(2, 50L);
    manifestTracking.set(3, 60L);

    assertThatThrownBy(() -> tracking.inheritFrom(manifestTracking))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Manifest data and file sequence numbers must be equal, got 50 and 60");
  }

  @Test
  void testNoDefaultingWithoutInheritance() {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(0, EntryStatus.ADDED.id());

    // no inheritance, nulls stay null
    assertThat(tracking.snapshotId()).isNull();
    assertThat(tracking.dataSequenceNumber()).isNull();
    assertThat(tracking.fileSequenceNumber()).isNull();
  }

  private static Tracking createManifestTracking(long snapshotId, long sequenceNumber) {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, snapshotId);
    tracking.set(2, sequenceNumber);
    tracking.set(3, sequenceNumber);
    return tracking;
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
  void testJavaSerializationRoundTrip() throws IOException, ClassNotFoundException {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);
    tracking.set(2, 10L);
    tracking.set(6, ByteBuffer.wrap(new byte[] {1, 2}));

    TrackingStruct deserialized = TestHelpers.roundTripSerialize(tracking);

    assertThat(deserialized.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(deserialized.snapshotId()).isEqualTo(42L);
    assertThat(deserialized.dataSequenceNumber()).isEqualTo(10L);
    assertThat(deserialized.deletedPositions()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2}));
  }

  @Test
  void testKryoSerializationRoundTrip() throws IOException {
    TrackingStruct tracking = new TrackingStruct(Tracking.schema());
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 42L);
    tracking.set(2, 10L);
    tracking.set(6, ByteBuffer.wrap(new byte[] {1, 2}));

    TrackingStruct deserialized = TestHelpers.KryoHelpers.roundTripSerialize(tracking);

    assertThat(deserialized.status()).isEqualTo(EntryStatus.ADDED);
    assertThat(deserialized.snapshotId()).isEqualTo(42L);
    assertThat(deserialized.dataSequenceNumber()).isEqualTo(10L);
    assertThat(deserialized.deletedPositions()).isEqualTo(ByteBuffer.wrap(new byte[] {1, 2}));
  }
}
