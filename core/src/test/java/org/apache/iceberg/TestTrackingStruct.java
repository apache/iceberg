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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestTrackingStruct {

  @Test
  void fieldAccess() {
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());

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
  void copy() {
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());

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

  @Test
  void allStatuses() {
    for (EntryStatus status : EntryStatus.values()) {
      TrackingStruct tracking = new TrackingStruct(Types.StructType.of());
      tracking.set(0, status.id());
      assertThat(tracking.status()).isEqualTo(status);
    }
  }

  @Test
  void isLive() {
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());

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
  void inheritSnapshotIdFromManifestContext() {
    TrackedFile manifest = createManifestContext(100L, 50L);
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());
    tracking.setManifestContext(manifest);
    tracking.set(0, EntryStatus.ADDED.id());

    // snapshotId is null, should inherit from manifest
    assertThat(tracking.snapshotId()).isEqualTo(100L);
  }

  @Test
  void inheritSequenceNumberForAddedEntries() {
    TrackedFile manifest = createManifestContext(100L, 50L);
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());
    tracking.setManifestContext(manifest);
    tracking.set(0, EntryStatus.ADDED.id());

    // sequence numbers are null and status is ADDED, should inherit
    assertThat(tracking.dataSequenceNumber()).isEqualTo(50L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(50L);
  }

  @Test
  void doNotInheritSequenceNumberForExistingEntries() {
    TrackedFile manifest = createManifestContext(100L, 50L);
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());
    tracking.setManifestContext(manifest);
    tracking.set(0, EntryStatus.EXISTING.id());

    // sequence numbers are null but status is EXISTING, should not inherit
    assertThat(tracking.dataSequenceNumber()).isNull();
    assertThat(tracking.fileSequenceNumber()).isNull();
  }

  @Test
  void explicitValuesOverrideManifestContext() {
    TrackedFile manifest = createManifestContext(100L, 50L);
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());
    tracking.setManifestContext(manifest);
    tracking.set(0, EntryStatus.ADDED.id());
    tracking.set(1, 200L);
    tracking.set(2, 75L);
    tracking.set(3, 76L);

    // explicit values should take precedence
    assertThat(tracking.snapshotId()).isEqualTo(200L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(75L);
    assertThat(tracking.fileSequenceNumber()).isEqualTo(76L);
  }

  @Test
  void noDefaultingWithoutManifestContext() {
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());
    tracking.set(0, EntryStatus.ADDED.id());

    // no manifest context, nulls stay null
    assertThat(tracking.snapshotId()).isNull();
    assertThat(tracking.dataSequenceNumber()).isNull();
    assertThat(tracking.fileSequenceNumber()).isNull();
  }

  private static TrackedFile createManifestContext(long snapshotId, long sequenceNumber) {
    TrackedFileStruct manifest = new TrackedFileStruct();
    TrackingStruct manifestTracking = new TrackingStruct(Types.StructType.of());
    manifestTracking.set(0, EntryStatus.ADDED.id());
    manifestTracking.set(1, snapshotId);
    manifestTracking.set(2, sequenceNumber);
    manifest.set(0, manifestTracking);
    manifest.set(1, FileContent.DATA_MANIFEST.id());
    manifest.set(2, "s3://bucket/manifest.avro");
    manifest.set(3, "avro");
    manifest.set(4, 0L);
    manifest.set(5, 0L);
    return manifest;
  }

  @Test
  void javaSerializationRoundTrip() throws IOException, ClassNotFoundException {
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());
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
  void kryoSerializationRoundTrip() throws IOException {
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());
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
