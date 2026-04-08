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
  void setters() {
    TrackingStruct tracking = new TrackingStruct(Types.StructType.of());

    tracking.set(0, EntryStatus.ADDED.id());
    tracking.setSnapshotId(100L);
    tracking.setSequenceNumber(200L);
    tracking.setFirstRowId(300L);

    assertThat(tracking.snapshotId()).isEqualTo(100L);
    assertThat(tracking.dataSequenceNumber()).isEqualTo(200L);
    assertThat(tracking.firstRowId()).isEqualTo(300L);
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
