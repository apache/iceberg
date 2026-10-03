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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestGenericIndexMetadata {

  private static final String TABLE_UUID = "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94";
  private static final String LOCATION = "s3://warehouse/db/orders/index/order_id_idx";

  private GenericIndexMetadata.Builder baseBuilder() {
    return GenericIndexMetadata.builder()
        .tableUuid(TABLE_UUID)
        .location(LOCATION)
        .type("SCALAR")
        .transformFunction("HASH")
        .keyColumnIds(ImmutableList.of(3));
  }

  private IndexSnapshot snapshot(long snapshotId, long sourceTableSnapshotId) {
    return GenericIndexSnapshot.builder()
        .snapshotId(snapshotId)
        .sourceTableSnapshotId(sourceTableSnapshotId)
        .timestampMs(1735689600000L)
        .trackingFile("s3://.../tracking-" + snapshotId + ".avro")
        .build();
  }

  @Test
  void removeSnapshotDropsNonCurrentSnapshot() {
    IndexMetadata metadata =
        baseBuilder().addSnapshot(snapshot(1L, 1000L)).addSnapshot(snapshot(2L, 2000L)).build();
    assertThat(metadata.currentSnapshotId()).isEqualTo(2L);

    IndexMetadata updated = GenericIndexMetadata.buildFrom(metadata).removeSnapshot(1L).build();

    assertThat(updated.snapshots()).hasSize(1);
    assertThat(updated.snapshots().get(0).snapshotId()).isEqualTo(2L);
    // Removing a non-current snapshot must not change currentSnapshotId.
    assertThat(updated.currentSnapshotId()).isEqualTo(2L);
  }

  @Test
  void removeSnapshotReassignsCurrentWhenCurrentIsRemoved() {
    IndexMetadata metadata =
        baseBuilder().addSnapshot(snapshot(1L, 1000L)).addSnapshot(snapshot(2L, 2000L)).build();
    assertThat(metadata.currentSnapshotId()).isEqualTo(2L);

    IndexMetadata updated = GenericIndexMetadata.buildFrom(metadata).removeSnapshot(2L).build();

    assertThat(updated.snapshots()).hasSize(1);
    assertThat(updated.snapshots().get(0).snapshotId()).isEqualTo(1L);
    // Removing the current snapshot falls back to the last remaining snapshot.
    assertThat(updated.currentSnapshotId()).isEqualTo(1L);
  }

  @Test
  void removeSnapshotClearsCurrentWhenLastSnapshotRemoved() {
    IndexMetadata metadata = baseBuilder().addSnapshot(snapshot(1L, 1000L)).build();
    assertThat(metadata.currentSnapshotId()).isEqualTo(1L);

    IndexMetadata updated = GenericIndexMetadata.buildFrom(metadata).removeSnapshot(1L).build();

    assertThat(updated.snapshots()).isEmpty();
    assertThat(updated.currentSnapshotId()).isNull();
  }

  @Test
  void removeSnapshotOfUnknownIdIsNoOp() {
    IndexMetadata metadata =
        baseBuilder().addSnapshot(snapshot(1L, 1000L)).addSnapshot(snapshot(2L, 2000L)).build();

    IndexMetadata updated = GenericIndexMetadata.buildFrom(metadata).removeSnapshot(999L).build();

    assertThat(updated.snapshots()).hasSize(2);
    assertThat(updated.currentSnapshotId()).isEqualTo(2L);
  }
}
