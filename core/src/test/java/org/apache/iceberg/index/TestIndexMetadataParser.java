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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestIndexMetadataParser {

  private static final String TABLE_UUID = "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94";
  private static final String INDEX_UUID = "9c12d441-03fe-4693-9a96-a0705ddf69c1";
  private static final String LOCATION = "s3://warehouse/db/orders/index/order_id_idx";
  private static final String TRACKING_FILE =
      "s3://warehouse/db/orders/index/order_id_idx/metadata/tracking-00001.parquet";

  @Test
  public void roundTripNoSnapshots() {
    IndexMetadata metadata =
        GenericIndexMetadata.builder()
            .uuid(INDEX_UUID)
            .tableUuid(TABLE_UUID)
            .location(LOCATION)
            .type("SCALAR")
            .transformFunction("HASH")
            .keyColumnIds(ImmutableList.of(3))
            .properties(ImmutableMap.of("hash.num-buckets", "256"))
            .build();

    String json = IndexMetadataParser.toJson(metadata, true);
    IndexMetadata restored = IndexMetadataParser.fromJson(json);

    assertThat(restored.formatVersion()).isEqualTo(1);
    assertThat(restored.uuid()).isEqualTo(INDEX_UUID);
    assertThat(restored.tableUuid()).isEqualTo(TABLE_UUID);
    assertThat(restored.location()).isEqualTo(LOCATION);
    assertThat(restored.type()).isEqualTo("SCALAR");
    assertThat(restored.transformFunction()).isEqualTo("HASH");
    assertThat(restored.keyColumnIds()).containsExactly(3);
    assertThat(restored.includedColumnIds()).isEmpty();
    assertThat(restored.properties()).containsEntry("hash.num-buckets", "256");
    assertThat(restored.currentSnapshotId()).isNull();
    assertThat(restored.snapshots()).isEmpty();
  }

  @Test
  public void roundTripWithSnapshot() {
    IndexSnapshot snapshot =
        GenericIndexSnapshot.builder()
            .snapshotId(1L)
            .sourceTableSnapshotId(3055729675574597004L)
            .timestampMs(1735689600000L)
            .trackingFile(TRACKING_FILE)
            .properties(ImmutableMap.of("build.engine", "spark"))
            .build();

    IndexMetadata metadata =
        GenericIndexMetadata.builder()
            .uuid(INDEX_UUID)
            .tableUuid(TABLE_UUID)
            .location(LOCATION)
            .type("SCALAR")
            .transformFunction("HASH")
            .keyColumnIds(ImmutableList.of(3))
            .addSnapshot(snapshot)
            .build();

    String json = IndexMetadataParser.toJson(metadata, true);
    IndexMetadata restored = IndexMetadataParser.fromJson(json);

    assertThat(restored.snapshots()).hasSize(1);
    assertThat(restored.currentSnapshotId()).isEqualTo(1L);

    IndexSnapshot restoredSnap = restored.snapshots().get(0);
    assertThat(restoredSnap.snapshotId()).isEqualTo(1L);
    assertThat(restoredSnap.sourceTableSnapshotId()).isEqualTo(3055729675574597004L);
    assertThat(restoredSnap.timestampMs()).isEqualTo(1735689600000L);
    assertThat(restoredSnap.trackingFile()).isEqualTo(TRACKING_FILE);
    assertThat(restoredSnap.properties()).containsEntry("build.engine", "spark");
  }

  @Test
  public void roundTripWithIncludedColumns() {
    IndexMetadata metadata =
        GenericIndexMetadata.builder()
            .uuid(INDEX_UUID)
            .tableUuid(TABLE_UUID)
            .location(LOCATION)
            .type("SCALAR")
            .transformFunction("IDENTITY")
            .keyColumnIds(ImmutableList.of(5))
            .includedColumnIds(ImmutableList.of(6, 7))
            .build();

    String json = IndexMetadataParser.toJson(metadata);
    IndexMetadata restored = IndexMetadataParser.fromJson(json);

    assertThat(restored.keyColumnIds()).containsExactly(5);
    assertThat(restored.includedColumnIds()).containsExactly(6, 7);
    assertThat(restored.transformFunction()).isEqualTo("IDENTITY");
  }

  @Test
  public void snapshotLookupByTableSnapshotId() {
    long tableSnapshotId = 3055729675574597004L;
    IndexSnapshot snapshot =
        GenericIndexSnapshot.builder()
            .snapshotId(1L)
            .sourceTableSnapshotId(tableSnapshotId)
            .timestampMs(1735689600000L)
            .trackingFile(TRACKING_FILE)
            .build();

    IndexMetadata metadata =
        GenericIndexMetadata.builder()
            .uuid(INDEX_UUID)
            .tableUuid(TABLE_UUID)
            .location(LOCATION)
            .type("SCALAR")
            .transformFunction("HASH")
            .keyColumnIds(ImmutableList.of(3))
            .addSnapshot(snapshot)
            .build();

    assertThat(metadata.snapshotForTableSnapshot(tableSnapshotId)).isNotNull();
    assertThat(metadata.snapshotForTableSnapshot(tableSnapshotId).snapshotId()).isEqualTo(1L);
    assertThat(metadata.snapshotForTableSnapshot(99999L)).isNull();
  }

  @Test
  public void builderRequiresKeyColumnIds() {
    assertThatThrownBy(
            () ->
                GenericIndexMetadata.builder()
                    .uuid(INDEX_UUID)
                    .tableUuid(TABLE_UUID)
                    .location(LOCATION)
                    .type("SCALAR")
                    .transformFunction("HASH")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("key-column-ids");
  }

  @Test
  public void matchesSpecJsonFormat() {
    // Verify the JSON output matches the field names defined in format/index.md
    IndexMetadata metadata =
        GenericIndexMetadata.builder()
            .uuid(INDEX_UUID)
            .tableUuid(TABLE_UUID)
            .location(LOCATION)
            .type("SCALAR")
            .transformFunction("HASH")
            .keyColumnIds(ImmutableList.of(1))
            .properties(ImmutableMap.of("hash.num-buckets", "256"))
            .build();

    String json = IndexMetadataParser.toJson(metadata);
    assertThat(json).contains("\"format-version\"");
    assertThat(json).contains("\"uuid\"");
    assertThat(json).contains("\"table-uuid\"");
    assertThat(json).contains("\"location\"");
    assertThat(json).contains("\"type\"");
    assertThat(json).contains("\"transform-function\"");
    assertThat(json).contains("\"key-column-ids\"");
    assertThat(json).contains("\"snapshots\"");
  }
}
