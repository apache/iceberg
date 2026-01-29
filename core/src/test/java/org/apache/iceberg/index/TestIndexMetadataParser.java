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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestIndexMetadataParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> IndexMetadataParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse index metadata from null string");

    assertThatThrownBy(() -> IndexMetadataParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse index metadata from null object");

    assertThatThrownBy(() -> IndexMetadataParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index metadata: null");
  }

  @Test
  public void testReadAndWriteValidIndexMetadata() {
    IndexVersion version1 =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(4353L)
            .properties(ImmutableMap.of("user-key", "user-value"))
            .build();

    IndexHistoryEntry historyEntry =
        ImmutableIndexHistoryEntry.builder().versionId(1).timestampMillis(4353L).build();

    IndexSnapshot snapshot =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(200L)
            .versionId(1)
            .properties(ImmutableMap.of("snapshot-key", "snapshot-value"))
            .build();

    IndexMetadata expectedMetadata =
        ImmutableIndexMetadata.of(
            "fa6506c3-7681-40c8-86dc-e36561f83385",
            1,
            IndexType.BTREE,
            ImmutableList.of(1, 2),
            ImmutableList.of(1),
            "s3://bucket/test/location",
            1,
            ImmutableList.of(version1),
            ImmutableList.of(historyEntry),
            ImmutableList.of(snapshot),
            ImmutableList.of(),
            null);

    String json = IndexMetadataParser.toJson(expectedMetadata);
    IndexMetadata actual = IndexMetadataParser.fromJson(json);

    assertThat(actual.uuid()).isEqualTo(expectedMetadata.uuid());
    assertThat(actual.formatVersion()).isEqualTo(expectedMetadata.formatVersion());
    assertThat(actual.type()).isEqualTo(expectedMetadata.type());
    assertThat(actual.indexColumnIds()).isEqualTo(expectedMetadata.indexColumnIds());
    assertThat(actual.optimizedColumnIds()).isEqualTo(expectedMetadata.optimizedColumnIds());
    assertThat(actual.location()).isEqualTo(expectedMetadata.location());
    assertThat(actual.currentVersionId()).isEqualTo(expectedMetadata.currentVersionId());
    assertThat(actual.versions()).hasSize(1);
    assertThat(actual.history()).hasSize(1);
    assertThat(actual.snapshots()).hasSize(1);
  }

  @Test
  public void testFailReadingIndexMetadataMissingUuid() {
    String json =
        """
        {
          "format-version": 1,
          "index-type": "btree",
          "index-column-ids": [1],
          "optimized-column-ids": [1],
          "location": "s3://bucket/test",
          "current-version-id": 1,
          "versions": [],
          "version-log": [],
          "snapshots": []
        }
        """;

    assertThatThrownBy(() -> IndexMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: index-uuid");
  }

  @Test
  public void testFailReadingIndexMetadataMissingFormatVersion() {
    String json =
        """
        {
          "index-uuid": "uuid",
          "index-type": "btree",
          "index-column-ids": [1],
          "optimized-column-ids": [1],
          "location": "s3://bucket/test",
          "current-version-id": 1,
          "versions": [],
          "version-log": [],
          "snapshots": []
        }
        """;

    assertThatThrownBy(() -> IndexMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: format-version");
  }

  @Test
  public void testFailReadingIndexMetadataMissingIndexType() {
    String json =
        """
        {
          "index-uuid": "uuid",
          "format-version": 1,
          "index-column-ids": [1],
          "optimized-column-ids": [1],
          "location": "s3://bucket/test",
          "current-version-id": 1,
          "versions": [],
          "version-log": [],
          "snapshots": []
        }
        """;

    assertThatThrownBy(() -> IndexMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: index-type");
  }

  @Test
  public void testFailReadingIndexMetadataMissingLocation() {
    String json =
        """
        {
          "index-uuid": "uuid",
          "format-version": 1,
          "index-type": "btree",
          "index-column-ids": [1],
          "optimized-column-ids": [1],
          "current-version-id": 1,
          "versions": [],
          "version-log": [],
          "snapshots": []
        }
        """;

    assertThatThrownBy(() -> IndexMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: location");
  }

  @Test
  public void testFailReadingIndexMetadataInvalidIndexType() {
    String json =
        """
        {
          "index-uuid": "uuid",
          "format-version": 1,
          "index-type": "invalid-type",
          "index-column-ids": [1],
          "optimized-column-ids": [1],
          "location": "s3://bucket/test",
          "current-version-id": 1,
          "versions": [],
          "version-log": [],
          "snapshots": []
        }
        """;

    assertThatThrownBy(() -> IndexMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown index type: invalid-type");
  }

  @ParameterizedTest
  @EnumSource(IndexType.class)
  public void testRoundTripWithAllIndexTypes(IndexType indexType) {
    IndexVersion version =
        ImmutableIndexVersion.builder().versionId(1).timestampMillis(12345L).build();

    IndexMetadata metadata =
        ImmutableIndexMetadata.of(
            "test-uuid",
            1,
            indexType,
            ImmutableList.of(1),
            ImmutableList.of(1),
            "s3://bucket/test",
            1,
            ImmutableList.of(version),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null);

    String json = IndexMetadataParser.toJson(metadata);
    IndexMetadata parsed = IndexMetadataParser.fromJson(json);

    assertThat(parsed.type()).isEqualTo(indexType);
  }

  @Test
  public void testRoundTripWithMultipleVersions() {
    IndexVersion version1 =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(1000L)
            .properties(ImmutableMap.of("v", "1"))
            .build();

    IndexVersion version2 =
        ImmutableIndexVersion.builder()
            .versionId(2)
            .timestampMillis(2000L)
            .properties(ImmutableMap.of("v", "2"))
            .build();

    IndexHistoryEntry entry1 =
        ImmutableIndexHistoryEntry.builder().versionId(1).timestampMillis(1000L).build();

    IndexHistoryEntry entry2 =
        ImmutableIndexHistoryEntry.builder().versionId(2).timestampMillis(2000L).build();

    IndexSnapshot snapshot1 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(10L)
            .indexSnapshotId(100L)
            .versionId(1)
            .build();

    IndexSnapshot snapshot2 =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(20L)
            .indexSnapshotId(200L)
            .versionId(2)
            .build();

    IndexMetadata metadata =
        ImmutableIndexMetadata.of(
            "multi-version-uuid",
            1,
            IndexType.TERM,
            ImmutableList.of(1, 2, 3),
            ImmutableList.of(1, 2),
            "s3://bucket/test/multi",
            2,
            ImmutableList.of(version1, version2),
            ImmutableList.of(entry1, entry2),
            ImmutableList.of(snapshot1, snapshot2),
            ImmutableList.of(),
            null);

    String json = IndexMetadataParser.toJson(metadata);
    IndexMetadata parsed = IndexMetadataParser.fromJson(json);

    assertThat(parsed.versions()).hasSize(2);
    assertThat(parsed.history()).hasSize(2);
    assertThat(parsed.snapshots()).hasSize(2);
    assertThat(parsed.currentVersionId()).isEqualTo(2);
    assertThat(parsed.indexColumnIds()).containsExactly(1, 2, 3);
    assertThat(parsed.optimizedColumnIds()).containsExactly(1, 2);
  }

  @Test
  public void testPrettyPrint() {
    IndexVersion version =
        ImmutableIndexVersion.builder().versionId(1).timestampMillis(12345L).build();

    IndexMetadata metadata =
        ImmutableIndexMetadata.of(
            "pretty-uuid",
            1,
            IndexType.IVF,
            ImmutableList.of(1),
            ImmutableList.of(1),
            "s3://bucket/test",
            1,
            ImmutableList.of(version),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null);

    String prettyJson = IndexMetadataParser.toJson(metadata, true);
    String compactJson = IndexMetadataParser.toJson(metadata, false);

    // Pretty JSON should contain newlines
    assertThat(prettyJson).contains("\n");
    // Compact JSON should not contain newlines
    assertThat(compactJson).doesNotContain("\n");

    // Both should parse to equivalent metadata
    assertThat(IndexMetadataParser.fromJson(prettyJson).uuid())
        .isEqualTo(IndexMetadataParser.fromJson(compactJson).uuid());
  }

  @Test
  public void testCurrentVersionAccess() {
    IndexVersion version1 =
        ImmutableIndexVersion.builder().versionId(1).timestampMillis(1000L).build();

    IndexVersion version2 =
        ImmutableIndexVersion.builder().versionId(2).timestampMillis(2000L).build();

    IndexMetadata metadata =
        ImmutableIndexMetadata.of(
            "test-uuid",
            1,
            IndexType.BTREE,
            ImmutableList.of(1),
            ImmutableList.of(1),
            "s3://bucket/test",
            2,
            ImmutableList.of(version1, version2),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null);

    assertThat(metadata.currentVersion().versionId()).isEqualTo(2);
    assertThat(metadata.version(1).timestampMillis()).isEqualTo(1000L);
    assertThat(metadata.version(2).timestampMillis()).isEqualTo(2000L);
  }

  @Test
  public void testToJsonWithExpectedString() {
    IndexVersion version =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(1234567890L)
            .properties(ImmutableMap.of("key", "value"))
            .build();

    IndexHistoryEntry historyEntry =
        ImmutableIndexHistoryEntry.builder().versionId(1).timestampMillis(1234567890L).build();

    IndexSnapshot snapshot =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(200L)
            .versionId(1)
            .properties(ImmutableMap.of("snap-key", "snap-value"))
            .build();

    IndexMetadata metadata =
        ImmutableIndexMetadata.of(
            "fa6506c3-7681-40c8-86dc-e36561f83385",
            1,
            IndexType.BTREE,
            ImmutableList.of(1, 2),
            ImmutableList.of(1),
            "s3://bucket/test/location",
            1,
            ImmutableList.of(version),
            ImmutableList.of(historyEntry),
            ImmutableList.of(snapshot),
            ImmutableList.of(),
            null);

    String expectedJson =
        """
        {
          "index-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
          "format-version": 1,
          "index-type": "btree",
          "index-column-ids": [1, 2],
          "optimized-column-ids": [1],
          "location": "s3://bucket/test/location",
          "current-version-id": 1,
          "versions": [
            {
              "version-id": 1,
              "timestamp-ms": 1234567890,
              "properties": {
                "key": "value"
              }
            }
          ],
          "version-log": [
            {
              "timestamp-ms": 1234567890,
              "version-id": 1
            }
          ],
          "snapshots": [
            {
              "table-snapshot-id": 100,
              "index-snapshot-id": 200,
              "version-id": 1,
              "properties": {
                "snap-key": "snap-value"
              }
            }
          ]
        }
        """
            .replaceAll("\\s+", "");

    String actualJson = IndexMetadataParser.toJson(metadata);
    assertThat(actualJson).isEqualTo(expectedJson);

    // Also verify round-trip
    IndexMetadata parsed = IndexMetadataParser.fromJson(actualJson);
    assertThat(parsed.uuid()).isEqualTo("fa6506c3-7681-40c8-86dc-e36561f83385");
    assertThat(parsed.formatVersion()).isEqualTo(1);
    assertThat(parsed.type()).isEqualTo(IndexType.BTREE);
    assertThat(parsed.indexColumnIds()).containsExactly(1, 2);
    assertThat(parsed.optimizedColumnIds()).containsExactly(1);
    assertThat(parsed.location()).isEqualTo("s3://bucket/test/location");
    assertThat(parsed.currentVersionId()).isEqualTo(1);
    assertThat(parsed.versions()).hasSize(1);
    assertThat(parsed.history()).hasSize(1);
    assertThat(parsed.snapshots()).hasSize(1);
  }
}
