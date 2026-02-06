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
package org.apache.iceberg.rest.responses;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.apache.iceberg.index.ImmutableIndexVersion;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexType;
import org.apache.iceberg.index.IndexVersion;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestLoadIndexResponseParser {

  private static final String TEST_LOCATION = "s3://bucket/test/location";
  private static final String TEST_METADATA_LOCATION = "s3://bucket/test/metadata/v1.metadata.json";
  private static final List<Integer> INDEX_COLUMN_IDS = ImmutableList.of(1, 2);
  private static final List<Integer> OPTIMIZED_COLUMN_IDS = ImmutableList.of(1);

  private static IndexMetadata createTestMetadata() {
    IndexVersion version =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(1234567890L)
            .properties(ImmutableMap.of("key", "value"))
            .build();

    return IndexMetadata.builder()
        .setLocation(TEST_LOCATION)
        .setType(IndexType.BTREE)
        .setIndexColumnIds(INDEX_COLUMN_IDS)
        .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
        .addVersion(version)
        .setCurrentVersion(version.versionId())
        .build();
  }

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> LoadIndexResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid load index response: null");

    assertThatThrownBy(() -> LoadIndexResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse load index response from null object");

    assertThatThrownBy(() -> LoadIndexResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: metadata");
  }

  @Test
  public void missingMetadataField() {
    assertThatThrownBy(
            () ->
                LoadIndexResponseParser.fromJson(
                    "{\"metadata-location\": \"s3://bucket/metadata.json\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: metadata");
  }

  @Test
  public void roundTripSerde() {
    IndexMetadata metadata = createTestMetadata();

    LoadIndexResponse response =
        LoadIndexResponse.builder()
            .withMetadata(metadata)
            .withMetadataLocation(TEST_METADATA_LOCATION)
            .build();

    String json = LoadIndexResponseParser.toJson(response);
    LoadIndexResponse parsed = LoadIndexResponseParser.fromJson(json);

    assertThat(parsed.metadataLocation()).isEqualTo(TEST_METADATA_LOCATION);
    assertThat(parsed.metadata().uuid()).isEqualTo(metadata.uuid());
    assertThat(parsed.metadata().formatVersion()).isEqualTo(metadata.formatVersion());
    assertThat(parsed.metadata().type()).isEqualTo(metadata.type());
    assertThat(parsed.metadata().indexColumnIds()).isEqualTo(metadata.indexColumnIds());
    assertThat(parsed.metadata().optimizedColumnIds()).isEqualTo(metadata.optimizedColumnIds());
    assertThat(parsed.metadata().location()).isEqualTo(metadata.location());
    assertThat(parsed.config()).isEmpty();
  }

  @Test
  public void roundTripSerdeWithConfig() {
    IndexMetadata metadata = createTestMetadata();

    LoadIndexResponse response =
        LoadIndexResponse.builder()
            .withMetadata(metadata)
            .withMetadataLocation(TEST_METADATA_LOCATION)
            .addConfig("key1", "value1")
            .addConfig("key2", "value2")
            .build();

    String json = LoadIndexResponseParser.toJson(response);
    LoadIndexResponse parsed = LoadIndexResponseParser.fromJson(json);

    assertThat(parsed.metadataLocation()).isEqualTo(TEST_METADATA_LOCATION);
    assertThat(parsed.metadata().uuid()).isEqualTo(metadata.uuid());
    assertThat(parsed.config())
        .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("key1", "value1", "key2", "value2"));
  }

  @Test
  public void roundTripSerdeWithoutMetadataLocation() {
    IndexMetadata metadata = createTestMetadata();

    LoadIndexResponse response = LoadIndexResponse.builder().withMetadata(metadata).build();

    String json = LoadIndexResponseParser.toJson(response);
    LoadIndexResponse parsed = LoadIndexResponseParser.fromJson(json);

    assertThat(parsed.metadataLocation()).isNull();
    assertThat(parsed.metadata().uuid()).isEqualTo(metadata.uuid());
  }

  @Test
  public void testPrettyJsonOutput() {
    IndexMetadata metadata = createTestMetadata();

    LoadIndexResponse response =
        LoadIndexResponse.builder()
            .withMetadata(metadata)
            .withMetadataLocation(TEST_METADATA_LOCATION)
            .build();

    String prettyJson = LoadIndexResponseParser.toJson(response, true);
    String compactJson = LoadIndexResponseParser.toJson(response, false);

    // Pretty JSON should contain newlines
    assertThat(prettyJson).contains("\n");
    // Compact JSON should not contain newlines
    assertThat(compactJson).doesNotContain("\n");

    // Both should parse to equivalent responses
    LoadIndexResponse fromPretty = LoadIndexResponseParser.fromJson(prettyJson);
    LoadIndexResponse fromCompact = LoadIndexResponseParser.fromJson(compactJson);

    assertThat(fromPretty.metadata().uuid()).isEqualTo(fromCompact.metadata().uuid());
    assertThat(fromPretty.metadataLocation()).isEqualTo(fromCompact.metadataLocation());
  }

  @Test
  public void testBuilderValidation() {
    assertThatThrownBy(() -> LoadIndexResponse.builder().build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid metadata: null");
  }

  @Test
  public void testAddAllConfig() {
    IndexMetadata metadata = createTestMetadata();

    LoadIndexResponse response =
        LoadIndexResponse.builder()
            .withMetadata(metadata)
            .addAllConfig(ImmutableMap.of("a", "1", "b", "2"))
            .build();

    assertThat(response.config())
        .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("a", "1", "b", "2"));
  }

  @Test
  public void testToJsonWithExpectedString() {
    IndexVersion version =
        ImmutableIndexVersion.builder()
            .versionId(1)
            .timestampMillis(1234567890L)
            .properties(ImmutableMap.of())
            .build();

    IndexMetadata metadata =
        IndexMetadata.builder()
            .setLocation(TEST_LOCATION)
            .setType(IndexType.BTREE)
            .setIndexColumnIds(INDEX_COLUMN_IDS)
            .setOptimizedColumnIds(OPTIMIZED_COLUMN_IDS)
            .addVersion(version)
            .setCurrentVersion(version.versionId())
            .build();

    LoadIndexResponse response =
        LoadIndexResponse.builder()
            .withMetadata(metadata)
            .withMetadataLocation(TEST_METADATA_LOCATION)
            .addConfig("key1", "value1")
            .build();

    String expectedJson =
        String.format(
            """
        {
          "metadata-location": "s3://bucket/test/metadata/v1.metadata.json",
          "metadata": {
            "index-uuid": "%s",
            "format-version": 1,
            "index-type": "btree",
            "index-column-ids": [1, 2],
            "optimized-column-ids": [1],
            "location": "s3://bucket/test/location",
            "current-version-id": 1,
            "versions": [
              {
                "version-id": 1,
                "timestamp-ms": 1234567890
              }
            ],
            "version-log": [
              {
                "timestamp-ms": 1234567890,
                "version-id": 1
              }
            ],
            "snapshots": []
          },
          "config": {
            "key1": "value1"
          }
        }
        """
                .replaceAll("\\s+", ""),
            metadata.uuid());

    String actualJson = LoadIndexResponseParser.toJson(response);
    assertThat(actualJson).isEqualTo(expectedJson);

    // Also verify round-trip
    LoadIndexResponse parsed = LoadIndexResponseParser.fromJson(actualJson);
    assertThat(parsed.metadata().uuid()).isEqualTo(metadata.uuid());
    assertThat(parsed.metadataLocation()).isEqualTo(TEST_METADATA_LOCATION);
    assertThat(parsed.config()).containsEntry("key1", "value1");
  }
}
