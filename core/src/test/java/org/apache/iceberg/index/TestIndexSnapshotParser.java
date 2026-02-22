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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestIndexSnapshotParser {
  private static final String INDEX_SNAPSHOT_JSON =
      """
            {
              "table-snapshot-id": 100,
              "index-snapshot-id": 200,
              "version-id": 1,
              "properties": {
                "user-key": "user-value"
              }
            }
            """
          .replaceAll("\\s+", "");

  private static final IndexSnapshot INDEX_SNAPSHOT =
      ImmutableIndexSnapshot.builder()
          .tableSnapshotId(100L)
          .indexSnapshotId(200L)
          .versionId(1)
          .properties(ImmutableMap.of("user-key", "user-value"))
          .build();

  private static final String INDEX_SNAPSHOT_WITHOUT_PROPERTIES_JSON =
      """
                  {
                    "table-snapshot-id": 100,
                    "index-snapshot-id": 200,
                    "version-id": 1
                  }
                  """
          .replaceAll("\\s+", "");

  private static final IndexSnapshot INDEX_SNAPSHOT_WITHOUT_PROPERTIES =
      ImmutableIndexSnapshot.builder()
          .tableSnapshotId(100L)
          .indexSnapshotId(200L)
          .versionId(1)
          .properties(ImmutableMap.of())
          .build();

  @Test
  public void testParseIndexSnapshot() {
    assertThat(IndexSnapshotParser.fromJson(INDEX_SNAPSHOT_JSON))
        .as("Should be able to parse valid index snapshot")
        .isEqualTo(INDEX_SNAPSHOT);
  }

  @Test
  public void testSerializeIndexSnapshot() {
    assertThat(IndexSnapshotParser.toJson(INDEX_SNAPSHOT))
        .as("Should be able to serialize valid index snapshot")
        .isEqualTo(INDEX_SNAPSHOT_JSON);
  }

  @Test
  public void testNullIndexSnapshot() {
    assertThatThrownBy(() -> IndexSnapshotParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index snapshot: null");

    assertThatThrownBy(() -> IndexSnapshotParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse index snapshot from null object");
  }

  @Test
  public void testIndexSnapshotMissingFields() {
    assertThatThrownBy(() -> IndexSnapshotParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: table-snapshot-id");

    assertThatThrownBy(() -> IndexSnapshotParser.fromJson("{\"table-snapshot-id\":100}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: index-snapshot-id");

    assertThatThrownBy(
            () ->
                IndexSnapshotParser.fromJson(
                    """
                    {
                      "table-snapshot-id": 100,
                      "index-snapshot-id": 200
                    }
                    """))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: version-id");
  }

  @Test
  public void testIndexSnapshotWithoutProperties() {
    IndexSnapshot snapshot = IndexSnapshotParser.fromJson(INDEX_SNAPSHOT_WITHOUT_PROPERTIES_JSON);
    assertThat(snapshot).isEqualTo(INDEX_SNAPSHOT_WITHOUT_PROPERTIES);
  }

  @Test
  public void testIndexSnapshotWithEmptyProperties() {
    // Empty properties should not be serialized
    String json = IndexSnapshotParser.toJson(INDEX_SNAPSHOT_WITHOUT_PROPERTIES);
    assertThat(json).isEqualTo(INDEX_SNAPSHOT_WITHOUT_PROPERTIES_JSON);
  }

  @Test
  public void testRoundTrip() {
    IndexSnapshot original =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(1000L)
            .indexSnapshotId(2000L)
            .versionId(5)
            .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
            .build();

    String json = IndexSnapshotParser.toJson(original);
    IndexSnapshot parsed = IndexSnapshotParser.fromJson(json);

    assertThat(parsed.tableSnapshotId()).isEqualTo(original.tableSnapshotId());
    assertThat(parsed.indexSnapshotId()).isEqualTo(original.indexSnapshotId());
    assertThat(parsed.versionId()).isEqualTo(original.versionId());
    assertThat(parsed.properties()).isEqualTo(original.properties());
  }

  @Test
  public void testRoundTripWithNullProperties() {
    IndexSnapshot original =
        ImmutableIndexSnapshot.builder()
            .tableSnapshotId(100L)
            .indexSnapshotId(200L)
            .versionId(1)
            .build();

    String json = IndexSnapshotParser.toJson(original);
    IndexSnapshot parsed = IndexSnapshotParser.fromJson(json);

    assertThat(parsed.tableSnapshotId()).isEqualTo(original.tableSnapshotId());
    assertThat(parsed.indexSnapshotId()).isEqualTo(original.indexSnapshotId());
    assertThat(parsed.versionId()).isEqualTo(original.versionId());
    assertThat(parsed.properties()).isEmpty();
  }
}
