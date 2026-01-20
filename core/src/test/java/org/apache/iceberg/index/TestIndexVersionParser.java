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

public class TestIndexVersionParser {
  private static final String INDEX_VERSION_ENTRY_JSON =
      """
            {
              "version-id": 1,
              "timestamp-ms": 12345,
              "properties": {
                "user-key": "user-value"
              }
            }
            """
          .replaceAll("\\s+", "");

  private static final IndexVersion INDEX_VERSION_ENTRY =
      ImmutableIndexVersion.builder()
          .versionId(1)
          .timestampMillis(12345)
          .properties(ImmutableMap.of("user-key", "user-value"))
          .build();

  private static final String INDEX_VERSION_WITHOUT_PROPERTIES_JSON =
      """
                  {
                    "version-id": 1,
                    "timestamp-ms": 12345
                  }
                  """
          .replaceAll("\\s+", "");

  private static final IndexVersion INDEX_VERSION_WITHOUT_PROPERTIES =
      ImmutableIndexVersion.builder()
          .versionId(1)
          .timestampMillis(12345)
          .properties(ImmutableMap.of())
          .build();

  @Test
  public void testParseIndexVersion() {
    assertThat(IndexVersionParser.fromJson(INDEX_VERSION_ENTRY_JSON))
        .as("Should be able to parse valid index version")
        .isEqualTo(INDEX_VERSION_ENTRY);
  }

  @Test
  public void testSerializeIndexVersion() {
    assertThat(IndexVersionParser.toJson(INDEX_VERSION_ENTRY))
        .as("Should be able to serialize valid index version")
        .isEqualTo(INDEX_VERSION_ENTRY_JSON);
  }

  @Test
  public void testNullIndexVersion() {
    assertThatThrownBy(() -> IndexVersionParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index version: null");

    assertThatThrownBy(() -> IndexVersionParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse index version from null object");
  }

  @Test
  public void testIndexVersionMissingFields() {
    assertThatThrownBy(() -> IndexVersionParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: version-id");

    assertThatThrownBy(() -> IndexVersionParser.fromJson("{\"version-id\":1}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: timestamp-ms");
  }

  @Test
  public void testIndexVersionWithoutProperties() {
    IndexVersion version = IndexVersionParser.fromJson(INDEX_VERSION_WITHOUT_PROPERTIES_JSON);
    assertThat(version).isEqualTo(INDEX_VERSION_WITHOUT_PROPERTIES);
  }

  @Test
  public void testIndexVersionWithEmptyProperties() {
    String json = IndexVersionParser.toJson(INDEX_VERSION_WITHOUT_PROPERTIES);
    assertThat(json).isEqualTo(INDEX_VERSION_WITHOUT_PROPERTIES_JSON);
  }

  @Test
  public void testRoundTrip() {
    IndexVersion original =
        ImmutableIndexVersion.builder()
            .versionId(42)
            .timestampMillis(1234567890L)
            .properties(ImmutableMap.of("key1", "value1", "key2", "value2"))
            .build();

    String json = IndexVersionParser.toJson(original);
    IndexVersion parsed = IndexVersionParser.fromJson(json);

    assertThat(parsed.versionId()).isEqualTo(original.versionId());
    assertThat(parsed.timestampMillis()).isEqualTo(original.timestampMillis());
    assertThat(parsed.properties()).isEqualTo(original.properties());
  }

  @Test
  public void testRoundTripWithNullProperties() {
    IndexVersion original =
        ImmutableIndexVersion.builder().versionId(1).timestampMillis(12345).build();

    String json = IndexVersionParser.toJson(original);
    IndexVersion parsed = IndexVersionParser.fromJson(json);

    assertThat(parsed.versionId()).isEqualTo(original.versionId());
    assertThat(parsed.timestampMillis()).isEqualTo(original.timestampMillis());
    assertThat(parsed.properties()).isEmpty();
  }
}
