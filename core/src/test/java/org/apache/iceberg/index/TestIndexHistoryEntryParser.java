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
import org.junit.jupiter.api.Test;

public class TestIndexHistoryEntryParser {
  private static final String INDEX_HISTORY_ENTRY_JSON =
      """
        {
          "timestamp-ms": 1622547800000,
          "version-id": 3
        }
        """
          .replaceAll("\\s+", "");

  private static final IndexHistoryEntry INDEX_HISTORY_ENTRY =
      ImmutableIndexHistoryEntry.builder().versionId(3).timestampMillis(1622547800000L).build();

  @Test
  public void testIndexHistoryEntryFromJson() {
    assertThat(IndexHistoryEntryParser.fromJson(INDEX_HISTORY_ENTRY_JSON))
        .as("Should be able to deserialize valid index history entry")
        .isEqualTo(INDEX_HISTORY_ENTRY);
  }

  @Test
  public void testIndexHistoryEntryToJson() {
    assertThat(IndexHistoryEntryParser.toJson(INDEX_HISTORY_ENTRY))
        .as("Should be able to serialize index history entry")
        .isEqualTo(INDEX_HISTORY_ENTRY_JSON);
  }

  @Test
  public void testNullIndexHistoryEntry() {
    assertThatThrownBy(() -> IndexHistoryEntryParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse index history entry from null object");

    assertThatThrownBy(() -> IndexHistoryEntryParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index history entry: null");
  }

  @Test
  public void testIndexHistoryEntryMissingFields() {
    assertThatThrownBy(() -> IndexHistoryEntryParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: version-id");

    assertThatThrownBy(() -> IndexHistoryEntryParser.fromJson("{\"timestamp-ms\":123}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: version-id");

    assertThatThrownBy(() -> IndexHistoryEntryParser.fromJson("{\"version-id\":1}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: timestamp-ms");
  }

  @Test
  public void testRoundTrip() {
    IndexHistoryEntry original =
        ImmutableIndexHistoryEntry.builder().versionId(42).timestampMillis(1234567890L).build();

    String json = IndexHistoryEntryParser.toJson(original);
    IndexHistoryEntry parsed = IndexHistoryEntryParser.fromJson(json);

    assertThat(parsed.versionId()).isEqualTo(original.versionId());
    assertThat(parsed.timestampMillis()).isEqualTo(original.timestampMillis());
  }
}
