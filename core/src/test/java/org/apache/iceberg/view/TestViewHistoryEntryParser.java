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
package org.apache.iceberg.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

public class TestViewHistoryEntryParser {

  @Test
  public void testViewHistoryEntryFromJson() {
    String json = "{\"timestamp-ms\":123,\"version-id\":1}";
    ViewHistoryEntry viewHistoryEntry =
        ImmutableViewHistoryEntry.builder().versionId(1).timestampMillis(123).build();
    assertThat(ViewHistoryEntryParser.fromJson(json))
        .as("Should be able to deserialize valid view history entry")
        .isEqualTo(viewHistoryEntry);
  }

  @Test
  public void testViewHistoryEntryToJson() {
    String json = "{\"timestamp-ms\":123,\"version-id\":1}";
    ViewHistoryEntry viewHistoryEntry =
        ImmutableViewHistoryEntry.builder().versionId(1).timestampMillis(123).build();
    assertThat(ViewHistoryEntryParser.toJson(viewHistoryEntry))
        .as("Should be able to serialize view history entry")
        .isEqualTo(json);
  }

  @Test
  public void testNullViewHistoryEntry() {
    assertThatThrownBy(() -> ViewHistoryEntryParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse view history entry from null object");

    assertThatThrownBy(() -> ViewHistoryEntryParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view history entry: null");
  }

  @Test
  public void testViewHistoryEntryMissingFields() {
    assertThatThrownBy(() -> ViewHistoryEntryParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: version-id");

    assertThatThrownBy(() -> ViewHistoryEntryParser.fromJson("{\"timestamp-ms\":\"123\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: version-id");

    assertThatThrownBy(() -> ViewHistoryEntryParser.fromJson("{\"version-id\":1}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: timestamp-ms");
  }
}
