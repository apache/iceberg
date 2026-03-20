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
package org.apache.iceberg.rest.requests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

class TestBatchLoadRequestedTableParser {

  @Test
  void nullCheck() {
    assertThatThrownBy(() -> BatchLoadRequestedTableParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid batch load requested table: null");

    assertThatThrownBy(() -> BatchLoadRequestedTableParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse batch load requested table from null object");
  }

  @Test
  void missingFields() {
    assertThatThrownBy(() -> BatchLoadRequestedTableParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: identifier");
  }

  @Test
  void roundTripSerdeMinimal() {
    BatchLoadRequestedTable table =
        ImmutableBatchLoadRequestedTable.builder()
            .identifier(TableIdentifier.of(Namespace.of("ns"), "tbl"))
            .build();

    String expectedJson =
        "{\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ \"ns\" ],\n"
            + "    \"name\" : \"tbl\"\n"
            + "  }\n"
            + "}";

    String json = BatchLoadRequestedTableParser.toJson(table, true);
    assertThat(json).isEqualTo(expectedJson);

    assertThat(
            BatchLoadRequestedTableParser.toJson(
                BatchLoadRequestedTableParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  void roundTripSerdeAllFields() {
    BatchLoadRequestedTable table =
        ImmutableBatchLoadRequestedTable.builder()
            .identifier(TableIdentifier.of(Namespace.of("db", "schema"), "my_table"))
            .ifNonMatch("abc123")
            .build();

    String expectedJson =
        "{\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ \"db\", \"schema\" ],\n"
            + "    \"name\" : \"my_table\"\n"
            + "  },\n"
            + "  \"if-non-match\" : \"abc123\"\n"
            + "}";

    String json = BatchLoadRequestedTableParser.toJson(table, true);
    assertThat(json).isEqualTo(expectedJson);

    assertThat(
            BatchLoadRequestedTableParser.toJson(
                BatchLoadRequestedTableParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
