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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.ImmutableBatchLoadRequestedTable;
import org.junit.jupiter.api.Test;

class TestBatchLoadTablesResponseParser {

  @Test
  void nullCheck() {
    assertThatThrownBy(() -> BatchLoadTablesResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid batch load tables response: null");

    assertThatThrownBy(() -> BatchLoadTablesResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse batch load tables response from null object");
  }

  @Test
  void missingFields() {
    assertThatThrownBy(() -> BatchLoadTablesResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: results");
  }

  @Test
  void roundTripSerdeNotFoundAndNotModified() {
    BatchLoadTablesResponse response =
        ImmutableBatchLoadTablesResponse.builder()
            .addResults(
                ImmutableBatchLoadTablesResultItem.builder()
                    .identifier(TableIdentifier.of(Namespace.of("ns"), "not_found"))
                    .status(404)
                    .build(),
                ImmutableBatchLoadTablesResultItem.builder()
                    .identifier(TableIdentifier.of(Namespace.of("ns"), "cached"))
                    .status(304)
                    .etag("abc123")
                    .build())
            .build();

    String expectedJson =
        "{\n"
            + "  \"results\" : [ {\n"
            + "    \"identifier\" : {\n"
            + "      \"namespace\" : [ \"ns\" ],\n"
            + "      \"name\" : \"not_found\"\n"
            + "    },\n"
            + "    \"status\" : 404\n"
            + "  }, {\n"
            + "    \"identifier\" : {\n"
            + "      \"namespace\" : [ \"ns\" ],\n"
            + "      \"name\" : \"cached\"\n"
            + "    },\n"
            + "    \"status\" : 304,\n"
            + "    \"etag\" : \"abc123\"\n"
            + "  } ]\n"
            + "}";

    String json = BatchLoadTablesResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);

    BatchLoadTablesResponse deserialized = BatchLoadTablesResponseParser.fromJson(json);
    assertThat(deserialized.results()).hasSize(2);
    assertThat(deserialized.results().get(0).status()).isEqualTo(404);
    assertThat(deserialized.results().get(0).result()).isNull();
    assertThat(deserialized.results().get(1).status()).isEqualTo(304);
    assertThat(deserialized.results().get(1).etag()).isEqualTo("abc123");
    assertThat(deserialized.unprocessedTables()).isNull();
  }

  @Test
  void roundTripSerdeWithUnprocessedTables() {
    BatchLoadTablesResponse response =
        ImmutableBatchLoadTablesResponse.builder()
            .addResults(
                ImmutableBatchLoadTablesResultItem.builder()
                    .identifier(TableIdentifier.of(Namespace.of("ns"), "tbl1"))
                    .status(404)
                    .build())
            .addUnprocessedTables(
                ImmutableBatchLoadRequestedTable.builder()
                    .identifier(TableIdentifier.of(Namespace.of("ns"), "tbl2"))
                    .build())
            .build();

    String expectedJson =
        "{\n"
            + "  \"results\" : [ {\n"
            + "    \"identifier\" : {\n"
            + "      \"namespace\" : [ \"ns\" ],\n"
            + "      \"name\" : \"tbl1\"\n"
            + "    },\n"
            + "    \"status\" : 404\n"
            + "  } ],\n"
            + "  \"unprocessed-tables\" : [ {\n"
            + "    \"identifier\" : {\n"
            + "      \"namespace\" : [ \"ns\" ],\n"
            + "      \"name\" : \"tbl2\"\n"
            + "    }\n"
            + "  } ]\n"
            + "}";

    String json = BatchLoadTablesResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);

    BatchLoadTablesResponse deserialized = BatchLoadTablesResponseParser.fromJson(json);
    assertThat(deserialized.results()).hasSize(1);
    assertThat(deserialized.unprocessedTables()).hasSize(1);
    assertThat(deserialized.unprocessedTables().get(0).identifier().name()).isEqualTo("tbl2");
  }
}
