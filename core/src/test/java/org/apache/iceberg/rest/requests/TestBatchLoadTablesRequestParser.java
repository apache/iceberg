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

class TestBatchLoadTablesRequestParser {

  @Test
  void nullCheck() {
    assertThatThrownBy(() -> BatchLoadTablesRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid batch load tables request: null");

    assertThatThrownBy(() -> BatchLoadTablesRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse batch load tables request from null object");
  }

  @Test
  void missingFields() {
    assertThatThrownBy(() -> BatchLoadTablesRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: tables");
  }

  @Test
  void roundTripSerde() {
    BatchLoadTablesRequest request =
        ImmutableBatchLoadTablesRequest.builder()
            .addTables(
                ImmutableBatchLoadRequestedTable.builder()
                    .identifier(TableIdentifier.of(Namespace.of("ns1"), "tbl1"))
                    .build(),
                ImmutableBatchLoadRequestedTable.builder()
                    .identifier(TableIdentifier.of(Namespace.of("ns2"), "tbl2"))
                    .ifNonMatch("etag123")
                    .build())
            .build();

    String expectedJson =
        "{\n"
            + "  \"tables\" : [ {\n"
            + "    \"identifier\" : {\n"
            + "      \"namespace\" : [ \"ns1\" ],\n"
            + "      \"name\" : \"tbl1\"\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"identifier\" : {\n"
            + "      \"namespace\" : [ \"ns2\" ],\n"
            + "      \"name\" : \"tbl2\"\n"
            + "    },\n"
            + "    \"if-non-match\" : \"etag123\"\n"
            + "  } ]\n"
            + "}";

    String json = BatchLoadTablesRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);

    assertThat(
            BatchLoadTablesRequestParser.toJson(BatchLoadTablesRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
