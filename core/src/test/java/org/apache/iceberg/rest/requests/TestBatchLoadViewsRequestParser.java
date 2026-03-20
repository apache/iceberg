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

class TestBatchLoadViewsRequestParser {

  @Test
  void nullCheck() {
    assertThatThrownBy(() -> BatchLoadViewsRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid batch load views request: null");

    assertThatThrownBy(() -> BatchLoadViewsRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse batch load views request from null object");
  }

  @Test
  void missingFields() {
    assertThatThrownBy(() -> BatchLoadViewsRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: views");
  }

  @Test
  void roundTripSerde() {
    BatchLoadViewsRequest request =
        ImmutableBatchLoadViewsRequest.builder()
            .addViews(
                TableIdentifier.of(Namespace.of("ns1"), "view1"),
                TableIdentifier.of(Namespace.of("ns2"), "view2"))
            .build();

    String expectedJson =
        "{\n"
            + "  \"views\" : [ {\n"
            + "    \"namespace\" : [ \"ns1\" ],\n"
            + "    \"name\" : \"view1\"\n"
            + "  }, {\n"
            + "    \"namespace\" : [ \"ns2\" ],\n"
            + "    \"name\" : \"view2\"\n"
            + "  } ]\n"
            + "}";

    String json = BatchLoadViewsRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);

    assertThat(BatchLoadViewsRequestParser.toJson(BatchLoadViewsRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
