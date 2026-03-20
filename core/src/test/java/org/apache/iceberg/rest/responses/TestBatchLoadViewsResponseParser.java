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
import org.junit.jupiter.api.Test;

class TestBatchLoadViewsResponseParser {

  @Test
  void nullCheck() {
    assertThatThrownBy(() -> BatchLoadViewsResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid batch load views response: null");

    assertThatThrownBy(() -> BatchLoadViewsResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse batch load views response from null object");
  }

  @Test
  void missingFields() {
    assertThatThrownBy(() -> BatchLoadViewsResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: views");
  }

  @Test
  void roundTripSerdeNotFound() {
    BatchLoadViewsResponse response =
        ImmutableBatchLoadViewsResponse.builder()
            .addViews(
                ImmutableBatchLoadViewsResultItem.builder()
                    .identifier(TableIdentifier.of(Namespace.of("ns"), "view1"))
                    .status(404)
                    .build(),
                ImmutableBatchLoadViewsResultItem.builder()
                    .identifier(TableIdentifier.of(Namespace.of("ns"), "view2"))
                    .status(404)
                    .build())
            .build();

    String expectedJson =
        "{\n"
            + "  \"views\" : [ {\n"
            + "    \"identifier\" : {\n"
            + "      \"namespace\" : [ \"ns\" ],\n"
            + "      \"name\" : \"view1\"\n"
            + "    },\n"
            + "    \"status\" : 404\n"
            + "  }, {\n"
            + "    \"identifier\" : {\n"
            + "      \"namespace\" : [ \"ns\" ],\n"
            + "      \"name\" : \"view2\"\n"
            + "    },\n"
            + "    \"status\" : 404\n"
            + "  } ]\n"
            + "}";

    String json = BatchLoadViewsResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);

    BatchLoadViewsResponse deserialized = BatchLoadViewsResponseParser.fromJson(json);
    assertThat(deserialized.views()).hasSize(2);
    assertThat(deserialized.views().get(0).status()).isEqualTo(404);
    assertThat(deserialized.views().get(0).result()).isNull();
    assertThat(deserialized.views().get(1).status()).isEqualTo(404);
  }
}
