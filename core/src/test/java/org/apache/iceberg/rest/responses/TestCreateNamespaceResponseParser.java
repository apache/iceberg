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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/**
 * We mainly test for nullability and do a round-trip. Everything else is tested in {@link
 * org.apache.iceberg.rest.TestNamespaceWithPropertiesParser}.
 */
public class TestCreateNamespaceResponseParser {

  @Test
  public void nullCheck() {
    Assertions.assertThatThrownBy(() -> CreateNamespaceResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace creation response: null");

    Assertions.assertThatThrownBy(() -> CreateNamespaceResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse namespace creation response from null object");
  }

  @Test
  public void namespaceWithoutProperties() {
    CreateNamespaceResponse expected =
        ImmutableCreateNamespaceResponse.newBuilder()
            .namespace(Namespace.of("accounting", "tax"))
            .build();

    CreateNamespaceResponse actual =
        CreateNamespaceResponseParser.fromJson("{\"namespace\":[\"accounting\",\"tax\"]}");
    Assertions.assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void roundTripSerde() {
    CreateNamespaceResponse response =
        ImmutableCreateNamespaceResponse.newBuilder()
            .namespace(Namespace.of("accounting", "tax"))
            .properties(ImmutableMap.of("a", "1", "b", "2"))
            .build();

    String expectedJson =
        "{\n"
            + "  \"namespace\" : [ \"accounting\", \"tax\" ],\n"
            + "  \"properties\" : {\n"
            + "    \"a\" : \"1\",\n"
            + "    \"b\" : \"2\"\n"
            + "  }\n"
            + "}";

    String json = CreateNamespaceResponseParser.toJson(response, true);
    Assertions.assertThat(json).isEqualTo(expectedJson);

    Assertions.assertThat(CreateNamespaceResponseParser.fromJson(json)).isEqualTo(response);
  }
}
