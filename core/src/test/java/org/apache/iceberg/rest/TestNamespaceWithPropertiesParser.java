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
package org.apache.iceberg.rest;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestNamespaceWithPropertiesParser {

  @Test
  public void nullCheck() {
    Assertions.assertThatThrownBy(() -> NamespaceWithPropertiesParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");

    Assertions.assertThatThrownBy(() -> NamespaceWithPropertiesParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse namespace from null object");
  }

  @Test
  public void missingFields() {
    Assertions.assertThatThrownBy(() -> NamespaceWithPropertiesParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: namespace");
  }

  @Test
  public void namespaceWithoutProperties() {
    NamespaceWithProperties expected =
        ImmutableNamespaceWithProperties.builder()
            .namespace(Namespace.of("accounting", "tax"))
            .build();

    NamespaceWithProperties actual =
        NamespaceWithPropertiesParser.fromJson("{\"namespace\":[\"accounting\",\"tax\"]}");
    Assertions.assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void invalidNamespaceType() {
    Assertions.assertThatThrownBy(
            () -> NamespaceWithPropertiesParser.fromJson("{\"namespace\":\"accounting%1Ftax\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string array from non-array: \"accounting%1Ftax\"");
  }

  @Test
  public void invalidPropertiesType() {
    Assertions.assertThatThrownBy(
            () ->
                NamespaceWithPropertiesParser.fromJson(
                    "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":[]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse from non-object value: properties: []");

    Assertions.assertThatThrownBy(
            () ->
                NamespaceWithPropertiesParser.fromJson(
                    "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":null}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse from non-object value: properties: null");
  }

  @Test
  public void emptyProperties() {
    Assertions.assertThat(
            NamespaceWithPropertiesParser.fromJson(
                "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{}}"))
        .isEqualTo(
            ImmutableNamespaceWithProperties.builder()
                .namespace(Namespace.of("accounting", "tax"))
                .build());
  }

  @Test
  public void emptyNamespace() {
    Assertions.assertThat(NamespaceWithPropertiesParser.fromJson("{\"namespace\":[]}"))
        .isEqualTo(ImmutableNamespaceWithProperties.builder().namespace(Namespace.empty()).build());
  }

  @Test
  public void roundTripSerde() {
    NamespaceWithProperties namespaceWithProperties =
        ImmutableNamespaceWithProperties.builder()
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

    String json = NamespaceWithPropertiesParser.toJson(namespaceWithProperties, true);
    Assertions.assertThat(json).isEqualTo(expectedJson);

    Assertions.assertThat(NamespaceWithPropertiesParser.fromJson(json))
        .isEqualTo(namespaceWithProperties);
  }
}
