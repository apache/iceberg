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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestEndpoint {
  @Test
  public void nullOrEmptyValues() {
    assertThatThrownBy(() -> Endpoint.create(null, "endpoint"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid HTTP method: null or empty");

    assertThatThrownBy(() -> Endpoint.create("", "endpoint"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid HTTP method: null or empty");

    assertThatThrownBy(() -> Endpoint.create("GET", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid path: null or empty");

    assertThatThrownBy(() -> Endpoint.create("GET", ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid path: null or empty");
  }

  @ParameterizedTest
  @ValueSource(strings = {"/path", " GET /path", "GET /path ", "GET  /path", "GET /path /other"})
  public void invalidFromString(String endpoint) {
    assertThatThrownBy(() -> Endpoint.fromString(endpoint))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid endpoint (must consist of two elements separated by a single space): %s",
            endpoint);
  }

  @Test
  public void validFromString() {
    Endpoint endpoint = Endpoint.fromString("GET /path");
    assertThat(endpoint.httpMethod()).isEqualTo("GET");
    assertThat(endpoint.path()).isEqualTo("/path");
  }

  @Test
  public void toStringRepresentation() {
    assertThat(Endpoint.create("POST", "/path/of/resource"))
        .asString()
        .isEqualTo("POST /path/of/resource");
    assertThat(Endpoint.create("GET", "/")).asString().isEqualTo("GET /");
    assertThat(Endpoint.create("PuT", "/")).asString().isEqualTo("PUT /");
    assertThat(Endpoint.create("PUT", "/namespaces/{namespace}/{x}"))
        .asString()
        .isEqualTo("PUT /namespaces/{namespace}/{x}");
  }

  @Test
  public void supportedEndpoints() {
    assertThatCode(
            () ->
                Endpoint.check(
                    ImmutableSet.of(ResourcePaths.V1_LOAD_TABLE), ResourcePaths.V1_LOAD_TABLE))
        .doesNotThrowAnyException();

    assertThatCode(
            () ->
                Endpoint.check(
                    ImmutableSet.of(ResourcePaths.V1_LOAD_TABLE, ResourcePaths.V1_LOAD_VIEW),
                    ResourcePaths.V1_LOAD_TABLE))
        .doesNotThrowAnyException();
  }

  @Test
  public void unsupportedEndpoints() {
    assertThatThrownBy(() -> Endpoint.check(ImmutableSet.of(), ResourcePaths.V1_LOAD_TABLE))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Server does not support endpoint: %s", ResourcePaths.V1_LOAD_TABLE);

    assertThatThrownBy(
            () ->
                Endpoint.check(
                    ImmutableSet.of(ResourcePaths.V1_LOAD_VIEW), ResourcePaths.V1_LOAD_TABLE))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Server does not support endpoint: %s", ResourcePaths.V1_LOAD_TABLE);
  }
}
