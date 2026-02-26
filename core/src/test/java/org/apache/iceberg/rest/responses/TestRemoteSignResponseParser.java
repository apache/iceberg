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
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestRemoteSignResponseParser {

  @Test
  public void nullResponse() {
    assertThatThrownBy(() -> RemoteSignResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse remote sign response from null object");

    assertThatThrownBy(() -> RemoteSignResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid remote sign response: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> RemoteSignResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: uri");

    assertThatThrownBy(
            () ->
                RemoteSignResponseParser.fromJson(
                    "{\"uri\" : \"http://localhost:49208/iceberg-signer-test\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: headers");
  }

  @Test
  public void invalidUri() {
    assertThatThrownBy(() -> RemoteSignResponseParser.fromJson("{\"uri\" : 45, \"headers\" : {}}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: uri: 45");
  }

  @Test
  public void roundTripSerde() {
    RemoteSignResponse response =
        ImmutableRemoteSignResponse.builder()
            .uri(URI.create("http://localhost:49208/iceberg-signer-test"))
            .headers(
                ImmutableMap.of(
                    "amz-sdk-request",
                    Arrays.asList("attempt=1", "max=4"),
                    "Content-Length",
                    Collections.singletonList("191"),
                    "Content-Type",
                    Collections.singletonList("application/json"),
                    "User-Agent",
                    Arrays.asList("aws-sdk-java/2.20.18", "Linux/5.4.0-126")))
            .build();

    String json = RemoteSignResponseParser.toJson(response, true);
    assertThat(RemoteSignResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"uri\" : \"http://localhost:49208/iceberg-signer-test\",\n"
                + "  \"headers\" : {\n"
                + "    \"amz-sdk-request\" : [ \"attempt=1\", \"max=4\" ],\n"
                + "    \"Content-Length\" : [ \"191\" ],\n"
                + "    \"Content-Type\" : [ \"application/json\" ],\n"
                + "    \"User-Agent\" : [ \"aws-sdk-java/2.20.18\", \"Linux/5.4.0-126\" ]\n"
                + "  }\n"
                + "}");
  }
}
