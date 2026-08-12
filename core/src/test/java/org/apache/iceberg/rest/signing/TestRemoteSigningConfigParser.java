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
package org.apache.iceberg.rest.signing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TestRemoteSigningConfigParser {

  @Test
  void nullCheck() {
    assertThatThrownBy(() -> RemoteSigningConfigParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid remote signing config: null");

    assertThatThrownBy(() -> RemoteSigningConfigParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse remote signing config from null object");
  }

  @Test
  void roundTripEmpty() {
    String json = RemoteSigningConfigParser.toJson(RemoteSigningConfig.EMPTY, true);
    assertThat(json).isEqualTo("{ }");
    assertThat(RemoteSigningConfigParser.fromJson(json)).isEqualTo(RemoteSigningConfig.EMPTY);
  }

  @Test
  void roundTripWithPropertiesOnly() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder()
            .putProperties("k1", "v1")
            .putProperties("k2", "v2")
            .build();

    String json = RemoteSigningConfigParser.toJson(config, true);
    assertThat(json)
        .isEqualTo(
            """
            {
              "properties" : {
                "k1" : "v1",
                "k2" : "v2"
              }
            }""");
    assertThat(RemoteSigningConfigParser.fromJson(json)).isEqualTo(config);
  }

  @Test
  void roundTripWithHeadersOnly() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder()
            .putHeaders("Authorization", List.of("Bearer token123"))
            .putHeaders("X-Custom", Arrays.asList("val1", "val2"))
            .build();

    String json = RemoteSigningConfigParser.toJson(config, true);
    assertThat(json)
        .isEqualTo(
            """
            {
              "headers" : {
                "Authorization" : [ "Bearer token123" ],
                "X-Custom" : [ "val1", "val2" ]
              }
            }""");
    assertThat(RemoteSigningConfigParser.fromJson(json)).isEqualTo(config);
  }

  @Test
  void roundTripWithPropertiesAndHeaders() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder()
            .properties(Map.of("prop1", "val1"))
            .headers(Map.of("Authorization", List.of("Bearer token123")))
            .build();

    String json = RemoteSigningConfigParser.toJson(config, true);
    assertThat(json)
        .isEqualTo(
            """
            {
              "headers" : {
                "Authorization" : [ "Bearer token123" ]
              },
              "properties" : {
                "prop1" : "val1"
              }
            }""");
    assertThat(RemoteSigningConfigParser.fromJson(json)).isEqualTo(config);
  }

  @Test
  void isEmpty() {
    RemoteSigningConfig empty = ImmutableRemoteSigningConfig.builder().build();
    assertThat(empty.isEmpty()).isTrue();

    RemoteSigningConfig withProps =
        ImmutableRemoteSigningConfig.builder().properties(Map.of("k", "v")).build();
    assertThat(withProps.isEmpty()).isFalse();

    RemoteSigningConfig withHeaders =
        ImmutableRemoteSigningConfig.builder().headers(Map.of("H", List.of("val"))).build();
    assertThat(withHeaders.isEmpty()).isFalse();
  }

  @Test
  void fromJsonIgnoresUnknownFields() {
    String json =
        """
        {
          "properties" : { "k" : "v" },
          "unknown-field" : "should-be-ignored"
        }""";
    RemoteSigningConfig config = RemoteSigningConfigParser.fromJson(json);
    assertThat(config.properties()).containsEntry("k", "v");
    assertThat(config.headers()).isEmpty();
  }
}
