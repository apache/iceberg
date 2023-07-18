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
package org.apache.iceberg.aws.lambda.rest;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.net.URI;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLambdaRESTRequestParser {

  @Test
  public void testNullRequest() {
    Assertions.assertThatThrownBy(() -> LambdaRESTRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse Lambda REST request from null object");

    Assertions.assertThatThrownBy(() -> LambdaRESTRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid Lambda REST request: null");
  }

  @Test
  public void testMissingFields() {
    Assertions.assertThatThrownBy(() -> LambdaRESTRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: method");

    Assertions.assertThatThrownBy(() -> LambdaRESTRequestParser.fromJson("{\"method\":\"GET\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: uri");

    Assertions.assertThatThrownBy(
            () -> LambdaRESTRequestParser.fromJson("{\"method\":\"GET\", \"uri\":\"test.com\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing map: headers");
  }

  @Test
  public void testRoundTripSerdeWithoutEntity() {
    ImmutableLambdaRESTRequest request =
        ImmutableLambdaRESTRequest.builder()
            .method("GET")
            .uri(URI.create("test.com"))
            .headers(ImmutableMap.of("key", "val"))
            .build();

    String json = LambdaRESTRequestParser.toJson(request, true);
    Assertions.assertThat(LambdaRESTRequestParser.fromJson(json)).isEqualTo(request);
    Assertions.assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"method\" : \"GET\",\n"
                + "  \"uri\" : \"test.com\",\n"
                + "  \"headers\" : {\n"
                + "    \"key\" : \"val\"\n"
                + "  }\n"
                + "}");
  }

  @Test
  public void testRoundTripSerde() {
    ImmutableLambdaRESTRequest request =
        ImmutableLambdaRESTRequest.builder()
            .method("POST")
            .uri(URI.create("test.com"))
            .headers(ImmutableMap.of("key", "val"))
            .entity("string")
            .build();

    String json = LambdaRESTRequestParser.toJson(request, true);
    Assertions.assertThat(LambdaRESTRequestParser.fromJson(json)).isEqualTo(request);
    Assertions.assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"method\" : \"POST\",\n"
                + "  \"uri\" : \"test.com\",\n"
                + "  \"headers\" : {\n"
                + "    \"key\" : \"val\"\n"
                + "  },\n"
                + "  \"entity\" : \"string\"\n"
                + "}");
  }

  @Test
  public void testRoundTripSerdeJsonStream() {
    ImmutableLambdaRESTRequest request =
        ImmutableLambdaRESTRequest.builder()
            .method("POST")
            .uri(URI.create("test.com"))
            .headers(ImmutableMap.of("key", "val"))
            .entity("string")
            .build();

    InputStream json = LambdaRESTRequestParser.toJsonStream(request, true);
    Assertions.assertThat(LambdaRESTRequestParser.fromJsonStream(json)).isEqualTo(request);
  }
}
