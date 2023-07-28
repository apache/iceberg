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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLambdaRESTResponseParser {

  @Test
  public void testNullRequest() {
    Assertions.assertThatThrownBy(() -> LambdaRESTResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse Lambda REST response from null object");

    Assertions.assertThatThrownBy(() -> LambdaRESTResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid Lambda REST response: null");
  }

  @Test
  public void testMissingFields() {
    Assertions.assertThatThrownBy(() -> LambdaRESTResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: code");
    Assertions.assertThatThrownBy(() -> LambdaRESTResponseParser.fromJson("{\"code\":1}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing map: headers");
  }

  @Test
  public void testRoundTripSerdeWithoutEntity() {
    ImmutableLambdaRESTResponse response =
        ImmutableLambdaRESTResponse.builder()
            .code(1)
            .headers(ImmutableMap.of("key", "val"))
            .reason("string")
            .build();

    String json = LambdaRESTResponseParser.toJson(response, true);
    Assertions.assertThat(LambdaRESTResponseParser.fromJson(json)).isEqualTo(response);
    Assertions.assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"code\" : 1,\n"
                + "  \"headers\" : {\n"
                + "    \"key\" : \"val\"\n"
                + "  },\n"
                + "  \"reason\" : \"string\"\n"
                + "}");
  }

  @Test
  public void testRoundTripSerdeWithoutReason() {
    ImmutableLambdaRESTResponse response =
        ImmutableLambdaRESTResponse.builder()
            .code(1)
            .headers(ImmutableMap.of("key", "val"))
            .entity("string")
            .build();

    String json = LambdaRESTResponseParser.toJson(response, true);
    Assertions.assertThat(LambdaRESTResponseParser.fromJson(json)).isEqualTo(response);
    Assertions.assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"code\" : 1,\n"
                + "  \"headers\" : {\n"
                + "    \"key\" : \"val\"\n"
                + "  },\n"
                + "  \"entity\" : \"string\"\n"
                + "}");
  }

  @Test
  public void testRoundTripSerde() {
    ImmutableLambdaRESTResponse response =
        ImmutableLambdaRESTResponse.builder()
            .code(1)
            .headers(ImmutableMap.of("key", "val"))
            .entity("string")
            .reason("string2")
            .build();

    String json = LambdaRESTResponseParser.toJson(response, true);
    Assertions.assertThat(LambdaRESTResponseParser.fromJson(json)).isEqualTo(response);
    Assertions.assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"code\" : 1,\n"
                + "  \"headers\" : {\n"
                + "    \"key\" : \"val\"\n"
                + "  },\n"
                + "  \"entity\" : \"string\",\n"
                + "  \"reason\" : \"string2\"\n"
                + "}");
  }

  @Test
  public void testRoundTripSerdeJsonStream() {
    ImmutableLambdaRESTResponse response =
        ImmutableLambdaRESTResponse.builder()
            .code(1)
            .headers(ImmutableMap.of("key", "val"))
            .entity("string")
            .reason("string2")
            .build();

    InputStream json = LambdaRESTResponseParser.toJsonStream(response, true);
    Assertions.assertThat(LambdaRESTResponseParser.fromJsonStream(json)).isEqualTo(response);
  }
}
