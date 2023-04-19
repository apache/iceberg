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
package org.apache.iceberg.aws.s3.signer;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestS3SignRequestParser {

  @Test
  public void nullRequest() {
    Assertions.assertThatThrownBy(() -> S3SignRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse s3 sign request from null object");

    Assertions.assertThatThrownBy(() -> S3SignRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid s3 sign request: null");
  }

  @Test
  public void missingFields() {
    Assertions.assertThatThrownBy(() -> S3SignRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: region");

    Assertions.assertThatThrownBy(() -> S3SignRequestParser.fromJson("{\"region\":\"us-west-2\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: method");

    Assertions.assertThatThrownBy(
            () -> S3SignRequestParser.fromJson("{\"region\":\"us-west-2\", \"method\" : \"PUT\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: uri");

    Assertions.assertThatThrownBy(
            () ->
                S3SignRequestParser.fromJson(
                    "{\n"
                        + "  \"region\" : \"us-west-2\",\n"
                        + "  \"method\" : \"PUT\",\n"
                        + "  \"uri\" : \"http://localhost:49208/iceberg-signer-test\"\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: headers");
  }

  @Test
  public void invalidMethod() {
    Assertions.assertThatThrownBy(
            () ->
                S3SignRequestParser.fromJson(
                    "{\n"
                        + "  \"region\" : \"us-west-2\",\n"
                        + "  \"method\" : 23,\n"
                        + "  \"uri\" : \"http://localhost:49208/iceberg-signer-test\",\n"
                        + "  \"headers\" : {}}\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: method: 23");
  }

  @Test
  public void invalidUri() {
    Assertions.assertThatThrownBy(
            () ->
                S3SignRequestParser.fromJson(
                    "{\n"
                        + "  \"region\" : \"us-west-2\",\n"
                        + "  \"method\" : \"PUT\",\n"
                        + "  \"uri\" : 45,\n"
                        + "  \"headers\" : {}}\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: uri: 45");
  }

  @Test
  public void invalidRegion() {
    Assertions.assertThatThrownBy(
            () ->
                S3SignRequestParser.fromJson(
                    "{\n"
                        + "  \"region\" : 23,\n"
                        + "  \"method\" : \"PUT\",\n"
                        + "  \"uri\" : \"http://localhost:49208/iceberg-signer-test\",\n"
                        + "  \"headers\" : {}\n"
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: region: 23");
  }

  @Test
  public void roundTripSerde() {
    ImmutableS3SignRequest s3SignRequest =
        ImmutableS3SignRequest.builder()
            .uri(URI.create("http://localhost:49208/iceberg-signer-test"))
            .method("PUT")
            .region("us-west-2")
            .headers(
                ImmutableMap.of(
                    "amz-sdk-request",
                    Arrays.asList("attempt=1", "max=4"),
                    "Content-Length",
                    Arrays.asList("191"),
                    "Content-Type",
                    Arrays.asList("application/json"),
                    "User-Agent",
                    Arrays.asList("aws-sdk-java/2.20.18", "Linux/5.4.0-126")))
            .build();

    String json = S3SignRequestParser.toJson(s3SignRequest, true);
    Assertions.assertThat(S3SignRequestParser.fromJson(json)).isEqualTo(s3SignRequest);
    Assertions.assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"region\" : \"us-west-2\",\n"
                + "  \"method\" : \"PUT\",\n"
                + "  \"uri\" : \"http://localhost:49208/iceberg-signer-test\",\n"
                + "  \"headers\" : {\n"
                + "    \"amz-sdk-request\" : [ \"attempt=1\", \"max=4\" ],\n"
                + "    \"Content-Length\" : [ \"191\" ],\n"
                + "    \"Content-Type\" : [ \"application/json\" ],\n"
                + "    \"User-Agent\" : [ \"aws-sdk-java/2.20.18\", \"Linux/5.4.0-126\" ]\n"
                + "  }\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithProperties() {
    ImmutableS3SignRequest s3SignRequest =
        ImmutableS3SignRequest.builder()
            .uri(URI.create("http://localhost:49208/iceberg-signer-test"))
            .method("PUT")
            .region("us-west-2")
            .headers(
                ImmutableMap.of(
                    "amz-sdk-request",
                    Arrays.asList("attempt=1", "max=4"),
                    "Content-Length",
                    Arrays.asList("191"),
                    "Content-Type",
                    Arrays.asList("application/json"),
                    "User-Agent",
                    Arrays.asList("aws-sdk-java/2.20.18", "Linux/5.4.0-126")))
            .properties(ImmutableMap.of("k1", "v1"))
            .build();

    String json = S3SignRequestParser.toJson(s3SignRequest, true);
    Assertions.assertThat(S3SignRequestParser.fromJson(json)).isEqualTo(s3SignRequest);
    Assertions.assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"region\" : \"us-west-2\",\n"
                + "  \"method\" : \"PUT\",\n"
                + "  \"uri\" : \"http://localhost:49208/iceberg-signer-test\",\n"
                + "  \"headers\" : {\n"
                + "    \"amz-sdk-request\" : [ \"attempt=1\", \"max=4\" ],\n"
                + "    \"Content-Length\" : [ \"191\" ],\n"
                + "    \"Content-Type\" : [ \"application/json\" ],\n"
                + "    \"User-Agent\" : [ \"aws-sdk-java/2.20.18\", \"Linux/5.4.0-126\" ]\n"
                + "  },\n"
                + "  \"properties\" : {\n"
                + "    \"k1\" : \"v1\"\n"
                + "  }\n"
                + "}");
  }
}
