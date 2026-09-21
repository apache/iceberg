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
import java.net.URI;
import org.junit.jupiter.api.Test;

public class TestRemoteSignBatchRequestParser {

  @Test
  public void nullRequest() {
    assertThatThrownBy(() -> RemoteSignBatchRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse remote sign batch request from null object");

    assertThatThrownBy(() -> RemoteSignBatchRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid remote sign batch request: null");
  }

  @Test
  public void invalidRequests() {
    assertThatThrownBy(() -> RemoteSignBatchRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: requests");

    assertThatThrownBy(() -> RemoteSignBatchRequestParser.fromJson("{\"requests\" : 23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse requests from non-array: 23");
  }

  @Test
  public void roundTripSerdeWithProperties() {
    RemoteSignBatchRequest request =
        ImmutableRemoteSignBatchRequest.builder()
            .addRequests(signRequest("s3://bucket/data/part-0.parquet"))
            .putProperties("tenant", "t1")
            .build();

    String json = RemoteSignBatchRequestParser.toJson(request, true);

    assertThat(RemoteSignBatchRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"requests\" : [ {\n"
                + "    \"region\" : \"us-east-1\",\n"
                + "    \"method\" : \"GET\",\n"
                + "    \"uri\" : \"s3://bucket/data/part-0.parquet\",\n"
                + "    \"headers\" : { },\n"
                + "    \"provider\" : \"s3\"\n"
                + "  } ],\n"
                + "  \"properties\" : {\n"
                + "    \"tenant\" : \"t1\"\n"
                + "  }\n"
                + "}");
  }

  @Test
  public void roundTripSerde() {
    RemoteSignBatchRequest request =
        ImmutableRemoteSignBatchRequest.builder()
            .addRequests(signRequest("s3://bucket/data/part-0.parquet"))
            .addRequests(signRequest("s3://bucket/data/part-1.parquet"))
            .build();

    String json = RemoteSignBatchRequestParser.toJson(request, true);
    assertThat(RemoteSignBatchRequestParser.fromJson(json)).isEqualTo(request);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"requests\" : [ {\n"
                + "    \"region\" : \"us-east-1\",\n"
                + "    \"method\" : \"GET\",\n"
                + "    \"uri\" : \"s3://bucket/data/part-0.parquet\",\n"
                + "    \"headers\" : { },\n"
                + "    \"provider\" : \"s3\"\n"
                + "  }, {\n"
                + "    \"region\" : \"us-east-1\",\n"
                + "    \"method\" : \"GET\",\n"
                + "    \"uri\" : \"s3://bucket/data/part-1.parquet\",\n"
                + "    \"headers\" : { },\n"
                + "    \"provider\" : \"s3\"\n"
                + "  } ]\n"
                + "}");
  }

  private static RemoteSignRequest signRequest(String location) {
    return ImmutableRemoteSignRequest.builder()
        .region("us-east-1")
        .method("GET")
        .uri(URI.create(location))
        .provider("s3")
        .build();
  }
}
