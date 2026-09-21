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
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.SignStatus;
import org.junit.jupiter.api.Test;

public class TestRemoteSignBatchResponseParser {

  @Test
  public void nullResponse() {
    assertThatThrownBy(() -> RemoteSignBatchResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse remote sign batch response from null object");

    assertThatThrownBy(() -> RemoteSignBatchResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid remote sign batch response: null");
  }

  @Test
  public void invalidResults() {
    assertThatThrownBy(() -> RemoteSignBatchResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: results");

    assertThatThrownBy(() -> RemoteSignBatchResponseParser.fromJson("{\"results\" : 23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse results from non-array: 23");
  }

  @Test
  public void roundTripSerdeWithCompletedResult() {
    RemoteSignBatchResponse response =
        ImmutableRemoteSignBatchResponse.builder()
            .addResults(
                ImmutableRemoteSignBatchResult.builder()
                    .status(SignStatus.COMPLETED)
                    .uri(URI.create("https://bucket.s3.amazonaws.com/part-0?X-Amz-Signature=abc"))
                    .headers(ImmutableMap.of("x-amz-request-payer", List.of("requester")))
                    .build())
            .build();

    String json = RemoteSignBatchResponseParser.toJson(response, true);
    assertThat(RemoteSignBatchResponseParser.fromJson(json)).isEqualTo(response);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"results\" : [ {\n"
                + "    \"status\" : \"completed\",\n"
                + "    \"uri\" : \"https://bucket.s3.amazonaws.com/part-0?X-Amz-Signature=abc\",\n"
                + "    \"headers\" : {\n"
                + "      \"x-amz-request-payer\" : [ \"requester\" ]\n"
                + "    }\n"
                + "  } ]\n"
                + "}");
  }

  @Test
  public void roundTripSerdeWithFailedResult() {
    RemoteSignBatchResponse response =
        ImmutableRemoteSignBatchResponse.builder()
            .addResults(
                ImmutableRemoteSignBatchResult.builder()
                    .status(SignStatus.FAILED)
                    .error(
                        ErrorResponse.builder()
                            .responseCode(403)
                            .withType("ForbiddenException")
                            .withMessage("Not authorized to sign s3://bucket/part-0")
                            .build())
                    .build())
            .build();

    String json = RemoteSignBatchResponseParser.toJson(response, true);
    assertThat(json)
        .isEqualTo(
            "{\n"
                + "  \"results\" : [ {\n"
                + "    \"status\" : \"failed\",\n"
                + "    \"error\" : {\n"
                + "      \"message\" : \"Not authorized to sign s3://bucket/part-0\",\n"
                + "      \"type\" : \"ForbiddenException\",\n"
                + "      \"code\" : 403\n"
                + "    }\n"
                + "  } ]\n"
                + "}");

    RemoteSignBatchResult parsed = RemoteSignBatchResponseParser.fromJson(json).results().get(0);
    assertThat(parsed.status()).isEqualTo(SignStatus.FAILED);
    assertThat(parsed.uri()).isNull();
    assertThat(parsed.error().code()).isEqualTo(403);
    assertThat(parsed.error().type()).isEqualTo("ForbiddenException");
    assertThat(parsed.error().message()).isEqualTo("Not authorized to sign s3://bucket/part-0");
  }
}
