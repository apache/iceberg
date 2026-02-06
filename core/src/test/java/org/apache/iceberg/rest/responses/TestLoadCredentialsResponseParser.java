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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.junit.jupiter.api.Test;

public class TestLoadCredentialsResponseParser {
  @Test
  public void nullCheck() {
    assertThatThrownBy(() -> LoadCredentialsResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid load credentials response: null");

    assertThatThrownBy(() -> LoadCredentialsResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse load credentials response from null object");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> LoadCredentialsResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: storage-credentials");

    assertThatThrownBy(() -> LoadCredentialsResponseParser.fromJson("{\"x\": \"val\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: storage-credentials");
  }

  @Test
  public void roundTripSerde() {
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix("s3://custom-uri")
                    .config(
                        ImmutableMap.of(
                            "s3.access-key-id",
                            "keyId",
                            "s3.secret-access-key",
                            "accessKey",
                            "s3.session-token",
                            "sessionToken"))
                    .build())
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix("gs://custom-uri")
                    .config(
                        ImmutableMap.of(
                            "gcs.oauth2.token", "gcsToken1", "gcs.oauth2.token-expires-at", "1000"))
                    .build())
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix("gs")
                    .config(
                        ImmutableMap.of(
                            "gcs.oauth2.token", "gcsToken2", "gcs.oauth2.token-expires-at", "2000"))
                    .build())
            .build();

    String expectedJson =
        "{\n"
            + "  \"storage-credentials\" : [ {\n"
            + "    \"prefix\" : \"s3://custom-uri\",\n"
            + "    \"config\" : {\n"
            + "      \"s3.access-key-id\" : \"keyId\",\n"
            + "      \"s3.secret-access-key\" : \"accessKey\",\n"
            + "      \"s3.session-token\" : \"sessionToken\"\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"prefix\" : \"gs://custom-uri\",\n"
            + "    \"config\" : {\n"
            + "      \"gcs.oauth2.token\" : \"gcsToken1\",\n"
            + "      \"gcs.oauth2.token-expires-at\" : \"1000\"\n"
            + "    }\n"
            + "  }, {\n"
            + "    \"prefix\" : \"gs\",\n"
            + "    \"config\" : {\n"
            + "      \"gcs.oauth2.token\" : \"gcsToken2\",\n"
            + "      \"gcs.oauth2.token-expires-at\" : \"2000\"\n"
            + "    }\n"
            + "  } ]\n"
            + "}";

    String json = LoadCredentialsResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(LoadCredentialsResponseParser.fromJson(json)).isEqualTo(response);
  }
}
