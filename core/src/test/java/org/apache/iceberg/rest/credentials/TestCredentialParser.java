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
package org.apache.iceberg.rest.credentials;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestCredentialParser {
  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> CredentialParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid credential: null");

    assertThatThrownBy(() -> CredentialParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse credential from null object");
  }

  @Test
  public void invalidOrMissingFields() {
    assertThatThrownBy(() -> CredentialParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: prefix");

    assertThatThrownBy(() -> CredentialParser.fromJson("{\"prefix\": \"y\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing map: config");

    assertThatThrownBy(
            () -> CredentialParser.fromJson("{\"prefix\": \"\", \"config\": {\"x\": \"23\"}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid prefix: must be non-empty");

    assertThatThrownBy(() -> CredentialParser.fromJson("{\"prefix\": \"s3\", \"config\": {}}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid config: must be non-empty");
  }

  @Test
  public void s3Credential() {
    Credential credential =
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
            .build();

    String expectedJson =
        "{\n"
            + "  \"prefix\" : \"s3://custom-uri\",\n"
            + "  \"config\" : {\n"
            + "    \"s3.access-key-id\" : \"keyId\",\n"
            + "    \"s3.secret-access-key\" : \"accessKey\",\n"
            + "    \"s3.session-token\" : \"sessionToken\"\n"
            + "  }\n"
            + "}";

    String json = CredentialParser.toJson(credential, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(CredentialParser.toJson(CredentialParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void gcsCredential() {
    Credential credential =
        ImmutableCredential.builder()
            .prefix("gs://custom-uri")
            .config(
                ImmutableMap.of(
                    "gcs.oauth2.token", "gcsToken", "gcs.oauth2.token-expires-at", "1000"))
            .build();

    String expectedJson =
        "{\n"
            + "  \"prefix\" : \"gs://custom-uri\",\n"
            + "  \"config\" : {\n"
            + "    \"gcs.oauth2.token\" : \"gcsToken\",\n"
            + "    \"gcs.oauth2.token-expires-at\" : \"1000\"\n"
            + "  }\n"
            + "}";

    String json = CredentialParser.toJson(credential, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(CredentialParser.toJson(CredentialParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void adlsCredential() {
    Credential credential =
        ImmutableCredential.builder()
            .prefix("abfs://custom-uri")
            .config(
                ImmutableMap.of(
                    "adls.sas-token.account",
                    "sasToken",
                    "adls.auth.shared-key.account.key",
                    "accountKey"))
            .build();

    String expectedJson =
        "{\n"
            + "  \"prefix\" : \"abfs://custom-uri\",\n"
            + "  \"config\" : {\n"
            + "    \"adls.sas-token.account\" : \"sasToken\",\n"
            + "    \"adls.auth.shared-key.account.key\" : \"accountKey\"\n"
            + "  }\n"
            + "}";

    String json = CredentialParser.toJson(credential, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(CredentialParser.toJson(CredentialParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
