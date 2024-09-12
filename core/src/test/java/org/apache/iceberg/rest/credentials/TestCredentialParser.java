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
import java.time.Instant;
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
  public void invalidTypeOrMissingType() {
    assertThat(CredentialParser.fromJson("{\"type\": \"unknown\",\"scheme\": \"s3\"}")).isNull();

    assertThatThrownBy(() -> CredentialParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: type");

    assertThatThrownBy(() -> CredentialParser.fromJson("{\"x\": \"y\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: type");
  }

  @Test
  public void s3Credential() {
    Instant expiresAt = Instant.now();

    Credential credential =
        ImmutableS3Credential.builder()
            .accessKeyId("keyId")
            .scheme("s3://custom-uri")
            .secretAccessKey("accessKey")
            .sessionToken("sessionToken")
            .expiresAt(expiresAt)
            .build();

    String expectedJson =
        String.format(
            "{\n"
                + "  \"type\" : \"s3\",\n"
                + "  \"scheme\" : \"s3://custom-uri\",\n"
                + "  \"access-key-id\" : \"keyId\",\n"
                + "  \"secret-access-key\" : \"accessKey\",\n"
                + "  \"session-token\" : \"sessionToken\",\n"
                + "  \"expires-at-ms\" : %s\n"
                + "}",
            expiresAt.toEpochMilli());

    String json = CredentialParser.toJson(credential, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(CredentialParser.toJson(CredentialParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void s3CredentialWithMissingProperties() {
    assertThatThrownBy(
            () ->
                CredentialParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"s3\",\n"
                        + "  \"access-key-id\" : \"keyId\",\n"
                        + "  \"secret-access-key\" : \"accessKey\",\n"
                        + "  \"session-token\" : \"sessionToken\""
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: scheme");

    assertThatThrownBy(
            () ->
                CredentialParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"s3\",\n"
                        + "  \"scheme\" : \"s3\",\n"
                        + "  \"access-key-id\" : \"keyId\",\n"
                        + "  \"secret-access-key\" : \"accessKey\",\n"
                        + "  \"session-token\" : \"sessionToken\""
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: expires-at-ms");

    assertThatThrownBy(
            () ->
                CredentialParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"s3\",\n"
                        + "  \"scheme\" : \"s3\",\n"
                        + "  \"access-key-id\" : \"keyId\",\n"
                        + "  \"secret-access-key\" : \"accessKey\",\n"
                        + "  \"expires-at-ms\" : \"1\""
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: session-token");

    assertThatThrownBy(
            () ->
                CredentialParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"s3\",\n"
                        + "  \"scheme\" : \"s3\",\n"
                        + "  \"access-key-id\" : \"keyId\",\n"
                        + "  \"session-token\" : \"sessionToken\",\n"
                        + "  \"expires-at-ms\" : \"1\""
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: secret-access-key");

    assertThatThrownBy(
            () ->
                CredentialParser.fromJson(
                    "{\n"
                        + "  \"type\" : \"s3\",\n"
                        + "  \"scheme\" : \"s3\",\n"
                        + "  \"secret-access-key\" : \"accessKey\",\n"
                        + "  \"expires-at-ms\" : \"1\""
                        + "}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: access-key-id");
  }

  @Test
  public void gcsCredential() {
    Instant expiresAt = Instant.now();

    Credential credential =
        ImmutableGcsCredential.builder().token("gcsToken").expiresAt(expiresAt).build();

    String expectedJson =
        String.format(
            "{\n"
                + "  \"type\" : \"gcs\",\n"
                + "  \"scheme\" : \"gs\",\n"
                + "  \"token\" : \"gcsToken\",\n"
                + "  \"expires-at-ms\" : %s\n"
                + "}",
            expiresAt.toEpochMilli());

    String json = CredentialParser.toJson(credential, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(CredentialParser.toJson(CredentialParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void gcsCredentialWithMissingProperties() {
    assertThatThrownBy(() -> CredentialParser.fromJson("{\"type\": \"gcs\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: scheme");

    assertThatThrownBy(
            () ->
                CredentialParser.fromJson(
                    "{\"type\": \"gcs\",\"scheme\":\"gs\",\"token\": \"gcsToken\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: expires-at-ms");

    assertThatThrownBy(
            () ->
                CredentialParser.fromJson(
                    "{\"type\": \"gcs\",\"scheme\":\"gs\",\"expires-at-ms\": 1}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: token");
  }

  @Test
  public void adlsCredential() {
    Instant expiresAt = Instant.now();

    Credential credential =
        ImmutableAdlsCredential.builder().sasToken("sasToken").expiresAt(expiresAt).build();

    String expectedJson =
        String.format(
            "{\n"
                + "  \"type\" : \"adls\",\n"
                + "  \"scheme\" : \"afbs\",\n"
                + "  \"sas-token\" : \"sasToken\",\n"
                + "  \"expires-at-ms\" : %s\n"
                + "}",
            expiresAt.toEpochMilli());

    String json = CredentialParser.toJson(credential, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(CredentialParser.toJson(CredentialParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void adlsCredentialWithMissingProperties() {
    assertThatThrownBy(() -> CredentialParser.fromJson("{\"type\": \"adls\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: scheme");
    assertThatThrownBy(
            () ->
                CredentialParser.fromJson(
                    "{\"type\": \"adls\",\"scheme\": \"afbs\",\"sas-token\": \"adlsToken\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: expires-at-ms");

    assertThatThrownBy(
            () ->
                CredentialParser.fromJson(
                    "{\"type\": \"adls\",\"scheme\": \"afbs\",\"expires-at-ms\": 1}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: sas-token");
  }
}
