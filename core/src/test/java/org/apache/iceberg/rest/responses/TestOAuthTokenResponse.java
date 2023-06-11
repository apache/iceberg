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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOAuthTokenResponse extends RequestResponseTestBase<OAuthTokenResponse> {
  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"access_token", "token_type", "issued_token_type", "expires_in", "scope"};
  }

  @Override
  public OAuthTokenResponse createExampleInstance() {
    return OAuthTokenResponse.builder()
        .setExpirationInSeconds(600)
        .withToken("test-token")
        .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
        .withTokenType("Bearer")
        .addScope("catalog")
        .build();
  }

  @Override
  public void assertEquals(OAuthTokenResponse actual, OAuthTokenResponse expected) {
    Assertions.assertThat(actual.token()).as("Token should match").isEqualTo(expected.token());
    Assertions.assertThat(actual.tokenType())
        .as("Token type should match")
        .isEqualTo(expected.tokenType());
    Assertions.assertThat(actual.issuedTokenType())
        .as("Issued token type should match")
        .isEqualTo(expected.issuedTokenType());
    Assertions.assertThat(actual.expiresInSeconds())
        .as("Expiration should match")
        .isEqualTo(expected.expiresInSeconds());
    Assertions.assertThat(actual.scopes()).as("Scope should match").isEqualTo(expected.scopes());
  }

  @Override
  public OAuthTokenResponse deserialize(String json) throws JsonProcessingException {
    return OAuth2Util.tokenResponseFromJson(json);
  }

  @Override
  public String serialize(OAuthTokenResponse response) throws JsonProcessingException {
    return OAuth2Util.tokenResponseToJson(response);
  }

  @Test
  public void testRoundTrip() throws Exception {
    assertRoundTripSerializesEquallyFrom(
        "{\"access_token\":\"bearer-token\",\"token_type\":\"bearer\"}",
        OAuthTokenResponse.builder().withToken("bearer-token").withTokenType("bearer").build());

    assertRoundTripSerializesEquallyFrom(
        "{\"access_token\":\"bearer-token\",\"token_type\":\"bearer\","
            + "\"issued_token_type\":\"urn:ietf:params:oauth:token-type:access_token\"}",
        OAuthTokenResponse.builder()
            .withToken("bearer-token")
            .withTokenType("bearer")
            .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
            .build());

    assertRoundTripSerializesEquallyFrom(
        "{\"access_token\":\"bearer-token\",\"token_type\":\"bearer\",\"expires_in\":600}",
        OAuthTokenResponse.builder()
            .withToken("bearer-token")
            .withTokenType("bearer")
            .setExpirationInSeconds(600)
            .build());

    assertRoundTripSerializesEquallyFrom(
        "{\"access_token\":\"bearer-token\",\"token_type\":\"bearer\",\"scope\":\"a b\"}",
        OAuthTokenResponse.builder()
            .withToken("bearer-token")
            .withTokenType("bearer")
            .addScope("a")
            .addScope("b")
            .build());

    assertRoundTripSerializesEquallyFrom(
        "{\"access_token\":\"bearer-token\",\"token_type\":\"bearer\","
            + "\"issued_token_type\":\"urn:ietf:params:oauth:token-type:access_token\","
            + "\"expires_in\":600,\"scope\":\"a b\"}",
        OAuthTokenResponse.builder()
            .withToken("bearer-token")
            .withTokenType("bearer")
            .withIssuedTokenType("urn:ietf:params:oauth:token-type:access_token")
            .setExpirationInSeconds(600)
            .addScope("a")
            .addScope("b")
            .build());
  }

  @Test
  public void testFailures() {
    Assertions.assertThatThrownBy(() -> deserialize("{\"token_type\":\"bearer\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing string: access_token");

    Assertions.assertThatThrownBy(
            () -> deserialize("{\"access_token\":34,\"token_type\":\"bearer\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse to a string value: access_token: 34");

    Assertions.assertThatThrownBy(() -> deserialize("{\"access_token\":\"bearer-token\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing string: token_type");

    Assertions.assertThatThrownBy(
            () -> deserialize("{\"access_token\":\"bearer-token\",\"token_type\":34}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse to a string value: token_type: 34");
  }
}
