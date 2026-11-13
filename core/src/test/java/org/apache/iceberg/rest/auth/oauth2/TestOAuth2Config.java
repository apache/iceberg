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
package org.apache.iceberg.rest.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestOAuth2Config {

  private static final Map<String, String> MINIMAL =
      Map.of(
          OAuth2Config.TOKEN_ENDPOINT, "https://auth.example.com/token",
          OAuth2Config.CLIENT_ID, "client1",
          OAuth2Config.CLIENT_SECRET, "s3cr3t");

  @Test
  void defaults() {
    OAuth2Config config = OAuth2Config.fromProperties(MINIMAL);
    assertThat(config.issuerUrl()).isNull();
    assertThat(config.tokenEndpoint()).isEqualTo(URI.create("https://auth.example.com/token"));
    assertThat(config.clientId()).isEqualTo(new ClientID("client1"));
    assertThat(config.clientSecret()).isEqualTo(new Secret("s3cr3t"));
    assertThat(config.clientAuth()).isEqualTo(ClientAuthenticationMethod.CLIENT_SECRET_BASIC);
    assertThat(config.scope()).isNull();
    assertThat(config.resources()).isEmpty();
    assertThat(config.audiences()).isEmpty();
    assertThat(config.extraParams()).isEmpty();
    assertThat(config.accessTokenLifespan()).isEqualTo(Duration.ofHours(1));
    assertThat(config.prefetch()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.jitter()).isEqualTo(Duration.ofSeconds(5));
  }

  @Test
  void allProperties() {
    OAuth2Config config =
        OAuth2Config.fromProperties(
            ImmutableMap.<String, String>builder()
                .put(OAuth2Config.ISSUER_URL, "https://auth.example.com/realms/master")
                .put(OAuth2Config.TOKEN_ENDPOINT, "https://auth.example.com/token")
                .put(OAuth2Config.CLIENT_ID, "client1")
                .put(OAuth2Config.CLIENT_AUTH, "client_secret_post")
                .put(OAuth2Config.CLIENT_SECRET, "s3cr3t")
                .put(OAuth2Config.SCOPE, "catalog  read")
                .put(OAuth2Config.RESOURCE, "https://catalog.example.com/api, urn:example:storage,")
                .put(OAuth2Config.AUDIENCE, "catalog-api,storage-api")
                .put(OAuth2Config.EXTRA_PARAMS_PREFIX + "extra", "value")
                .put(OAuth2Config.TOKEN_REFRESH_ACCESS_TOKEN_LIFESPAN, "PT30M")
                .put(OAuth2Config.TOKEN_REFRESH_PREFETCH, "PT1M")
                .put(OAuth2Config.TOKEN_REFRESH_JITTER, "PT0S")
                .put("unrelated", "value")
                .buildOrThrow());
    assertThat(config.issuerUrl()).isEqualTo(URI.create("https://auth.example.com/realms/master"));
    assertThat(config.tokenEndpoint()).isEqualTo(URI.create("https://auth.example.com/token"));
    assertThat(config.clientAuth()).isEqualTo(ClientAuthenticationMethod.CLIENT_SECRET_POST);
    assertThat(config.scope()).isEqualTo(Scope.parse("catalog read"));
    assertThat(config.resources())
        .containsExactly(
            URI.create("https://catalog.example.com/api"), URI.create("urn:example:storage"));
    assertThat(config.audiences()).containsExactly("catalog-api", "storage-api");
    assertThat(config.extraParams()).containsExactly(Map.entry("extra", "value"));
    assertThat(config.accessTokenLifespan()).isEqualTo(Duration.ofMinutes(30));
    assertThat(config.prefetch()).isEqualTo(Duration.ofMinutes(1));
    assertThat(config.jitter()).isZero();
  }

  @Test
  void equalityAndRedaction() {
    OAuth2Config config1 = OAuth2Config.fromProperties(MINIMAL);
    OAuth2Config config2 = OAuth2Config.fromProperties(ImmutableMap.copyOf(MINIMAL));
    assertThat(config1).isEqualTo(config2).hasSameHashCodeAs(config2);
    assertThat(config1.toString()).doesNotContain("s3cr3t");
  }

  @ParameterizedTest
  @MethodSource("invalidProperties")
  void invalid(Map<String, String> overrides, String expectedMessage) {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder().putAll(MINIMAL).putAll(overrides).buildKeepingLast();
    assertThatThrownBy(() -> OAuth2Config.fromProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(expectedMessage);
  }

  static Stream<Arguments> invalidProperties() {
    return Stream.of(
        Arguments.of(Map.of(OAuth2Config.TOKEN_ENDPOINT, ""), "Token endpoint must be absolute"),
        Arguments.of(Map.of(OAuth2Config.ISSUER_URL, "ht tp://x"), "Invalid value for property"),
        Arguments.of(Map.of(OAuth2Config.CLIENT_ID, ""), "Invalid value for property"),
        Arguments.of(Map.of(OAuth2Config.CLIENT_SECRET, ""), "Client secret must not be empty"),
        Arguments.of(
            Map.of(OAuth2Config.CLIENT_AUTH, "none"),
            "Client authentication method must be one of"),
        Arguments.of(
            Map.of(OAuth2Config.CLIENT_AUTH, "private_key_jwt"),
            "Client authentication method must be one of"),
        Arguments.of(
            Map.of(OAuth2Config.TOKEN_ENDPOINT, "/relative/token"),
            "Token endpoint must be absolute"),
        Arguments.of(
            Map.of(OAuth2Config.TOKEN_ENDPOINT, "https://auth.example.com/token?x=y"),
            "Token endpoint must not have a query part"),
        Arguments.of(
            Map.of(OAuth2Config.ISSUER_URL, "https://user:pw@auth.example.com"),
            "Issuer URL must not have a user info part"),
        Arguments.of(
            Map.of(OAuth2Config.RESOURCE, "https://a.example.com,/relative"),
            "Resource must be an absolute URI without a fragment part: /relative"),
        Arguments.of(
            Map.of(OAuth2Config.RESOURCE, "https://a.example.com#frag"),
            "Resource must be an absolute URI without a fragment part: https://a.example.com#frag"),
        Arguments.of(
            Map.of(OAuth2Config.TOKEN_REFRESH_ACCESS_TOKEN_LIFESPAN, "PT0S"),
            "Access token lifespan must be positive"),
        Arguments.of(
            Map.of(OAuth2Config.TOKEN_REFRESH_PREFETCH, "PT-1S"),
            "Token refresh prefetch must not be negative"),
        Arguments.of(
            Map.of(OAuth2Config.TOKEN_REFRESH_JITTER, "PT-1S"),
            "Token refresh jitter must not be negative"),
        Arguments.of(
            Map.of(OAuth2Config.TOKEN_REFRESH_ACCESS_TOKEN_LIFESPAN, "PT10S"),
            "prefetch plus jitter must be less than the access token lifespan"),
        Arguments.of(
            Map.of(OAuth2Config.TOKEN_REFRESH_PREFETCH, "abc"), "Invalid value for property"));
  }

  @Test
  void missingClientCredentials() {
    assertThatThrownBy(
            () ->
                OAuth2Config.fromProperties(
                    Map.of(
                        OAuth2Config.TOKEN_ENDPOINT, "https://auth.example.com/token",
                        OAuth2Config.CLIENT_SECRET, "s3cr3t")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Client ID must be set (rest.auth.oauth2.client-id)");
    assertThatThrownBy(
            () ->
                OAuth2Config.fromProperties(
                    Map.of(
                        OAuth2Config.TOKEN_ENDPOINT, "https://auth.example.com/token",
                        OAuth2Config.CLIENT_ID, "client1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Client secret must be set (rest.auth.oauth2.client-secret)");
  }

  @Test
  void missingEndpoint() {
    assertThatThrownBy(
            () ->
                OAuth2Config.fromProperties(
                    Map.of(
                        OAuth2Config.CLIENT_ID, "client1", OAuth2Config.CLIENT_SECRET, "s3cr3t")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Either the issuer URL");
  }
}
