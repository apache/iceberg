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
package org.apache.iceberg.rest.auth.oauth2.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestBasicConfig {

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("ResultOfMethodCallIgnored")
  void testValidate(Map<String, String> properties, List<String> expected) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> BasicConfig.parse(properties).build())
        .withMessage(ConfigValidator.buildDescription(expected.stream()));
  }

  @SuppressWarnings("MethodLength")
  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            Map.of(BasicConfig.CLIENT_ID, "Client1", BasicConfig.CLIENT_SECRET, "s3cr3t"),
            List.of(
                "either issuer URL or token endpoint must be set (rest.auth.oauth2.issuer-url / rest.auth.oauth2.token-endpoint)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.ISSUER_URL,
                "realms/master"),
            List.of("Issuer URL must not be relative (rest.auth.oauth2.issuer-url)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.ISSUER_URL,
                "https://example.com?query"),
            List.of("Issuer URL must not have a query part (rest.auth.oauth2.issuer-url)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.ISSUER_URL,
                "https://example.com#fragment"),
            List.of("Issuer URL must not have a fragment part (rest.auth.oauth2.issuer-url)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.TOKEN_ENDPOINT,
                "https://user:pass@example.com"),
            List.of(
                "Token endpoint must not have a user info part (rest.auth.oauth2.token-endpoint)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com?query"),
            List.of("Token endpoint must not have a query part (rest.auth.oauth2.token-endpoint)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com#fragment"),
            List.of(
                "Token endpoint must not have a fragment part (rest.auth.oauth2.token-endpoint)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.TOKEN_ENDPOINT,
                "/token"),
            List.of("Token endpoint must not be relative (rest.auth.oauth2.token-endpoint)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.TOKEN_ENDPOINT,
                "token"),
            List.of("Token endpoint must not be relative (rest.auth.oauth2.token-endpoint)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token"),
            List.of("client ID must not be empty (rest.auth.oauth2.client-id)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_AUTH,
                ClientAuthenticationMethod.CLIENT_SECRET_BASIC.getValue(),
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token"),
            List.of(
                "client secret must not be empty when client authentication is 'client_secret_basic' (rest.auth.oauth2.client-auth / rest.auth.oauth2.client-secret)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_AUTH,
                ClientAuthenticationMethod.CLIENT_SECRET_POST.getValue(),
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token"),
            List.of(
                "client secret must not be empty when client authentication is 'client_secret_post' (rest.auth.oauth2.client-auth / rest.auth.oauth2.client-secret)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.GRANT_TYPE,
                GrantType.TOKEN_EXCHANGE.getValue(),
                BasicConfig.CLIENT_AUTH,
                ClientAuthenticationMethod.NONE.getValue(),
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token"),
            List.of(
                "client secret must not be set when client authentication is 'none' (rest.auth.oauth2.client-auth / rest.auth.oauth2.client-secret)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.GRANT_TYPE,
                GrantType.CLIENT_CREDENTIALS.getValue(),
                BasicConfig.CLIENT_AUTH,
                ClientAuthenticationMethod.NONE.getValue(),
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token"),
            List.of(
                "grant type must not be 'client_credentials' when client authentication is 'none' (rest.auth.oauth2.client-auth / rest.auth.oauth2.grant-type)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.GRANT_TYPE,
                GrantType.REFRESH_TOKEN.getValue(),
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token"),
            List.of(
                "grant type must be one of: 'client_credentials', 'urn:ietf:params:oauth:grant-type:token-exchange' (rest.auth.oauth2.grant-type)")),
        Arguments.of(
            Map.of(
                BasicConfig.CLIENT_AUTH,
                "unknown",
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token"),
            List.of(
                "client authentication method must be one of: 'none', 'client_secret_basic', 'client_secret_post' (rest.auth.oauth2.client-auth)")),
        Arguments.of(
            Map.of(
                BasicConfig.TIMEOUT,
                "PT1S",
                BasicConfig.CLIENT_ID,
                "Client1",
                BasicConfig.CLIENT_SECRET,
                "s3cr3t",
                BasicConfig.ISSUER_URL,
                "https://example.com"),
            List.of("timeout must be greater than or equal to PT30S (rest.auth.oauth2.timeout)")));
  }

  @ParameterizedTest
  @MethodSource
  void testParse(Map<String, String> properties, BasicConfig expected) {
    BasicConfig actual = BasicConfig.parse(properties).build();
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testParse() {
    return Stream.of(
        Arguments.of(
            Map.of(BasicConfig.ISSUER_URL, "https://example.com", BasicConfig.TOKEN, "my-token"),
            ImmutableBasicConfig.builder()
                .issuerUrl(URI.create("https://example.com"))
                .token(new BearerAccessToken("my-token"))
                .build()),
        Arguments.of(
            Map.of(
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token",
                BasicConfig.GRANT_TYPE,
                GrantType.TOKEN_EXCHANGE.getValue(),
                BasicConfig.CLIENT_AUTH,
                "client_secret_post",
                BasicConfig.CLIENT_ID,
                "my-client",
                BasicConfig.CLIENT_SECRET,
                "my-secret",
                BasicConfig.SCOPE,
                "read write",
                BasicConfig.TIMEOUT,
                "PT10M",
                BasicConfig.SESSION_CACHE_TIMEOUT,
                "PT30M",
                BasicConfig.EXTRA_PARAMS + ".param1",
                "value1",
                BasicConfig.EXTRA_PARAMS + ".param2",
                "value2"),
            ImmutableBasicConfig.builder()
                .tokenEndpoint(URI.create("https://example.com/token"))
                .grantType(GrantType.TOKEN_EXCHANGE)
                .clientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_POST)
                .clientId(new ClientID("my-client"))
                .clientSecret(new Secret("my-secret"))
                .scope(new Scope("read", "write"))
                .tokenAcquisitionTimeout(Duration.ofMinutes(10))
                .sessionCacheTimeout(Duration.ofMinutes(30))
                .putExtraRequestParameters("param1", "value1")
                .putExtraRequestParameters("param2", "value2")
                .build()));
  }
}
