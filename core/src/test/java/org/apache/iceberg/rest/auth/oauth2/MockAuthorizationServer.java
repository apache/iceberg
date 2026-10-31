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

import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.model.MediaType;
import org.mockserver.model.Parameter;
import org.mockserver.model.ParameterBody;
import org.mockserver.verify.VerificationTimes;

/**
 * A MockServer-backed OAuth2 authorization server. Expectations are installed per test with the
 * {@code expect*} methods and cleared with {@link #reset()}.
 */
final class MockAuthorizationServer implements AutoCloseable {

  static final String CLIENT_ID = "client1";
  static final String CLIENT_SECRET = "s3cr3t";
  static final String SCOPE = "catalog";
  static final String ACCESS_TOKEN = "access_initial";
  static final String RENEWED_ACCESS_TOKEN = "access_renewed";
  static final Duration ACCESS_TOKEN_LIFESPAN = Duration.ofHours(1);

  private static final String REALM_PATH = "/realms/master";
  private static final String TOKEN_PATH = REALM_PATH + "/protocol/openid-connect/token";
  private static final String OIDC_DISCOVERY_PATH =
      REALM_PATH + "/.well-known/openid-configuration";
  // Nimbus appends the well-known path to the issuer path (RFC 8414 section 3.1 variant)
  private static final String OAUTH_DISCOVERY_PATH =
      REALM_PATH + "/.well-known/oauth-authorization-server";

  private final ClientAndServer server = ClientAndServer.startClientAndServer();
  private final RESTClient restClient;

  MockAuthorizationServer() {
    this.restClient = HTTPClient.builder(Map.of()).uri(baseUrl()).build();
  }

  URI baseUrl() {
    return URI.create("http://localhost:" + server.getLocalPort());
  }

  URI issuerUrl() {
    return baseUrl().resolve(REALM_PATH);
  }

  URI tokenEndpoint() {
    return baseUrl().resolve(TOKEN_PATH);
  }

  /** A REST client pointing at this server, to use with {@link RESTClientAdapter}. */
  RESTClient restClient() {
    return restClient;
  }

  OAuth2Runtime runtime() {
    return new OAuth2Runtime(() -> restClient);
  }

  /** Properties for a client credentials flow against this server, with a token endpoint. */
  Map<String, String> clientProperties() {
    return Map.of(
        OAuth2Config.TOKEN_ENDPOINT, tokenEndpoint().toString(),
        OAuth2Config.CLIENT_ID, CLIENT_ID,
        OAuth2Config.CLIENT_SECRET, CLIENT_SECRET,
        OAuth2Config.SCOPE, SCOPE);
  }

  OAuth2Config clientConfig() {
    return OAuth2Config.fromProperties(clientProperties());
  }

  /** Properties for a client credentials flow against this server, with an issuer URL. */
  Map<String, String> issuerProperties() {
    return Map.of(
        OAuth2Config.ISSUER_URL, issuerUrl().toString(),
        OAuth2Config.CLIENT_ID, CLIENT_ID,
        OAuth2Config.CLIENT_SECRET, CLIENT_SECRET,
        OAuth2Config.SCOPE, SCOPE);
  }

  void reset() {
    server.reset();
  }

  /** Expects OpenID Connect discovery requests and replies with this server's token endpoint. */
  void expectOidcDiscovery() {
    expectDiscovery(OIDC_DISCOVERY_PATH);
  }

  /** Expects RFC 8414 discovery requests and replies with this server's token endpoint. */
  void expectOAuthDiscovery() {
    expectDiscovery(OAUTH_DISCOVERY_PATH);
  }

  /**
   * Expects client credentials requests from the default client and replies with the default access
   * token.
   */
  void expectClientCredentials() {
    expectClientCredentials(ClientAuthenticationMethod.CLIENT_SECRET_BASIC);
  }

  void expectClientCredentials(ClientAuthenticationMethod method) {
    expectClientCredentials(method, ACCESS_TOKEN, ACCESS_TOKEN_LIFESPAN);
  }

  /**
   * Expects client credentials requests from the default client, authenticated with the given
   * method, and replies with the given access token and lifespan ({@code expires_in} is omitted
   * when the lifespan is null).
   */
  void expectClientCredentials(
      ClientAuthenticationMethod method, String accessToken, Duration lifespan) {
    expectClientCredentials(
        CLIENT_ID, CLIENT_SECRET, SCOPE, method, accessToken, lifespan, Times.unlimited());
  }

  /** Expects exactly one client credentials request and replies with the given access token. */
  void expectClientCredentialsOnce(String accessToken, Duration lifespan) {
    expectClientCredentials(
        CLIENT_ID,
        CLIENT_SECRET,
        SCOPE,
        ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
        accessToken,
        lifespan,
        Times.once());
  }

  void expectClientCredentials(
      String clientId,
      String clientSecret,
      String scope,
      ClientAuthenticationMethod method,
      String accessToken,
      Duration lifespan,
      Times times,
      Parameter... extraParams) {
    List<Parameter> params = Lists.newArrayList(extraParams);
    params.add(Parameter.param("grant_type", "client_credentials"));
    params.add(Parameter.param("scope", scope));
    HttpRequest request =
        HttpRequest.request()
            .withMethod("POST")
            .withPath(TOKEN_PATH)
            .withHeader("Content-Type", "application/x-www-form-urlencoded(; charset=UTF-8)?");
    if (method.equals(ClientAuthenticationMethod.CLIENT_SECRET_POST)) {
      params.add(Parameter.param("client_id", clientId));
      params.add(Parameter.param("client_secret", clientSecret));
    } else {
      request.withHeader("Authorization", basicAuth(clientId, clientSecret));
    }

    request.withBody(ParameterBody.params(params));

    String body =
        lifespan == null
            ? String.format("{\"access_token\":\"%s\",\"token_type\":\"bearer\"}", accessToken)
            : String.format(
                "{\"access_token\":\"%s\",\"token_type\":\"bearer\",\"expires_in\":%d}",
                accessToken, lifespan.toSeconds());
    server
        .when(request, times)
        .respond(
            HttpResponse.response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(JsonBody.json(body)));
  }

  /** Expects any token request and replies with an {@code invalid_client} error. */
  void expectInvalidClient() {
    server
        .when(HttpRequest.request().withMethod("POST").withPath(TOKEN_PATH))
        .respond(
            HttpResponse.response()
                .withStatusCode(401)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(
                    JsonBody.json(
                        "{\"error\":\"invalid_client\","
                            + "\"error_description\":\"Invalid client credentials\"}")));
  }

  /**
   * Expects client credentials requests from any client on the legacy REST catalog token endpoint
   * ({@code v1/oauth/tokens} under this server's base URL) and replies with the default token.
   */
  void expectLegacyTokenEndpoint() {
    expectLegacyTokenEndpoint("", ACCESS_TOKEN);
  }

  /**
   * Expects client credentials requests from any client on the legacy REST catalog token endpoint
   * of a catalog served under the given path, and replies with the given token.
   */
  void expectLegacyTokenEndpoint(String catalogPath, String accessToken) {
    server
        .when(HttpRequest.request().withMethod("POST").withPath(catalogPath + "/v1/oauth/tokens"))
        .respond(
            HttpResponse.response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(
                    JsonBody.json(
                        String.format(
                            "{\"access_token\":\"%s\",\"token_type\":\"bearer\",\"expires_in\":%d}",
                            accessToken, ACCESS_TOKEN_LIFESPAN.toSeconds()))));
  }

  /** Expects exactly one token request and replies with a 503 error. */
  void expectServerErrorOnce() {
    server
        .when(HttpRequest.request().withMethod("POST").withPath(TOKEN_PATH), Times.once())
        .respond(
            HttpResponse.response()
                .withStatusCode(503)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(
                    JsonBody.json(
                        "{\"error\":\"temporarily_unavailable\","
                            + "\"error_description\":\"Try again later\"}")));
  }

  /** Verifies the number of token requests received since the last reset. */
  void verifyTokenRequests(int times) {
    server.verify(
        HttpRequest.request().withMethod("POST").withPath(TOKEN_PATH),
        VerificationTimes.exactly(times));
  }

  @Override
  public void close() {
    try {
      restClient.close();
    } catch (Exception ignored) {
      // best effort
    }

    server.stop();
  }

  private void expectDiscovery(String path) {
    String body =
        String.format(
            "{\"issuer\":\"%s\",\"token_endpoint\":\"%s\"}", issuerUrl(), tokenEndpoint());
    server
        .when(HttpRequest.request().withMethod("GET").withPath(path))
        .respond(
            HttpResponse.response()
                .withStatusCode(200)
                .withContentType(MediaType.APPLICATION_JSON)
                .withBody(JsonBody.json(body)));
  }

  private static String basicAuth(String clientId, String clientSecret) {
    return "Basic "
        + Base64.getEncoder()
            .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
  }
}
