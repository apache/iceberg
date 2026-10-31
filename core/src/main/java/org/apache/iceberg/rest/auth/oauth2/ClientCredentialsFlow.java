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

import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ClientCredentialsGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.ClientSecretPost;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches access tokens with the <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.4">client credentials grant</a>.
 */
final class ClientCredentialsFlow {

  private static final Logger LOG = LoggerFactory.getLogger(ClientCredentialsFlow.class);

  private static final AuthorizationGrant GRANT = new ClientCredentialsGrant();

  private final OAuth2Config config;
  private final OAuth2Runtime runtime;
  private final EndpointProvider endpointProvider;

  ClientCredentialsFlow(OAuth2Config config, OAuth2Runtime runtime) {
    this.config = config;
    this.runtime = runtime;
    this.endpointProvider = new EndpointProvider(config, runtime);
  }

  /**
   * Invokes the token endpoint and returns the issued tokens. This method blocks until the server
   * replies.
   *
   * @throws OAuth2Exception if the server replied with an OAuth2 error
   */
  TokensResult fetchTokens() {
    HTTPRequest request = newTokenRequest().toHTTPRequest();
    LOG.debug("Invoking token endpoint: {}", request.getURI());

    HTTPResponse httpResponse;
    try {
      httpResponse = request.send(runtime.httpClient());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to invoke token endpoint: " + request.getURI(), e);
    }

    LOG.debug(
        "Received status {} from token endpoint: {}",
        httpResponse.getStatusCode(),
        request.getURI());

    TokenResponse response;
    try {
      response = TokenResponse.parse(httpResponse);
    } catch (ParseException e) {
      throw new IllegalStateException(
          "Failed to parse response from token endpoint: " + request.getURI(), e);
    }

    if (!response.indicatesSuccess()) {
      throw new OAuth2Exception(
          httpResponse.getStatusCode(), response.toErrorResponse().getErrorObject());
    }

    return new TokensResult(
        response.toSuccessResponse().getTokens().getAccessToken(), runtime.clock().instant());
  }

  private TokenRequest newTokenRequest() {
    TokenRequest.Builder builder =
        new TokenRequest.Builder(
            endpointProvider.tokenEndpoint(), newClientAuthentication(), GRANT);
    if (config.scope() != null) {
      builder.scope(config.scope());
    }

    if (!config.resources().isEmpty()) {
      builder.resources(config.resources().toArray(new URI[0]));
    }

    if (!config.audiences().isEmpty()) {
      builder.customParameter("audience", config.audiences().toArray(new String[0]));
    }

    config.extraParams().forEach(builder::customParameter);
    return builder.build();
  }

  private ClientAuthentication newClientAuthentication() {
    return config.clientAuth().equals(ClientAuthenticationMethod.CLIENT_SECRET_POST)
        ? new ClientSecretPost(config.clientId(), config.clientSecret())
        : new ClientSecretBasic(config.clientId(), config.clientSecret());
  }
}
