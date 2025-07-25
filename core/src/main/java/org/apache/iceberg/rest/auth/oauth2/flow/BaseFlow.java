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
package org.apache.iceberg.rest.auth.oauth2.flow;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.ErrorObject;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.ClientSecretPost;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.ClientID;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.apache.iceberg.rest.auth.oauth2.ImmutableOAuth2Error;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Exception;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Runtime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseFlow implements Flow {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseFlow.class);

  abstract OAuth2Config config();

  abstract OAuth2Runtime runtime();

  abstract EndpointProvider endpointProvider();

  interface Builder<F extends BaseFlow, B extends Builder<F, B>> {

    @CanIgnoreReturnValue
    B config(OAuth2Config config);

    @CanIgnoreReturnValue
    B runtime(OAuth2Runtime runtime);

    @CanIgnoreReturnValue
    B endpointProvider(EndpointProvider endpointProvider);

    F build();
  }

  CompletionStage<TokensResult> invokeTokenEndpoint(AuthorizationGrant grant) {
    HTTPRequest request;
    try {
      TokenRequest.Builder builder = newTokenRequestBuilder(grant);
      request = builder.build().toHTTPRequest();
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
    return CompletableFuture.supplyAsync(() -> sendAndReceive(request), runtime().executor())
        .whenComplete((response, error) -> log(request, response, error))
        .thenApply(this::parseTokenResponse)
        .thenCompose(Function.identity())
        .thenApply(this::toTokensResult);
  }

  TokenRequest.Builder newTokenRequestBuilder(AuthorizationGrant grant) {
    URI tokenEndpoint = endpointProvider().resolvedTokenEndpoint();
    TokenRequest.Builder builder =
        publicClient()
            ? new TokenRequest.Builder(tokenEndpoint, clientId(), grant)
            : new TokenRequest.Builder(tokenEndpoint, createClientAuthentication(), grant);
    config().basicConfig().scope().ifPresent(builder::scope);
    config().basicConfig().extraRequestParameters().forEach(builder::customParameter);
    return builder;
  }

  HTTPResponse sendAndReceive(HTTPRequest request) {
    try {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Invoking endpoint: {}", request.getURI());
      }

      return request.send(runtime().httpClient());
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke endpoint: " + request.getURI(), e);
    }
  }

  CompletionStage<AccessTokenResponse> parseTokenResponse(HTTPResponse httpResponse) {
    try {
      TokenResponse response = TokenResponse.parse(httpResponse);
      if (!response.indicatesSuccess()) {
        TokenErrorResponse errorResponse = response.toErrorResponse();
        ErrorObject errorObject = errorResponse.getErrorObject();
        return CompletableFuture.failedFuture(newOAuth2Exception(httpResponse, errorObject));
      }

      return CompletableFuture.completedFuture(response.toSuccessResponse());
    } catch (ParseException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  OAuth2Exception newOAuth2Exception(HTTPResponse httpResponse, ErrorObject errorObject) {
    return new OAuth2Exception(
        httpResponse.getStatusCode(),
        ImmutableOAuth2Error.builder()
            // Code can be null if the error is not a standard OAuth2 error, e.g. a 404 Not Found
            .code(Optional.ofNullable(errorObject.getCode()).orElse("unknown_error"))
            .description(Optional.ofNullable(errorObject.getDescription()))
            .uri(Optional.ofNullable(errorObject.getURI()))
            .parameters(errorObject.getCustomParams())
            .build());
  }

  TokensResult toTokensResult(AccessTokenResponse response) {
    return TokensResult.of(response, runtime().clock());
  }

  void log(HTTPRequest request, HTTPResponse response, Throwable error) {
    if (error == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Received {} response from endpoint: {}", response.getStatusCode(), request.getURI());
      }
    } else {
      LOGGER.warn("Error invoking endpoint: {}", request.getURI(), error);
    }
  }

  boolean publicClient() {
    return config()
        .basicConfig()
        .clientAuthenticationMethod()
        .equals(ClientAuthenticationMethod.NONE);
  }

  ClientID clientId() {
    return config()
        .basicConfig()
        .clientId()
        .orElseThrow(() -> new IllegalStateException("Client ID is required"));
  }

  Secret clientSecret() {
    return config()
        .basicConfig()
        .clientSecret()
        .orElseThrow(() -> new IllegalStateException("Client secret is required"));
  }

  ClientAuthentication createClientAuthentication() {
    ClientAuthenticationMethod method = config().basicConfig().clientAuthenticationMethod();

    if (method.equals(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)) {
      return new ClientSecretBasic(clientId(), clientSecret());

    } else if (method.equals(ClientAuthenticationMethod.CLIENT_SECRET_POST)) {
      return new ClientSecretPost(clientId(), clientSecret());
    }

    throw new IllegalArgumentException("Unsupported client authentication method: " + method);
  }
}
