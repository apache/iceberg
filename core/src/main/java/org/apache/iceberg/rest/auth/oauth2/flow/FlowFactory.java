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

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Runtime;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2Client;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtil;
import org.immutables.value.Value;

/**
 * A factory for creating {@link Flow} instances. This is one of the main components of the OAuth2
 * client, responsible for creating flows for fetching new tokens and refreshing tokens.
 */
@Value.Immutable
public abstract class FlowFactory {

  public static FlowFactory of(OAuth2Config config, OAuth2Runtime runtime) {
    return ImmutableFlowFactory.builder().config(config).runtime(runtime).build();
  }

  /** Creates a flow for fetching new tokens. This is used for the initial token fetch. */
  public Flow newInitialFlow() {
    return newInitialFlowBuilder()
        .config(config())
        .runtime(runtime())
        .endpointProvider(endpointProvider())
        .build();
  }

  /**
   * Creates a flow for refreshing tokens. This is used for refreshing tokens when the access token
   * expires.
   */
  public Flow newRefreshFlow(Tokens currentTokens) {
    return newRefreshFlowBuilder(currentTokens)
        .config(config())
        .runtime(runtime())
        .endpointProvider(endpointProvider())
        .build();
  }

  abstract OAuth2Config config();

  abstract OAuth2Runtime runtime();

  @Value.Default
  EndpointProvider endpointProvider() {
    return EndpointProvider.of(config(), runtime());
  }

  private BaseFlow.Builder<? extends Flow, ?> newInitialFlowBuilder() {

    GrantType grantType = config().basicConfig().grantType();

    if (grantType.equals(GrantType.CLIENT_CREDENTIALS)) {
      return ImmutableClientCredentialsFlow.builder();

    } else if (grantType.equals(GrantType.TOKEN_EXCHANGE)) {
      AccessToken subjectToken =
          config()
              .tokenExchangeConfig()
              .subjectTokenString()
              .map(TypelessAccessToken::new)
              .orElseThrow(() -> new IllegalStateException("Subject token is required"));
      AccessToken actorToken =
          config()
              .tokenExchangeConfig()
              .actorTokenString()
              .map(TypelessAccessToken::new)
              .orElse(null);
      return ImmutableTokenExchangeFlow.builder()
          .subjectTokenStage(asTokenStage(subjectToken))
          .actorTokenStage(asTokenStage(actorToken));
    }

    // Should never happen since the grant type is validated by the config.
    throw new IllegalArgumentException(
        "Unknown or invalid grant type for initial token fetch: "
            + config().basicConfig().grantType());
  }

  private CompletionStage<AccessToken> asTokenStage(@Nullable AccessToken token) {
    if (token != null && token.getValue().equals(ConfigUtil.PARENT_TOKEN)) {
      @SuppressWarnings("resource")
      OAuth2Client parentClient =
          runtime()
              .parent()
              .orElseThrow(() -> new IllegalStateException("Parent OAuth2 client is required"));
      return parentClient.authenticateAsync();
    } else {
      return CompletableFuture.completedFuture(token);
    }
  }

  private BaseFlow.Builder<? extends Flow, ?> newRefreshFlowBuilder(Tokens currentTokens) {

    if (currentTokens.getRefreshToken() != null) {
      return ImmutableRefreshTokenFlow.builder().refreshToken(currentTokens.getRefreshToken());
    }

    boolean refreshWithTokenExchange =
        config()
            .tokenRefreshConfig()
            .tokenExchangeEnabled()
            .orElseGet(
                () -> config().basicConfig().grantType().equals(GrantType.CLIENT_CREDENTIALS));
    if (refreshWithTokenExchange) {
      return ImmutableTokenExchangeFlow.builder()
          .subjectTokenStage(CompletableFuture.completedFuture(currentTokens.getAccessToken()))
          .actorTokenStage(CompletableFuture.completedFuture(null));
    }

    throw new IllegalStateException(
        "Cannot create refresh token flow: no refresh token present and token exchange is disabled");
  }
}
