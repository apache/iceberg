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
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.tokenexchange.TokenExchangeGrant;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;
import org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig;
import org.immutables.value.Value;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc8693">Token
 * Exchange</a> flow.
 */
@Value.Immutable
abstract class TokenExchangeFlow extends BaseFlow {

  interface Builder extends BaseFlow.Builder<TokenExchangeFlow, Builder> {
    @CanIgnoreReturnValue
    Builder subjectTokenStage(CompletionStage<AccessToken> subjectTokenStage);

    @CanIgnoreReturnValue
    Builder actorTokenStage(CompletionStage<AccessToken> actorTokenStage);
  }

  @Override
  public final GrantType grantType() {
    return GrantType.TOKEN_EXCHANGE;
  }

  abstract CompletionStage<AccessToken> subjectTokenStage();

  abstract CompletionStage</* @Nullable */ AccessToken> actorTokenStage();

  @Override
  public CompletionStage<TokensResult> execute() {
    return subjectTokenStage()
        .thenCombine(
            actorTokenStage(),
            (subjectToken, actorToken) -> {
              Objects.requireNonNull(
                  subjectToken, "Cannot execute token exchange: missing required subject token");
              return newTokenExchangeGrant(
                  subjectToken, actorToken, config().tokenExchangeConfig());
            })
        .thenCompose(this::invokeTokenEndpoint);
  }

  @Override
  TokenRequest.Builder newTokenRequestBuilder(AuthorizationGrant grant) {
    return super.newTokenRequestBuilder(grant)
        .resources(config().tokenExchangeConfig().resources().toArray(URI[]::new));
  }

  private TokenExchangeGrant newTokenExchangeGrant(
      AccessToken subjectToken,
      @Nullable AccessToken actorToken,
      TokenExchangeConfig tokenExchangeConfig) {
    TokenTypeURI subjectTokenType =
        tokenType(subjectToken, config().tokenExchangeConfig().subjectTokenType().orElse(null));
    TokenTypeURI actorTokenType =
        actorToken == null
            ? null
            : tokenType(actorToken, config().tokenExchangeConfig().actorTokenType().orElse(null));
    return new TokenExchangeGrant(
        subjectToken,
        subjectTokenType,
        actorToken,
        actorTokenType,
        tokenExchangeConfig.requestedTokenType().orElse(null),
        tokenExchangeConfig.audiences());
  }

  private static TokenTypeURI tokenType(AccessToken token, @Nullable TokenTypeURI tokenType) {
    return tokenType != null
        ? tokenType
        : token.getIssuedTokenType() != null
            ? token.getIssuedTokenType()
            : TokenTypeURI.ACCESS_TOKEN;
  }
}
