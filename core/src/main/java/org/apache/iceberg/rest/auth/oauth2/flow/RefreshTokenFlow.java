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
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.RefreshTokenGrant;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import java.util.concurrent.CompletionStage;
import org.immutables.value.Value;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-6">Token
 * Refresh</a> flow.
 */
@Value.Immutable
abstract class RefreshTokenFlow extends BaseFlow {

  interface Builder extends BaseFlow.Builder<RefreshTokenFlow, Builder> {

    @CanIgnoreReturnValue
    Builder refreshToken(RefreshToken refreshToken);
  }

  @Override
  public final GrantType grantType() {
    return GrantType.REFRESH_TOKEN;
  }

  abstract RefreshToken refreshToken();

  @Override
  public CompletionStage<TokensResult> execute() {
    return invokeTokenEndpoint(new RefreshTokenGrant(refreshToken()));
  }

  @Override
  TokensResult toTokensResult(AccessTokenResponse response) {
    Tokens tokens = response.toSuccessResponse().getTokens();
    // if the server doesn't return a new refresh token,
    // this means the current one is still valid, so we can reuse it
    if (tokens.getRefreshToken() == null) {
      tokens = new Tokens(tokens.getAccessToken(), refreshToken());
    }

    return TokensResult.of(tokens, runtime().clock());
  }
}
