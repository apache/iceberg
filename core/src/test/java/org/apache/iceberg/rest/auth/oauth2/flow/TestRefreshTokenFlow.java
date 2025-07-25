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

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.junit.EnumLike;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

class TestRefreshTokenFlow {

  /**
   * Emulates token refresh with standard refresh_token grant. The initial grant is token exchange,
   * in order to exercise public clients as well.
   */
  @CartesianTest
  void fetchNewTokens(
      @EnumLike ClientAuthenticationMethod authenticationMethod,
      @Values(booleans = {true, false}) boolean returnRefreshTokens)
      throws ExecutionException, InterruptedException {
    Tokens currentTokens =
        new Tokens(new BearerAccessToken("access_initial"), new RefreshToken("refresh_initial"));
    try (TestEnvironment env =
        TestEnvironment.builder()
            .grantType(GrantType.TOKEN_EXCHANGE)
            .clientAuthenticationMethod(authenticationMethod)
            .tokenRefreshWithTokenExchangeEnabled(false)
            .returnRefreshTokens(returnRefreshTokens)
            .build()) {
      FlowFactory flowFactory = env.newFlowFactory();
      Flow flow = flowFactory.newRefreshFlow(currentTokens);
      TokensResult tokens = flow.execute().toCompletableFuture().get();
      assertThat(flow).isInstanceOf(RefreshTokenFlow.class);
      env.assertTokensResult(
          tokens,
          "access_refreshed",
          returnRefreshTokens ? "refresh_refreshed" : "refresh_initial");
    }
  }

  /** Emulates token refresh with legacy catalog servers using token exchange. */
  @CartesianTest
  void fetchNewTokensWithTokenExchange(
      @EnumLike(excludes = "none") ClientAuthenticationMethod authenticationMethod)
      throws ExecutionException, InterruptedException {
    Tokens currentTokens = new Tokens(new BearerAccessToken("access_initial"), null);
    try (TestEnvironment env =
        TestEnvironment.builder()
            .grantType(GrantType.CLIENT_CREDENTIALS)
            .clientAuthenticationMethod(authenticationMethod)
            .tokenRefreshWithTokenExchangeEnabled(true)
            .returnRefreshTokens(false)
            .build()) {
      FlowFactory flowFactory = env.newFlowFactory();
      Flow flow = flowFactory.newRefreshFlow(currentTokens);
      TokensResult tokens = flow.execute().toCompletableFuture().get();
      assertThat(flow).isInstanceOf(TokenExchangeFlow.class);
      env.assertTokensResult(tokens, "access_refreshed", null);
    }
  }
}
