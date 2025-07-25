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
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2Client;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtil;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.junit.EnumLike;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

class TestTokenExchangeFlow {

  @CartesianTest
  void fetchNewTokensStatic(
      @EnumLike ClientAuthenticationMethod authenticationMethod,
      @Values(booleans = {true, false}) boolean returnRefreshTokens)
      throws InterruptedException, ExecutionException {
    try (TestEnvironment env =
        TestEnvironment.builder()
            .grantType(GrantType.TOKEN_EXCHANGE)
            .clientAuthenticationMethod(authenticationMethod)
            .returnRefreshTokens(returnRefreshTokens)
            // test without audiences and resources
            .audiences(List.of())
            .resources(List.of())
            .build()) {
      FlowFactory flowFactory = env.newFlowFactory();
      Flow flow = flowFactory.newInitialFlow();
      assertThat(flow).isInstanceOf(TokenExchangeFlow.class);
      TokensResult tokens = flow.execute().toCompletableFuture().get();
      env.assertTokensResult(
          tokens, "access_initial", returnRefreshTokens ? "refresh_initial" : null);
    }
  }

  @CartesianTest
  void fetchNewTokensStaticWithTokenTypes(
      @EnumLike ClientAuthenticationMethod authenticationMethod,
      @EnumLike(
              includes = {
                "urn:ietf:params:oauth:token-type:access_token",
                "urn:ietf:params:oauth:token-type:jwt"
              })
          TokenTypeURI tokenType,
      @Values(booleans = {true, false}) boolean returnRefreshTokens)
      throws InterruptedException, ExecutionException {
    try (TestEnvironment env =
        TestEnvironment.builder()
            .grantType(GrantType.TOKEN_EXCHANGE)
            .clientAuthenticationMethod(authenticationMethod)
            .returnRefreshTokens(returnRefreshTokens)
            .subjectTokenType(tokenType)
            .actorTokenType(tokenType)
            .requestedTokenType(tokenType)
            // test multiple audiences and resources
            .addAudiences(new Audience("audience1"))
            .addAudiences(new Audience("audience2"))
            .addResources(URI.create("https://example.com/api1"))
            .addResources(URI.create("https://example.com/api2"))
            .build()) {
      FlowFactory flowFactory = env.newFlowFactory();
      Flow flow = flowFactory.newInitialFlow();
      assertThat(flow).isInstanceOf(TokenExchangeFlow.class);
      TokensResult tokens = flow.execute().toCompletableFuture().get();
      env.assertTokensResult(
          tokens, "access_initial", returnRefreshTokens ? "refresh_initial" : null);
    }
  }

  @CartesianTest
  void fetchNewTokensParent(
      @EnumLike ClientAuthenticationMethod authenticationMethod,
      @Values(booleans = {true, false}) boolean returnRefreshTokens)
      throws InterruptedException, ExecutionException {
    try (TestEnvironment parent = TestEnvironment.builder().build();
        OAuth2Client parentClient = parent.newOAuth2Client();
        TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.TOKEN_EXCHANGE)
                .clientAuthenticationMethod(authenticationMethod)
                .returnRefreshTokens(returnRefreshTokens)
                .subjectTokenString(ConfigUtil.PARENT_TOKEN)
                .actorTokenString(ConfigUtil.PARENT_TOKEN)
                .parentClient(parentClient)
                .build()) {
      FlowFactory flowFactory = env.newFlowFactory();
      Flow flow = flowFactory.newInitialFlow();
      assertThat(flow).isInstanceOf(TokenExchangeFlow.class);
      TokensResult tokens = flow.execute().toCompletableFuture().get();
      env.assertTokensResult(
          tokens, "access_initial", returnRefreshTokens ? "refresh_initial" : null);
    }
  }
}
