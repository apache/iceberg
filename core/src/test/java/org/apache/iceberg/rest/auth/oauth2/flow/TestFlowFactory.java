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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import java.util.stream.Stream;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestFlowFactory {

  @ParameterizedTest
  @MethodSource
  void testNewInitialFlow(Class<Flow> flowClass, GrantType grantType) {
    try (TestEnvironment env = TestEnvironment.builder().grantType(grantType).build()) {
      FlowFactory flowFactory = env.newFlowFactory();
      Flow flow = flowFactory.newInitialFlow();
      assertThat(flow).isNotNull();
      assertThat(flow).isInstanceOf(flowClass);
      assertThat(flow.grantType()).isEqualTo(grantType);
    }
  }

  static Stream<Arguments> testNewInitialFlow() {
    return Stream.of(
        Arguments.of(ClientCredentialsFlow.class, GrantType.CLIENT_CREDENTIALS),
        Arguments.of(TokenExchangeFlow.class, GrantType.TOKEN_EXCHANGE));
  }

  @Test
  void testNewRefreshFlow() {
    Tokens currentTokens =
        new Tokens(new BearerAccessToken("access_token"), new RefreshToken("refresh_token"));
    try (TestEnvironment env =
        TestEnvironment.builder().grantType(GrantType.TOKEN_EXCHANGE).build()) {
      FlowFactory flowFactory = env.newFlowFactory();
      Flow flow = flowFactory.newRefreshFlow(currentTokens);
      assertThat(flow).isNotNull();
      assertThat(flow).isInstanceOf(RefreshTokenFlow.class);
      assertThat(flow.grantType()).isEqualTo(GrantType.REFRESH_TOKEN);
    }
  }

  @Test
  void testNewRefreshFlowWithTokenExchange() {
    Tokens currentTokens = new Tokens(new BearerAccessToken("access_token"), null);
    try (TestEnvironment env =
        TestEnvironment.builder().tokenRefreshWithTokenExchangeEnabled(true).build()) {
      FlowFactory flowFactory = env.newFlowFactory();
      Flow flow = flowFactory.newRefreshFlow(currentTokens);
      assertThat(flow).isNotNull();
      assertThat(flow).isInstanceOf(TokenExchangeFlow.class);
      assertThat(flow.grantType()).isEqualTo(GrantType.TOKEN_EXCHANGE);
    }
  }

  @Test
  void testNewRefreshFlowFailure() {
    Tokens currentTokens = new Tokens(new BearerAccessToken("access_token"), null);
    try (TestEnvironment env =
        TestEnvironment.builder().tokenRefreshWithTokenExchangeEnabled(false).build()) {
      FlowFactory flowFactory = env.newFlowFactory();
      assertThatThrownBy(() -> flowFactory.newRefreshFlow(currentTokens))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(
              "Cannot create refresh token flow: no refresh token present and token exchange is disabled");
    }
  }
}
