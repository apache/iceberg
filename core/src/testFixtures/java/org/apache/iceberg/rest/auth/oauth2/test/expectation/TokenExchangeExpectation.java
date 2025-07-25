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
package org.apache.iceberg.rest.auth.oauth2.test.expectation;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.net.URI;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestServer;
import org.immutables.value.Value;
import org.mockserver.model.Parameter;

@Value.Immutable
@SuppressWarnings("resource")
public abstract class TokenExchangeExpectation extends TokenEndpointExpectation {

  // Accept both constant tokens defined in TestEnvironment
  // and tokens starting with "access_"
  // (since other expectations return tokens starting with "access_")
  private static final String ACCEPTED_SUBJECT_TOKENS =
      String.format("(%s|%s)", TestEnvironment.SUBJECT_TOKEN, "access_.*");
  private static final String ACCEPTED_ACTOR_TOKENS =
      String.format("(%s|%s)", TestEnvironment.ACTOR_TOKEN, "access_.*");

  @Override
  public void create() {
    TestServer.instance().when(request()).respond(response("access_initial", "refresh_initial"));
  }

  @Override
  protected ImmutableList.Builder<Parameter> requestBody() {
    ImmutableList.Builder<Parameter> builder =
        super.requestBody()
            .add(param("grant_type", GrantType.TOKEN_EXCHANGE))
            .add(param("subject_token", ACCEPTED_SUBJECT_TOKENS))
            .add(param("scope", ACCEPTED_SCOPES))
            .add(
                param(
                    "subject_token_type",
                    testEnvironment().subjectTokenType().orElse(TokenTypeURI.ACCESS_TOKEN)));

    testEnvironment()
        .requestedTokenType()
        .ifPresent(tokenType -> builder.add(param("requested_token_type", tokenType)));

    if (testEnvironment().actorTokenString().isPresent()) {
      builder
          .add(param("actor_token", ACCEPTED_ACTOR_TOKENS))
          .add(
              param(
                  "actor_token_type",
                  testEnvironment().actorTokenType().orElse(TokenTypeURI.ACCESS_TOKEN)));
    }

    for (URI resource : testEnvironment().resources()) {
      builder.add(param("resource", resource));
    }

    for (Audience audience : testEnvironment().audiences()) {
      builder.add(param("audience", audience.getValue()));
    }

    return builder;
  }

  @Override
  protected ImmutableMap.Builder<String, Object> responseBody(
      String accessToken, String refreshToken) {
    return super.responseBody(accessToken, refreshToken)
        .put(
            "issued_token_type",
            testEnvironment().requestedTokenType().orElse(TokenTypeURI.ACCESS_TOKEN).toString());
  }
}
