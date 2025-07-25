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
import org.apache.iceberg.rest.auth.oauth2.test.TestServer;
import org.immutables.value.Value;
import org.mockserver.model.Parameter;

@Value.Immutable
@SuppressWarnings("resource")
public abstract class RefreshTokenExpectation extends TokenEndpointExpectation {

  @Override
  public void create() {
    TestServer.instance()
        .when(request())
        .respond(response("access_refreshed", "refresh_refreshed"));
    if (testEnvironment().tokenRefreshWithTokenExchangeEnabled()) {
      TestServer.instance()
          .when(requestStub().withBody(parameterBody(requestBodyTokenExchange().build())))
          .respond(response("access_refreshed", "refresh_refreshed"));
    }
  }

  @Override
  protected ImmutableList.Builder<Parameter> requestBody() {
    return super.requestBody()
        .add(param("grant_type", GrantType.REFRESH_TOKEN))
        .add(param("refresh_token", "refresh_.*"))
        .add(param("scope", ACCEPTED_SCOPES));
  }

  /**
   * Note: this expectation is very similar to the {@link TokenExchangeExpectation}, but token
   * refreshes never use actor tokens, so we need a separate expectation.
   */
  protected ImmutableList.Builder<Parameter> requestBodyTokenExchange() {
    ImmutableList.Builder<Parameter> builder =
        super.requestBody()
            .add(param("grant_type", GrantType.TOKEN_EXCHANGE))
            .add(param("subject_token", "access_.*"))
            .add(
                param(
                    "subject_token_type",
                    testEnvironment().subjectTokenType().orElse(TokenTypeURI.ACCESS_TOKEN)))
            .add(param("scope", ACCEPTED_SCOPES));

    testEnvironment()
        .requestedTokenType()
        .ifPresent(tokenType -> builder.add(param("requested_token_type", tokenType)));

    for (URI resource : testEnvironment().resources()) {
      builder.add(param("resource", resource));
    }

    for (Audience audience : testEnvironment().audiences()) {
      builder.add(param("audience", audience.getValue()));
    }

    return builder;
  }
}
