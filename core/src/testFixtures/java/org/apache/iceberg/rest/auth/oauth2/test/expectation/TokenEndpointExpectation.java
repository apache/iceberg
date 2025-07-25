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

import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;

@SuppressWarnings("resource")
public abstract class TokenEndpointExpectation extends BaseExpectation {

  protected static final String ACCEPTED_SCOPES =
      String.format("(%s|%s)", TestEnvironment.SCOPE1, TestEnvironment.SCOPE2);

  protected static final String ACCEPTED_CLIENT_IDS =
      String.format("(%s|%s)", TestEnvironment.CLIENT_ID1, TestEnvironment.CLIENT_ID2);

  protected static final String ACCEPTED_CLIENT_SECRETS =
      String.format(
          "(%s|%s)",
          TestEnvironment.CLIENT_SECRET1.getValue(), TestEnvironment.CLIENT_SECRET2.getValue());

  protected static final String ACCEPTED_EXTRA_PARAM_NAMES = "(extra1|extra2|extra3)";
  protected static final String ACCEPTED_EXTRA_PARAM_VALUES = "(value1|value2|value3)";

  private static final String CLIENT1_AUTH_HEADER =
      "Basic "
          + Base64.getEncoder()
              .encodeToString(
                  (TestEnvironment.CLIENT_ID1.getValue()
                          + ":"
                          + TestEnvironment.CLIENT_SECRET1.getValue())
                      .getBytes(StandardCharsets.UTF_8));
  private static final String CLIENT2_AUTH_HEADER =
      "Basic "
          + Base64.getEncoder()
              .encodeToString(
                  (TestEnvironment.CLIENT_ID2.getValue()
                          + ":"
                          + TestEnvironment.CLIENT_SECRET2.getValue())
                      .getBytes(StandardCharsets.UTF_8));

  private static final String ACCEPTED_AUTH_HEADERS =
      String.format("(%s|%s)", CLIENT1_AUTH_HEADER, CLIENT2_AUTH_HEADER);

  protected HttpRequest request() {
    return requestStub().withBody(parameterBody(requestBody().build()));
  }

  protected static Parameter param(String name, Object value) {
    return Parameter.param(name, value.toString());
  }

  protected HttpRequest requestStub() {
    URI tokenEndpoint = testEnvironment().tokenEndpoint();
    String path =
        tokenEndpoint.isAbsolute()
            ? tokenEndpoint.getPath()
            : testEnvironment().catalogServerContextPath() + tokenEndpoint.getPath();
    return HttpRequest.request()
        .withMethod("POST")
        .withPath(path)
        .withHeader("Content-Type", "application/x-www-form-urlencoded(; charset=UTF-8)?")
        .withHeader("Accept", "application/json")
        .withHeaders(requestHeaders().build());
  }

  protected ImmutableList.Builder<Header> requestHeaders() {
    ImmutableList.Builder<Header> builder = ImmutableList.builder();
    if (testEnvironment()
        .clientAuthenticationMethod()
        .equals(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)) {
      builder.add(new Header("Authorization", ACCEPTED_AUTH_HEADERS));
    }

    return builder;
  }

  protected ImmutableList.Builder<Parameter> requestBody() {
    ImmutableList.Builder<Parameter> builder =
        ImmutableList.<Parameter>builder()
            .add(param(ACCEPTED_EXTRA_PARAM_NAMES, ACCEPTED_EXTRA_PARAM_VALUES));
    if (testEnvironment().clientAuthenticationMethod().equals(ClientAuthenticationMethod.NONE)) {
      builder.add(param("client_id", ACCEPTED_CLIENT_IDS));
    } else if (testEnvironment()
        .clientAuthenticationMethod()
        .equals(ClientAuthenticationMethod.CLIENT_SECRET_POST)) {
      builder.add(param("client_id", ACCEPTED_CLIENT_IDS));
      builder.add(param("client_secret", ACCEPTED_CLIENT_SECRETS));
    }

    return builder;
  }

  protected HttpResponse response(String accessToken, String refreshToken) {
    return HttpResponse.response()
        .withBody(jsonBody(responseBody(accessToken, refreshToken).build()));
  }

  protected ImmutableMap.Builder<String, Object> responseBody(
      String accessToken, String refreshToken) {
    ImmutableMap.Builder<String, Object> builder =
        ImmutableMap.<String, Object>builder()
            .put("access_token", accessToken)
            .put("token_type", "bearer")
            .put("expires_in", testEnvironment().accessTokenLifespan().toSeconds());
    if (testEnvironment().returnRefreshTokens()) {
      builder.put("refresh_token", refreshToken);
    }

    return builder;
  }
}
