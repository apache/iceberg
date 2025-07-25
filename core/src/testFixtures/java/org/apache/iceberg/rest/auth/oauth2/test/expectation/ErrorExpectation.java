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

import org.apache.iceberg.rest.auth.oauth2.ImmutableOAuth2Error;
import org.apache.iceberg.rest.auth.oauth2.test.TestServer;
import org.immutables.value.Value;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.model.MediaType;

@Value.Immutable
public abstract class ErrorExpectation extends BaseExpectation {

  public static final ImmutableOAuth2Error OAUTH2_ERROR =
      ImmutableOAuth2Error.builder()
          .code("invalid_request")
          .description("Invalid request (MockServer Authorization Server)")
          .build();

  public static final HttpResponse AUTHORIZATION_SERVER_ERROR_RESPONSE =
      HttpResponse.response()
          .withStatusCode(400) // recommended by RFC 6749, section 5.2
          .withContentType(MediaType.APPLICATION_JSON)
          .withBody(
              JsonBody.json(
                  "{\"error\":\"invalid_request\", "
                      + "\"error_description\":\"Invalid request (MockServer Authorization Server)\"}"));

  public static final HttpResponse CATALOG_SERVER_ERROR_RESPONSE =
      HttpResponse.response()
          .withStatusCode(401)
          .withContentType(MediaType.APPLICATION_JSON)
          .withBody(
              JsonBody.json(
                  "{\"error\":{"
                      + "\"code\":401,"
                      + "\"type\":\"invalid_request\","
                      + "\"message\":\"Invalid request (MockServer Catalog Server)\"}}"));

  @Override
  @SuppressWarnings("resource")
  public void create() {
    TestServer.instance()
        .when(
            HttpRequest.request()
                .withPath(testEnvironment().authorizationServerUrl().getPath() + ".*"))
        .respond(AUTHORIZATION_SERVER_ERROR_RESPONSE);
    TestServer.instance()
        .when(HttpRequest.request().withPath(testEnvironment().catalogServerUrl().getPath() + ".*"))
        .respond(CATALOG_SERVER_ERROR_RESPONSE);
  }
}
