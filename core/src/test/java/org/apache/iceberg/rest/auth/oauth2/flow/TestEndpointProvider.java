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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;

import com.nimbusds.oauth2.sdk.ParseException;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestServer;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.model.MediaType;

class TestEndpointProvider {

  private static final String INVALID_METADATA = "{\"token_endpoint\":\"not a valid URL\"}";

  @Test
  void withoutDiscovery() {
    try (TestEnvironment env = TestEnvironment.builder().discoveryEnabled(false).build()) {
      EndpointProvider endpointProvider = EndpointProvider.of(env.config(), env.runtime());
      assertThat(endpointProvider.resolvedTokenEndpoint()).isEqualTo(env.tokenEndpoint());
    }
  }

  @CartesianTest
  void withDiscovery(
      @Values(
              strings = {
                ".well-known/openid-configuration",
                ".well-known/oauth-authorization-server"
              })
          String wellKnownPath) {
    try (TestEnvironment env =
        TestEnvironment.builder().discoveryEnabled(true).wellKnownPath(wellKnownPath).build()) {
      EndpointProvider endpointProvider = EndpointProvider.of(env.config(), env.runtime());
      assertThat(endpointProvider.resolvedTokenEndpoint()).isEqualTo(env.tokenEndpoint());
    }
  }

  @Test
  void fetchServerMetadataWrongEndpoint() {
    try (TestEnvironment env = TestEnvironment.builder().createDefaultExpectations(false).build()) {
      env.createErrorExpectations();
      EndpointProvider endpointProvider = EndpointProvider.of(env.config(), env.runtime());
      Throwable throwable = catchThrowable(endpointProvider::serverMetadata);
      assertThat(throwable)
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Failed to fetch provider metadata");
      // OIDC well-known path
      assertThat(throwable.getCause())
          .asInstanceOf(throwable(RuntimeException.class))
          .hasMessageContaining("Failed to fetch OIDC provider metadata")
          .hasMessageContaining("Invalid request");
      // OAuth well-known path
      assertThat(throwable.getSuppressed())
          .singleElement()
          .asInstanceOf(throwable(RuntimeException.class))
          .hasMessageContaining("Failed to fetch OAuth provider metadata")
          .hasMessageContaining("Invalid request");
    }
  }

  @Test
  @SuppressWarnings("resource")
  void fetchServerMetadataWrongData() {
    try (TestEnvironment env = TestEnvironment.builder().createDefaultExpectations(false).build()) {
      TestServer.instance()
          .when(
              HttpRequest.request()
                  .withPath(env.authorizationServerUrl().getPath() + ".well-known/.*"))
          .respond(
              HttpResponse.response()
                  .withStatusCode(200)
                  .withContentType(MediaType.APPLICATION_JSON)
                  .withBody(JsonBody.json(INVALID_METADATA)));
      EndpointProvider endpointProvider = EndpointProvider.of(env.config(), env.runtime());
      Throwable throwable = catchThrowable(endpointProvider::serverMetadata);
      // OIDC well-known path
      assertThat(throwable)
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Failed to fetch provider metadata");
      assertThat(throwable.getCause())
          .asInstanceOf(throwable(ParseException.class))
          .hasMessageContaining("Illegal character in path at index 3: not a valid URL");
      // OAuth well-known path
      assertThat(throwable.getSuppressed())
          .singleElement()
          .asInstanceOf(throwable(ParseException.class))
          .hasMessageContaining("Illegal character in path at index 3: not a valid URL");
    }
  }
}
