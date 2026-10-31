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
package org.apache.iceberg.rest.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestEndpointProvider {

  private static MockAuthorizationServer server;

  @BeforeAll
  static void startServer() {
    server = new MockAuthorizationServer();
  }

  @AfterAll
  static void stopServer() {
    server.close();
  }

  @BeforeEach
  void reset() {
    server.reset();
  }

  @Test
  void configuredTokenEndpoint() {
    EndpointProvider provider = new EndpointProvider(server.clientConfig(), server.runtime());
    assertThat(provider.tokenEndpoint()).isEqualTo(server.tokenEndpoint());
  }

  @Test
  void oidcDiscovery() {
    server.expectOidcDiscovery();
    EndpointProvider provider = new EndpointProvider(issuerConfig(), server.runtime());
    assertThat(provider.tokenEndpoint()).isEqualTo(server.tokenEndpoint());
    // cached
    server.reset();
    assertThat(provider.tokenEndpoint()).isEqualTo(server.tokenEndpoint());
  }

  @Test
  void oauthDiscoveryFallback() {
    server.expectOAuthDiscovery();
    EndpointProvider provider = new EndpointProvider(issuerConfig(), server.runtime());
    assertThat(provider.tokenEndpoint()).isEqualTo(server.tokenEndpoint());
  }

  @Test
  void discoveryFailure() {
    EndpointProvider provider = new EndpointProvider(issuerConfig(), server.runtime());
    assertThatThrownBy(provider::tokenEndpoint)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Failed to fetch server metadata for issuer " + server.issuerUrl())
        .cause()
        .hasMessageContaining("Failed to fetch OAuth2 server metadata: server returned status 404");
  }

  private static OAuth2Config issuerConfig() {
    return OAuth2Config.fromProperties(server.issuerProperties());
  }
}
