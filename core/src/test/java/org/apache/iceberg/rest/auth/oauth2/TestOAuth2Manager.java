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

import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;

class TestOAuth2Manager {

  private static final String CONTEXT_CLIENT_ID = "client2";
  private static final String CONTEXT_CLIENT_SECRET = "s3cr3t2";
  private static final String CONTEXT_ACCESS_TOKEN = "access_context";

  private static final HTTPRequest REQUEST =
      ImmutableHTTPRequest.builder()
          .baseUri(URI.create("http://localhost:8181"))
          .method(HTTPMethod.GET)
          .path("v1/config")
          .build();

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
    server.expectClientCredentials();
    server.expectClientCredentials(
        CONTEXT_CLIENT_ID,
        CONTEXT_CLIENT_SECRET,
        MockAuthorizationServer.SCOPE,
        ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
        CONTEXT_ACCESS_TOKEN,
        MockAuthorizationServer.ACCESS_TOKEN_LIFESPAN,
        Times.unlimited());
  }

  @Test
  void loadByClassName() {
    try (AuthManager manager =
        AuthManagers.loadAuthManager(
            "test", Map.of(AuthProperties.AUTH_TYPE, OAuth2Manager.class.getName()))) {
      assertThat(manager).isInstanceOf(OAuth2Manager.class);
    }
  }

  @Test
  void initAndCatalogSessions() {
    try (OAuth2Manager manager = new OAuth2Manager("test")) {
      try (AuthSession initSession =
          manager.initSession(server.restClient(), catalogProperties())) {
        assertAuthorization(initSession, MockAuthorizationServer.ACCESS_TOKEN);
      }

      try (AuthSession catalogSession =
          manager.catalogSession(server.restClient(), catalogProperties())) {
        assertAuthorization(catalogSession, MockAuthorizationServer.ACCESS_TOKEN);
      }
    }

    // each session fetches its own token
    server.verifyTokenRequests(2);
  }

  @Test
  void catalogSessionWithLegacyProperties() {
    Map<String, String> properties =
        Map.of(
            CatalogProperties.URI,
            server.baseUrl().toString(),
            OAuth2Properties.CREDENTIAL,
            MockAuthorizationServer.CLIENT_ID + ":" + "s3cr3t",
            OAuth2Properties.OAUTH2_SERVER_URI,
            server.tokenEndpoint().toString(),
            OAuth2Properties.SCOPE,
            MockAuthorizationServer.SCOPE);
    try (OAuth2Manager manager = new OAuth2Manager("test");
        AuthSession catalogSession = manager.catalogSession(server.restClient(), properties)) {
      assertAuthorization(catalogSession, MockAuthorizationServer.ACCESS_TOKEN);
    }
  }

  @Test
  void contextualSessionWithoutOAuth2Properties() {
    try (OAuth2Manager manager = new OAuth2Manager("test");
        AuthSession catalogSession =
            manager.catalogSession(server.restClient(), catalogProperties())) {
      SessionContext context =
          new SessionContext("session1", "user", Map.of("other", "value"), Map.of());
      assertThat(manager.contextualSession(context, catalogSession)).isSameAs(catalogSession);
      assertThat(manager.contextualSession(SessionContext.createEmpty(), catalogSession))
          .isSameAs(catalogSession);
      assertThat(manager.cacheById().asMap()).isEmpty();
    }
  }

  @Test
  void contextualSessionWithUnsupportedLegacyProperties() {
    try (OAuth2Manager manager = new OAuth2Manager("test");
        AuthSession catalogSession =
            manager.catalogSession(server.restClient(), catalogProperties())) {
      SessionContext context =
          new SessionContext("session1", "user", Map.of(), Map.of(OAuth2Properties.TOKEN, "t0k3n"));
      assertThatThrownBy(() -> manager.contextualSession(context, catalogSession))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Session context contains unsupported legacy OAuth2 properties [token]");
    }
  }

  @Test
  void contextualSessionWithSameConfig() {
    try (OAuth2Manager manager = new OAuth2Manager("test");
        AuthSession catalogSession =
            manager.catalogSession(server.restClient(), catalogProperties())) {
      SessionContext context =
          new SessionContext(
              "session1",
              "user",
              Map.of(
                  OAuth2Properties.CREDENTIAL,
                  "client1:s3cr3t",
                  OAuth2Properties.OAUTH2_SERVER_URI,
                  server.tokenEndpoint().toString()),
              Map.of(OAuth2Config.SCOPE, MockAuthorizationServer.SCOPE));
      assertThat(manager.contextualSession(context, catalogSession)).isSameAs(catalogSession);
      assertThat(manager.cacheById().asMap()).isEmpty();
    }
  }

  @Test
  void contextualSessionWithOwnConfig() {
    try (OAuth2Manager manager = new OAuth2Manager("test");
        AuthSession catalogSession =
            manager.catalogSession(server.restClient(), catalogProperties())) {
      assertAuthorization(catalogSession, MockAuthorizationServer.ACCESS_TOKEN);

      SessionContext context1 =
          new SessionContext(
              "session1",
              "user",
              Map.of(OAuth2Properties.CREDENTIAL, CONTEXT_CLIENT_ID + ":" + CONTEXT_CLIENT_SECRET),
              Map.of(OAuth2Properties.OAUTH2_SERVER_URI, server.tokenEndpoint().toString()));
      SessionContext context2 =
          new SessionContext(
              "session2",
              "user",
              Map.of(),
              Map.of(
                  OAuth2Config.TOKEN_ENDPOINT,
                  server.tokenEndpoint().toString(),
                  OAuth2Config.CLIENT_ID,
                  CONTEXT_CLIENT_ID,
                  OAuth2Config.CLIENT_SECRET,
                  CONTEXT_CLIENT_SECRET,
                  OAuth2Config.SCOPE,
                  MockAuthorizationServer.SCOPE));

      AuthSession contextualSession1 = manager.contextualSession(context1, catalogSession);
      AuthSession contextualSession2 = manager.contextualSession(context2, catalogSession);
      assertThat(contextualSession1).isNotSameAs(catalogSession);
      assertThat(contextualSession2).isNotSameAs(catalogSession).isNotSameAs(contextualSession1);
      assertThat(manager.contextualSession(context1, catalogSession)).isSameAs(contextualSession1);
      assertThat(manager.cacheById().asMap()).containsOnlyKeys("session1", "session2");

      assertAuthorization(contextualSession1, CONTEXT_ACCESS_TOKEN);
      assertAuthorization(contextualSession2, CONTEXT_ACCESS_TOKEN);
    }

    server.verifyTokenRequests(3);
  }

  @Test
  void contextualSessionDoesNotInheritCatalogConfig() {
    try (OAuth2Manager manager = new OAuth2Manager("test");
        AuthSession catalogSession =
            manager.catalogSession(server.restClient(), catalogProperties())) {
      // no token endpoint in the context: the legacy fallback to the catalog URI applies,
      // and the catalog's token endpoint is not inherited
      server.expectLegacyTokenEndpoint();
      SessionContext context =
          new SessionContext(
              "session1",
              "user",
              Map.of(OAuth2Properties.CREDENTIAL, CONTEXT_CLIENT_ID + ":" + CONTEXT_CLIENT_SECRET),
              Map.of());
      OAuth2Session contextualSession =
          (OAuth2Session) manager.contextualSession(context, catalogSession);
      assertThat(contextualSession.config().tokenEndpoint())
          .hasToString(server.baseUrl() + "/v1/oauth/tokens");
    }
  }

  @Test
  void tableSessionReturnsParent() {
    try (OAuth2Manager manager = new OAuth2Manager("test");
        AuthSession catalogSession =
            manager.catalogSession(server.restClient(), catalogProperties())) {
      AuthSession tableSession =
          manager.tableSession(
              TableIdentifier.of("ns", "t1"),
              Map.of(OAuth2Properties.TOKEN, "vended-token"),
              catalogSession);
      assertThat(tableSession).isSameAs(catalogSession);
      assertThat(manager.cacheByConfig()).isNull();
    }
  }

  @Test
  void standaloneTableSessionCachedByConfig() {
    try (OAuth2Manager manager = new OAuth2Manager("test")) {
      Map<String, String> properties1 = catalogProperties();
      Map<String, String> properties2 =
          ImmutableMap.<String, String>builder()
              .putAll(catalogProperties())
              .put(OAuth2Config.CLIENT_ID, CONTEXT_CLIENT_ID)
              .put(OAuth2Config.CLIENT_SECRET, CONTEXT_CLIENT_SECRET)
              .buildKeepingLast();

      AuthSession session1 = manager.tableSession(server.restClient(), properties1);
      AuthSession session2 = manager.tableSession(server.restClient(), properties2);
      assertThat(session1).isNotSameAs(session2);
      assertThat(manager.tableSession(server.restClient(), properties1)).isSameAs(session1);
      assertThat(manager.tableSession(server.restClient(), properties2)).isSameAs(session2);
      assertThat(manager.cacheByConfig().asMap()).hasSize(2);

      assertAuthorization(session1, MockAuthorizationServer.ACCESS_TOKEN);
      assertAuthorization(session2, CONTEXT_ACCESS_TOKEN);
    }

    server.verifyTokenRequests(2);
  }

  /**
   * Standalone sessions for different catalogs must not share tokens, even when the catalogs are
   * configured with the same credentials; see https://github.com/apache/iceberg/pull/14178. With
   * the legacy token endpoint fallback, the endpoint resolved against each catalog URI is part of
   * the session configuration, and hence of the cache key.
   */
  @Test
  void standaloneTableSessionsForDifferentCatalogs() {
    server.expectLegacyTokenEndpoint("/catalog-a", "access_a");
    server.expectLegacyTokenEndpoint("/catalog-b", "access_b");
    try (OAuth2Manager manager = new OAuth2Manager("test")) {
      Map<String, String> propertiesA =
          Map.of(
              CatalogProperties.URI,
              server.baseUrl() + "/catalog-a",
              OAuth2Properties.CREDENTIAL,
              "client1:s3cr3t");
      Map<String, String> propertiesB =
          Map.of(
              CatalogProperties.URI,
              server.baseUrl() + "/catalog-b",
              OAuth2Properties.CREDENTIAL,
              "client1:s3cr3t");

      AuthSession sessionA = manager.tableSession(server.restClient(), propertiesA);
      AuthSession sessionB = manager.tableSession(server.restClient(), propertiesB);
      assertThat(sessionA).isNotSameAs(sessionB);
      assertThat(manager.cacheByConfig().asMap()).hasSize(2);
      assertAuthorization(sessionA, "access_a");
      assertAuthorization(sessionB, "access_b");
    }
  }

  @Test
  void closeClearsCaches() {
    server.expectLegacyTokenEndpoint();
    OAuth2Manager manager = new OAuth2Manager("test");
    AuthSession catalogSession = manager.catalogSession(server.restClient(), catalogProperties());
    SessionContext context =
        new SessionContext(
            "session1",
            "user",
            Map.of(OAuth2Properties.CREDENTIAL, CONTEXT_CLIENT_ID + ":" + CONTEXT_CLIENT_SECRET),
            Map.of());
    manager.contextualSession(context, catalogSession);
    manager.tableSession(server.restClient(), catalogProperties());
    assertThat(manager.cacheById()).isNotNull();
    assertThat(manager.cacheByConfig()).isNotNull();
    manager.close();
    assertThat(manager.cacheById()).isNull();
    assertThat(manager.cacheByConfig()).isNull();
  }

  private static Map<String, String> catalogProperties() {
    return ImmutableMap.<String, String>builder()
        .put(CatalogProperties.URI, server.baseUrl().toString())
        .putAll(server.clientProperties())
        .buildOrThrow();
  }

  private static void assertAuthorization(AuthSession session, String expectedToken) {
    HTTPRequest authenticated = session.authenticate(REQUEST);
    assertThat(authenticated.headers().entries("Authorization"))
        .containsOnly(HTTPHeader.of("Authorization", "Bearer " + expectedToken));
  }
}
