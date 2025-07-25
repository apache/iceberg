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
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.github.benmanes.caffeine.cache.Cache;
import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.oauth2.config.BasicConfig;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.junit.EnumLike;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

/**
 * Tests for {@link OAuth2Manager}.
 *
 * <p>The tests in this class are split into two categories:
 *
 * <ul>
 *   <li>Unit tests that instantiate an {@link OAuth2Manager} directly and exercise its public API.
 *   <li>"Catalog" tests that instantiate a full {@link RESTCatalog} embedding an {@link
 *       OAuth2Manager}. <em>These tests focus on the interaction between {@link RESTCatalog} and
 *       {@link OAuth2Manager}</em>. Complete tests for {@link RESTCatalog} functionality can be
 *       found in {@link TestOAuth2RESTCatalog}.
 * </ul>
 *
 * @see TestOAuth2RESTCatalog
 */
class TestOAuth2Manager {

  /** Tests that instantiate an {@link OAuth2Manager} directly. */
  @Nested
  class UnitTests {

    private final TableIdentifier table = TableIdentifier.of("t1");

    private final HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://localhost:8181"))
            .method(HTTPMethod.GET)
            .path("v1/config")
            .build();

    @Test
    void catalogSessionWithoutInit() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test");
          AuthSession session = manager.catalogSession(env.httpClient(), env.catalogProperties())) {
        HTTPRequest actual = session.authenticate(request);
        assertThat(actual.headers().entries("Authorization"))
            .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
      }
    }

    @Test
    void catalogSessionWithInit() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        try (AuthSession session = manager.initSession(env.httpClient(), env.catalogProperties())) {
          HTTPRequest actual = session.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }

        try (AuthSession session =
            manager.catalogSession(env.httpClient(), env.catalogProperties())) {
          HTTPRequest actual = session.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void contextualSessionEmpty() {
      SessionContext context = SessionContext.createEmpty();
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test");
          AuthSession catalogSession =
              manager.catalogSession(env.httpClient(), env.catalogProperties());
          AuthSession contextualSession = manager.contextualSession(context, catalogSession)) {
        catalogSession.authenticate(request);
        assertThat(contextualSession).isSameAs(catalogSession);
        HTTPRequest actual = contextualSession.authenticate(request);
        assertThat(actual.headers().entries("Authorization"))
            .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
      }
    }

    @Test
    void contextualSessionNotCached() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        // Identical to catalog properties, so should not be cached
        SessionContext context =
            new SessionContext("test", "test", env.catalogProperties(), Map.of());
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession contextualSession = manager.contextualSession(context, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(contextualSession).isSameAs(catalogSession);
          HTTPRequest actual = contextualSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void contextualSessionCacheHit() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        SessionContext context =
            new SessionContext(
                "test",
                "test",
                Map.of(
                    BasicConfig.TOKEN_ENDPOINT,
                    env.tokenEndpoint().toString(),
                    BasicConfig.CLIENT_ID,
                    TestEnvironment.CLIENT_ID1.getValue(),
                    BasicConfig.CLIENT_SECRET,
                    TestEnvironment.CLIENT_SECRET1.getValue()),
                Map.of(BasicConfig.EXTRA_PARAMS + ".extra2", "value2"));
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession contextualSession1 = manager.contextualSession(context, catalogSession);
            AuthSession contextualSession2 = manager.contextualSession(context, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(contextualSession1).isNotSameAs(catalogSession);
          assertThat(contextualSession2).isNotSameAs(catalogSession);
          assertThat(contextualSession1).isSameAs(contextualSession2);
          HTTPRequest actual = contextualSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void contextualSessionCacheMiss() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        SessionContext context1 =
            new SessionContext(
                "test1",
                "test",
                Map.of(
                    BasicConfig.CLIENT_ID,
                    TestEnvironment.CLIENT_ID1.getValue(),
                    BasicConfig.CLIENT_SECRET,
                    TestEnvironment.CLIENT_SECRET1.getValue()),
                Map.of(
                    BasicConfig.TOKEN_ENDPOINT,
                    env.tokenEndpoint().toString(),
                    BasicConfig.SCOPE,
                    TestEnvironment.SCOPE1.toString(),
                    BasicConfig.EXTRA_PARAMS + ".extra2",
                    "value2"));
        SessionContext context2 =
            new SessionContext(
                "test2",
                "test",
                Map.of(
                    BasicConfig.CLIENT_ID,
                    TestEnvironment.CLIENT_ID2.getValue(),
                    BasicConfig.CLIENT_SECRET,
                    TestEnvironment.CLIENT_SECRET2.getValue()),
                Map.of(
                    BasicConfig.TOKEN_ENDPOINT,
                    env.tokenEndpoint().toString(),
                    BasicConfig.SCOPE,
                    TestEnvironment.SCOPE2.toString(),
                    BasicConfig.EXTRA_PARAMS + ".extra3",
                    "value3"));
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession contextualSession1 = manager.contextualSession(context1, catalogSession);
            AuthSession contextualSession2 = manager.contextualSession(context2, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(contextualSession1).isNotSameAs(catalogSession);
          assertThat(contextualSession2).isNotSameAs(catalogSession);
          assertThat(contextualSession1).isNotSameAs(contextualSession2);
          HTTPRequest actual = contextualSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
          actual = contextualSession2.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    @SuppressWarnings("deprecation")
    void contextualSessionLegacyProperties() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        SessionContext context =
            new SessionContext(
                "test",
                "test",
                Map.of(
                    OAuth2Properties.OAUTH2_SERVER_URI,
                    env.tokenEndpoint().toString(),
                    OAuth2Properties.CREDENTIAL,
                    TestEnvironment.CLIENT_ID2.getValue()
                        + ":"
                        + TestEnvironment.CLIENT_SECRET2.getValue(),
                    BasicConfig.EXTRA_PARAMS + ".extra2",
                    "value2"),
                Map.of(OAuth2Properties.SCOPE, TestEnvironment.SCOPE2.toString()));
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession contextualSession = manager.contextualSession(context, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(contextualSession).isNotSameAs(catalogSession);
          HTTPRequest actual = contextualSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    @SuppressWarnings("deprecation")
    void contextualSessionLegacyPropertiesToken() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        SessionContext context =
            new SessionContext(
                "test",
                "test",
                Map.of(
                    OAuth2Properties.OAUTH2_SERVER_URI,
                    env.tokenEndpoint().toString(),
                    OAuth2Properties.TOKEN,
                    "access_context",
                    BasicConfig.EXTRA_PARAMS + ".extra2",
                    "value2"),
                Map.of(OAuth2Properties.SCOPE, TestEnvironment.SCOPE2.toString()));
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession contextualSession = manager.contextualSession(context, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(contextualSession).isNotSameAs(catalogSession);
          HTTPRequest actual = contextualSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_context"));
        }
      }
    }

    @Test
    @SuppressWarnings("deprecation")
    void contextualSessionLegacyPropertiesTokenExchange() {
      try (TestEnvironment env =
              TestEnvironment.builder()
                  .grantType(GrantType.TOKEN_EXCHANGE)
                  .subjectTokenType(TokenTypeURI.ACCESS_TOKEN)
                  .build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        SessionContext context =
            new SessionContext(
                "test",
                "test",
                Map.of(
                    OAuth2Properties.OAUTH2_SERVER_URI,
                    env.tokenEndpoint().toString(),
                    OAuth2Properties.ACCESS_TOKEN_TYPE,
                    "access_context",
                    BasicConfig.EXTRA_PARAMS + ".extra2",
                    "value2"),
                Map.of(OAuth2Properties.SCOPE, TestEnvironment.SCOPE2.toString()));
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession contextualSession = manager.contextualSession(context, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(contextualSession).isNotSameAs(catalogSession);
          HTTPRequest actual = contextualSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              // access_initial is the exchanged token
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void tableSessionEmpty() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> tableProperties = Map.of();
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession tableSession =
                manager.tableSession(table, tableProperties, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(tableSession).isSameAs(catalogSession);
          HTTPRequest actual = tableSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void tableSessionNotCached() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> catalogProperties =
            Map.of(
                CatalogProperties.URI,
                env.catalogServerUrl().toString(),
                BasicConfig.TOKEN,
                "token");
        // Virtually identical to catalog properties, so should not be cached
        Map<String, String> tableProperties = Map.of(BasicConfig.TOKEN, "token");
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), catalogProperties);
            AuthSession tableSession =
                manager.tableSession(table, tableProperties, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(tableSession).isSameAs(catalogSession);
          HTTPRequest actual = tableSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer token"));
        }
      }
    }

    @Test
    void tableSessionCacheHit() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> tableProperties = Map.of(BasicConfig.TOKEN, "token");
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession tableSession1 =
                manager.tableSession(table, tableProperties, catalogSession);
            AuthSession tableSession2 =
                manager.tableSession(table, tableProperties, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(tableSession1).isNotSameAs(catalogSession);
          assertThat(tableSession2).isNotSameAs(catalogSession);
          assertThat(tableSession1).isSameAs(tableSession2);
          HTTPRequest actual = tableSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer token"));
        }
      }
    }

    @Test
    void tableSessionCacheMiss() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> tableProperties1 = Map.of(BasicConfig.TOKEN, "token1");
        Map<String, String> tableProperties2 = Map.of(BasicConfig.TOKEN, "token2");
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession tableSession1 =
                manager.tableSession(table, tableProperties1, catalogSession);
            AuthSession tableSession2 =
                manager.tableSession(table, tableProperties2, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(tableSession1).isNotSameAs(catalogSession);
          assertThat(tableSession2).isNotSameAs(catalogSession);
          assertThat(tableSession1).isNotSameAs(tableSession2);
          HTTPRequest actual = tableSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer token1"));
          actual = tableSession2.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer token2"));
        }
      }
    }

    @Test
    @SuppressWarnings("deprecation")
    void tableSessionLegacyPropertiesVendedToken() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> tableProperties = Map.of(OAuth2Properties.TOKEN, "access_vended");
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession tableSession =
                manager.tableSession(table, tableProperties, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(tableSession).isNotSameAs(catalogSession);
          HTTPRequest actual = tableSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_vended"));
        }
      }
    }

    @Test
    @SuppressWarnings("deprecation")
    void tableSessionLegacyPropertiesVendedTokenExchange() {
      try (TestEnvironment env =
              TestEnvironment.builder().grantType(GrantType.TOKEN_EXCHANGE).build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> tableProperties =
            Map.of(OAuth2Properties.ACCESS_TOKEN_TYPE, "access_vended");
        try (AuthSession catalogSession =
                manager.catalogSession(env.httpClient(), env.catalogProperties());
            AuthSession tableSession =
                manager.tableSession(table, tableProperties, catalogSession)) {
          catalogSession.authenticate(request);
          assertThat(tableSession).isNotSameAs(catalogSession);
          HTTPRequest actual = tableSession.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              // access_initial is the exchanged token
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void standaloneTableSessionCacheMiss() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> standaloneProperties1 =
            Map.of(
                CatalogProperties.URI,
                env.catalogServerUrl().toString(),
                BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        Map<String, String> standaloneProperties2 =
            Map.of(
                CatalogProperties.URI,
                env.catalogServerUrl().toString(),
                BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID2.getValue(),
                BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET2.getValue(),
                BasicConfig.SCOPE,
                TestEnvironment.SCOPE2.toString(),
                BasicConfig.EXTRA_PARAMS + ".extra2",
                "value2");
        try (AuthSession standaloneSession1 =
                manager.tableSession(env.httpClient(), standaloneProperties1);
            AuthSession standaloneSession2 =
                manager.tableSession(env.httpClient(), standaloneProperties2)) {
          assertThat(standaloneSession1).isNotSameAs(standaloneSession2);
          HTTPRequest actual = standaloneSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
          actual = standaloneSession2.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }

    @Test
    void standaloneTableSessionCacheHit() {
      try (TestEnvironment env = TestEnvironment.builder().build();
          OAuth2Manager manager = new OAuth2Manager("test")) {
        Map<String, String> standaloneProperties1 =
            Map.of(
                CatalogProperties.URI,
                env.catalogServerUrl().toString(),
                BasicConfig.TOKEN_ENDPOINT,
                env.tokenEndpoint().toString(),
                BasicConfig.CLIENT_ID,
                TestEnvironment.CLIENT_ID1.getValue(),
                BasicConfig.CLIENT_SECRET,
                TestEnvironment.CLIENT_SECRET1.getValue(),
                BasicConfig.SCOPE,
                TestEnvironment.SCOPE1.toString(),
                BasicConfig.EXTRA_PARAMS + ".extra1",
                "value1");
        // Same OAuth2 config shared by 2 catalog servers => same OAuth2 session
        Map<String, String> standaloneProperties2 =
            ImmutableMap.<String, String>builder()
                .putAll(standaloneProperties1)
                .put(CatalogProperties.URI, "https://other.com")
                .buildKeepingLast();
        try (AuthSession standaloneSession1 =
                manager.tableSession(env.httpClient(), standaloneProperties1);
            AuthSession standaloneSession2 =
                manager.tableSession(env.httpClient(), standaloneProperties2)) {
          assertThat(standaloneSession1).isSameAs(standaloneSession2);
          HTTPRequest actual = standaloneSession1.authenticate(request);
          assertThat(actual.headers().entries("Authorization"))
              .containsOnly(HTTPHeader.of("Authorization", "Bearer access_initial"));
        }
      }
    }
  }

  /**
   * Tests that instantiate a full {@link RESTCatalog} embedding an {@link OAuth2Manager}.
   *
   * <p>These tests focus on the interaction between {@link RESTCatalog} and {@link OAuth2Manager}.
   * Complete tests for {@link RESTCatalog} functionality can be found in {@link
   * TestOAuth2RESTCatalog}.
   *
   * @see TestOAuth2RESTCatalog
   */
  @Nested
  class CatalogTests {

    @CartesianTest
    void testCatalogProperties(
        @EnumLike(excludes = "refresh_token") GrantType grantType,
        @EnumLike ClientAuthenticationMethod authenticationMethod)
        throws IOException {
      assumeTrue(
          !grantType.equals(GrantType.CLIENT_CREDENTIALS)
              || !authenticationMethod.equals(ClientAuthenticationMethod.NONE));
      try (TestEnvironment env =
              TestEnvironment.builder()
                  .grantType(grantType)
                  .clientAuthenticationMethod(authenticationMethod)
                  .sessionContext(SessionContext.createEmpty())
                  .tableProperties(Map.of())
                  .build();
          RESTCatalog catalog = env.newCatalog(Map.of())) {
        Table table = catalog.loadTable(TestEnvironment.TABLE_IDENTIFIER);
        assertThat(table).isNotNull();
        assertThat(table.name()).isEqualTo(catalog.name() + "." + TestEnvironment.TABLE_IDENTIFIER);
        assertThat(cacheById(catalog)).isNull();
        assertThat(cacheByConfig(catalog)).isNull();
      }
    }

    @Test
    void testCatalogAndContextProperties() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().tableProperties(Map.of()).build();
          RESTCatalog catalog = env.newCatalog(Map.of())) {
        Table table = catalog.loadTable(TestEnvironment.TABLE_IDENTIFIER);
        assertThat(table).isNotNull();
        assertThat(table.name()).isEqualTo(catalog.name() + "." + TestEnvironment.TABLE_IDENTIFIER);
        assertThat(cacheById(catalog).asMap())
            .hasSize(1)
            .containsKey(env.sessionContext().sessionId());
        assertThat(cacheByConfig(catalog)).isNull();
      }
    }

    @Test
    void testCatalogAndTableProperties() throws IOException {
      try (TestEnvironment env =
              TestEnvironment.builder()
                  .sessionContext(SessionContext.createEmpty())
                  .tableProperties(Map.of(BasicConfig.TOKEN, "token"))
                  .build();
          RESTCatalog catalog = env.newCatalog(Map.of())) {
        Table table = catalog.loadTable(TestEnvironment.TABLE_IDENTIFIER);
        assertThat(table).isNotNull();
        assertThat(table.name()).isEqualTo(catalog.name() + "." + TestEnvironment.TABLE_IDENTIFIER);
        assertThat(cacheById(catalog)).isNull();
        assertThat(cacheByConfig(catalog).asMap()).hasSize(1);
      }
    }

    @Test
    void testCatalogAndContextAndTableProperties() throws IOException {
      try (TestEnvironment env = TestEnvironment.builder().build();
          RESTCatalog catalog = env.newCatalog(Map.of())) {
        Table table = catalog.loadTable(TestEnvironment.TABLE_IDENTIFIER);
        assertThat(table).isNotNull();
        assertThat(table.name()).isEqualTo(catalog.name() + "." + TestEnvironment.TABLE_IDENTIFIER);
        assertThat(cacheById(catalog).asMap())
            .hasSize(1)
            .containsKey(env.sessionContext().sessionId());
        assertThat(cacheByConfig(catalog).asMap()).hasSize(1);
      }
    }

    private static Cache<String, OAuth2Session> cacheById(RESTCatalog catalog) {
      return authManager(catalog).cacheById();
    }

    private static Cache<OAuth2Config, OAuth2Session> cacheByConfig(RESTCatalog catalog) {
      return authManager(catalog).cacheByConfig();
    }

    private static OAuth2Manager authManager(RESTCatalog catalog) {
      return assertThat(catalog)
          .extracting("sessionCatalog.authManager")
          .asInstanceOf(type(OAuth2Manager.class))
          .actual();
    }
  }
}
