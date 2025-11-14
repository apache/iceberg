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
package org.apache.iceberg.rest.auth;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.Map;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestOAuth2Manager {

  private RESTClient client;

  @BeforeEach
  void before() {
    client = Mockito.mock(RESTClient.class);
    when(client.withAuthSession(any())).thenReturn(client);
    when(client.postForm(any(), any(), eq(OAuthTokenResponse.class), anyMap(), any()))
        .thenReturn(
            OAuthTokenResponse.builder()
                .withToken("test")
                .addScope("scope")
                .withIssuedTokenType(OAuth2Properties.ACCESS_TOKEN_TYPE)
                .withTokenType("bearer")
                .setExpirationInSeconds(3600)
                .build());
  }

  @Test
  void initSessionNoOAuth2Properties() {
    Map<String, String> properties = Map.of();
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession session = manager.initSession(client, properties)) {
      assertThat(session.headers()).isEmpty();
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should not create refresh executor for init session")
          .isNull();
    }
    Mockito.verifyNoInteractions(client);
  }

  @Test
  void initSessionTokenProvided() {
    Map<String, String> properties = Map.of(OAuth2Properties.TOKEN, "test");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession session = manager.initSession(client, properties)) {
      assertThat(session.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should not create refresh executor for init session")
          .isNull();
    }
    Mockito.verifyNoInteractions(client);
  }

  @Test
  void initSessionCredentialsProvided() {
    Map<String, String> properties = Map.of(OAuth2Properties.CREDENTIAL, "client:secret");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession session = manager.initSession(client, properties)) {
      assertThat(session.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should not create refresh executor for init session")
          .isNull();
    }
    Mockito.verify(client)
        .postForm(
            any(),
            eq(
                Map.of(
                    "grant_type", "client_credentials",
                    "client_id", "client",
                    "client_secret", "secret",
                    "scope", "catalog")),
            eq(OAuthTokenResponse.class),
            eq(Map.of()),
            any());
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void catalogSessionNoOAuth2Properties() {
    Map<String, String> properties = Map.of();
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties)) {
      assertThat(catalogSession.headers()).isEmpty();
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should not create refresh executor when no token and no credentials provided")
          .isNull();
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void catalogSessionTokenProvided() {
    Map<String, String> properties = Map.of(OAuth2Properties.TOKEN, "test");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties)) {
      assertThat(catalogSession.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should create refresh executor when token provided")
          .isNotNull();
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void catalogSessionRefreshDisabled() {
    Map<String, String> properties =
        Map.of(OAuth2Properties.TOKEN, "test", OAuth2Properties.TOKEN_REFRESH_ENABLED, "false");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties)) {
      assertThat(catalogSession.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should not create refresh executor when refresh disabled")
          .isNull();
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void catalogSessionCredentialsProvidedWithInitSession() {
    // Emulates the cases where the credentials are exchanged for a token when initSession is
    // called, and the obtained token is used for the catalog session.
    Map<String, String> properties = Map.of(OAuth2Properties.CREDENTIAL, "client:secret");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession ignored = manager.initSession(client, properties);
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties)) {
      assertThat(catalogSession.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should create refresh executor when credentials provided")
          .isNotNull();
    }
    Mockito.verify(client)
        .postForm(
            any(),
            eq(
                Map.of(
                    "grant_type", "client_credentials",
                    "client_id", "client",
                    "client_secret", "secret",
                    "scope", "catalog")),
            eq(OAuthTokenResponse.class),
            eq(Map.of()),
            any());
    Mockito.verify(client, times(2)).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void catalogSessionCredentialsProvidedWithoutInitSession() {
    // Emulate the case where initSession is not called before catalogSession,
    // so the credentials are exchanged for a token during the catalogSession call.
    Map<String, String> properties = Map.of(OAuth2Properties.CREDENTIAL, "client:secret");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties)) {
      assertThat(catalogSession.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should create refresh executor when credentials provided")
          .isNotNull();
    }
    Mockito.verify(client)
        .postForm(
            any(),
            eq(
                Map.of(
                    "grant_type", "client_credentials",
                    "client_id", "client",
                    "client_secret", "secret",
                    "scope", "catalog")),
            eq(OAuthTokenResponse.class),
            eq(Map.of()),
            any());
  }

  @Test
  void contextualSessionEmptyContext() {
    SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
    Map<String, String> properties = Map.of();
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties);
        OAuth2Util.AuthSession contextualSession =
            manager.contextualSession(context, catalogSession)) {
      assertThat(contextualSession).isSameAs(catalogSession);
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should not create refresh executor when no context credentials provided")
          .isNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should not create session cache for empty context")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).isEmpty());
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void contextualSessionTokenProvided() {
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            "test", "test", Map.of(OAuth2Properties.TOKEN, "context-token"), Map.of());
    Map<String, String> properties = Map.of();
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties);
        OAuth2Util.AuthSession contextualSession =
            manager.contextualSession(context, catalogSession)) {
      assertThat(contextualSession).isNotSameAs(catalogSession);
      assertThat(contextualSession.headers())
          .containsOnly(entry("Authorization", "Bearer context-token"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should create refresh executor when contextual session created")
          .isNotNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should create session cache for context with token")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).hasSize(1));
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void contextualSessionCredentialsProvided() {
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            "test", "test", Map.of(OAuth2Properties.CREDENTIAL, "client:secret"), Map.of());
    Map<String, String> properties = Map.of();
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties);
        OAuth2Util.AuthSession contextualSession =
            manager.contextualSession(context, catalogSession)) {
      assertThat(contextualSession).isNotSameAs(catalogSession);
      assertThat(contextualSession.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should create refresh executor when contextual session created")
          .isNotNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should create session cache for context with credentials")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).hasSize(1));
    }
    Mockito.verify(client)
        .postForm(
            any(),
            eq(
                Map.of(
                    "grant_type", "client_credentials",
                    "client_id", "client",
                    "client_secret", "secret",
                    "scope", "catalog")),
            eq(OAuthTokenResponse.class),
            eq(Map.of()),
            any());
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void contextualSessionTokenExchange() {
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            "test", "test", Map.of(OAuth2Properties.ACCESS_TOKEN_TYPE, "context-token"), Map.of());
    Map<String, String> properties = Map.of(OAuth2Properties.TOKEN, "catalog-token");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties);
        OAuth2Util.AuthSession contextualSession =
            manager.contextualSession(context, catalogSession)) {
      assertThat(contextualSession.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should create refresh executor when contextual session created")
          .isNotNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should create session cache for context with token exchange")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).hasSize(1));
    }
    Mockito.verify(client)
        .postForm(
            any(),
            eq(
                Map.of(
                    "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                    "subject_token", "context-token",
                    "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                    "actor_token", "catalog-token",
                    "actor_token_type", "urn:ietf:params:oauth:token-type:access_token",
                    "scope", "catalog")),
            eq(OAuthTokenResponse.class),
            eq(Map.of("Authorization", "Bearer catalog-token")),
            any());
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void contextualSessionCacheHit() {
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            "test", "test", Map.of(OAuth2Properties.TOKEN, "context-token"), Map.of());
    Map<String, String> properties = Map.of();
    try (OAuth2Manager manager = Mockito.spy(new OAuth2Manager("test"));
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties);
        OAuth2Util.AuthSession contextualSession1 =
            manager.contextualSession(context, catalogSession);
        OAuth2Util.AuthSession contextualSession2 =
            manager.contextualSession(context, catalogSession)) {
      assertThat(contextualSession1).isNotSameAs(catalogSession);
      assertThat(contextualSession2).isNotSameAs(catalogSession);
      assertThat(contextualSession1).isSameAs(contextualSession2);
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should only create and cache contextual session once")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).hasSize(1));
      Mockito.verify(manager, times(1))
          .newSessionFromAccessToken("context-token", Map.of(), catalogSession);
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void tableSessionNoTableCredentials() {
    Map<String, String> properties = Map.of();
    TableIdentifier table = TableIdentifier.of("ns", "tbl");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, properties);
        OAuth2Util.AuthSession tableSession =
            manager.tableSession(table, properties, catalogSession)) {
      assertThat(tableSession).isSameAs(catalogSession);
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should not create refresh executor when no table credentials provided")
          .isNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should not create session cache for empty table credentials")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).isEmpty());
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void tableSessionTokenProvided() {
    Map<String, String> catalogProperties = Map.of();
    Map<String, String> tableProperties = Map.of(OAuth2Properties.TOKEN, "table-token");
    TableIdentifier table = TableIdentifier.of("ns", "tbl");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
        OAuth2Util.AuthSession tableSession =
            manager.tableSession(table, tableProperties, catalogSession)) {
      assertThat(tableSession).isNotSameAs(catalogSession);
      assertThat(tableSession.headers()).containsOnly(entry("Authorization", "Bearer table-token"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should create refresh executor when table session created")
          .isNotNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should create session cache for table with token")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).hasSize(1));
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void tableSessionTokenExchange() {
    Map<String, String> catalogProperties = Map.of(OAuth2Properties.TOKEN, "catalog-token");
    Map<String, String> tableProperties = Map.of(OAuth2Properties.ACCESS_TOKEN_TYPE, "table-token");
    TableIdentifier table = TableIdentifier.of("ns", "tbl");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
        OAuth2Util.AuthSession tableSession =
            manager.tableSession(table, tableProperties, catalogSession)) {
      assertThat(tableSession.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should create refresh executor when table session created")
          .isNotNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should create session cache for table with token exchange")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).hasSize(1));
    }
    Mockito.verify(client)
        .postForm(
            any(),
            eq(
                Map.of(
                    "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                    "subject_token", "table-token",
                    "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                    "actor_token", "catalog-token",
                    "actor_token_type", "urn:ietf:params:oauth:token-type:access_token",
                    "scope", "catalog")),
            eq(OAuthTokenResponse.class),
            eq(Map.of("Authorization", "Bearer catalog-token")),
            any());
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void tableSessionCacheHit() {
    Map<String, String> catalogProperties = Map.of();
    Map<String, String> tableProperties = Map.of(OAuth2Properties.TOKEN, "table-token");
    TableIdentifier table = TableIdentifier.of("ns", "tbl");
    try (OAuth2Manager manager = Mockito.spy(new OAuth2Manager("test"));
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
        OAuth2Util.AuthSession tableSession1 =
            manager.tableSession(table, tableProperties, catalogSession);
        OAuth2Util.AuthSession tableSession2 =
            manager.tableSession(table, tableProperties, catalogSession)) {
      assertThat(tableSession1).isNotSameAs(catalogSession);
      assertThat(tableSession2).isNotSameAs(catalogSession);
      assertThat(tableSession1).isSameAs(tableSession2);
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should only create and cache table session once")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).hasSize(1));
      Mockito.verify(manager, times(1))
          .newSessionFromAccessToken("table-token", Map.of("token", "table-token"), catalogSession);
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void tableSessionDisallowedTableProperties() {
    // Servers should not include sensitive information in table properties;
    // if they do, such properties should be ignored.
    Map<String, String> catalogProperties = Map.of();
    Map<String, String> tableProperties = Map.of(OAuth2Properties.CREDENTIAL, "client:secret");
    TableIdentifier table = TableIdentifier.of("ns", "tbl");
    try (OAuth2Manager manager = Mockito.spy(new OAuth2Manager("test"));
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
        OAuth2Util.AuthSession tableSession =
            manager.tableSession(table, tableProperties, catalogSession)) {
      assertThat(tableSession).isSameAs(catalogSession);
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should not create refresh executor when table credentials were filtered out")
          .isNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should not create session cache for ignored table credentials")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).isEmpty());
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void standaloneTableSessionEmptyProperties() {
    Map<String, String> properties = Map.of();
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession tableSession =
            (OAuth2Util.AuthSession) manager.tableSession(client, properties)) {
      assertThat(tableSession.headers()).isEmpty();
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should not create refresh executor when no table credentials provided")
          .isNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should create session cache for empty table properties")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).isEmpty());
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void standaloneTableSessionTokenProvided() {
    Map<String, String> tableProperties = Map.of(OAuth2Properties.TOKEN, "table-token");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession tableSession =
            (OAuth2Util.AuthSession) manager.tableSession(client, tableProperties)) {
      assertThat(tableSession.headers()).containsOnly(entry("Authorization", "Bearer table-token"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should create refresh executor when table session created")
          .isNotNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should create session cache for table with token")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).hasSize(1));
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void standaloneTableSessionCredentialProvided() {
    Map<String, String> tableProperties = Map.of(OAuth2Properties.CREDENTIAL, "client:secret");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession tableSession =
            (OAuth2Util.AuthSession) manager.tableSession(client, tableProperties)) {
      assertThat(tableSession.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should create refresh executor when table session created")
          .isNotNull();
      assertThat(manager)
          .extracting("sessionCache")
          .asInstanceOf(type(AuthSessionCache.class))
          .as("should create session cache for table with token")
          .satisfies(cache -> assertThat(cache.sessionCache().asMap()).hasSize(1));
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verify(client)
        .postForm(
            any(),
            eq(
                Map.of(
                    "grant_type", "client_credentials",
                    "client_id", "client",
                    "client_secret", "secret",
                    "scope", "catalog")),
            eq(OAuthTokenResponse.class),
            eq(Map.of()),
            any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void standaloneTableSessionCredentialProvidedMultipleAuthServers() {
    Map<String, String> tableProperties1 = Map.of(OAuth2Properties.CREDENTIAL, "client:secret");
    Map<String, String> tableProperties2 =
        Map.of(
            OAuth2Properties.OAUTH2_SERVER_URI, "https://auth-server2.com/v1/token",
            OAuth2Properties.CREDENTIAL, "client:secret");
    try (OAuth2Manager manager = new OAuth2Manager("test");
        OAuth2Util.AuthSession tableSession1 =
            (OAuth2Util.AuthSession) manager.tableSession(client, tableProperties1);
        OAuth2Util.AuthSession tableSession2 =
            (OAuth2Util.AuthSession) manager.tableSession(client, tableProperties2)) {
      assertThat(tableSession1.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(tableSession2.headers()).containsOnly(entry("Authorization", "Bearer test"));
      assertThat(tableSession1).isNotSameAs(tableSession2);
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verify(client, times(2))
        .postForm(
            any(),
            eq(
                Map.of(
                    "grant_type", "client_credentials",
                    "client_id", "client",
                    "client_secret", "secret",
                    "scope", "catalog")),
            eq(OAuthTokenResponse.class),
            eq(Map.of()),
            any());
    Mockito.verifyNoMoreInteractions(client);
  }

  @Test
  void close() {
    Map<String, String> catalogProperties = Map.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            "test", "test", Map.of(OAuth2Properties.TOKEN, "context-token"), Map.of());
    Map<String, String> tableProperties = Map.of(OAuth2Properties.TOKEN, "table-token");
    TableIdentifier table = TableIdentifier.of("ns", "tbl");
    try (OAuth2Manager manager =
            new OAuth2Manager("test") {
              @Override
              protected AuthSessionCache newSessionCache(
                  String name, Map<String, String> properties) {
                return new AuthSessionCache(
                    Duration.ofHours(1), Runnable::run, Ticker.systemTicker());
              }

              @Override
              protected OAuth2Util.AuthSession newSessionFromAccessToken(
                  String token, Map<String, String> properties, OAuth2Util.AuthSession parent) {
                return Mockito.spy(super.newSessionFromAccessToken(token, properties, parent));
              }
            };
        OAuth2Util.AuthSession catalogSession = manager.catalogSession(client, catalogProperties);
        OAuth2Util.AuthSession contextualSession =
            manager.contextualSession(context, catalogSession);
        OAuth2Util.AuthSession tableSession =
            manager.tableSession(table, tableProperties, contextualSession)) {
      manager.close();
      assertThat(manager)
          .extracting("refreshExecutor")
          .as("should close refresh executor")
          .isNull();
      assertThat(manager).extracting("sessionCache").as("should close session cache").isNull();
      // all cached sessions should be closed
      Mockito.verify(contextualSession).close();
      Mockito.verify(tableSession).close();
    }
    Mockito.verify(client).withAuthSession(any());
    Mockito.verifyNoMoreInteractions(client);
  }
}
