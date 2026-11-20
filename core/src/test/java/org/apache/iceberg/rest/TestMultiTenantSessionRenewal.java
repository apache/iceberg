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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.SupportsKeyedTokenAuthSession;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.KeyedTokenAuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestMultiTenantSessionRenewal {
  private static final Namespace NAMESPACE = Namespace.of("ns");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "t");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final String CATALOG_CLIENT_TOKEN = "client-credentials-token:sub=catalog";
  private static final String EXCHANGE_TOKEN_PREFIX = "token-exchange-token:";

  @TempDir private Path temp;

  private InMemoryCatalog backendCatalog;
  private UserTokenServer server;
  private RESTSessionCatalog catalog;

  public static class SessionFileIO extends InMemoryFileIO
      implements SupportsKeyedTokenAuthSession {
    private KeyedTokenAuthSession authSession = null;

    @Override
    public void setAuthSession(KeyedTokenAuthSession session) {
      this.authSession = session;
    }

    @Override
    public KeyedTokenAuthSession authSession() {
      return authSession;
    }
  }

  private static class UserTokenServer extends RESTCatalogAdapter {
    private final Set<String> revoked = Sets.newConcurrentHashSet();
    private final AtomicInteger rejected = new AtomicInteger();
    private final long firstSeconds;
    private final long renewalSeconds;

    UserTokenServer(InMemoryCatalog catalog, long firstSeconds, long renewalSeconds) {
      super(catalog);
      this.firstSeconds = firstSeconds;
      this.renewalSeconds = renewalSeconds;
    }

    void revoke(String subjectToken) {
      revoked.add(subjectToken);
    }

    int rejected() {
      return rejected.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends RESTResponse> T handleRequest(
        Route route,
        Map<String, String> vars,
        HTTPRequest httpRequest,
        Class<T> responseType,
        Consumer<Map<String, String>> responseHeaders) {
      String subject = null;
      if (route == Route.TOKENS) {
        subject = ((Map<String, String>) httpRequest.body()).get("subject_token");
        if (subject != null && revoked.stream().anyMatch(subject::contains)) {
          rejected.incrementAndGet();
          throw new NotAuthorizedException("Subject token revoked");
        }
      }

      T response = super.handleRequest(route, vars, httpRequest, responseType, responseHeaders);
      if (subject != null && response instanceof OAuthTokenResponse token) {
        long lifetime = subject.startsWith(EXCHANGE_TOKEN_PREFIX) ? renewalSeconds : firstSeconds;
        return castResponse(
            responseType,
            OAuthTokenResponse.builder()
                .withToken(token.token())
                .withTokenType(token.tokenType())
                .withIssuedTokenType(token.issuedTokenType())
                .setExpirationInSeconds((int) lifetime)
                .build());
      }

      return response;
    }
  }

  @BeforeEach
  void before() {
    this.backendCatalog = new InMemoryCatalog();
    backendCatalog.initialize(
        "backend",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, temp.toFile().getAbsolutePath()));
    backendCatalog.createNamespace(NAMESPACE);
    backendCatalog.createTable(TABLE, SCHEMA);
  }

  @AfterEach
  void after() throws IOException {
    if (catalog != null) {
      catalog.close();
    }

    backendCatalog.close();
  }

  private void startCatalog(long firstSeconds, long renewalSeconds) {
    this.server = new UserTokenServer(backendCatalog, firstSeconds, renewalSeconds);
    this.catalog = new RESTSessionCatalog(config -> server, null);
    catalog.initialize(
        "prod",
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, "http://localhost:8181/catalog")
            .put(OAuth2Properties.CREDENTIAL, "catalog:secret")
            .put(OAuth2Properties.OAUTH2_SERVER_URI, "v1/oauth/tokens")
            .put(OAuth2Properties.TOKEN_EXCHANGE_ENABLED, "true")
            .put(CatalogProperties.FILE_IO_IMPL, SessionFileIO.class.getName())
            .put(AuthProperties.FILE_IO_SESSION_ENABLED, "true")
            .buildOrThrow());
  }

  private KeyedTokenAuthSession loadAs(String user) {
    SessionContext context =
        new SessionContext(
            user + "-query",
            user,
            ImmutableMap.of(OAuth2Properties.ACCESS_TOKEN_TYPE, user + "-subject"),
            ImmutableMap.of());
    Table table = catalog.loadTable(context, TABLE);
    return ((SupportsKeyedTokenAuthSession) table.io()).authSession();
  }

  @Test
  void eachUserKeepsItsOwnTokenAcrossScheduledRenewal() {
    startCatalog(1, 1);
    KeyedTokenAuthSession alice = loadAs("alice");
    KeyedTokenAuthSession bob = loadAs("bob");
    String firstAliceToken = token(alice);

    await()
        .ignoreExceptions()
        .atMost(Duration.ofSeconds(30))
        .until(() -> !firstAliceToken.equals(token(alice)));

    assertThat(token(alice)).contains("alice-subject").doesNotContain("bob-subject");
    assertThat(token(bob)).contains("bob-subject").doesNotContain("alice-subject");
  }

  @Test
  void rejectedRenewalFailsOnlyThatUser() {
    startCatalog(1, 3600);
    KeyedTokenAuthSession bob = loadAs("bob");
    KeyedTokenAuthSession alice = loadAs("alice");
    server.revoke("alice-subject");

    await()
        .ignoreExceptions()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              assertThat(bob.currentToken().expiresAtMillis())
                  .isGreaterThan(System.currentTimeMillis() + Duration.ofMinutes(5).toMillis());
              assertThatThrownBy(alice::currentToken)
                  .isInstanceOf(NotAuthorizedException.class)
                  .hasMessage("No token for auth session: %s", alice.key())
                  .cause()
                  .hasMessage("Cannot provide a token: expired");
            });
    assertThat(token(bob)).contains("bob-subject");
  }

  private static String token(KeyedTokenAuthSession session) {
    return session.currentToken().value();
  }
}
