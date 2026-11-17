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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOParser;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SupportsKeyedTokenAuthSession;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.DefaultAuthSession;
import org.apache.iceberg.rest.auth.IssuedToken;
import org.apache.iceberg.rest.auth.KeyedTokenAuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.auth.TokenSource;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestRESTSessionCatalogFileIOSession {

  private static final Namespace NAMESPACE = Namespace.of("ns");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "t");
  private static final TableIdentifier OTHER_TABLE = TableIdentifier.of(NAMESPACE, "other");
  private static final TableIdentifier TOKEN_TABLE = TableIdentifier.of(NAMESPACE, "token_table");
  private static final String TABLE_TOKEN = "server-table-token";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final HTTPRequest REQUEST =
      ImmutableHTTPRequest.builder()
          .baseUri(URI.create("http://localhost:8181/catalog"))
          .method(HTTPRequest.HTTPMethod.GET)
          .path("v1/config")
          .build();

  @TempDir private Path temp;

  private InMemoryCatalog backendCatalog;
  private SessionAdapter adapter;
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

  private static class CustomSession implements AuthSession, TokenSource {
    private final String token;

    private CustomSession(String token) {
      this.token = token;
    }

    @Override
    public IssuedToken currentToken() {
      return IssuedToken.of(token, null);
    }

    @Override
    public HTTPRequest authenticate(HTTPRequest request) {
      return DefaultAuthSession.of(HTTPHeaders.of(OAuth2Util.authHeaders(token)))
          .authenticate(request);
    }

    @Override
    public void close() {}
  }

  public static class CustomAuthManager implements AuthManager {
    @SuppressWarnings("checkstyle:RedundantModifier")
    public CustomAuthManager(String name) {}

    @Override
    public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
      return AuthSession.EMPTY;
    }

    @Override
    public AuthSession contextualSession(SessionContext context, AuthSession parent) {
      return new CustomSession("custom:" + context.sessionId());
    }

    @Override
    public void close() {}
  }

  public static class SwitchingAuthManager implements AuthManager {
    private static final AtomicReference<AuthSession> SESSION = new AtomicReference<>();

    @SuppressWarnings("checkstyle:RedundantModifier")
    public SwitchingAuthManager(String name) {}

    @Override
    public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
      return AuthSession.EMPTY;
    }

    @Override
    public AuthSession contextualSession(SessionContext context, AuthSession parent) {
      return SESSION.get();
    }

    @Override
    public void close() {}
  }

  public static class NonBearerAuthManager implements AuthManager {
    private static final AuthSession SESSION =
        DefaultAuthSession.of(HTTPHeaders.of(HTTPHeader.of("Authorization", "Basic dXNlcg==")));

    @SuppressWarnings("checkstyle:RedundantModifier")
    public NonBearerAuthManager(String name) {}

    @Override
    public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
      return AuthSession.EMPTY;
    }

    @Override
    public AuthSession contextualSession(SessionContext context, AuthSession parent) {
      return SESSION;
    }

    @Override
    public void close() {}
  }

  private static class SessionAdapter extends RESTCatalogAdapter {
    private final AtomicInteger issuedTokens = new AtomicInteger();
    private final Map<String, String> serverConfig;
    private volatile String loadTableAuthorization = null;

    SessionAdapter(Catalog catalog, Map<String, String> serverConfig) {
      super(catalog);
      this.serverConfig = serverConfig;
    }

    @Override
    public <T extends RESTResponse> T handleRequest(
        Route route,
        Map<String, String> vars,
        HTTPRequest httpRequest,
        Class<T> responseType,
        Consumer<Map<String, String>> responseHeaders) {
      T response = super.handleRequest(route, vars, httpRequest, responseType, responseHeaders);
      if (response instanceof OAuthTokenResponse token) {
        return castResponse(
            responseType,
            OAuthTokenResponse.builder()
                .withToken(token.token() + "#" + issuedTokens.incrementAndGet())
                .withTokenType(token.tokenType())
                .withIssuedTokenType(token.issuedTokenType())
                .build());
      } else if (response instanceof ConfigResponse config) {
        return castResponse(
            responseType,
            ConfigResponse.builder()
                .withDefaults(config.defaults())
                .withOverrides(config.overrides())
                .withOverrides(serverConfig)
                .withEndpoints(config.endpoints())
                .build());
      } else if (route == Route.LOAD_TABLE && response instanceof LoadTableResponse table) {
        this.loadTableAuthorization = authorization(httpRequest);
        LoadTableResponse.Builder builder =
            LoadTableResponse.builder()
                .withTableMetadata(table.tableMetadata())
                .addAllConfig(table.config())
                .addAllConfig(serverConfig)
                .addAllCredentials(table.credentials());
        if (TOKEN_TABLE.name().equals(vars.get("table"))) {
          builder.addConfig(OAuth2Properties.TOKEN, TABLE_TOKEN);
        }

        return castResponse(responseType, builder.build());
      } else if (response instanceof PlanTableScanResponse plan) {
        return castResponse(
            responseType,
            plan.toBuilder()
                .withCredentials(
                    ImmutableList.of(
                        ImmutableCredential.builder()
                            .prefix("memory")
                            .putConfig("key", "value")
                            .build()))
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
    backendCatalog.createTable(OTHER_TABLE, SCHEMA);
    backendCatalog.createTable(TOKEN_TABLE, SCHEMA);
  }

  @AfterEach
  void after() throws IOException {
    SwitchingAuthManager.SESSION.set(null);
    if (catalog != null) {
      catalog.close();
    }

    backendCatalog.close();
  }

  private RESTSessionCatalog catalog(Map<String, String> properties) {
    return catalog(properties, ImmutableMap.of(), null);
  }

  private RESTSessionCatalog catalog(
      Map<String, String> properties,
      Map<String, String> serverConfig,
      BiFunction<SessionContext, Map<String, String>, FileIO> ioBuilder) {
    this.adapter = new SessionAdapter(backendCatalog, serverConfig);
    this.catalog = newCatalog(adapter, properties, ioBuilder);
    return catalog;
  }

  private static RESTSessionCatalog newCatalog(
      RESTCatalogAdapter adapter,
      Map<String, String> properties,
      BiFunction<SessionContext, Map<String, String>, FileIO> ioBuilder) {
    RESTSessionCatalog sessionCatalog = new RESTSessionCatalog(config -> adapter, ioBuilder);
    sessionCatalog.initialize(
        "prod",
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, "http://localhost:8181/catalog")
            .put(OAuth2Properties.TOKEN, "catalog-token")
            .put(OAuth2Properties.OAUTH2_SERVER_URI, "v1/oauth/tokens")
            .put(CatalogProperties.FILE_IO_IMPL, SessionFileIO.class.getName())
            .putAll(properties)
            .buildKeepingLast());
    return sessionCatalog;
  }

  private RESTSessionCatalog enabledCatalog(Map<String, String> properties) {
    return catalog(enabled(properties));
  }

  private static Map<String, String> enabled(Map<String, String> properties) {
    return ImmutableMap.<String, String>builder()
        .put(AuthProperties.FILE_IO_SESSION_ENABLED, "true")
        .putAll(properties)
        .buildKeepingLast();
  }

  private static SessionContext userContext() {
    return new SessionContext(
        UUID.randomUUID().toString(),
        "user",
        ImmutableMap.of(OAuth2Properties.CREDENTIAL, "user:secret"),
        ImmutableMap.of());
  }

  private static KeyedTokenAuthSession authSession(FileIO fileIO) {
    return ((SupportsKeyedTokenAuthSession) fileIO).authSession();
  }

  private static String authorization(HTTPRequest request) {
    return request.headers().firstEntry("Authorization").map(HTTPHeader::value).orElse(null);
  }

  @Test
  void tableFileIOPresentsSessionToken() {
    Table table = enabledCatalog(ImmutableMap.of()).loadTable(userContext(), TABLE);

    assertThat(authorization(authSession(table.io()).authenticate(REQUEST)))
        .isEqualTo(adapter.loadTableAuthorization);
  }

  @Test
  void resolvingFileIOPresentsSessionToken() {
    Table table =
        enabledCatalog(
                ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, ResolvingFileIO.class.getName()))
            .loadTable(userContext(), TABLE);

    assertThat(table.io()).isInstanceOf(ResolvingFileIO.class);
    assertThat(authorization(authSession(table.io()).authenticate(REQUEST)))
        .isEqualTo(adapter.loadTableAuthorization);
  }

  @Test
  void tokenExchangeSessionPresentsExchangedToken() {
    SessionContext context =
        new SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of(OAuth2Properties.ACCESS_TOKEN_TYPE, "subject-token"),
            ImmutableMap.of());
    Table table = enabledCatalog(ImmutableMap.of()).loadTable(context, TABLE);

    assertThat(authorization(authSession(table.io()).authenticate(REQUEST)))
        .isEqualTo(adapter.loadTableAuthorization)
        .startsWith("Bearer token-exchange-token:sub=subject-token");
  }

  @Test
  void serializedTableCarriesNoToken() throws IOException {
    String subjectToken = "subject-token";
    SessionContext context =
        new SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of(OAuth2Properties.ACCESS_TOKEN_TYPE, subjectToken),
            ImmutableMap.of());
    Table table = enabledCatalog(ImmutableMap.of()).loadTable(context, TABLE);
    String accessToken = token(authSession(table.io()));

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(SerializableTable.copyOf(table));
    }

    assertThat(bytes.toString(StandardCharsets.ISO_8859_1))
        .contains(authSession(table.io()).key())
        .doesNotContain(subjectToken)
        .doesNotContain(accessToken);
  }

  @Test
  void tablesOfOneSessionShareKey() {
    RESTSessionCatalog sessionCatalog = enabledCatalog(ImmutableMap.of());
    SessionContext context = userContext();

    Table table = sessionCatalog.loadTable(context, TABLE);
    Table other = sessionCatalog.loadTable(context, OTHER_TABLE);

    assertThat(authSession(other.io()).key()).isEqualTo(authSession(table.io()).key());
  }

  @Test
  void sessionsHaveDistinctKeys() {
    RESTSessionCatalog sessionCatalog = enabledCatalog(ImmutableMap.of());

    Table table = sessionCatalog.loadTable(userContext(), TABLE);
    Table other = sessionCatalog.loadTable(userContext(), TABLE);

    assertThat(authSession(other.io()).key()).isNotEqualTo(authSession(table.io()).key());
  }

  @Test
  void catalogsWithSameNameHaveDistinctKeys() throws IOException {
    SessionContext context = userContext();
    Table table = enabledCatalog(ImmutableMap.of()).loadTable(context, TABLE);

    try (RESTSessionCatalog sameName =
        newCatalog(
            new SessionAdapter(backendCatalog, ImmutableMap.of()),
            enabled(ImmutableMap.of()),
            null)) {
      Table other = sameName.loadTable(context, TABLE);

      assertThat(authSession(other.io()).key()).isNotEqualTo(authSession(table.io()).key());
    }
  }

  @Test
  void tableTokenSessionPresentsTableToken() {
    Table table = enabledCatalog(ImmutableMap.of()).loadTable(userContext(), TOKEN_TABLE);

    assertThat(authorization(authSession(table.io()).authenticate(REQUEST)))
        .isEqualTo("Bearer " + TABLE_TOKEN);
  }

  @Test
  void tableTokenSessionHasTableKey() {
    RESTSessionCatalog sessionCatalog = enabledCatalog(ImmutableMap.of());
    SessionContext context = userContext();

    Table table = sessionCatalog.loadTable(context, TABLE);
    Table tokenTable = sessionCatalog.loadTable(context, TOKEN_TABLE);

    assertThat(authSession(tokenTable.io()).key()).isNotEqualTo(authSession(table.io()).key());
  }

  @Test
  void sessionsHaveDistinctTableTokenKeys() {
    RESTSessionCatalog sessionCatalog = enabledCatalog(ImmutableMap.of());

    Table table = sessionCatalog.loadTable(userContext(), TOKEN_TABLE);
    Table other = sessionCatalog.loadTable(userContext(), TOKEN_TABLE);

    assertThat(authSession(other.io()).key()).isNotEqualTo(authSession(table.io()).key());
  }

  @Test
  void evictedSessionIsAcquiredAgain() {
    Table table =
        enabledCatalog(ImmutableMap.of(CatalogProperties.AUTH_SESSION_TIMEOUT_MS, "0"))
            .loadTable(userContext(), TABLE);
    String first = token(authSession(table.io()));

    assertThat(token(authSession(table.io()))).isNotNull().isNotEqualTo(first);
  }

  @Test
  void closedCatalogProvidesNoToken() throws IOException {
    Table table = enabledCatalog(ImmutableMap.of()).loadTable(userContext(), TABLE);
    KeyedTokenAuthSession session = authSession(table.io());

    catalog.close();
    this.catalog = null;

    assertThatThrownBy(() -> session.authenticate(REQUEST))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("No token for auth session: %s", session.key())
        .cause()
        .hasMessage("No token: catalog prod is closed");
  }

  @Test
  void customAuthManagerSessionIsPresented() {
    SessionContext context = userContext();
    Table table =
        enabledCatalog(ImmutableMap.of(AuthProperties.AUTH_TYPE, CustomAuthManager.class.getName()))
            .loadTable(context, TABLE);

    assertThat(authorization(authSession(table.io()).authenticate(REQUEST)))
        .isEqualTo("Bearer custom:" + context.sessionId());
  }

  @Test
  void rederivedSessionWithoutTokenSourceFails() {
    SwitchingAuthManager.SESSION.set(new CustomSession("custom"));
    Table table =
        enabledCatalog(
                ImmutableMap.of(AuthProperties.AUTH_TYPE, SwitchingAuthManager.class.getName()))
            .loadTable(userContext(), TABLE);
    KeyedTokenAuthSession session = authSession(table.io());

    SwitchingAuthManager.SESSION.set(NonBearerAuthManager.SESSION);

    assertThatThrownBy(() -> session.currentToken())
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("No token for auth session: %s", session.key())
        .cause()
        .hasMessage(
            "No token: %s is not a token source",
            NonBearerAuthManager.SESSION.getClass().getName());
  }

  @Test
  void sessionWithoutBearerTokenFails() {
    RESTSessionCatalog sessionCatalog =
        enabledCatalog(
            ImmutableMap.of(AuthProperties.AUTH_TYPE, NonBearerAuthManager.class.getName()));

    assertThatThrownBy(() -> sessionCatalog.loadTable(userContext(), TABLE))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot set auth session for table %s: %s is not a token source",
            TABLE, NonBearerAuthManager.SESSION.getClass().getName());
  }

  @Test
  void catalogSessionIsNotSet() {
    Table table = enabledCatalog(ImmutableMap.of()).loadTable(SessionContext.createEmpty(), TABLE);

    assertThat(authSession(table.io())).isNull();
    assertThat(table.io().properties()).doesNotContainKey(AuthProperties.FILE_IO_SESSION_REQUIRED);
  }

  @Test
  void disabledByDefault() {
    Table table = catalog(ImmutableMap.of()).loadTable(userContext(), TABLE);

    assertThat(authSession(table.io())).isNull();
  }

  @Test
  void serverConfigCannotEnableFileIOSessions() {
    Table table =
        catalog(
                ImmutableMap.of(),
                ImmutableMap.of(AuthProperties.FILE_IO_SESSION_ENABLED, "true"),
                null)
            .loadTable(userContext(), TABLE);

    assertThat(authSession(table.io())).isNull();
  }

  @Test
  void serverConfigCannotClearSessionRequirement() {
    Table table =
        catalog(
                enabled(ImmutableMap.of()),
                ImmutableMap.of(AuthProperties.FILE_IO_SESSION_REQUIRED, "false"),
                null)
            .loadTable(userContext(), TABLE);

    assertThat(table.io().properties())
        .containsEntry(AuthProperties.FILE_IO_SESSION_REQUIRED, "true");
  }

  @Test
  void fileIOWithoutAuthSessionSupportFails() {
    RESTSessionCatalog sessionCatalog =
        enabledCatalog(
            ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName()));

    assertThatThrownBy(() -> sessionCatalog.loadTable(userContext(), TABLE))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot set auth session: %s does not implement SupportsKeyedTokenAuthSession",
            InMemoryFileIO.class.getName());
  }

  @Test
  void sharedFileIOCannotTakeSecondSession() {
    SessionFileIO shared = new SessionFileIO();
    RESTSessionCatalog sessionCatalog =
        catalog(enabled(ImmutableMap.of()), ImmutableMap.of(), (context, properties) -> shared);
    sessionCatalog.loadTable(userContext(), TABLE);

    assertThatThrownBy(() -> sessionCatalog.loadTable(userContext(), TABLE))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot set auth session: %s already has a session", SessionFileIO.class.getName());
  }

  private static FileIO scanFileIO(Table table) throws IOException {
    TableScan scan = table.newScan();
    try (CloseableIterable<?> tasks = scan.planFiles()) {
      assertThat(tasks).isEmpty();
    }

    FileIO scanIO = ((RESTTableScan) scan).fileIO().get();
    assertThat(scanIO).isNotSameAs(table.io());
    return scanIO;
  }

  @Test
  void scanPlanningFileIOCarriesTableSession() throws IOException {
    Table table =
        enabledCatalog(
                ImmutableMap.of(
                    RESTCatalogProperties.SCAN_PLANNING_MODE,
                    RESTCatalogProperties.ScanPlanningMode.SERVER.modeName()))
            .loadTable(userContext(), TABLE);

    FileIO scanIO = scanFileIO(table);

    assertThat(authSession(scanIO).key()).isEqualTo(authSession(table.io()).key());
    assertThat(scanIO.properties()).containsEntry(AuthProperties.FILE_IO_SESSION_REQUIRED, "true");
  }

  @Test
  void fileIOParserKeepsAuthSessionKey() {
    Table table = enabledCatalog(ImmutableMap.of()).loadTable(userContext(), TABLE);
    KeyedTokenAuthSession session = authSession(table.io());

    FileIO parsed = FileIOParser.fromJson(FileIOParser.toJson(table.io()));

    assertThat(authSession(parsed).key()).isEqualTo(session.key());
    assertThat(parsed.properties()).containsEntry(AuthProperties.FILE_IO_SESSION_REQUIRED, "true");
    assertThatThrownBy(() -> authSession(parsed).authenticate(REQUEST))
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("No token resolver for auth session: %s", session.key());
  }

  @Test
  void fileIOParserRequiresAuthSessionSupport() {
    String json =
        "{\"io-impl\":\""
            + InMemoryFileIO.class.getName()
            + "\",\"properties\":{},\"auth-session-key\":\"key\"}";

    assertThatThrownBy(() -> FileIOParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot set auth session: %s does not implement SupportsKeyedTokenAuthSession",
            InMemoryFileIO.class.getName());
  }

  private static String token(KeyedTokenAuthSession session) {
    return session.currentToken().value();
  }
}
