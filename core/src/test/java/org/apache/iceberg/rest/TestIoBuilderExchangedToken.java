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

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.BearerAuthManager;
import org.apache.iceberg.rest.auth.OAuth2Manager;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIoBuilderExchangedToken {

  private static final Namespace NAMESPACE = Namespace.of("ns");
  private static final TableIdentifier TABLE = TableIdentifier.of(NAMESPACE, "t");
  private static final TableIdentifier TOKEN_TABLE = TableIdentifier.of(NAMESPACE, "token_table");
  private static final TableIdentifier SERVER_PLANNED_TABLE =
      TableIdentifier.of(NAMESPACE, "server_planned_table");
  private static final String TABLE_TOKEN = "server-table-token";
  private static final String BEARER_TOKEN = "rest.auth.bearer.token";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
  private static final HTTPRequest REQUEST =
      ImmutableHTTPRequest.builder()
          .baseUri(URI.create("http://localhost:8181"))
          .path("v1/namespaces/ns/tables/t")
          .method(HTTPRequest.HTTPMethod.GET)
          .build();

  @TempDir private Path temp;
  private InMemoryCatalog backendCatalog;
  private RecordingAdapter adapter;
  private RESTSessionCatalog catalog;

  public static class CapturingOAuth2Manager extends OAuth2Manager {
    static final Map<String, CapturingOAuth2Manager> MANAGERS = new ConcurrentHashMap<>();
    private volatile AuthSession catalogSession;

    public CapturingOAuth2Manager(String managerName) {
      super(managerName);
      MANAGERS.put(managerName, this);
    }

    @Override
    public OAuth2Util.AuthSession catalogSession(
        RESTClient sharedClient, Map<String, String> properties) {
      OAuth2Util.AuthSession session = super.catalogSession(sharedClient, properties);
      this.catalogSession = session;
      return session;
    }

    String sessionToken(SessionContext context) {
      OAuth2Util.AuthSession session = contextualSession(context, catalogSession);
      return session == catalogSession ? null : session.token();
    }
  }

  private static class RecordingAdapter extends RESTCatalogAdapter {
    private final AtomicInteger issuedTokens = new AtomicInteger();
    private volatile String loadTableAuthorization = null;

    RecordingAdapter(Catalog catalog) {
      super(catalog);
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
                .withToken(encode(token.token() + "#" + issuedTokens.incrementAndGet()))
                .withTokenType(token.tokenType())
                .withIssuedTokenType(token.issuedTokenType())
                .build());
      } else if (route == Route.LOAD_TABLE && response instanceof LoadTableResponse table) {
        this.loadTableAuthorization = authorization(httpRequest);
        if (TOKEN_TABLE.name().equals(vars.get("table"))) {
          return castResponse(
              responseType,
              LoadTableResponse.builder()
                  .withTableMetadata(table.tableMetadata())
                  .addAllConfig(table.config())
                  .addConfig(OAuth2Properties.TOKEN, TABLE_TOKEN)
                  .build());
        } else if (SERVER_PLANNED_TABLE.name().equals(vars.get("table"))) {
          return castResponse(
              responseType,
              LoadTableResponse.builder()
                  .withTableMetadata(table.tableMetadata())
                  .addAllConfig(table.config())
                  .addConfig(
                      RESTCatalogProperties.SCAN_PLANNING_MODE,
                      RESTCatalogProperties.ScanPlanningMode.SERVER.modeName())
                  .build());
        }
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
    backendCatalog.createTable(TOKEN_TABLE, SCHEMA);
    backendCatalog.createTable(SERVER_PLANNED_TABLE, SCHEMA);
  }

  @AfterEach
  void after() throws IOException {
    CapturingOAuth2Manager.MANAGERS.clear();
    if (catalog != null) {
      catalog.close();
    }

    backendCatalog.close();
  }

  private RESTSessionCatalog catalog(AtomicReference<String> ioBuilderToken) {
    this.adapter = new RecordingAdapter(backendCatalog);
    this.catalog =
        new RESTSessionCatalog(
            config -> adapter,
            (context, properties) -> {
              String sessionToken =
                  CapturingOAuth2Manager.MANAGERS.get("prod").sessionToken(context);
              if (sessionToken == null) {
                return CatalogUtil.loadFileIO(InMemoryFileIO.class.getName(), properties, null);
              }

              ioBuilderToken.set(sessionToken);
              Map<String, String> ioProperties =
                  ImmutableMap.<String, String>builder()
                      .putAll(
                          Maps.filterKeys(
                              properties,
                              propertyName ->
                                  !propertyName.startsWith("rest.auth.")
                                      && !propertyName.equals(OAuth2Properties.TOKEN)
                                      && !propertyName.equals(OAuth2Properties.CREDENTIAL)))
                      .put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_BEARER)
                      .put(BEARER_TOKEN, sessionToken)
                      .buildOrThrow();
              return CatalogUtil.loadFileIO(InMemoryFileIO.class.getName(), ioProperties, null);
            });
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "http://localhost:8181/catalog",
            OAuth2Properties.TOKEN,
            "catalog-token",
            OAuth2Properties.OAUTH2_SERVER_URI,
            "v1/oauth/tokens",
            CatalogProperties.FILE_IO_IMPL,
            InMemoryFileIO.class.getName(),
            AuthProperties.AUTH_TYPE,
            CapturingOAuth2Manager.class.getName()));
    return catalog;
  }

  private static SessionContext subjectTokenContext(String user) {
    return new SessionContext(
        UUID.randomUUID().toString(),
        user,
        ImmutableMap.of(OAuth2Properties.ACCESS_TOKEN_TYPE, "subject-token-" + user),
        ImmutableMap.of());
  }

  private static String encode(String token) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(token.getBytes(StandardCharsets.UTF_8));
  }

  private static String decode(String token) {
    return new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
  }

  private static String authorization(HTTPRequest request) {
    return request.headers().firstEntry("Authorization").map(HTTPHeader::value).orElse(null);
  }

  private String fileIOAuthorization(FileIO fileIO) {
    try (AuthManager fileIOAuthManager =
        AuthManagers.loadAuthManager("file-io", fileIO.properties())) {
      assertThat(fileIOAuthManager).isInstanceOf(BearerAuthManager.class);
      AuthSession session = fileIOAuthManager.catalogSession(adapter, fileIO.properties());
      return authorization(session.authenticate(REQUEST));
    }
  }

  @Test
  void ioBuilderObtainsTheExchangedToken() {
    AtomicReference<String> ioBuilderToken = new AtomicReference<>();
    Table table = catalog(ioBuilderToken).loadTable(subjectTokenContext("alice"), TABLE);

    assertThat(decode(ioBuilderToken.get()))
        .startsWith("token-exchange-token:sub=subject-token-alice");
    assertThat(adapter.loadTableAuthorization).isEqualTo("Bearer " + ioBuilderToken.get());
    assertThat(fileIOAuthorization(table.io())).isEqualTo(adapter.loadTableAuthorization);
  }

  @Test
  void ioBuilderObtainsEachSessionsToken() {
    AtomicReference<String> ioBuilderToken = new AtomicReference<>();
    RESTSessionCatalog sessionCatalog = catalog(ioBuilderToken);

    Table aliceTable = sessionCatalog.loadTable(subjectTokenContext("alice"), TABLE);
    String aliceAuthorization = adapter.loadTableAuthorization;
    Table bobTable = sessionCatalog.loadTable(subjectTokenContext("bob"), TABLE);
    String bobAuthorization = adapter.loadTableAuthorization;

    assertThat(fileIOAuthorization(aliceTable.io())).isEqualTo(aliceAuthorization);
    assertThat(fileIOAuthorization(bobTable.io())).isEqualTo(bobAuthorization);
    assertThat(aliceAuthorization).isNotEqualTo(bobAuthorization);
  }

  @Test
  void ioBuilderDoesNotObtainTheTableSessionToken() {
    AtomicReference<String> ioBuilderToken = new AtomicReference<>();
    Table table = catalog(ioBuilderToken).loadTable(subjectTokenContext("alice"), TOKEN_TABLE);

    assertThat(fileIOAuthorization(table.io())).isNotEqualTo("Bearer " + TABLE_TOKEN);
    assertThat(decode(ioBuilderToken.get()))
        .startsWith("token-exchange-token:sub=subject-token-alice");
  }

  @Test
  void ioBuilderBuildsTheServerPlannedScanFileIO() throws IOException {
    AtomicReference<String> ioBuilderToken = new AtomicReference<>();
    Table table =
        catalog(ioBuilderToken).loadTable(subjectTokenContext("alice"), SERVER_PLANNED_TABLE);

    TableScan scan = table.newScan();
    try (CloseableIterable<?> tasks = scan.planFiles()) {
      assertThat(tasks).isEmpty();
    }

    FileIO scanFileIO = ((RESTTableScan) scan).fileIO().get();
    assertThat(scanFileIO).isNotSameAs(table.io());
    assertThat(scanFileIO.properties())
        .containsKey(RESTCatalogProperties.REST_SCAN_PLAN_ID)
        .doesNotContainKey(OAuth2Properties.TOKEN);
    assertThat(fileIOAuthorization(scanFileIO)).isEqualTo(adapter.loadTableAuthorization);
  }
}
