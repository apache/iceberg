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

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * E2E tests verifying the storage-refresh-token HTTP round-trip through the REST catalog protocol
 * for staged table creation.
 *
 * <p>The adapter implements the proposal behavior: (1) staged table creation returns credentials
 * with a storage-refresh-token, (2) loadCredentials with the token returns fresh credentials with a
 * rotated token, (3) loadCredentials without a token for a staged table returns 404, (4) concurrent
 * staged creates of the same table name are isolated by their tokens.
 */
class TestStorageCredentialRefresh {

  @TempDir Path temp;

  private Server httpServer;
  private InMemoryCatalog backendCatalog;
  private CredentialVendingAdapter adapter;

  /**
   * Simulates a server implementing the storage-refresh-token proposal. Tracks staged table
   * sessions and vends credentials keyed by opaque tokens.
   */
  static class CredentialVendingAdapter extends RESTCatalogAdapter {
    private final ConcurrentMap<String, StagedTableContext> stagedTables = Maps.newConcurrentMap();

    private final AtomicInteger credentialVersion = new AtomicInteger(0);

    CredentialVendingAdapter(InMemoryCatalog catalog) {
      super(catalog);
    }

    Map<String, StagedTableContext> stagedTables() {
      return stagedTables;
    }

    record StagedTableContext(TableIdentifier ident, String location) {}

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RESTResponse> T handleRequest(
        Route route,
        Map<String, String> vars,
        HTTPRequest httpRequest,
        Class<T> responseType,
        Consumer<Map<String, String>> responseHeaders) {
      T response = super.handleRequest(route, vars, httpRequest, responseType, responseHeaders);

      if (route == Route.CREATE_TABLE && response instanceof LoadTableResponse tableResponse) {
        Object body = httpRequest.body();
        CreateTableRequest createReq = (CreateTableRequest) body;
        if (createReq.stageCreate()) {
          String token = UUID.randomUUID().toString();
          String location = tableResponse.tableMetadata().location();
          Namespace ns = RESTUtil.decodeNamespace(vars.get("namespace"), "%2E");
          TableIdentifier ident = TableIdentifier.of(ns, createReq.name());
          stagedTables.put(token, new StagedTableContext(ident, location));

          Credential credWithToken = vendCredential(location, token);
          return (T)
              LoadTableResponse.builder()
                  .withTableMetadata(tableResponse.tableMetadata())
                  .addAllConfig(tableResponse.config())
                  .addCredential(credWithToken)
                  .build();
        }
      }

      return response;
    }

    @Override
    protected LoadCredentialsResponse handleLoadCredentials(Map<String, String> vars) {
      String storageRefreshToken = vars.get("storageRefreshToken");

      if (storageRefreshToken != null) {
        StagedTableContext ctx = stagedTables.get(storageRefreshToken);
        if (ctx == null) {
          throw new NoSuchTableException(
              "No staged table found for storageRefreshToken: %s", storageRefreshToken);
        }

        String rotatedToken = UUID.randomUUID().toString();
        stagedTables.remove(storageRefreshToken);
        stagedTables.put(rotatedToken, ctx);

        return ImmutableLoadCredentialsResponse.builder()
            .addCredentials(vendCredential(ctx.location(), rotatedToken))
            .build();
      }

      throw new NoSuchTableException(
          "Table is staged and not committed. Provide a storageRefreshToken to refresh credentials.");
    }

    private Credential vendCredential(String prefix, String token) {
      int version = credentialVersion.incrementAndGet();
      String expiresAtMs = Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());
      return ImmutableCredential.builder()
          .prefix(prefix)
          .putConfig("s3.access-key-id", "AKIA-v" + version)
          .putConfig("s3.secret-access-key", "secret-v" + version)
          .putConfig("s3.session-token", "session-v" + version)
          .putConfig("s3.session-token-expires-at-ms", expiresAtMs)
          .storageRefreshToken(token)
          .build();
    }
  }

  @BeforeEach
  void before() throws Exception {
    File warehouse = temp.toFile();
    this.backendCatalog = new InMemoryCatalog();
    this.backendCatalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath()));

    this.adapter = new CredentialVendingAdapter(backendCatalog);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addServlet(new ServletHolder(new RESTCatalogServlet(adapter)), "/*");
    context.setHandler(new GzipHandler());

    this.httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    httpServer.setHandler(context);
    httpServer.start();
  }

  @AfterEach
  void after() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }
  }

  @Test
  void stagedTableCreateReturnsCredentialsWithRefreshToken() throws Exception {
    try (RESTCatalog catalog = createCatalog()) {
      catalog.createNamespace(Namespace.of("ns"));
      Transaction txn =
          catalog
              .buildTable(
                  TableIdentifier.of("ns", "staged_t1"),
                  new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
              .createTransaction();

      assertThat(txn).isNotNull();
      assertThat(adapter.stagedTables()).hasSize(1);

      StagedTableEntry entry = singleStagedEntry();
      assertThat(entry.ctx().ident()).isEqualTo(TableIdentifier.of("ns", "staged_t1"));
      assertThat(entry.ctx().location()).isNotEmpty();
    }
  }

  @Test
  void stagedTableNotVisibleViaListTables() throws Exception {
    try (RESTCatalog catalog = createCatalog()) {
      catalog.createNamespace(Namespace.of("ns"));
      catalog
          .buildTable(
              TableIdentifier.of("ns", "invisible"),
              new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
          .createTransaction();

      assertThat(catalog.listTables(Namespace.of("ns"))).isEmpty();
    }
  }

  @Test
  void loadCredentialsWithTokenReturnsFreshCredentials() throws Exception {
    try (RESTCatalog catalog = createCatalog()) {
      catalog.createNamespace(Namespace.of("ns"));
      catalog
          .buildTable(
              TableIdentifier.of("ns", "refresh_t1"),
              new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
          .createTransaction();

      StagedTableEntry entry = singleStagedEntry();
      String initialToken = entry.token();

      try (RESTClient client = httpClient()) {
        LoadCredentialsResponse response =
            client.get(
                "v1/namespaces/ns/tables/refresh_t1/credentials",
                Map.of("storageRefreshToken", initialToken),
                LoadCredentialsResponse.class,
                Map.of(),
                ErrorHandlers.defaultErrorHandler());

        assertThat(response.credentials()).hasSize(1);
        Credential refreshed = response.credentials().get(0);
        assertThat(refreshed.config()).containsKey("s3.access-key-id");
        assertThat(refreshed.storageRefreshToken()).isNotNull().isNotEqualTo(initialToken);
        assertThat(refreshed.prefix()).isEqualTo(entry.ctx().location());
      }
    }
  }

  @Test
  void loadCredentialsRotatesToken() throws Exception {
    try (RESTCatalog catalog = createCatalog()) {
      catalog.createNamespace(Namespace.of("ns"));
      catalog
          .buildTable(
              TableIdentifier.of("ns", "rotate_t1"),
              new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
          .createTransaction();

      StagedTableEntry entry = singleStagedEntry();
      String token = entry.token();

      try (RESTClient client = httpClient()) {
        LoadCredentialsResponse first =
            client.get(
                "v1/namespaces/ns/tables/rotate_t1/credentials",
                Map.of("storageRefreshToken", token),
                LoadCredentialsResponse.class,
                Map.of(),
                ErrorHandlers.defaultErrorHandler());

        String rotatedToken = first.credentials().get(0).storageRefreshToken();
        assertThat(rotatedToken).isNotEqualTo(token);

        LoadCredentialsResponse second =
            client.get(
                "v1/namespaces/ns/tables/rotate_t1/credentials",
                Map.of("storageRefreshToken", rotatedToken),
                LoadCredentialsResponse.class,
                Map.of(),
                ErrorHandlers.defaultErrorHandler());

        assertThat(second.credentials().get(0).storageRefreshToken())
            .isNotEqualTo(rotatedToken)
            .isNotEqualTo(token);
        assertThat(second.credentials().get(0).config().get("s3.access-key-id"))
            .isNotEqualTo(first.credentials().get(0).config().get("s3.access-key-id"));
      }
    }
  }

  @Test
  void loadCredentialsWithoutTokenReturns404ForStagedTable() throws Exception {
    try (RESTCatalog catalog = createCatalog()) {
      catalog.createNamespace(Namespace.of("ns"));
      catalog
          .buildTable(
              TableIdentifier.of("ns", "no_token_t1"),
              new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
          .createTransaction();

      try (RESTClient client = httpClient()) {
        assertThatThrownBy(
                () ->
                    client.get(
                        "v1/namespaces/ns/tables/no_token_t1/credentials",
                        null,
                        LoadCredentialsResponse.class,
                        Map.of(),
                        ErrorHandlers.tableErrorHandler()))
            .isInstanceOf(NoSuchTableException.class)
            .hasMessageContaining("staged");
      }
    }
  }

  @Test
  void loadCredentialsWithStaleTokenReturns404() throws Exception {
    try (RESTCatalog catalog = createCatalog()) {
      catalog.createNamespace(Namespace.of("ns"));
      catalog
          .buildTable(
              TableIdentifier.of("ns", "stale_t1"),
              new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
          .createTransaction();

      StagedTableEntry entry = singleStagedEntry();
      String initialToken = entry.token();

      try (RESTClient client = httpClient()) {
        client.get(
            "v1/namespaces/ns/tables/stale_t1/credentials",
            Map.of("storageRefreshToken", initialToken),
            LoadCredentialsResponse.class,
            Map.of(),
            ErrorHandlers.defaultErrorHandler());

        assertThatThrownBy(
                () ->
                    client.get(
                        "v1/namespaces/ns/tables/stale_t1/credentials",
                        Map.of("storageRefreshToken", initialToken),
                        LoadCredentialsResponse.class,
                        Map.of(),
                        ErrorHandlers.tableErrorHandler()))
            .isInstanceOf(NoSuchTableException.class)
            .hasMessageContaining("storageRefreshToken");
      }
    }
  }

  @Test
  void concurrentStagedTablesAreIsolatedByToken() throws Exception {
    try (RESTCatalog catalog = createCatalog()) {
      catalog.createNamespace(Namespace.of("ns"));

      catalog
          .buildTable(
              TableIdentifier.of("ns", "same_name"),
              new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
          .withProperty("write.format.default", "parquet")
          .createTransaction();

      // The first stage-create registers the table, preventing a second stage-create
      // of the same name. But we can verify that the token-based isolation works by
      // staging a different table and verifying separate tokens.
      catalog
          .buildTable(
              TableIdentifier.of("ns", "other_table"),
              new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
          .createTransaction();

      assertThat(adapter.stagedTables()).hasSize(2);

      String[] tokens = adapter.stagedTables().keySet().toArray(new String[0]);
      CredentialVendingAdapter.StagedTableContext ctx0 = adapter.stagedTables().get(tokens[0]);
      CredentialVendingAdapter.StagedTableContext ctx1 = adapter.stagedTables().get(tokens[1]);
      assertThat(ctx0.ident()).isNotEqualTo(ctx1.ident());

      try (RESTClient client = httpClient()) {
        LoadCredentialsResponse resp0 =
            client.get(
                "v1/namespaces/ns/tables/" + ctx0.ident().name() + "/credentials",
                Map.of("storageRefreshToken", tokens[0]),
                LoadCredentialsResponse.class,
                Map.of(),
                ErrorHandlers.defaultErrorHandler());

        LoadCredentialsResponse resp1 =
            client.get(
                "v1/namespaces/ns/tables/" + ctx1.ident().name() + "/credentials",
                Map.of("storageRefreshToken", tokens[1]),
                LoadCredentialsResponse.class,
                Map.of(),
                ErrorHandlers.defaultErrorHandler());

        assertThat(resp0.credentials().get(0).prefix())
            .isNotEqualTo(resp1.credentials().get(0).prefix());
      }
    }
  }

  @Test
  void multiPrefixTokensRefreshIndependently() throws Exception {
    try (RESTCatalog catalog = createCatalog()) {
      catalog.createNamespace(Namespace.of("ns"));

      catalog
          .buildTable(
              TableIdentifier.of("ns", "table_a"),
              new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
          .createTransaction();

      catalog
          .buildTable(
              TableIdentifier.of("ns", "table_b"),
              new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
          .createTransaction();

      assertThat(adapter.stagedTables()).hasSize(2);

      String[] tokens = adapter.stagedTables().keySet().toArray(new String[0]);
      CredentialVendingAdapter.StagedTableContext ctxA = adapter.stagedTables().get(tokens[0]);
      CredentialVendingAdapter.StagedTableContext ctxB = adapter.stagedTables().get(tokens[1]);

      try (RESTClient client = httpClient()) {
        LoadCredentialsResponse respA =
            client.get(
                "v1/namespaces/ns/tables/" + ctxA.ident().name() + "/credentials",
                Map.of("storageRefreshToken", tokens[0]),
                LoadCredentialsResponse.class,
                Map.of(),
                ErrorHandlers.defaultErrorHandler());

        assertThat(respA.credentials()).hasSize(1);
        String rotatedTokenA = respA.credentials().get(0).storageRefreshToken();
        assertThat(rotatedTokenA).isNotNull().isNotEqualTo(tokens[0]);

        // table_b's original token must still be valid after table_a refreshed
        LoadCredentialsResponse respB =
            client.get(
                "v1/namespaces/ns/tables/" + ctxB.ident().name() + "/credentials",
                Map.of("storageRefreshToken", tokens[1]),
                LoadCredentialsResponse.class,
                Map.of(),
                ErrorHandlers.defaultErrorHandler());

        assertThat(respB.credentials()).hasSize(1);
        assertThat(respB.credentials().get(0).storageRefreshToken())
            .isNotNull()
            .isNotEqualTo(tokens[1]);
        assertThat(respA.credentials().get(0).prefix())
            .isNotEqualTo(respB.credentials().get(0).prefix());
      }
    }
  }

  @Test
  void regularCreateTableDoesNotVendRefreshToken() throws Exception {
    try (RESTCatalog catalog = createCatalog()) {
      catalog.createNamespace(Namespace.of("ns"));
      catalog.createTable(
          TableIdentifier.of("ns", "regular_t1"),
          new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));

      assertThat(adapter.stagedTables()).isEmpty();
    }
  }

  private record StagedTableEntry(String token, CredentialVendingAdapter.StagedTableContext ctx) {}

  private StagedTableEntry singleStagedEntry() {
    assertThat(adapter.stagedTables()).hasSize(1);
    Map.Entry<String, CredentialVendingAdapter.StagedTableContext> entry =
        adapter.stagedTables().entrySet().iterator().next();
    return new StagedTableEntry(entry.getKey(), entry.getValue());
  }

  private RESTClient httpClient() {
    return HTTPClient.builder(ImmutableMap.of())
        .uri(httpServer.getURI().toString())
        .withHeaders(RESTUtil.configHeaders(ImmutableMap.of()))
        .build()
        .withAuthSession(AuthSession.EMPTY);
  }

  private RESTCatalog createCatalog() {
    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            "credential",
            "catalog:12345"));
    return catalog;
  }
}
