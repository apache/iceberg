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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.types.Types;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockserver.matchers.Times;

/**
 * Exercises a {@link RESTCatalog} configured with {@link OAuth2Manager} against a REST catalog
 * server that records the bearer tokens it receives, and a mock authorization server.
 */
class TestOAuth2RESTCatalog extends CatalogTests<RESTCatalog> {

  private static final TableIdentifier TABLE = TableIdentifier.of("ns", "t1");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private static final String USER_CLIENT_ID = "user";
  private static final String USER_CLIENT_SECRET = "us3r";
  private static final String USER_ACCESS_TOKEN = "access_user";

  @TempDir static Path tempDir;

  private static MockAuthorizationServer authServer;
  private static InMemoryCatalog backendCatalog;
  private static Server catalogServer;
  private static final List<String> RECEIVED_TOKENS = new CopyOnWriteArrayList<>();

  private RESTCatalog catalog;

  @BeforeAll
  static void startServers() throws Exception {
    authServer = new MockAuthorizationServer();
    backendCatalog = new InMemoryCatalog();
    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.addServlet(
        new ServletHolder(new RESTCatalogServlet(new TokenRecordingAdapter(backendCatalog))), "/*");
    catalogServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    catalogServer.setHandler(servletContext);
    catalogServer.start();
  }

  @AfterAll
  static void stopServers() throws Exception {
    if (catalogServer != null) {
      catalogServer.stop();
      catalogServer.join();
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }

    if (authServer != null) {
      authServer.close();
    }
  }

  @BeforeEach
  void reset() {
    backendCatalog.initialize(
        "in-memory",
        Map.of(
            CatalogProperties.WAREHOUSE_LOCATION,
            tempDir.resolve("warehouse").toAbsolutePath().toString()));
    RECEIVED_TOKENS.clear();
    authServer.reset();
    authServer.expectClientCredentials();
    authServer.expectClientCredentials(
        USER_CLIENT_ID,
        USER_CLIENT_SECRET,
        MockAuthorizationServer.SCOPE,
        ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
        USER_ACCESS_TOKEN,
        MockAuthorizationServer.ACCESS_TOKEN_LIFESPAN,
        Times.unlimited());
  }

  @AfterEach
  void closeCatalog() throws Exception {
    if (catalog != null) {
      catalog.close();
    }

    backendCatalog.close(); // clears in-memory data
  }

  @Override
  protected RESTCatalog catalog() {
    if (catalog == null) {
      SessionContext context = SessionContext.createEmpty();
      catalog = newCatalog(context, "oauth2", authServer.clientProperties(), Map.of());
    }

    return catalog;
  }

  @Override
  protected RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    backendCatalog.initialize(
        "in-memory",
        ImmutableMap.<String, String>builder()
            .put(
                CatalogProperties.WAREHOUSE_LOCATION,
                tempDir.resolve("warehouse").toAbsolutePath().toString())
            .putAll(additionalProperties)
            .build());

    return newCatalog(
        SessionContext.createEmpty(),
        catalogName,
        authServer.clientProperties(),
        additionalProperties);
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean supportsVariant() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    return false;
  }

  @Test
  void catalogSession() {
    SessionContext context = SessionContext.createEmpty();
    catalog = newCatalog(context, "oauth2", authServer.clientProperties(), Map.of());
    createAndLoadTable();
    assertThat(RECEIVED_TOKENS)
        .isNotEmpty()
        .containsOnly("Bearer " + MockAuthorizationServer.ACCESS_TOKEN);
    // one token for the init session, one for the catalog session
    authServer.verifyTokenRequests(2);
  }

  @Test
  void catalogSessionWithLegacyProperties() {
    SessionContext context = SessionContext.createEmpty();
    catalog =
        newCatalog(
            context,
            "oauth2",
            Map.of(
                OAuth2Properties.CREDENTIAL,
                MockAuthorizationServer.CLIENT_ID + ":" + MockAuthorizationServer.CLIENT_SECRET,
                OAuth2Properties.OAUTH2_SERVER_URI,
                authServer.tokenEndpoint().toString(),
                OAuth2Properties.SCOPE,
                MockAuthorizationServer.SCOPE),
            Map.of());
    createAndLoadTable();
    assertThat(RECEIVED_TOKENS)
        .isNotEmpty()
        .containsOnly("Bearer " + MockAuthorizationServer.ACCESS_TOKEN);
  }

  @Test
  void contextualSession() {
    SessionContext context =
        new SessionContext(
            "session1",
            "user",
            Map.of(OAuth2Properties.CREDENTIAL, USER_CLIENT_ID + ":" + USER_CLIENT_SECRET),
            Map.of(OAuth2Properties.OAUTH2_SERVER_URI, authServer.tokenEndpoint().toString()));
    catalog = newCatalog(context, "oauth2", authServer.clientProperties(), Map.of());
    createAndLoadTable();
    // the config endpoint is fetched with the catalog session, everything else with the
    // contextual session
    assertThat(RECEIVED_TOKENS.get(0)).isEqualTo("Bearer " + MockAuthorizationServer.ACCESS_TOKEN);
    assertThat(RECEIVED_TOKENS.subList(1, RECEIVED_TOKENS.size()))
        .isNotEmpty()
        .containsOnly("Bearer " + USER_ACCESS_TOKEN);
    authServer.verifyTokenRequests(3);
  }

  /**
   * The inherited test assumes filesystem-backed metadata and uses Files.move(...). This is not
   * compatible with InMemoryFileIO, which is used in this test. Instead, the reimplemented test
   * deletes the metadata through table.io().deleteFile(...).
   */
  @Override
  @Test
  @SuppressWarnings("resource")
  public void testLoadTableWithMissingMetadataFile(@TempDir Path ignored) {
    if (requiresNamespaceCreate()) {
      catalog().createNamespace(TBL.namespace());
    }

    catalog().buildTable(TBL, SCHEMA).create();
    assertThat(catalog().tableExists(TBL)).isTrue();

    Table table = catalog().loadTable(TBL);
    String metadataFileLocation =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    table.io().deleteFile(metadataFileLocation);

    assertThatThrownBy(() -> catalog().loadTable(TBL))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("No in-memory file found for location: " + metadataFileLocation);
  }

  private void createAndLoadTable() {
    catalog.createNamespace(Namespace.of("ns"));
    catalog.createTable(TABLE, SCHEMA);
    Table table = catalog.loadTable(TABLE);
    assertThat(table.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
  }

  private static RESTCatalog newCatalog(
      SessionContext context,
      String catalogName,
      Map<String, String> oauth2Props,
      Map<String, String> additionalProperties) {
    RESTCatalog restCatalog =
        new RESTCatalog(
            context,
            config ->
                HTTPClient.builder(config)
                    .uri(config.get(CatalogProperties.URI))
                    .withHeaders(RESTUtil.configHeaders(config))
                    .build());
    restCatalog.initialize(
        catalogName,
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, catalogServer.getURI().toString())
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO")
            .put(CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1", "catalog-default-key1")
            .put(CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2", "catalog-default-key2")
            .put(CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3", "catalog-default-key3")
            .put(CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3", "catalog-override-key3")
            .put(CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4", "catalog-override-key4")
            .put(AuthProperties.AUTH_TYPE, OAuth2Manager.class.getName())
            .putAll(oauth2Props)
            .putAll(additionalProperties)
            .buildOrThrow());
    return restCatalog;
  }

  /** Records the Authorization header of every request. */
  private static class TokenRecordingAdapter extends RESTCatalogAdapter {

    TokenRecordingAdapter(InMemoryCatalog catalog) {
      super(catalog);
    }

    @Override
    protected <T extends RESTResponse> T execute(
        HTTPRequest request,
        Class<T> responseType,
        Consumer<ErrorResponse> errorHandler,
        Consumer<Map<String, String>> responseHeaders) {
      request
          .headers()
          .firstEntry("Authorization")
          .map(HTTPHeader::value)
          .ifPresent(RECEIVED_TOKENS::add);
      return super.execute(request, responseType, errorHandler, responseHeaders);
    }
  }
}
