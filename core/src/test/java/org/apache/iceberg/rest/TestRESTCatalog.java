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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalogAdapter.HTTPMethod;
import org.apache.iceberg.rest.RESTSessionCatalog.SnapshotMode;
import org.apache.iceberg.rest.auth.AuthSessionUtil;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.types.Types;
import org.awaitility.Awaitility;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class TestRESTCatalog extends CatalogTests<RESTCatalog> {
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();

  @TempDir public Path temp;

  private RESTCatalog restCatalog;
  private JdbcCatalog backendCatalog;
  private Server httpServer;

  @BeforeEach
  public void createCatalog() throws Exception {
    File warehouse = temp.toFile();
    Configuration conf = new Configuration();

    this.backendCatalog = new JdbcCatalog();
    backendCatalog.setConf(conf);
    Map<String, String> backendCatalogProperties =
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouse.getAbsolutePath(),
            CatalogProperties.URI,
            "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", ""),
            JdbcCatalog.PROPERTY_PREFIX + "username",
            "user",
            JdbcCatalog.PROPERTY_PREFIX + "password",
            "password");
    backendCatalog.initialize("backend", backendCatalogProperties);

    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");
    Map<String, String> contextHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user");

    RESTCatalogAdapter adaptor =
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          public <T extends RESTResponse> T execute(
              RESTCatalogAdapter.HTTPMethod method,
              String path,
              Map<String, String> queryParams,
              Object body,
              Class<T> responseType,
              Map<String, String> headers,
              Consumer<ErrorResponse> errorHandler) {
            // this doesn't use a Mockito spy because this is used for catalog tests, which have
            // different method calls
            if (!"v1/oauth/tokens".equals(path)) {
              if ("v1/config".equals(path)) {
                assertThat(headers).containsAllEntriesOf(catalogHeaders);
              } else {
                assertThat(headers).containsAllEntriesOf(contextHeaders);
              }
            }
            Object request = roundTripSerialize(body, "request");
            T response =
                super.execute(
                    method, path, queryParams, request, responseType, headers, errorHandler);
            T responseAfterSerialization = roundTripSerialize(response, "response");
            return responseAfterSerialization;
          }
        };

    RESTCatalogServlet servlet = new RESTCatalogServlet(adaptor);
    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.setContextPath("/");
    ServletHolder servletHolder = new ServletHolder(servlet);
    servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
    servletContext.addServlet(servletHolder, "/*");
    servletContext.setVirtualHosts(null);
    servletContext.setGzipHandler(new GzipHandler());

    this.httpServer = new Server(0);
    httpServer.setHandler(servletContext);
    httpServer.start();

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", "user:12345"),
            ImmutableMap.of());

    this.restCatalog =
        new RESTCatalog(
            context,
            (config) -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());
    restCatalog.setConf(conf);
    restCatalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI, httpServer.getURI().toString(), "credential", "catalog:12345"));
  }

  @SuppressWarnings("unchecked")
  public static <T> T roundTripSerialize(T payload, String description) {
    if (payload != null) {
      try {
        if (payload instanceof RESTMessage) {
          return (T) MAPPER.readValue(MAPPER.writeValueAsString(payload), payload.getClass());
        } else {
          // use Map so that Jackson doesn't try to instantiate ImmutableMap from payload.getClass()
          return (T) MAPPER.readValue(MAPPER.writeValueAsString(payload), Map.class);
        }
      } catch (JsonProcessingException e) {
        throw new RuntimeException(
            String.format("Failed to serialize and deserialize %s: %s", description, payload), e);
      }
    }
    return null;
  }

  @AfterEach
  public void closeCatalog() throws Exception {
    if (restCatalog != null) {
      restCatalog.close();
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }

    if (httpServer != null) {
      httpServer.stop();
      httpServer.join();
    }
  }

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  /* RESTCatalog specific tests */

  @Test
  public void testConfigRoute() throws IOException {
    RESTClient testClient =
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          public <T extends RESTResponse> T get(
              String path,
              Map<String, String> queryParams,
              Class<T> responseType,
              Map<String, String> headers,
              Consumer<ErrorResponse> errorHandler) {
            if ("v1/config".equals(path)) {
              return castResponse(
                  responseType,
                  ConfigResponse.builder()
                      .withDefaults(ImmutableMap.of(CatalogProperties.CLIENT_POOL_SIZE, "1"))
                      .withOverrides(
                          ImmutableMap.of(
                              CatalogProperties.CACHE_ENABLED,
                              "false",
                              CatalogProperties.WAREHOUSE_LOCATION,
                              queryParams.get(CatalogProperties.WAREHOUSE_LOCATION) + "warehouse"))
                      .build());
            }
            return super.get(path, queryParams, responseType, headers, errorHandler);
          }
        };

    RESTCatalog restCat = new RESTCatalog((config) -> testClient);
    Map<String, String> initialConfig =
        ImmutableMap.of(
            CatalogProperties.URI, "http://localhost:8080",
            CatalogProperties.CACHE_ENABLED, "true",
            CatalogProperties.WAREHOUSE_LOCATION, "s3://bucket/");

    restCat.setConf(new Configuration());
    restCat.initialize("prod", initialConfig);

    Assert.assertEquals(
        "Catalog properties after initialize should use the server's override properties",
        "false",
        restCat.properties().get(CatalogProperties.CACHE_ENABLED));

    Assert.assertEquals(
        "Catalog after initialize should use the server's default properties if not specified",
        "1",
        restCat.properties().get(CatalogProperties.CLIENT_POOL_SIZE));

    Assert.assertEquals(
        "Catalog should return final warehouse location",
        "s3://bucket/warehouse",
        restCat.properties().get(CatalogProperties.WAREHOUSE_LOCATION));

    restCat.close();
  }

  @Test
  public void testInitializeWithBadArguments() throws IOException {
    RESTCatalog restCat = new RESTCatalog();
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> restCat.initialize("prod", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid configuration: null");

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> restCat.initialize("prod", ImmutableMap.of()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid uri for http client: null");

    restCat.close();
  }

  @Test
  public void testCatalogBasicBearerToken() {
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer bearer-token");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", "bearer-token"));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // the bearer token should be used for all interactions
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(catalogHeaders),
            any());
  }

  @Test
  public void testCatalogCredential() {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", "catalog:secret"));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // no token or credential for catalog token exchange
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            eq(emptyHeaders),
            any());
    // no token or credential for config
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());
    // use the catalog token for all interactions
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(catalogHeaders),
            any());
  }

  @Test
  public void testCatalogBearerTokenWithClientCredential() {
    Map<String, String> contextHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user");
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer bearer-token");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", "user:secret"),
            ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", "bearer-token"));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // use the bearer token for config
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());
    // use the bearer token to fetch the context token
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            eq(catalogHeaders),
            any());
    // use the context token for table load
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(contextHeaders),
            any());
  }

  @Test
  public void testCatalogCredentialWithClientCredential() {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> contextHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user");
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", "user:secret"),
            ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", "catalog:secret"));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // call client credentials with no initial auth
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            eq(emptyHeaders),
            any());
    // use the client credential token for config
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());
    // use the client credential to fetch the context token
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            eq(catalogHeaders),
            any());
    // use the context token for table load
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(contextHeaders),
            any());
  }

  @Test
  public void testCatalogBearerTokenAndCredentialWithClientCredential() {
    Map<String, String> contextHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user");
    Map<String, String> initHeaders = ImmutableMap.of("Authorization", "Bearer bearer-token");
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", "user:secret"),
            ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            "catalog:secret",
            "token",
            "bearer-token"));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // use the bearer token for client credentials
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            eq(initHeaders),
            any());
    // use the client credential token for config
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());
    // use the client credential to fetch the context token
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            eq(catalogHeaders),
            any());
    // use the context token for table load
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(contextHeaders),
            any());
  }

  @Test
  public void testClientBearerToken() {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "token", "client-bearer-token",
            "credential", "user:secret",
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer client-bearer-token"));
  }

  @Test
  public void testClientCredential() {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "credential", "user:secret",
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user"));
  }

  @Test
  public void testClientIDToken() {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=id-token,act=bearer-token"));
  }

  @Test
  public void testClientAccessToken() {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=access-token,act=bearer-token"));
  }

  @Test
  public void testClientJWTToken() {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=jwt-token,act=bearer-token"));
  }

  @Test
  public void testClientSAML2Token() {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=saml2-token,act=bearer-token"));
  }

  @Test
  public void testClientSAML1Token() {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=saml1-token,act=bearer-token"));
  }

  private void testClientAuth(
      String catalogToken, Map<String, String> credentials, Map<String, String> expectedHeaders) {
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer " + catalogToken);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", credentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", catalogToken));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());

    // token passes a static token. otherwise, validate a client credentials or token exchange
    // request
    if (!credentials.containsKey("token")) {
      Mockito.verify(adapter)
          .execute(
              eq(HTTPMethod.POST),
              eq("v1/oauth/tokens"),
              any(),
              any(),
              eq(OAuthTokenResponse.class),
              eq(catalogHeaders),
              any());
    }

    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(expectedHeaders),
            any());
  }

  @Test
  public void testTableBearerToken() {
    testTableAuth(
        "catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("token", "table-bearer-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of("Authorization", "Bearer table-bearer-token"));
  }

  @Test
  public void testTableIDToken() {
    testTableAuth(
        "catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "table-id-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of(
            "Authorization",
            "Bearer token-exchange-token:sub=table-id-token,act=token-exchange-token:sub=id-token,act=catalog"));
  }

  @Test
  public void testTableCredential() {
    testTableAuth(
        "catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("credential", "table-user:secret"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=table-user"));
  }

  @Test
  public void testSnapshotParams() {
    assertThat(SnapshotMode.ALL.params()).isEqualTo(ImmutableMap.of("snapshots", "all"));

    assertThat(SnapshotMode.REFS.params()).isEqualTo(ImmutableMap.of("snapshots", "refs"));
  }

  @Test
  public void testTableSnapshotLoading() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            // default loading to refs only
            "snapshot-loading-mode",
            "refs"));

    // Create a table with multiple snapshots
    Table table = catalog.createTable(TABLE, SCHEMA);
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-a.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .commit();

    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-b.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .commit();

    ResourcePaths paths = ResourcePaths.forCatalogProperties(Maps.newHashMap());

    // Respond with only referenced snapshots
    Answer<?> refsAnswer =
        invocation -> {
          LoadTableResponse originalResponse = (LoadTableResponse) invocation.callRealMethod();
          TableMetadata fullTableMetadata = originalResponse.tableMetadata();

          Set<Long> referencedSnapshotIds =
              fullTableMetadata.refs().values().stream()
                  .map(SnapshotRef::snapshotId)
                  .collect(Collectors.toSet());

          TableMetadata refsMetadata =
              fullTableMetadata.removeSnapshotsIf(
                  s -> !referencedSnapshotIds.contains(s.snapshotId()));

          return LoadTableResponse.builder()
              .withTableMetadata(refsMetadata)
              .addAllConfig(originalResponse.config())
              .build();
        };

    Mockito.doAnswer(refsAnswer)
        .when(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq(paths.table(TABLE)),
            eq(ImmutableMap.of("snapshots", "refs")),
            any(),
            eq(LoadTableResponse.class),
            any(),
            any());

    Table refsTables = catalog.loadTable(TABLE);

    assertThat(refsTables.currentSnapshot()).isEqualTo(table.currentSnapshot());
    // verify that the table was loaded with the refs argument
    verify(adapter, times(1))
        .execute(
            eq(HTTPMethod.GET),
            eq(paths.table(TABLE)),
            eq(ImmutableMap.of("snapshots", "refs")),
            any(),
            eq(LoadTableResponse.class),
            any(),
            any());

    // verify that all snapshots are loaded when referenced
    assertThat(refsTables.snapshots()).containsExactlyInAnyOrderElementsOf(table.snapshots());
    verify(adapter, times(1))
        .execute(
            eq(HTTPMethod.GET),
            eq(paths.table(TABLE)),
            eq(ImmutableMap.of("snapshots", "all")),
            any(),
            eq(LoadTableResponse.class),
            any(),
            any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "2"})
  public void testTableSnapshotLoadingWithDivergedBranches(String formatVersion) {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            "snapshot-loading-mode",
            "refs"));

    Table table =
        catalog.createTable(
            TABLE,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", formatVersion));

    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-a.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .commit();

    String branch = "divergedBranch";
    table.manageSnapshots().createBranch(branch, table.currentSnapshot().snapshotId()).commit();

    // branch and main are diverged now
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-b.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .toBranch(branch)
        .commit();

    ResourcePaths paths = ResourcePaths.forCatalogProperties(Maps.newHashMap());

    // Respond with only referenced snapshots
    Answer<?> refsAnswer =
        invocation -> {
          LoadTableResponse originalResponse = (LoadTableResponse) invocation.callRealMethod();
          TableMetadata fullTableMetadata = originalResponse.tableMetadata();

          Set<Long> referencedSnapshotIds =
              fullTableMetadata.refs().values().stream()
                  .map(SnapshotRef::snapshotId)
                  .collect(Collectors.toSet());

          TableMetadata refsMetadata =
              fullTableMetadata.removeSnapshotsIf(
                  s -> !referencedSnapshotIds.contains(s.snapshotId()));

          return LoadTableResponse.builder()
              .withTableMetadata(refsMetadata)
              .addAllConfig(originalResponse.config())
              .build();
        };

    Mockito.doAnswer(refsAnswer)
        .when(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq(paths.table(TABLE)),
            eq(ImmutableMap.of("snapshots", "refs")),
            any(),
            eq(LoadTableResponse.class),
            any(),
            any());

    Table refsTables = catalog.loadTable(TABLE);
    assertThat(refsTables.currentSnapshot()).isEqualTo(table.currentSnapshot());

    // verify that the table was loaded with the refs argument
    verify(adapter, times(1))
        .execute(
            eq(HTTPMethod.GET),
            eq(paths.table(TABLE)),
            eq(ImmutableMap.of("snapshots", "refs")),
            any(),
            eq(LoadTableResponse.class),
            any(),
            any());

    // verify that all snapshots are loaded when referenced
    assertThat(catalog.loadTable(TABLE).snapshots())
        .containsExactlyInAnyOrderElementsOf(table.snapshots());
    verify(adapter, times(1))
        .execute(
            eq(HTTPMethod.GET),
            eq(paths.table(TABLE)),
            eq(ImmutableMap.of("snapshots", "all")),
            any(),
            eq(LoadTableResponse.class),
            any(),
            any());

    // verify that committing to branch is possible
    catalog
        .loadTable(TABLE)
        .newAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-c.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .toBranch(branch)
        .commit();

    assertThat(catalog.loadTable(TABLE).snapshots())
        .hasSizeGreaterThan(Lists.newArrayList(table.snapshots()).size());
  }

  public void testTableAuth(
      String catalogToken,
      Map<String, String> credentials,
      Map<String, String> tableConfig,
      Map<String, String> expectedContextHeaders,
      Map<String, String> expectedTableHeaders) {
    TableIdentifier ident = TableIdentifier.of("ns", "table");
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer " + catalogToken);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    // inject the expected table config
    Answer<LoadTableResponse> addTableConfig =
        invocation -> {
          LoadTableResponse loadTable = (LoadTableResponse) invocation.callRealMethod();
          return LoadTableResponse.builder()
              .withTableMetadata(loadTable.tableMetadata())
              .addAllConfig(loadTable.config())
              .addAllConfig(tableConfig)
              .build();
        };

    Mockito.doAnswer(addTableConfig)
        .when(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/namespaces/ns/tables"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(expectedContextHeaders),
            any());

    Mockito.doAnswer(addTableConfig)
        .when(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(expectedContextHeaders),
            any());

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", credentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", catalogToken));

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get(), "unique ID"),
            required(2, "data", Types.StringType.get()));

    Table table = catalog.createTable(ident, expectedSchema);
    Assertions.assertEquals(
        expectedSchema.asStruct(), table.schema().asStruct(), "Schema should match");

    Table loaded = catalog.loadTable(ident); // the first load will send the token
    Assertions.assertEquals(
        expectedSchema.asStruct(), loaded.schema().asStruct(), "Schema should match");

    loaded.refresh(); // refresh to force reload

    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());
    // session client credentials flow
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            eq(catalogHeaders),
            any());

    // create table request
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/namespaces/ns/tables"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(expectedContextHeaders),
            any());

    // if the table returned a bearer token, there will be no token request
    if (!tableConfig.containsKey("token")) {
      // client credentials or token exchange to get a table token
      Mockito.verify(adapter, times(2))
          .execute(
              eq(HTTPMethod.POST),
              eq("v1/oauth/tokens"),
              any(),
              any(),
              eq(OAuthTokenResponse.class),
              eq(expectedContextHeaders),
              any());
    }

    // automatic refresh when metadata is accessed after commit
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(expectedTableHeaders),
            any());

    // load table from catalog
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(expectedContextHeaders),
            any());

    // refresh loaded table
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(expectedTableHeaders),
            any());
  }

  @Test
  public void testCatalogTokenRefresh() {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration =
        invocation -> {
          OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
          return OAuthTokenResponse.builder()
              .withToken(response.token())
              .withTokenType(response.tokenType())
              .withIssuedTokenType(response.issuedTokenType())
              .addScopes(response.scopes())
              .setExpirationInSeconds(1)
              .build();
        };

    Mockito.doAnswer(addOneSecondExpiration)
        .when(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", "catalog:secret"));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq("v1/oauth/tokens"),
                      any(),
                      any(),
                      eq(OAuthTokenResponse.class),
                      eq(emptyHeaders),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.GET),
                      eq("v1/config"),
                      any(),
                      any(),
                      eq(ConfigResponse.class),
                      eq(catalogHeaders),
                      any());

              // verify the first token exchange
              Map<String, String> firstRefreshRequest =
                  ImmutableMap.of(
                      "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                      "subject_token", "client-credentials-token:sub=catalog",
                      "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                      "scope", "catalog");
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq("v1/oauth/tokens"),
                      any(),
                      Mockito.argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(catalogHeaders),
                      any());

              // verify that a second exchange occurs
              Map<String, String> secondRefreshRequest =
                  ImmutableMap.of(
                      "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                      "subject_token",
                          "token-exchange-token:sub=client-credentials-token:sub=catalog",
                      "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                      "scope", "catalog");
              Map<String, String> secondRefreshHeaders =
                  ImmutableMap.of(
                      "Authorization",
                      "Bearer token-exchange-token:sub=client-credentials-token:sub=catalog");
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq("v1/oauth/tokens"),
                      any(),
                      Mockito.argThat(secondRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(secondRefreshHeaders),
                      any());
            });
  }

  @Test
  public void testCatalogRefreshedTokenIsUsed() {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration =
        invocation -> {
          OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
          return OAuthTokenResponse.builder()
              .withToken(response.token())
              .withTokenType(response.tokenType())
              .withIssuedTokenType(response.issuedTokenType())
              .addScopes(response.scopes())
              .setExpirationInSeconds(1)
              .build();
        };

    Mockito.doAnswer(addOneSecondExpiration)
        .when(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", "catalog:secret"));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // use the exchanged catalog token
              Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq("v1/oauth/tokens"),
                      any(),
                      any(),
                      eq(OAuthTokenResponse.class),
                      eq(emptyHeaders),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.GET),
                      eq("v1/config"),
                      any(),
                      any(),
                      eq(ConfigResponse.class),
                      eq(catalogHeaders),
                      any());

              // verify the first token exchange
              Map<String, String> firstRefreshRequest =
                  ImmutableMap.of(
                      "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                      "subject_token", "client-credentials-token:sub=catalog",
                      "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                      "scope", "catalog");
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq("v1/oauth/tokens"),
                      any(),
                      Mockito.argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(catalogHeaders),
                      any());

              // use the refreshed context token for table load
              Map<String, String> refreshedCatalogHeader =
                  ImmutableMap.of(
                      "Authorization",
                      "Bearer token-exchange-token:sub=client-credentials-token:sub=catalog");
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.GET),
                      eq("v1/namespaces/ns/tables/table"),
                      any(),
                      any(),
                      eq(LoadTableResponse.class),
                      eq(refreshedCatalogHeader),
                      any());
            });
  }

  @Test
  public void testCatalogWithCustomMetricsReporter() throws IOException {
    this.restCatalog =
        new RESTCatalog(
            new SessionCatalog.SessionContext(
                UUID.randomUUID().toString(),
                "user",
                ImmutableMap.of("credential", "user:12345"),
                ImmutableMap.of()),
            (config) -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());
    restCatalog.setConf(new Configuration());
    restCatalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            "credential",
            "catalog:12345",
            CatalogProperties.METRICS_REPORTER_IMPL,
            CustomMetricsReporter.class.getName()));

    restCatalog.buildTable(TABLE, SCHEMA).create();
    Table table = restCatalog.loadTable(TABLE);
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-a.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(2)
                .build())
        .commit();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      assertThat(tasks.iterator()).hasNext();
    }

    // counter of custom metrics reporter should have been increased
    // 1x for commit metrics / 1x for scan metrics
    assertThat(CustomMetricsReporter.COUNTER.get()).isEqualTo(2);
  }

  public static class CustomMetricsReporter implements MetricsReporter {
    static final AtomicInteger COUNTER = new AtomicInteger(0);

    @Override
    public void report(MetricsReport report) {
      COUNTER.incrementAndGet();
    }
  }

  @Test
  public void testCatalogExpiredBearerTokenRefreshWithoutCredential() {
    // expires at epoch second = 1
    String token =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjF9.gQADTbdEv-rpDWKSkGLbmafyB5UUjTdm9B_1izpuZ6E";

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Map<String, String> contextCredentials = ImmutableMap.of("token", token);
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);

    catalog.initialize("prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", token));
  }

  @Test
  public void testCatalogExpiredBearerTokenIsRefreshedWithCredential() {
    // expires at epoch second = 1
    String token =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjF9.gQADTbdEv-rpDWKSkGLbmafyB5UUjTdm9B_1izpuZ6E";
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    String credential = "catalog:12345";
    Map<String, String> contextCredentials = ImmutableMap.of("token", token);
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    // the init token at the catalog level is a valid token
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", credential));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    // call client credentials with no initial auth
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            eq(emptyHeaders),
            any());

    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());

    Map<String, String> firstRefreshRequest =
        ImmutableMap.of(
            "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token", token,
            "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
            "scope", "catalog");
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            Mockito.argThat(firstRefreshRequest::equals),
            eq(OAuthTokenResponse.class),
            eq(OAuth2Util.basicAuthHeaders(credential)),
            any());

    // verify that a second exchange occurs
    Map<String, String> secondRefreshRequest =
        ImmutableMap.of(
            "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token", token,
            "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
            "scope", "catalog");
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            Mockito.argThat(secondRefreshRequest::equals),
            eq(OAuthTokenResponse.class),
            eq(OAuth2Util.basicAuthHeaders(credential)),
            any());

    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=" + token)),
            any());
  }

  @Test
  public void testCatalogValidBearerTokenIsNotRefreshed() {
    // expires at epoch second = 19999999999
    String token =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjE5OTk5OTk5OTk5fQ._3k92KJi2NTyTG6V1s2mzJ__GiQtL36DnzsZSkBdYPw";
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer " + token);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    String credential = "catalog:12345";
    Map<String, String> contextCredentials =
        ImmutableMap.of("token", token, "credential", credential);
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize("prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", token));

    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());

    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/namespaces/ns/tables/table"),
            any(),
            any(),
            eq(LoadTableResponse.class),
            eq(OAuth2Util.authHeaders(token)),
            any());
  }

  @Test
  public void testCatalogTokenRefreshFailsAndUsesCredentialForRefresh() {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    String credential = "catalog:secret";
    Map<String, String> basicHeaders = OAuth2Util.basicAuthHeaders(credential);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration =
        invocation -> {
          OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
          return OAuthTokenResponse.builder()
              .withToken(response.token())
              .withTokenType(response.tokenType())
              .withIssuedTokenType(response.issuedTokenType())
              .addScopes(response.scopes())
              .setExpirationInSeconds(1)
              .build();
        };

    Mockito.doAnswer(addOneSecondExpiration)
        .when(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    Map<String, String> firstRefreshRequest =
        ImmutableMap.of(
            "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token", "client-credentials-token:sub=catalog",
            "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
            "scope", "catalog");

    // simulate that the token expired when it was about to be refreshed
    Mockito.doThrow(new RuntimeException("token expired"))
        .when(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            Mockito.argThat(firstRefreshRequest::equals),
            eq(OAuthTokenResponse.class),
            eq(catalogHeaders),
            any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    // lower retries
    AuthSessionUtil.setTokenRefreshNumRetries(1);

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", credential));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // use the exchanged catalog token
              Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("ns", "table")));

              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq("v1/oauth/tokens"),
                      any(),
                      any(),
                      eq(OAuthTokenResponse.class),
                      eq(emptyHeaders),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.GET),
                      eq("v1/config"),
                      any(),
                      any(),
                      eq(ConfigResponse.class),
                      eq(catalogHeaders),
                      any());

              // verify the first token exchange - since an exception is thrown, we're performing
              // retries
              Mockito.verify(adapter, times(2))
                  .execute(
                      eq(HTTPMethod.POST),
                      eq("v1/oauth/tokens"),
                      any(),
                      Mockito.argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(catalogHeaders),
                      any());

              // here we make sure that the basic auth header is used after token refresh retries
              // failed
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq("v1/oauth/tokens"),
                      any(),
                      Mockito.argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(basicHeaders),
                      any());

              // use the refreshed context token for table load
              Map<String, String> refreshedCatalogHeader =
                  ImmutableMap.of(
                      "Authorization",
                      "Bearer token-exchange-token:sub=client-credentials-token:sub=catalog");
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.GET),
                      eq("v1/namespaces/ns/tables/table"),
                      any(),
                      any(),
                      eq(LoadTableResponse.class),
                      eq(refreshedCatalogHeader),
                      any());
            });
  }

  @Test
  public void testCatalogWithCustomTokenScope() {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration =
        invocation -> {
          OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
          return OAuthTokenResponse.builder()
              .withToken(response.token())
              .withTokenType(response.tokenType())
              .withIssuedTokenType(response.issuedTokenType())
              .addScopes(response.scopes())
              .setExpirationInSeconds(1)
              .build();
        };

    Mockito.doAnswer(addOneSecondExpiration)
        .when(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    String scope = "custom_catalog_scope";
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            "catalog:secret",
            OAuth2Properties.SCOPE,
            scope));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq("v1/oauth/tokens"),
                      any(),
                      any(),
                      eq(OAuthTokenResponse.class),
                      eq(emptyHeaders),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.GET),
                      eq("v1/config"),
                      any(),
                      any(),
                      eq(ConfigResponse.class),
                      eq(catalogHeaders),
                      any());

              // verify the token exchange uses the right scope
              Map<String, String> firstRefreshRequest =
                  ImmutableMap.of(
                      "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                      "subject_token", "client-credentials-token:sub=catalog",
                      "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                      "scope", scope);
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq("v1/oauth/tokens"),
                      any(),
                      Mockito.argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(catalogHeaders),
                      any());
            });
  }

  public void testCatalogTokenRefreshDisabledWithToken() {
    String token = "some-token";
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer " + token);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    Answer<OAuthTokenResponse> addOneSecondExpiration =
        invocation -> {
          OAuthTokenResponse response = (OAuthTokenResponse) invocation.callRealMethod();
          return OAuthTokenResponse.builder()
              .withToken(response.token())
              .withTokenType(response.tokenType())
              .withIssuedTokenType(response.issuedTokenType())
              .addScopes(response.scopes())
              .setExpirationInSeconds(1)
              .build();
        };

    Mockito.doAnswer(addOneSecondExpiration)
        .when(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            any(),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    Map<String, String> contextCredentials = ImmutableMap.of();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", contextCredentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            OAuth2Properties.TOKEN,
            token,
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            "false"));

    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());
  }

  @Test
  public void testCatalogTokenRefreshDisabledWithCredential() {
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", ImmutableMap.of(), ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            OAuth2Properties.CREDENTIAL,
            "catalog:12345",
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            "false"));

    // fetch token from client credential
    Map<String, String> fetchTokenFromCredential =
        ImmutableMap.of(
            "grant_type",
            "client_credentials",
            "client_id",
            "catalog",
            "client_secret",
            "12345",
            "scope",
            "catalog");
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq("v1/oauth/tokens"),
            any(),
            Mockito.argThat(fetchTokenFromCredential::equals),
            eq(OAuthTokenResponse.class),
            eq(ImmutableMap.of()),
            any());

    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.GET),
            eq("v1/config"),
            any(),
            any(),
            eq(ConfigResponse.class),
            eq(catalogHeaders),
            any());
  }
}
