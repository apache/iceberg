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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
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
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class TestRESTCatalog extends CatalogTests<RESTCatalog> {
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();
  private static final ResourcePaths RESOURCE_PATHS =
      ResourcePaths.forCatalogProperties(Maps.newHashMap());

  @TempDir public Path temp;

  private RESTCatalog restCatalog;
  private InMemoryCatalog backendCatalog;
  private Server httpServer;

  @BeforeEach
  public void createCatalog() throws Exception {
    File warehouse = temp.toFile();
    Configuration conf = new Configuration();

    this.backendCatalog = new InMemoryCatalog();
    this.backendCatalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath()));

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
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            "credential",
            "catalog:12345"));
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

  @Override
  protected boolean requiresNamespaceCreate() {
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

    Assertions.assertThat(restCat.properties().get(CatalogProperties.CACHE_ENABLED))
        .as("Catalog properties after initialize should use the server's override properties")
        .isEqualTo("false");

    Assertions.assertThat(restCat.properties().get(CatalogProperties.CLIENT_POOL_SIZE))
        .as("Catalog after initialize should use the server's default properties if not specified")
        .isEqualTo("1");

    Assertions.assertThat(restCat.properties().get(CatalogProperties.WAREHOUSE_LOCATION))
        .as("Catalog should return final warehouse location")
        .isEqualTo("s3://bucket/warehouse");

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

    Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table"))).isFalse();

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
  public void testCatalogCredentialNoOauth2ServerUri() {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "credential", "catalog:secret"));

    Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table"))).isFalse();

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

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogCredential(String oauth2ServerUri) {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            "catalog:secret",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table"))).isFalse();

    // no token or credential for catalog token exchange
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq(oauth2ServerUri),
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

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogBearerTokenWithClientCredential(String oauth2ServerUri) {
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
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "token",
            "bearer-token",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table"))).isFalse();

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
            eq(oauth2ServerUri),
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

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogCredentialWithClientCredential(String oauth2ServerUri) {
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
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            "catalog:secret",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table"))).isFalse();

    // call client credentials with no initial auth
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq(oauth2ServerUri),
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
            eq(oauth2ServerUri),
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

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogBearerTokenAndCredentialWithClientCredential(String oauth2ServerUri) {
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
            "bearer-token",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table"))).isFalse();

    // use the bearer token for client credentials
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq(oauth2ServerUri),
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
            eq(oauth2ServerUri),
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

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientBearerToken(String oauth2ServerUri) {
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
        ImmutableMap.of("Authorization", "Bearer client-bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientCredential(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "credential", "user:secret",
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=user"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientIDToken(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:id_token", "id-token",
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=id-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientAccessToken(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=access-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientAccessTokenWithOptionalParams(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:access_token", "access-token",
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=access-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of(
            "scope", "custom_scope", "audience", "test_audience", "resource", "test_resource"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientJWTToken(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:jwt", "jwt-token",
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=jwt-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientSAML2Token(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of(
            "urn:ietf:params:oauth:token-type:saml2", "saml2-token",
            "urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=saml2-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testClientSAML1Token(String oauth2ServerUri) {
    testClientAuth(
        "bearer-token",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:saml1", "saml1-token"),
        ImmutableMap.of(
            "Authorization", "Bearer token-exchange-token:sub=saml1-token,act=bearer-token"),
        oauth2ServerUri,
        ImmutableMap.of());
  }

  private void testClientAuth(
      String catalogToken,
      Map<String, String> credentials,
      Map<String, String> expectedHeaders,
      String oauth2ServerUri,
      Map<String, String> optionalOAuthParams) {
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer " + catalogToken);

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(), "user", credentials, ImmutableMap.of());

    RESTCatalog catalog = new RESTCatalog(context, (config) -> adapter);

    ImmutableMap.Builder<String, String> propertyBuilder = ImmutableMap.builder();
    Map<String, String> initializationProperties =
        propertyBuilder
            .put(CatalogProperties.URI, "ignored")
            .put("token", catalogToken)
            .put(OAuth2Properties.OAUTH2_SERVER_URI, oauth2ServerUri)
            .putAll(optionalOAuthParams)
            .build();
    catalog.initialize("prod", initializationProperties);

    Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table"))).isFalse();

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
              eq(oauth2ServerUri),
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
    if (!optionalOAuthParams.isEmpty()) {
      Mockito.verify(adapter)
          .execute(
              eq(HTTPMethod.POST),
              eq(oauth2ServerUri),
              any(),
              Mockito.argThat(
                  body ->
                      ((Map<String, String>) body)
                          .keySet()
                          .containsAll(optionalOAuthParams.keySet())),
              eq(OAuthTokenResponse.class),
              eq(catalogHeaders),
              any());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testTableBearerToken(String oauth2ServerUri) {
    testTableAuth(
        "catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("token", "table-bearer-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of("Authorization", "Bearer table-bearer-token"),
        oauth2ServerUri);
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testTableIDToken(String oauth2ServerUri) {
    testTableAuth(
        "catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "table-id-token"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of(
            "Authorization",
            "Bearer token-exchange-token:sub=table-id-token,act=token-exchange-token:sub=id-token,act=catalog"),
        oauth2ServerUri);
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testTableCredential(String oauth2ServerUri) {
    testTableAuth(
        "catalog",
        ImmutableMap.of("urn:ietf:params:oauth:token-type:id_token", "id-token"),
        ImmutableMap.of("credential", "table-user:secret"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=table-user"),
        oauth2ServerUri);
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

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

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
          TableMetadata refsMetadata =
              TableMetadata.buildFrom(originalResponse.tableMetadata())
                  .suppressHistoricalSnapshots()
                  .build();

          // don't call snapshots() directly as that would cause to load all snapshots. Instead,
          // make sure the snapshots field holds exactly 1 snapshot
          Assertions.assertThat(refsMetadata)
              .extracting("snapshots")
              .asInstanceOf(InstanceOfAssertFactories.list(Snapshot.class))
              .hasSize(1);

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

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

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

    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-c.parquet")
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
          TableMetadata refsMetadata =
              TableMetadata.buildFrom(originalResponse.tableMetadata())
                  .suppressHistoricalSnapshots()
                  .build();

          // don't call snapshots() directly as that would cause to load all snapshots. Instead,
          // make sure the snapshots field holds exactly 2 snapshots (the latest snapshot for main
          // and the branch)
          Assertions.assertThat(refsMetadata)
              .extracting("snapshots")
              .asInstanceOf(InstanceOfAssertFactories.list(Snapshot.class))
              .hasSize(2);

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

  @Test
  public void lazySnapshotLoadingWithDivergedHistory() {
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

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    Table table =
        catalog.createTable(TABLE, SCHEMA, PartitionSpec.unpartitioned(), ImmutableMap.of());

    int numSnapshots = 5;

    for (int i = 0; i < numSnapshots; i++) {
      table
          .newFastAppend()
          .appendFile(
              DataFiles.builder(PartitionSpec.unpartitioned())
                  .withPath(String.format("/path/to/data-%s.parquet", i))
                  .withFileSizeInBytes(10)
                  .withRecordCount(2)
                  .build())
          .commit();
    }

    ResourcePaths paths = ResourcePaths.forCatalogProperties(Maps.newHashMap());

    // Respond with only referenced snapshots
    Answer<?> refsAnswer =
        invocation -> {
          LoadTableResponse originalResponse = (LoadTableResponse) invocation.callRealMethod();
          TableMetadata refsMetadata =
              TableMetadata.buildFrom(originalResponse.tableMetadata())
                  .suppressHistoricalSnapshots()
                  .build();

          // don't call snapshots() directly as that would cause to load all snapshots. Instead,
          // make sure the snapshots field holds exactly 1 snapshot
          Assertions.assertThat(refsMetadata)
              .extracting("snapshots")
              .asInstanceOf(InstanceOfAssertFactories.list(Snapshot.class))
              .hasSize(1);

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
    assertThat(refsTables.snapshots()).hasSize(numSnapshots);
    assertThat(refsTables.history()).hasSize(numSnapshots);
  }

  @SuppressWarnings("MethodLength")
  public void testTableAuth(
      String catalogToken,
      Map<String, String> credentials,
      Map<String, String> tableConfig,
      Map<String, String> expectedContextHeaders,
      Map<String, String> expectedTableHeaders,
      String oauth2ServerUri) {
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
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "token",
            catalogToken,
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Schema expectedSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get(), "unique ID"),
            required(2, "data", Types.StringType.get()));

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    Table table = catalog.createTable(ident, expectedSchema);
    Assertions.assertThat(table.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expectedSchema.asStruct());

    Table loaded = catalog.loadTable(ident); // the first load will send the token
    Assertions.assertThat(loaded.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expectedSchema.asStruct());

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
            eq(oauth2ServerUri),
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
      Mockito.verify(adapter, times(1))
          .execute(
              eq(HTTPMethod.POST),
              eq(oauth2ServerUri),
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

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogTokenRefresh(String oauth2ServerUri) {
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
            eq(oauth2ServerUri),
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
            "credential",
            "catalog:secret",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq(oauth2ServerUri),
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
                      eq(oauth2ServerUri),
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
                      eq(oauth2ServerUri),
                      any(),
                      Mockito.argThat(secondRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(secondRefreshHeaders),
                      any());
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogRefreshedTokenIsUsed(String oauth2ServerUri) {
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
            eq(oauth2ServerUri),
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
            "credential",
            "catalog:secret",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // use the exchanged catalog token
              Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table")))
                  .isFalse();

              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq(oauth2ServerUri),
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
                      eq(oauth2ServerUri),
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

    if (requiresNamespaceCreate()) {
      restCatalog.createNamespace(TABLE.namespace());
    }

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

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogExpiredBearerTokenIsRefreshedWithCredential(String oauth2ServerUri) {
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
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            credential,
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table"))).isFalse();

    // call client credentials with no initial auth
    Mockito.verify(adapter)
        .execute(
            eq(HTTPMethod.POST),
            eq(oauth2ServerUri),
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
            eq(oauth2ServerUri),
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
            eq(oauth2ServerUri),
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
  public void testCatalogWithPagaintionTokenIssue() {
    //TODO remove this test, Used to highlight issue with namespaces
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
                    eq("v1/namespaces"),
                    any(),
                    any(),
                    eq(ListNamespacesResponse.class),
                    eq(catalogHeaders),
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

    Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table"))).isFalse();

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

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogTokenRefreshFailsAndUsesCredentialForRefresh(String oauth2ServerUri) {
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
            eq(oauth2ServerUri),
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
            eq(oauth2ServerUri),
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
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            "credential",
            credential,
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // use the exchanged catalog token
              Assertions.assertThat(catalog.tableExists(TableIdentifier.of("ns", "table")))
                  .isFalse();

              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq(oauth2ServerUri),
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
                      eq(oauth2ServerUri),
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
                      eq(oauth2ServerUri),
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

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogWithCustomTokenScope(String oauth2ServerUri) {
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
            eq(oauth2ServerUri),
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
            scope,
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .execute(
                      eq(HTTPMethod.POST),
                      eq(oauth2ServerUri),
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
                      eq(oauth2ServerUri),
                      any(),
                      Mockito.argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(catalogHeaders),
                      any());
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogTokenRefreshDisabledWithToken(String oauth2ServerUri) {
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
            eq(oauth2ServerUri),
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
            "false",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

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

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogTokenRefreshDisabledWithCredential(String oauth2ServerUri) {
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
            "false",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

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
            eq(oauth2ServerUri),
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

  @Test
  public void diffAgainstSingleTable() {
    Namespace namespace = Namespace.of("namespace");
    TableIdentifier identifier = TableIdentifier.of(namespace, "multipleDiffsAgainstSingleTable");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    Table table = catalog().buildTable(identifier, SCHEMA).create();
    Transaction transaction = table.newTransaction();

    UpdateSchema updateSchema =
        transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdatePartitionSpec updateSpec =
        transaction.updateSpec().addField("shard", Expressions.bucket("id", 16));
    PartitionSpec expectedSpec = updateSpec.apply();
    updateSpec.commit();

    TableCommit tableCommit =
        TableCommit.create(
            identifier,
            ((BaseTransaction) transaction).startMetadata(),
            ((BaseTransaction) transaction).currentMetadata());

    restCatalog.commitTransaction(tableCommit);

    Table loaded = catalog().loadTable(identifier);
    assertThat(loaded.schema().asStruct()).isEqualTo(expectedSchema.asStruct());
    assertThat(loaded.spec().fields()).isEqualTo(expectedSpec.fields());
  }

  @Test
  public void multipleDiffsAgainstMultipleTables() {
    Namespace namespace = Namespace.of("multiDiffNamespace");
    TableIdentifier identifier1 = TableIdentifier.of(namespace, "multiDiffTable1");
    TableIdentifier identifier2 = TableIdentifier.of(namespace, "multiDiffTable2");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    Table table1 = catalog().buildTable(identifier1, SCHEMA).create();
    Table table2 = catalog().buildTable(identifier2, SCHEMA).create();
    Transaction t1Transaction = table1.newTransaction();
    Transaction t2Transaction = table2.newTransaction();

    UpdateSchema updateSchema =
        t1Transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdateSchema updateSchema2 =
        t2Transaction.updateSchema().addColumn("new_col2", Types.LongType.get());
    Schema expectedSchema2 = updateSchema2.apply();
    updateSchema2.commit();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier1,
            ((BaseTransaction) t1Transaction).startMetadata(),
            ((BaseTransaction) t1Transaction).currentMetadata());

    TableCommit tableCommit2 =
        TableCommit.create(
            identifier2,
            ((BaseTransaction) t2Transaction).startMetadata(),
            ((BaseTransaction) t2Transaction).currentMetadata());

    restCatalog.commitTransaction(tableCommit1, tableCommit2);

    assertThat(catalog().loadTable(identifier1).schema().asStruct())
        .isEqualTo(expectedSchema.asStruct());

    assertThat(catalog().loadTable(identifier2).schema().asStruct())
        .isEqualTo(expectedSchema2.asStruct());
  }

  @Test
  public void multipleDiffsAgainstMultipleTablesLastFails() {
    Namespace namespace = Namespace.of("multiDiffNamespace");
    TableIdentifier identifier1 = TableIdentifier.of(namespace, "multiDiffTable1");
    TableIdentifier identifier2 = TableIdentifier.of(namespace, "multiDiffTable2");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(namespace);
    }

    catalog().createTable(identifier1, SCHEMA);
    catalog().createTable(identifier2, SCHEMA);

    Table table1 = catalog().loadTable(identifier1);
    Table table2 = catalog().loadTable(identifier2);
    Schema originalSchemaOne = table1.schema();

    Transaction t1Transaction = catalog().loadTable(identifier1).newTransaction();
    t1Transaction.updateSchema().addColumn("new_col1", Types.LongType.get()).commit();

    Transaction t2Transaction = catalog().loadTable(identifier2).newTransaction();
    t2Transaction.updateSchema().renameColumn("data", "new-column").commit();

    // delete the colum that is being renamed in the above TX to cause a conflict
    table2.updateSchema().deleteColumn("data").commit();
    Schema updatedSchemaTwo = table2.schema();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier1,
            ((BaseTransaction) t1Transaction).startMetadata(),
            ((BaseTransaction) t1Transaction).currentMetadata());

    TableCommit tableCommit2 =
        TableCommit.create(
            identifier2,
            ((BaseTransaction) t2Transaction).startMetadata(),
            ((BaseTransaction) t2Transaction).currentMetadata());

    assertThatThrownBy(() -> restCatalog.commitTransaction(tableCommit1, tableCommit2))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Requirement failed: current schema changed: expected id 0 != 1");

    Schema schema1 = catalog().loadTable(identifier1).schema();
    assertThat(schema1.asStruct()).isEqualTo(originalSchemaOne.asStruct());

    Schema schema2 = catalog().loadTable(identifier2).schema();
    assertThat(schema2.asStruct()).isEqualTo(updatedSchemaTwo.asStruct());
    assertThat(schema2.findField("data")).isNull();
    assertThat(schema2.findField("new-column")).isNull();
    assertThat(schema2.columns()).hasSize(1);
  }

  @Test
  public void testInvalidRestPageSize() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
            new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> catalog.initialize("test", ImmutableMap.of(RESTSessionCatalog.REST_PAGE_SIZE, "-1")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid value for pageSize, must be a positive integer");
  }

  @Test
  public void testPaginationForListNamespaces() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of(RESTSessionCatalog.REST_PAGE_SIZE, "10"));
    int numberOfItems = 100;
    String namespaceName = "newdb";

    // create several namespaces for listing and verify
    for (int i = 0; i < numberOfItems; i++) {
      String nameSpaceName = namespaceName + i;
      catalog.createNamespace(Namespace.of(nameSpaceName));
    }

    List<Namespace> results = catalog.listNamespaces();
    assertThat(results).hasSize(numberOfItems);
  }

  @Test
  public void testPaginationForListTables() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of(RESTSessionCatalog.REST_PAGE_SIZE, "10"));
    int numberOfItems = 100;
    String namespaceName = "newdb";
    String tableName = "newtable";
    catalog.createNamespace(Namespace.of(namespaceName));

    // create several tables under namespace for listing and verify
    for (int i = 0; i < numberOfItems; i++) {
      TableIdentifier tableIdentifier = TableIdentifier.of(namespaceName, tableName + i);
      catalog.createTable(tableIdentifier, SCHEMA);
    }

    List<TableIdentifier> tables = catalog.listTables(Namespace.of(namespaceName));
    assertThat(tables).hasSize(numberOfItems);
  }

  @Test
  public void testCleanupUncommitedFilesForCleanableFailures() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(2)
            .build();

    Table table = catalog.loadTable(TABLE);
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    Mockito.doThrow(new NotAuthorizedException("not authorized"))
        .when(adapter)
        .post(any(), any(), any(), any(Map.class), any());
    assertThatThrownBy(() -> catalog.loadTable(TABLE).newFastAppend().appendFile(file).commit())
        .isInstanceOf(NotAuthorizedException.class);
    verify(adapter, atLeastOnce())
        .post(eq(RESOURCE_PATHS.table(TABLE)), captor.capture(), any(), any(Map.class), any());

    // Extract the UpdateTableRequest to determine the path of the manifest list that should be
    // cleaned up
    UpdateTableRequest request = captor.getValue();
    MetadataUpdate.AddSnapshot addSnapshot = (MetadataUpdate.AddSnapshot) request.updates().get(0);
    Assertions.assertThatThrownBy(
            () -> table.io().newInputFile(addSnapshot.snapshot().manifestListLocation()))
        .isInstanceOf(NotFoundException.class);
  }

  @Test
  public void testNoCleanupForNonCleanableExceptions() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    Table table = catalog.loadTable(TABLE);

    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    Mockito.doThrow(new ServiceFailureException("some service failure"))
        .when(adapter)
        .post(any(), any(), any(), any(Map.class), any());
    assertThatThrownBy(() -> catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit())
        .isInstanceOf(ServiceFailureException.class);
    verify(adapter, atLeastOnce())
        .post(eq(RESOURCE_PATHS.table(TABLE)), captor.capture(), any(), any(Map.class), any());

    // Extract the UpdateTableRequest to determine the path of the manifest list that should still
    // exist even though the commit failed
    UpdateTableRequest request = captor.getValue();
    MetadataUpdate.AddSnapshot addSnapshot = (MetadataUpdate.AddSnapshot) request.updates().get(0);
    Assertions.assertThat(
            table.io().newInputFile(addSnapshot.snapshot().manifestListLocation()).exists())
        .isTrue();
  }

  @Test
  public void testCleanupCleanableExceptionsCreate() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    TableIdentifier newTable = TableIdentifier.of(TABLE.namespace(), "some_table");
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    Mockito.doThrow(new NotAuthorizedException("not authorized"))
        .when(adapter)
        .post(eq(RESOURCE_PATHS.table(newTable)), any(), any(), any(Map.class), any());

    Transaction createTableTransaction = catalog.newCreateTableTransaction(newTable, SCHEMA);
    createTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(createTableTransaction::commitTransaction)
        .isInstanceOf(NotAuthorizedException.class);
    verify(adapter, atLeastOnce())
        .post(eq(RESOURCE_PATHS.table(newTable)), captor.capture(), any(), any(Map.class), any());
    UpdateTableRequest request = captor.getValue();
    Optional<MetadataUpdate> appendSnapshot =
        request.updates().stream()
            .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
            .findFirst();

    assertThat(appendSnapshot).isPresent();
    MetadataUpdate.AddSnapshot addSnapshot = (MetadataUpdate.AddSnapshot) appendSnapshot.get();
    Assertions.assertThatThrownBy(
            () ->
                catalog
                    .loadTable(TABLE)
                    .io()
                    .newInputFile(addSnapshot.snapshot().manifestListLocation()))
        .isInstanceOf(NotFoundException.class);
  }

  @Test
  public void testNoCleanupForNonCleanableCreateTransaction() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    TableIdentifier newTable = TableIdentifier.of(TABLE.namespace(), "some_table");
    Mockito.doThrow(new ServiceFailureException("some service failure"))
        .when(adapter)
        .post(eq(RESOURCE_PATHS.table(newTable)), any(), any(), any(Map.class), any());
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    Transaction createTableTransaction = catalog.newCreateTableTransaction(newTable, SCHEMA);
    createTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(createTableTransaction::commitTransaction)
        .isInstanceOf(ServiceFailureException.class);
    verify(adapter, atLeastOnce())
        .post(eq(RESOURCE_PATHS.table(newTable)), captor.capture(), any(), any(Map.class), any());
    UpdateTableRequest request = captor.getValue();
    Optional<MetadataUpdate> appendSnapshot =
        request.updates().stream()
            .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
            .findFirst();
    assertThat(appendSnapshot).isPresent();

    MetadataUpdate.AddSnapshot addSnapshot = (MetadataUpdate.AddSnapshot) appendSnapshot.get();
    Assertions.assertThat(
            catalog
                .loadTable(TABLE)
                .io()
                .newInputFile(addSnapshot.snapshot().manifestListLocation())
                .exists())
        .isTrue();
  }

  @Test
  public void testCleanupCleanableExceptionsReplace() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    Mockito.doThrow(new NotAuthorizedException("not authorized"))
        .when(adapter)
        .post(eq(RESOURCE_PATHS.table(TABLE)), any(), any(), any(Map.class), any());

    Transaction replaceTableTransaction = catalog.newReplaceTableTransaction(TABLE, SCHEMA, false);
    replaceTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(replaceTableTransaction::commitTransaction)
        .isInstanceOf(NotAuthorizedException.class);
    verify(adapter, atLeastOnce())
        .post(eq(RESOURCE_PATHS.table(TABLE)), captor.capture(), any(), any(Map.class), any());
    UpdateTableRequest request = captor.getValue();
    Optional<MetadataUpdate> appendSnapshot =
        request.updates().stream()
            .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
            .findFirst();

    assertThat(appendSnapshot).isPresent();
    MetadataUpdate.AddSnapshot addSnapshot = (MetadataUpdate.AddSnapshot) appendSnapshot.get();
    Assertions.assertThatThrownBy(
            () ->
                catalog
                    .loadTable(TABLE)
                    .io()
                    .newInputFile(addSnapshot.snapshot().manifestListLocation()))
        .isInstanceOf(NotFoundException.class);
  }

  @Test
  public void testNoCleanupForNonCleanableReplaceTransaction() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    Mockito.doThrow(new ServiceFailureException("some service failure"))
        .when(adapter)
        .post(eq(RESOURCE_PATHS.table(TABLE)), any(), any(), any(Map.class), any());
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    Transaction replaceTableTransaction = catalog.newReplaceTableTransaction(TABLE, SCHEMA, false);
    replaceTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(replaceTableTransaction::commitTransaction)
        .isInstanceOf(ServiceFailureException.class);
    verify(adapter, atLeastOnce())
        .post(eq(RESOURCE_PATHS.table(TABLE)), captor.capture(), any(), any(Map.class), any());
    UpdateTableRequest request = captor.getValue();
    Optional<MetadataUpdate> appendSnapshot =
        request.updates().stream()
            .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
            .findFirst();
    assertThat(appendSnapshot).isPresent();

    MetadataUpdate.AddSnapshot addSnapshot = (MetadataUpdate.AddSnapshot) appendSnapshot.get();
    Assertions.assertThat(
            catalog
                .loadTable(TABLE)
                .io()
                .newInputFile(addSnapshot.snapshot().manifestListLocation())
                .exists())
        .isTrue();
  }

  private RESTCatalog catalog(RESTCatalogAdapter adapter) {
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
    return catalog;
  }
}
