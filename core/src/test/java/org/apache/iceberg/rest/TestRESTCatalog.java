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

import static org.apache.iceberg.rest.RequestMatcher.containsHeaders;
import static org.apache.iceberg.rest.RequestMatcher.matches;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.RESTCatalogProperties.SnapshotMode;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.AuthSessionUtil;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
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
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class TestRESTCatalog extends CatalogTests<RESTCatalog> {
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();
  private static final ResourcePaths RESOURCE_PATHS =
      ResourcePaths.forCatalogProperties(
          ImmutableMap.of(
              RESTCatalogProperties.NAMESPACE_SEPARATOR,
              RESTCatalogAdapter.NAMESPACE_SEPARATOR_URLENCODED_UTF_8));

  private static final class IdempotentEnv {
    private final TableIdentifier ident;
    private final RESTClient http;
    private final Map<String, String> headers;

    private IdempotentEnv(TableIdentifier ident, RESTClient http, Map<String, String> headers) {
      this.ident = ident;
      this.http = http;
      this.headers = headers;
    }
  }

  /**
   * Test-only adapter that keeps request/response round-trip serialization and header validation
   * from the base test setup, while also allowing specific tests to inject transient failures.
   */
  private static class HeaderValidatingAdapter extends RESTCatalogAdapter {
    private final HTTPHeaders catalogHeaders;
    private final HTTPHeaders contextHeaders;
    private final java.util.concurrent.ConcurrentMap<String, RuntimeException>
        simulateFailureOnFirstSuccessByKey = new java.util.concurrent.ConcurrentHashMap<>();

    HeaderValidatingAdapter(
        Catalog catalog, HTTPHeaders catalogHeaders, HTTPHeaders contextHeaders) {
      super(catalog);
      this.catalogHeaders = catalogHeaders;
      this.contextHeaders = contextHeaders;
    }

    /**
     * Test helper to simulate a transient failure after the first successful mutation for a key.
     *
     * <p>Useful to validate that idempotency correctly replays a finalized result when the client
     * retries after a post-success transient failure.
     */
    public void simulateFailureOnFirstSuccessForKey(String key, RuntimeException failure) {
      Preconditions.checkArgument(key != null, "Invalid idempotency key: null");
      Preconditions.checkArgument(failure != null, "Invalid failure: null");
      simulateFailureOnFirstSuccessByKey.put(key, failure);
    }

    /** Test helper to simulate a transient 503 after the first successful mutation for a key. */
    public void simulate503OnFirstSuccessForKey(String key) {
      simulateFailureOnFirstSuccessForKey(
          key,
          new CommitStateUnknownException(
              new RuntimeException("simulated transient 503 after success")));
    }

    @Override
    public <T extends RESTResponse> T execute(
        HTTPRequest request,
        Class<T> responseType,
        Consumer<ErrorResponse> errorHandler,
        Consumer<Map<String, String>> responseHeaders) {
      if (!ResourcePaths.tokens().equals(request.path())) {
        if (ResourcePaths.config().equals(request.path())) {
          assertThat(request.headers().entries()).containsAll(catalogHeaders.entries());
        } else {
          assertThat(request.headers().entries()).containsAll(contextHeaders.entries());
        }
      }

      Object body = roundTripSerialize(request.body(), "request");
      HTTPRequest req = ImmutableHTTPRequest.builder().from(request).body(body).build();
      T response = super.execute(req, responseType, errorHandler, responseHeaders);
      return roundTripSerialize(response, "response");
    }

    @Override
    protected <T extends RESTResponse> T execute(
        HTTPRequest request,
        Class<T> responseType,
        Consumer<ErrorResponse> errorHandler,
        Consumer<Map<String, String>> responseHeaders,
        ParserContext parserContext) {
      ErrorResponse.Builder errorBuilder = ErrorResponse.builder();
      Pair<Route, Map<String, String>> routeAndVars = Route.from(request.method(), request.path());
      if (routeAndVars != null) {
        try {
          ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
          vars.putAll(request.queryParameters());
          vars.putAll(routeAndVars.second());

          T resp =
              handleRequest(
                  routeAndVars.first(), vars.build(), request, responseType, responseHeaders);

          // For tests: simulate a transient 503 after the first successful mutation for a key.
          Optional<HTTPHeaders.HTTPHeader> keyHeader =
              request.headers().firstEntry(RESTUtil.IDEMPOTENCY_KEY_HEADER);
          boolean isMutation =
              request.method() == HTTPMethod.POST || request.method() == HTTPMethod.DELETE;
          if (isMutation && keyHeader.isPresent()) {
            String key = keyHeader.get().value();
            RuntimeException failure = simulateFailureOnFirstSuccessByKey.remove(key);
            if (failure != null) {
              throw failure;
            }
          }

          return resp;
        } catch (RuntimeException e) {
          configureResponseFromException(e, errorBuilder);
        }

      } else {
        errorBuilder
            .responseCode(400)
            .withType("BadRequestException")
            .withMessage(
                String.format("No route for request: %s %s", request.method(), request.path()));
      }

      ErrorResponse error = errorBuilder.build();
      errorHandler.accept(error);

      // if the error handler doesn't throw an exception, throw a generic one
      throw new RESTException("Unhandled error: %s", error);
    }
  }

  @TempDir public Path temp;

  private RESTCatalog restCatalog;
  private InMemoryCatalog backendCatalog;
  private Server httpServer;
  private HeaderValidatingAdapter adapterForRESTServer;

  @BeforeEach
  public void createCatalog() throws Exception {
    File warehouse = temp.toFile();

    this.backendCatalog = new InMemoryCatalog();
    this.backendCatalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath()));

    HTTPHeaders catalogHeaders =
        HTTPHeaders.of(
            Map.of(
                "Authorization",
                "Bearer client-credentials-token:sub=catalog",
                "test-header",
                "test-value"));
    HTTPHeaders contextHeaders =
        HTTPHeaders.of(
            Map.of(
                "Authorization",
                "Bearer client-credentials-token:sub=user",
                "test-header",
                "test-value"));

    adapterForRESTServer =
        Mockito.spy(new HeaderValidatingAdapter(backendCatalog, catalogHeaders, contextHeaders));

    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.addServlet(
        new ServletHolder(new RESTCatalogServlet(adapterForRESTServer)), "/*");
    servletContext.setHandler(new GzipHandler());

    this.httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    httpServer.setHandler(servletContext);
    httpServer.start();

    this.restCatalog = initCatalog("prod", ImmutableMap.of());
  }

  @Override
  protected RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    Configuration conf = new Configuration();

    RESTCatalog catalog =
        new RESTCatalog(
            new SessionCatalog.SessionContext(
                UUID.randomUUID().toString(),
                "user",
                ImmutableMap.of("credential", "user:12345"),
                ImmutableMap.of()),
            (config) ->
                HTTPClient.builder(config)
                    .uri(config.get(CatalogProperties.URI))
                    .withHeaders(RESTUtil.configHeaders(config))
                    .build());
    catalog.setConf(conf);
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1",
            "catalog-default-key1",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2",
            "catalog-default-key2",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3",
            "catalog-default-key3",
            CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3",
            "catalog-override-key3",
            CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4",
            "catalog-override-key4",
            "credential",
            "catalog:12345",
            "header.test-header",
            "test-value");
    catalog.initialize(
        catalogName,
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .putAll(additionalProperties)
            .build());
    return catalog;
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
          public <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            if (ResourcePaths.config().equals(request.path())) {
              return castResponse(
                  responseType,
                  ConfigResponse.builder()
                      .withDefaults(ImmutableMap.of(CatalogProperties.CLIENT_POOL_SIZE, "1"))
                      .withOverrides(
                          ImmutableMap.of(
                              CatalogProperties.CACHE_ENABLED,
                              "false",
                              CatalogProperties.WAREHOUSE_LOCATION,
                              request.queryParameters().get(CatalogProperties.WAREHOUSE_LOCATION)
                                  + "warehouse"))
                      .build());
            }
            return super.execute(request, responseType, errorHandler, responseHeaders);
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

    assertThat(restCat.properties().get(CatalogProperties.CACHE_ENABLED))
        .as("Catalog properties after initialize should use the server's override properties")
        .isEqualTo("false");

    assertThat(restCat.properties().get(CatalogProperties.CLIENT_POOL_SIZE))
        .as("Catalog after initialize should use the server's default properties if not specified")
        .isEqualTo("1");

    assertThat(restCat.properties().get(CatalogProperties.WAREHOUSE_LOCATION))
        .as("Catalog should return final warehouse location")
        .isEqualTo("s3://bucket/warehouse");

    restCat.close();
  }

  @Test
  public void testInitializeWithBadArguments() throws IOException {
    RESTCatalog restCat = new RESTCatalog();
    assertThatThrownBy(() -> restCat.initialize("prod", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid configuration: null");

    assertThatThrownBy(() -> restCat.initialize("prod", ImmutableMap.of()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid uri for http client: null");

    restCat.close();
  }

  @Test
  public void testDefaultHeadersPropagated() {
    RESTCatalog catalog = new RESTCatalog();
    Map<String, String> properties =
        Map.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            OAuth2Properties.CREDENTIAL,
            "catalog:secret",
            "header.test-header",
            "test-value",
            "header.test-header2",
            "test-value2");
    catalog.initialize("test", properties);
    assertThat(catalog)
        .extracting("sessionCatalog.client.baseHeaders")
        .asInstanceOf(map(String.class, String.class))
        .containsEntry("test-header2", "test-value2");
  }

  @Test
  public void testCatalogBasicBearerToken() {
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer bearer-token");

    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "prod", ImmutableMap.of(CatalogProperties.URI, "ignored", "token", "bearer-token"));

    assertThat(catalog.tableExists(TBL)).isFalse();

    // the bearer token should be used for all interactions
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), catalogHeaders),
            any(),
            any(),
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

    assertThat(catalog.tableExists(TBL)).isFalse();

    // no token or credential for catalog token exchange
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.POST, ResourcePaths.tokens(), emptyHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // no token or credential for config
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // use the catalog token for all interactions
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), catalogHeaders),
            any(),
            any(),
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

    assertThat(catalog.tableExists(TBL)).isFalse();

    // no token or credential for catalog token exchange
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.POST, oauth2ServerUri, emptyHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // no token or credential for config
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // use the catalog token for all interactions
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), catalogHeaders),
            any(),
            any(),
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

    assertThat(catalog.tableExists(TBL)).isFalse();

    // use the bearer token for config
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // use the bearer token to fetch the context token
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.POST, oauth2ServerUri, catalogHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // use the context token for table existence check
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), contextHeaders),
            any(),
            any(),
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

    assertThat(catalog.tableExists(TBL)).isFalse();

    // call client credentials with no initial auth
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.POST, oauth2ServerUri, emptyHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // use the client credential token for config
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // use the client credential to fetch the context token
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.POST, oauth2ServerUri, catalogHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // use the context token for table existence check
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), contextHeaders),
            any(),
            any(),
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

    assertThat(catalog.tableExists(TBL)).isFalse();

    // use the bearer token for client credentials
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.POST, oauth2ServerUri, initHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // use the client credential token for config
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // use the client credential to fetch the context token
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.POST, oauth2ServerUri, catalogHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());
    // use the context token for table existence check
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), contextHeaders),
            any(),
            any(),
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

    assertThat(catalog.tableExists(TBL)).isFalse();

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());

    // token passes a static token. otherwise, validate a client credentials or token exchange
    // request
    if (!credentials.containsKey("token")) {
      Mockito.verify(adapter)
          .execute(
              matches(HTTPMethod.POST, oauth2ServerUri, catalogHeaders),
              eq(OAuthTokenResponse.class),
              any(),
              any());
    }
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), expectedHeaders),
            any(),
            any(),
            any());
    if (!optionalOAuthParams.isEmpty()) {
      Mockito.verify(adapter)
          .postForm(
              eq(oauth2ServerUri),
              Mockito.argThat(body -> body.keySet().containsAll(optionalOAuthParams.keySet())),
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
        ImmutableMap.of("credential", "table-user:secret"), // will be ignored
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        ImmutableMap.of("Authorization", "Bearer token-exchange-token:sub=id-token,act=catalog"),
        oauth2ServerUri);
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
            RESTCatalogProperties.SNAPSHOT_LOADING_MODE,
            SnapshotMode.REFS.name()));

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

    Table refsTable = catalog.loadTable(TABLE);

    // don't call snapshots() directly as that would cause to load all snapshots. Instead,
    // make sure the snapshots field holds exactly 1 snapshot
    assertThat(((BaseTable) refsTable).operations().current())
        .extracting("snapshots")
        .asInstanceOf(InstanceOfAssertFactories.list(Snapshot.class))
        .hasSize(1);

    assertThat(refsTable.currentSnapshot()).isEqualTo(table.currentSnapshot());

    // verify that the table was loaded with the refs argument
    verify(adapter, times(1))
        .execute(
            matches(
                HTTPMethod.GET, RESOURCE_PATHS.table(TABLE), Map.of(), Map.of("snapshots", "refs")),
            eq(LoadTableResponse.class),
            any(),
            any());

    // verify that all snapshots are loaded when referenced
    assertThat(refsTable.snapshots()).containsExactlyInAnyOrderElementsOf(table.snapshots());
    verify(adapter, times(1))
        .execute(
            matches(
                HTTPMethod.GET, RESOURCE_PATHS.table(TABLE), Map.of(), Map.of("snapshots", "all")),
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
            RESTCatalogProperties.SNAPSHOT_LOADING_MODE,
            SnapshotMode.REFS.name()));

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

    Table refsTable = catalog.loadTable(TABLE);

    // don't call snapshots() directly as that would cause to load all snapshots. Instead,
    // make sure the snapshots field holds exactly 2 snapshots
    assertThat(((BaseTable) refsTable).operations().current())
        .extracting("snapshots")
        .asInstanceOf(InstanceOfAssertFactories.list(Snapshot.class))
        .hasSize(2);

    assertThat(refsTable.currentSnapshot()).isEqualTo(table.currentSnapshot());

    // verify that the table was loaded with the refs argument
    verify(adapter, times(1))
        .execute(
            matches(
                HTTPMethod.GET, RESOURCE_PATHS.table(TABLE), Map.of(), Map.of("snapshots", "refs")),
            eq(LoadTableResponse.class),
            any(),
            any());

    // verify that all snapshots are loaded when referenced
    assertThat(catalog.loadTable(TABLE).snapshots())
        .containsExactlyInAnyOrderElementsOf(table.snapshots());
    verify(adapter, times(1))
        .execute(
            matches(
                HTTPMethod.GET, RESOURCE_PATHS.table(TABLE), Map.of(), Map.of("snapshots", "all")),
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
            RESTCatalogProperties.SNAPSHOT_LOADING_MODE,
            SnapshotMode.REFS.name()));

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

    Table refsTable = catalog.loadTable(TABLE);

    // don't call snapshots() directly as that would cause to load all snapshots. Instead,
    // make sure the snapshots field holds exactly 1 snapshot
    assertThat(((BaseTable) refsTable).operations().current())
        .extracting("snapshots")
        .asInstanceOf(InstanceOfAssertFactories.list(Snapshot.class))
        .hasSize(1);

    assertThat(refsTable.currentSnapshot()).isEqualTo(table.currentSnapshot());
    assertThat(refsTable.snapshots()).hasSize(numSnapshots);
    assertThat(refsTable.history()).hasSize(numSnapshots);
  }

  @SuppressWarnings("MethodLength")
  public void testTableAuth(
      String catalogToken,
      Map<String, String> credentials,
      Map<String, String> tableConfig,
      Map<String, String> expectedContextHeaders,
      Map<String, String> expectedTableHeaders,
      String oauth2ServerUri) {
    Map<String, String> catalogHeaders = ImmutableMap.of("Authorization", "Bearer " + catalogToken);
    Namespace namespace = Namespace.of("ns");

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
            matches(HTTPMethod.POST, RESOURCE_PATHS.tables(namespace), expectedContextHeaders),
            eq(LoadTableResponse.class),
            any(),
            any());

    Mockito.doAnswer(addTableConfig)
        .when(adapter)
        .execute(
            matches(HTTPMethod.GET, RESOURCE_PATHS.table(TBL), expectedContextHeaders),
            eq(LoadTableResponse.class),
            any(),
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
      catalog.createNamespace(TBL.namespace());
    }

    Table table = catalog.createTable(TBL, expectedSchema);
    assertThat(table.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expectedSchema.asStruct());

    Table loaded = catalog.loadTable(TBL); // the first load will send the token
    assertThat(loaded.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expectedSchema.asStruct());

    loaded.refresh(); // refresh to force reload

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());
    // session client credentials flow
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.POST, oauth2ServerUri, catalogHeaders),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    // create table request
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.POST, RESOURCE_PATHS.tables(namespace), expectedContextHeaders),
            eq(LoadTableResponse.class),
            any(),
            any());

    // if the table returned a bearer token or a credential, there will be no token request
    if (!tableConfig.containsKey("token") && !tableConfig.containsKey("credential")) {
      // token exchange to get a table token
      Mockito.verify(adapter, times(1))
          .execute(
              matches(HTTPMethod.POST, oauth2ServerUri, expectedContextHeaders),
              eq(OAuthTokenResponse.class),
              any(),
              any());
    }

    if (expectedContextHeaders.equals(expectedTableHeaders)) {
      // load table from catalog + refresh loaded table
      Mockito.verify(adapter, times(2))
          .execute(
              matches(HTTPMethod.GET, RESOURCE_PATHS.table(TBL), expectedTableHeaders),
              eq(LoadTableResponse.class),
              any(),
              any());
    } else {
      // load table from catalog
      Mockito.verify(adapter)
          .execute(
              matches(HTTPMethod.GET, RESOURCE_PATHS.table(TBL), expectedContextHeaders),
              eq(LoadTableResponse.class),
              any(),
              any());

      // refresh loaded table
      Mockito.verify(adapter)
          .execute(
              matches(HTTPMethod.GET, RESOURCE_PATHS.table(TBL), expectedTableHeaders),
              eq(LoadTableResponse.class),
              any(),
              any());
    }
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
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

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
                      matches(HTTPMethod.POST, oauth2ServerUri, emptyHeaders),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
                      eq(ConfigResponse.class),
                      any(),
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
                      matches(
                          HTTPMethod.POST,
                          oauth2ServerUri,
                          catalogHeaders,
                          Map.of(),
                          firstRefreshRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
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
                      matches(
                          HTTPMethod.POST,
                          oauth2ServerUri,
                          secondRefreshHeaders,
                          Map.of(),
                          secondRefreshRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
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
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

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
              assertThat(catalog.tableExists(TBL)).isFalse();

              // call client credentials with no initial auth
              Map<String, String> clientCredentialsRequest =
                  ImmutableMap.of(
                      "grant_type", "client_credentials",
                      "client_id", "catalog",
                      "client_secret", "secret",
                      "scope", "catalog");
              Mockito.verify(adapter)
                  .execute(
                      matches(
                          HTTPMethod.POST,
                          oauth2ServerUri,
                          emptyHeaders,
                          Map.of(),
                          clientCredentialsRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
                      eq(ConfigResponse.class),
                      any(),
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
                      matches(
                          HTTPMethod.POST,
                          oauth2ServerUri,
                          catalogHeaders,
                          Map.of(),
                          firstRefreshRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());

              // use the refreshed context token for table existence check
              Map<String, String> refreshedCatalogHeader =
                  ImmutableMap.of(
                      "Authorization",
                      "Bearer token-exchange-token:sub=client-credentials-token:sub=catalog");
              Mockito.verify(adapter)
                  .execute(
                      matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), refreshedCatalogHeader),
                      any(),
                      any(),
                      any());
            });
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogTokenRefreshExchangeDisabled(String oauth2ServerUri) {
    Map<String, String> emptyHeaders = ImmutableMap.of();
    Map<String, String> catalogHeaders =
        ImmutableMap.of("Authorization", "Bearer client-credentials-token:sub=catalog");

    Map<String, String> refreshRequest =
        ImmutableMap.of(
            "grant_type", "client_credentials",
            "client_id", "catalog",
            "client_secret", "secret",
            "scope", "catalog");

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
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

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
            OAuth2Properties.CREDENTIAL,
            "catalog:secret",
            OAuth2Properties.TOKEN_EXCHANGE_ENABLED,
            "false",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // call client credentials with no initial auth
              Mockito.verify(adapter)
                  .execute(
                      matches(HTTPMethod.POST, oauth2ServerUri, emptyHeaders),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      matches(HTTPMethod.GET, "v1/config", catalogHeaders),
                      eq(ConfigResponse.class),
                      any(),
                      any());

              // verify the new token request is issued
              Mockito.verify(adapter)
                  .execute(
                      matches(
                          HTTPMethod.POST, oauth2ServerUri, emptyHeaders, Map.of(), refreshRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());
            });
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

    assertThat(catalog.tableExists(TBL)).isFalse();

    // call client credentials with no initial auth
    Map<String, String> clientCredentialsRequest =
        ImmutableMap.of(
            "grant_type", "client_credentials",
            "client_id", "catalog",
            "client_secret", "12345",
            "scope", "catalog");
    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.POST, oauth2ServerUri, emptyHeaders, Map.of(), clientCredentialsRequest),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());

    Map<String, String> firstRefreshRequest =
        ImmutableMap.of(
            "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token", token,
            "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
            "scope", "catalog");
    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.POST,
                oauth2ServerUri,
                OAuth2Util.basicAuthHeaders(credential),
                Map.of(),
                firstRefreshRequest),
            eq(OAuthTokenResponse.class),
            any(),
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
            matches(
                HTTPMethod.POST,
                oauth2ServerUri,
                OAuth2Util.basicAuthHeaders(credential),
                Map.of(),
                secondRefreshRequest),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.HEAD,
                RESOURCE_PATHS.table(TBL),
                Map.of("Authorization", "Bearer token-exchange-token:sub=" + token)),
            any(),
            any(),
            any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/oauth/tokens", "https://auth-server.com/token"})
  public void testCatalogExpiredTokenCredentialRefreshWithExchangeDisabled(String oauth2ServerUri) {
    // expires at epoch second = 1
    String token =
        ".eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyLCJleHAiOjF9.";
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
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            "ignored",
            OAuth2Properties.CREDENTIAL,
            credential,
            OAuth2Properties.TOKEN_EXCHANGE_ENABLED,
            "false",
            OAuth2Properties.OAUTH2_SERVER_URI,
            oauth2ServerUri));

    // call client credentials with no initial auth
    Map<String, String> clientCredentialsRequest =
        ImmutableMap.of(
            "grant_type", "client_credentials",
            "client_id", "catalog",
            "client_secret", "12345",
            "scope", "catalog");

    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.POST, oauth2ServerUri, emptyHeaders, Map.of(), clientCredentialsRequest),
            eq(OAuthTokenResponse.class),
            any(),
            any());

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, "v1/config", catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());

    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.POST, oauth2ServerUri, emptyHeaders, Map.of(), clientCredentialsRequest),
            eq(OAuthTokenResponse.class),
            any(),
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

    assertThat(catalog.tableExists(TBL)).isFalse();

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
            any());

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), OAuth2Util.authHeaders(token)),
            any(),
            any(),
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
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

    Map<String, String> firstRefreshRequest =
        ImmutableMap.of(
            "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token", "client-credentials-token:sub=catalog",
            "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
            "scope", "catalog");

    // simulate that the token expired when it was about to be refreshed
    Mockito.doThrow(new RuntimeException("token expired"))
        .when(adapter)
        .postForm(
            eq(oauth2ServerUri),
            argThat(firstRefreshRequest::equals),
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
              assertThat(catalog.tableExists(TBL)).isFalse();

              // call client credentials with no initial auth
              Map<String, String> clientCredentialsRequest =
                  ImmutableMap.of(
                      "grant_type", "client_credentials",
                      "client_id", "catalog",
                      "client_secret", "secret",
                      "scope", "catalog");
              Mockito.verify(adapter)
                  .execute(
                      matches(
                          HTTPMethod.POST,
                          oauth2ServerUri,
                          emptyHeaders,
                          Map.of(),
                          clientCredentialsRequest),
                      eq(OAuthTokenResponse.class),
                      any(),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
                      eq(ConfigResponse.class),
                      any(),
                      any());

              // verify the first token exchange - since an exception is thrown, we're performing
              // retries
              Mockito.verify(adapter, times(2))
                  .postForm(
                      eq(oauth2ServerUri),
                      argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(catalogHeaders),
                      any());

              // here we make sure that the basic auth header is used after token refresh retries
              // failed
              Mockito.verify(adapter)
                  .postForm(
                      eq(oauth2ServerUri),
                      argThat(firstRefreshRequest::equals),
                      eq(OAuthTokenResponse.class),
                      eq(basicHeaders),
                      any());

              // use the refreshed context token for table existence check
              Map<String, String> refreshedCatalogHeader =
                  ImmutableMap.of(
                      "Authorization",
                      "Bearer token-exchange-token:sub=client-credentials-token:sub=catalog");
              Mockito.verify(adapter)
                  .execute(
                      matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TBL), refreshedCatalogHeader),
                      any(),
                      any(),
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
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

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
                  .postForm(
                      eq(oauth2ServerUri),
                      anyMap(),
                      eq(OAuthTokenResponse.class),
                      eq(emptyHeaders),
                      any());

              // use the client credential token for config
              Mockito.verify(adapter)
                  .execute(
                      matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
                      eq(ConfigResponse.class),
                      any(),
                      any());

              // verify the token exchange uses the right scope
              Map<String, String> firstRefreshRequest =
                  ImmutableMap.of(
                      "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                      "subject_token", "client-credentials-token:sub=catalog",
                      "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                      "scope", scope);
              Mockito.verify(adapter)
                  .postForm(
                      eq(oauth2ServerUri),
                      argThat(firstRefreshRequest::equals),
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
        .postForm(eq(oauth2ServerUri), anyMap(), eq(OAuthTokenResponse.class), anyMap(), any());

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
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
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
        .postForm(
            eq(oauth2ServerUri),
            argThat(fetchTokenFromCredential::equals),
            eq(OAuthTokenResponse.class),
            eq(ImmutableMap.of()),
            any());

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), catalogHeaders),
            eq(ConfigResponse.class),
            any(),
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
  public void testInvalidPageSize() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    assertThatThrownBy(
            () ->
                catalog.initialize("test", ImmutableMap.of(RESTCatalogProperties.PAGE_SIZE, "-1")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid value for %s, must be a positive integer", RESTCatalogProperties.PAGE_SIZE);
  }

  @ParameterizedTest
  @ValueSource(ints = {21, 30})
  public void testPaginationForListNamespaces(int numberOfItems) {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of(RESTCatalogProperties.PAGE_SIZE, "10"));
    String namespaceName = "newdb";

    // create several namespaces for listing and verify
    for (int i = 0; i < numberOfItems; i++) {
      String nameSpaceName = namespaceName + i;
      catalog.createNamespace(Namespace.of(nameSpaceName));
    }

    assertThat(catalog.listNamespaces()).hasSize(numberOfItems);

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());

    Mockito.verify(adapter, times(numberOfItems))
        .execute(
            matches(HTTPMethod.POST, RESOURCE_PATHS.namespaces(), Map.of(), Map.of()),
            eq(CreateNamespaceResponse.class),
            any(),
            any());

    // verify initial request with empty pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(Route.LIST_NAMESPACES),
            eq(ImmutableMap.of("pageToken", "", "pageSize", "10")),
            any(),
            eq(ListNamespacesResponse.class),
            any());

    // verify second request with updated pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(Route.LIST_NAMESPACES),
            eq(ImmutableMap.of("pageToken", "10", "pageSize", "10")),
            any(),
            eq(ListNamespacesResponse.class),
            any());

    // verify third request with update pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(Route.LIST_NAMESPACES),
            eq(ImmutableMap.of("pageToken", "20", "pageSize", "10")),
            any(),
            eq(ListNamespacesResponse.class),
            any());
  }

  @ParameterizedTest
  @ValueSource(ints = {21, 30})
  public void testPaginationForListTables(int numberOfItems) {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of(RESTCatalogProperties.PAGE_SIZE, "10"));
    String namespaceName = "newdb";
    String tableName = "newtable";
    Namespace namespace = Namespace.of(namespaceName);
    catalog.createNamespace(namespace);

    // create several tables under namespace for listing and verify
    for (int i = 0; i < numberOfItems; i++) {
      TableIdentifier tableIdentifier = TableIdentifier.of(namespaceName, tableName + i);
      catalog.createTable(tableIdentifier, SCHEMA);
    }

    assertThat(catalog.listTables(namespace)).hasSize(numberOfItems);

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());

    Mockito.verify(adapter, times(numberOfItems))
        .execute(
            matches(HTTPMethod.POST, RESOURCE_PATHS.tables(namespace), Map.of(), Map.of()),
            eq(LoadTableResponse.class),
            any(),
            any());

    // verify initial request with empty pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(Route.LIST_TABLES),
            eq(ImmutableMap.of("pageToken", "", "pageSize", "10", "namespace", namespaceName)),
            any(),
            eq(ListTablesResponse.class),
            any());

    // verify second request with updated pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(Route.LIST_TABLES),
            eq(ImmutableMap.of("pageToken", "10", "pageSize", "10", "namespace", namespaceName)),
            any(),
            eq(ListTablesResponse.class),
            any());

    // verify third request with update pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(Route.LIST_TABLES),
            eq(ImmutableMap.of("pageToken", "20", "pageSize", "10", "namespace", namespaceName)),
            any(),
            eq(ListTablesResponse.class),
            any());
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
    Mockito.doThrow(new NotAuthorizedException("not authorized"))
        .when(adapter)
        .execute(matches(HTTPMethod.POST), any(), any(), any());
    assertThatThrownBy(() -> catalog.loadTable(TABLE).newFastAppend().appendFile(file).commit())
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("not authorized");

    // Extract the UpdateTableRequest to determine the path of the manifest list that should be
    // cleaned up
    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(TABLE));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest body = (UpdateTableRequest) req.body();
              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) body.updates().get(0);
              assertThatThrownBy(
                      () -> table.io().newInputFile(addSnapshot.snapshot().manifestListLocation()))
                  .isInstanceOf(NotFoundException.class)
                  .hasMessageContaining("No in-memory file found");
            });
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

    Mockito.doThrow(new ServiceFailureException("some service failure"))
        .when(adapter)
        .execute(matches(HTTPMethod.POST), any(), any(), any());
    assertThatThrownBy(() -> catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit())
        .isInstanceOf(ServiceFailureException.class)
        .hasMessage("some service failure");

    // Extract the UpdateTableRequest to determine the path of the manifest list that should still
    // exist even though the commit failed
    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(TABLE));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest body = (UpdateTableRequest) req.body();
              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) body.updates().get(0);
              String manifestListLocation = addSnapshot.snapshot().manifestListLocation();
              assertThat(table.io().newInputFile(manifestListLocation).exists()).isTrue();
            });
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
    Mockito.doThrow(new NotAuthorizedException("not authorized"))
        .when(adapter)
        .execute(matches(HTTPMethod.POST, RESOURCE_PATHS.table(newTable)), any(), any(), any());

    Transaction createTableTransaction = catalog.newCreateTableTransaction(newTable, SCHEMA);
    createTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(createTableTransaction::commitTransaction)
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("not authorized");

    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(newTable));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest body = (UpdateTableRequest) req.body();
              Optional<MetadataUpdate> appendSnapshot =
                  body.updates().stream()
                      .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
                      .findFirst();

              assertThat(appendSnapshot).isPresent();
              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) appendSnapshot.get();
              assertThatThrownBy(
                      () ->
                          catalog
                              .loadTable(TABLE)
                              .io()
                              .newInputFile(addSnapshot.snapshot().manifestListLocation()))
                  .isInstanceOf(NotFoundException.class)
                  .hasMessageContaining("No in-memory file found");
            });
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
        .execute(matches(HTTPMethod.POST, RESOURCE_PATHS.table(newTable)), any(), any(), any());

    Transaction createTableTransaction = catalog.newCreateTableTransaction(newTable, SCHEMA);
    createTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(createTableTransaction::commitTransaction)
        .isInstanceOf(ServiceFailureException.class)
        .hasMessage("some service failure");

    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(newTable));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest body = (UpdateTableRequest) req.body();
              Optional<MetadataUpdate> appendSnapshot =
                  body.updates().stream()
                      .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
                      .findFirst();
              assertThat(appendSnapshot).isPresent();

              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) appendSnapshot.get();
              String manifestListLocation = addSnapshot.snapshot().manifestListLocation();
              assertThat(catalog.loadTable(TABLE).io().newInputFile(manifestListLocation).exists())
                  .isTrue();
            });
  }

  @Test
  public void testNoCleanupOnCreate503() {
    RESTCatalogAdapter adapter =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              protected <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                var response = super.execute(request, responseType, errorHandler, responseHeaders);
                if (request.method() == HTTPMethod.POST && request.path().contains(TABLE.name())) {
                  // Simulate a 503 Service Unavailable error
                  ErrorResponse error =
                      ErrorResponse.builder()
                          .responseCode(503)
                          .withMessage("Service unavailable")
                          .build();

                  errorHandler.accept(error);
                  throw new IllegalStateException("Error handler should have thrown");
                }
                return response;
              }
            });

    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    Transaction createTableTransaction = catalog.newCreateTableTransaction(TABLE, SCHEMA);
    createTableTransaction.newAppend().appendFile(FILE_A).commit();

    // Verify that 503 is mapped to CommitStateUnknownException (not just ServiceFailureException)
    assertThatThrownBy(createTableTransaction::commitTransaction)
        .isInstanceOf(CommitStateUnknownException.class)
        .cause()
        .isInstanceOf(ServiceFailureException.class)
        .hasMessageContaining("Service failed: 503");

    // Verify files are NOT cleaned up (because commit state is unknown)
    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(TABLE));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest body = (UpdateTableRequest) req.body();
              assertThat(
                      body.updates().stream()
                          .filter(MetadataUpdate.AddSnapshot.class::isInstance)
                          .map(MetadataUpdate.AddSnapshot.class::cast)
                          .findFirst())
                  .hasValueSatisfying(
                      addSnapshot -> {
                        String manifestListLocation = addSnapshot.snapshot().manifestListLocation();
                        // Files should still exist because we don't know if commit succeeded
                        assertThat(
                                catalog
                                    .loadTable(TABLE)
                                    .io()
                                    .newInputFile(manifestListLocation)
                                    .exists())
                            .isTrue();
                      });
            });
  }

  @Test
  public void testCleanupCleanableExceptionsReplace() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);
    Mockito.doThrow(new NotAuthorizedException("not authorized"))
        .when(adapter)
        .execute(matches(HTTPMethod.POST, RESOURCE_PATHS.table(TABLE)), any(), any(), any());

    Transaction replaceTableTransaction = catalog.newReplaceTableTransaction(TABLE, SCHEMA, false);
    replaceTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(replaceTableTransaction::commitTransaction)
        .isInstanceOf(NotAuthorizedException.class)
        .hasMessage("not authorized");

    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(TABLE));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest request = (UpdateTableRequest) req.body();
              Optional<MetadataUpdate> appendSnapshot =
                  request.updates().stream()
                      .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
                      .findFirst();

              assertThat(appendSnapshot).isPresent();
              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) appendSnapshot.get();
              String manifestListLocation = addSnapshot.snapshot().manifestListLocation();
              assertThatThrownBy(
                      () -> catalog.loadTable(TABLE).io().newInputFile(manifestListLocation))
                  .isInstanceOf(NotFoundException.class)
                  .hasMessageContaining("No in-memory file found");
            });
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
        .execute(matches(HTTPMethod.POST, RESOURCE_PATHS.table(TABLE)), any(), any(), any());

    Transaction replaceTableTransaction = catalog.newReplaceTableTransaction(TABLE, SCHEMA, false);
    replaceTableTransaction.newAppend().appendFile(FILE_A).commit();
    assertThatThrownBy(replaceTableTransaction::commitTransaction)
        .isInstanceOf(ServiceFailureException.class)
        .hasMessage("some service failure");

    assertThat(allRequests(adapter))
        .anySatisfy(
            req -> {
              assertThat(req.method()).isEqualTo(HTTPMethod.POST);
              assertThat(req.path()).isEqualTo(RESOURCE_PATHS.table(TABLE));
              assertThat(req.body()).isInstanceOf(UpdateTableRequest.class);
              UpdateTableRequest request = (UpdateTableRequest) req.body();
              Optional<MetadataUpdate> appendSnapshot =
                  request.updates().stream()
                      .filter(update -> update instanceof MetadataUpdate.AddSnapshot)
                      .findFirst();
              assertThat(appendSnapshot).isPresent();

              MetadataUpdate.AddSnapshot addSnapshot =
                  (MetadataUpdate.AddSnapshot) appendSnapshot.get();
              String manifestListLocation = addSnapshot.snapshot().manifestListLocation();
              assertThat(catalog.loadTable(TABLE).io().newInputFile(manifestListLocation).exists())
                  .isTrue();
            });
  }

  @Test
  public void testNamespaceExistsViaHEADRequest() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of());

    Namespace namespace = Namespace.of("non-existing");
    assertThat(catalog.namespaceExists(namespace)).isFalse();

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.HEAD, RESOURCE_PATHS.namespace(namespace), Map.of(), Map.of()),
            any(),
            any(),
            any());
  }

  @Test
  public void testNamespaceExistsFallbackToGETRequest() {
    // server indicates support of loading a namespace only via GET, which is
    // what older REST servers would send back too
    verifyNamespaceExistsFallbackToGETRequest(
        ConfigResponse.builder()
            .withEndpoints(ImmutableList.of(Endpoint.V1_LOAD_NAMESPACE))
            .build());
  }

  private void verifyNamespaceExistsFallbackToGETRequest(ConfigResponse configResponse) {
    RESTCatalogAdapter adapter =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                if (ResourcePaths.config().equals(request.path())) {
                  return castResponse(responseType, configResponse);
                }

                return super.execute(request, responseType, errorHandler, responseHeaders);
              }
            });

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of());

    Namespace namespace = Namespace.of("non-existing");
    assertThat(catalog.namespaceExists(namespace)).isFalse();

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());

    // verifies that the namespace is loaded via a GET instead of HEAD (V1_NAMESPACE_EXISTS)
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, RESOURCE_PATHS.namespace(namespace), Map.of(), Map.of()),
            any(),
            any(),
            any());
  }

  @Test
  public void testNamespaceExistsFallbackToGETRequestWithLegacyServer() {
    // simulate a legacy server that doesn't send back supported endpoints, thus the
    // client relies on the default endpoints
    verifyNamespaceExistsFallbackToGETRequest(ConfigResponse.builder().build());
  }

  @Test
  public void testTableExistsViaHEADRequest() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of());

    assertThat(catalog.tableExists(TABLE)).isFalse();

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.HEAD, RESOURCE_PATHS.table(TABLE), Map.of(), Map.of()),
            any(),
            any(),
            any());
  }

  @Test
  public void testTableExistsFallbackToGETRequest() {
    // server indicates support of loading a table only via GET, which is
    // what older REST servers would send back too
    verifyTableExistsFallbackToGETRequest(
        ConfigResponse.builder().withEndpoints(ImmutableList.of(Endpoint.V1_LOAD_TABLE)).build());
  }

  private void verifyTableExistsFallbackToGETRequest(ConfigResponse configResponse) {
    RESTCatalogAdapter adapter =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                if (ResourcePaths.config().equals(request.path())) {
                  return castResponse(responseType, configResponse);
                }

                return super.execute(request, responseType, errorHandler, responseHeaders);
              }
            });

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of());

    assertThat(catalog.tableExists(TABLE)).isFalse();

    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config(), Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());

    // verifies that the table is loaded via a GET instead of HEAD (V1_LOAD_TABLE)
    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.GET, RESOURCE_PATHS.table(TABLE), Map.of(), Map.of("snapshots", "all")),
            any(),
            any(),
            any());
  }

  @Test
  public void testTableExistsFallbackToGETRequestWithLegacyServer() {
    // simulate a legacy server that doesn't send back supported endpoints, thus the
    // client relies on the default endpoints
    verifyTableExistsFallbackToGETRequest(ConfigResponse.builder().build());
  }

  @Test
  void testDifferentTableUUID() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    catalog.createNamespace(TABLE.namespace());
    catalog.createTable(TABLE, SCHEMA);

    // simulate drop and re-create the table with same name
    String newUUID = "386b9f01-002b-4d8c-b77f-42c3fd3b7c9b";
    Answer<LoadTableResponse> updateTable =
        invocation -> {
          LoadTableResponse loadTable = (LoadTableResponse) invocation.callRealMethod();
          TableMetadata current = loadTable.tableMetadata();
          assertThat(current.uuid()).isNotEqualTo(newUUID);
          TableMetadata newMetadata = TableMetadata.buildFrom(current).assignUUID(newUUID).build();
          return LoadTableResponse.builder()
              .withTableMetadata(newMetadata)
              .addAllConfig(loadTable.config())
              .build();
        };

    Mockito.doAnswer(updateTable)
        .when(adapter)
        .execute(
            matches(HTTPMethod.POST, RESOURCE_PATHS.table(TABLE)),
            eq(LoadTableResponse.class),
            any(),
            any());

    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(2)
            .build();

    assertThatThrownBy(() -> catalog.loadTable(TABLE).newFastAppend().appendFile(file).commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageMatching("Table UUID does not match: current=.* != refreshed=" + newUUID);
  }

  @Test
  public void testReconcileOnUnknownSnapshotAddMatchesSnapshotId() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);

    // Simulate: server commits, but client receives CommitStateUnknown (transient 5xx)
    Mockito.doAnswer(
            invocation -> {
              invocation.callRealMethod();
              throw new CommitStateUnknownException(
                  new ServiceFailureException("Service failed: 503"));
            })
        .when(adapter)
        .execute(
            matches(HTTPMethod.POST, RESOURCE_PATHS.table(TABLE)),
            eq(LoadTableResponse.class),
            any(),
            any());

    Table table = catalog.loadTable(TABLE);

    // Perform a snapshot-adding commit; should reconcile instead of failing
    table.newFastAppend().appendFile(FILE_A).commit();

    // Extract the snapshot id we attempted to commit from the request body
    long expectedSnapshotId =
        allRequests(adapter).stream()
            .filter(
                r -> r.method() == HTTPMethod.POST && r.path().equals(RESOURCE_PATHS.table(TABLE)))
            .map(HTTPRequest::body)
            .filter(UpdateTableRequest.class::isInstance)
            .map(UpdateTableRequest.class::cast)
            .map(
                req ->
                    (MetadataUpdate.AddSnapshot)
                        req.updates().stream()
                            .filter(u -> u instanceof MetadataUpdate.AddSnapshot)
                            .findFirst()
                            .orElseThrow())
            .map(add -> add.snapshot().snapshotId())
            .findFirst()
            .orElseThrow();

    Table reloaded = catalog.loadTable(TABLE);
    assertThat(reloaded.currentSnapshot()).isNotNull();
    assertThat(reloaded.snapshot(expectedSnapshotId)).isNotNull();
  }

  @Test
  public void testCommitStateUnknownNotReconciled() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    catalog.createTable(TABLE, SCHEMA);

    // Simulate: server returns CommitStateUnknown and does NOT apply the commit
    Mockito.doThrow(
            new CommitStateUnknownException(new ServiceFailureException("Service failed: 503")))
        .when(adapter)
        .execute(
            matches(HTTPMethod.POST, RESOURCE_PATHS.table(TABLE)),
            eq(LoadTableResponse.class),
            any(),
            any());

    Table table = catalog.loadTable(TABLE);

    assertThatThrownBy(() -> table.newFastAppend().appendFile(FILE_A).commit())
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageContaining("Cannot determine whether the commit was successful")
        .satisfies(ex -> assertThat(((CommitStateUnknownException) ex).getSuppressed()).isEmpty());
  }

  @Test
  public void testCustomTableOperationsInjection() throws IOException {
    AtomicBoolean customTableOpsCalled = new AtomicBoolean();
    AtomicBoolean customTransactionTableOpsCalled = new AtomicBoolean();
    AtomicReference<RESTTableOperations> capturedOps = new AtomicReference<>();
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    Map<String, String> customHeaders =
        ImmutableMap.of("X-Custom-Table-Header", "custom-value-12345");

    // Custom RESTTableOperations that adds a custom header
    class CustomRESTTableOperations extends RESTTableOperations {
      CustomRESTTableOperations(
          RESTClient client,
          String path,
          Supplier<Map<String, String>> headers,
          FileIO fileIO,
          TableMetadata current,
          Set<Endpoint> supportedEndpoints) {
        super(client, path, () -> customHeaders, fileIO, current, supportedEndpoints);
        customTableOpsCalled.set(true);
      }

      CustomRESTTableOperations(
          RESTClient client,
          String path,
          Supplier<Map<String, String>> headers,
          FileIO fileIO,
          RESTTableOperations.UpdateType updateType,
          List<MetadataUpdate> createChanges,
          TableMetadata current,
          Set<Endpoint> supportedEndpoints) {
        super(
            client,
            path,
            () -> customHeaders,
            fileIO,
            updateType,
            createChanges,
            current,
            supportedEndpoints);
        customTransactionTableOpsCalled.set(true);
      }
    }

    // Custom RESTSessionCatalog that overrides table operations creation
    class CustomRESTSessionCatalog extends RESTSessionCatalog {
      CustomRESTSessionCatalog(
          Function<Map<String, String>, RESTClient> clientBuilder,
          BiFunction<SessionCatalog.SessionContext, Map<String, String>, FileIO> ioBuilder) {
        super(clientBuilder, ioBuilder);
      }

      @Override
      protected RESTTableOperations newTableOps(
          RESTClient restClient,
          String path,
          Supplier<Map<String, String>> readHeaders,
          Supplier<Map<String, String>> mutationHeaders,
          FileIO fileIO,
          TableMetadata current,
          Set<Endpoint> supportedEndpoints) {
        RESTTableOperations ops =
            new CustomRESTTableOperations(
                restClient, path, mutationHeaders, fileIO, current, supportedEndpoints);
        RESTTableOperations spy = Mockito.spy(ops);
        capturedOps.set(spy);
        return spy;
      }

      @Override
      protected RESTTableOperations newTableOps(
          RESTClient restClient,
          String path,
          Supplier<Map<String, String>> readHeaders,
          Supplier<Map<String, String>> mutationHeaders,
          FileIO fileIO,
          RESTTableOperations.UpdateType updateType,
          List<MetadataUpdate> createChanges,
          TableMetadata current,
          Set<Endpoint> supportedEndpoints) {
        RESTTableOperations ops =
            new CustomRESTTableOperations(
                restClient,
                path,
                mutationHeaders,
                fileIO,
                updateType,
                createChanges,
                current,
                supportedEndpoints);
        RESTTableOperations spy = Mockito.spy(ops);
        capturedOps.set(spy);
        return spy;
      }
    }

    try (RESTCatalog catalog =
        catalog(adapter, clientBuilder -> new CustomRESTSessionCatalog(clientBuilder, null))) {
      catalog.createNamespace(NS);

      // Test table operations without UpdateType
      assertThat(customTableOpsCalled).isFalse();
      assertThat(customTransactionTableOpsCalled).isFalse();
      Table table = catalog.createTable(TABLE, SCHEMA);
      assertThat(customTableOpsCalled).isTrue();
      assertThat(customTransactionTableOpsCalled).isFalse();

      // Trigger a commit through the custom operations
      table.updateProperties().set("test-key", "test-value").commit();

      // Verify the custom operations object was created and used
      assertThat(capturedOps.get()).isNotNull();
      Mockito.verify(capturedOps.get(), Mockito.atLeastOnce()).current();
      Mockito.verify(capturedOps.get(), Mockito.atLeastOnce()).commit(any(), any());

      // Verify the custom operations were used with custom headers
      Mockito.verify(adapter, Mockito.atLeastOnce())
          .execute(
              matches(HTTPMethod.POST, RESOURCE_PATHS.table(TABLE), customHeaders),
              eq(LoadTableResponse.class),
              any(),
              any());

      // Test table operations with UpdateType and createChanges
      capturedOps.set(null);
      customTableOpsCalled.set(false);
      TableIdentifier table2 = TableIdentifier.of(NS, "table2");
      catalog.buildTable(table2, SCHEMA).createTransaction().commitTransaction();
      assertThat(customTableOpsCalled).isFalse();
      assertThat(customTransactionTableOpsCalled).isTrue();

      // Trigger another commit to verify transaction operations also work
      catalog.loadTable(table2).updateProperties().set("test-key-2", "test-value-2").commit();

      // Verify the custom operations object was created and used
      assertThat(capturedOps.get()).isNotNull();
      Mockito.verify(capturedOps.get(), Mockito.atLeastOnce()).current();
      Mockito.verify(capturedOps.get(), Mockito.atLeastOnce()).commit(any(), any());

      // Verify the custom operations were used with custom headers
      Mockito.verify(adapter, Mockito.atLeastOnce())
          .execute(
              matches(HTTPMethod.POST, RESOURCE_PATHS.table(table2), customHeaders),
              eq(LoadTableResponse.class),
              any(),
              any());
    }
  }

  @Test
  public void testClientAutoSendsIdempotencyWhenServerAdvertises() {
    ConfigResponse cfgWithIdem =
        ConfigResponse.builder()
            .withIdempotencyKeyLifetime("PT30M")
            .withEndpoints(
                Arrays.stream(Route.values())
                    .map(r -> Endpoint.create(r.method().name(), r.resourcePath()))
                    .collect(Collectors.toList()))
            .build();

    RESTCatalog local = createCatalogWithIdempAdapter(cfgWithIdem, true);

    Namespace ns = Namespace.of("ns_cfg_idem");
    TableIdentifier ident = TableIdentifier.of(ns, "t_cfg_idem");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    local.createNamespace(ns, ImmutableMap.of());
    local.createTable(ident, schema);
    assertThat(local.tableExists(ident)).isTrue();
    local.dropTable(ident);
  }

  @Test
  public void testClientDoesNotSendIdempotencyWhenServerNotAdvertising() {
    ConfigResponse cfgNoIdem =
        ConfigResponse.builder()
            .withEndpoints(
                Arrays.stream(Route.values())
                    .map(r -> Endpoint.create(r.method().name(), r.resourcePath()))
                    .collect(Collectors.toList()))
            .build();

    RESTCatalog local = createCatalogWithIdempAdapter(cfgNoIdem, false);

    Namespace ns = Namespace.of("ns_cfg_no_idem");
    TableIdentifier ident = TableIdentifier.of(ns, "t_cfg_no_idem");
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    local.createNamespace(ns, ImmutableMap.of());
    local.createTable(ident, schema);
    assertThat(local.tableExists(ident)).isTrue();
    local.dropTable(ident);
  }

  @Test
  public void testIdempotentDuplicateCreateReturnsCached() {
    String key = "dup-create-key";
    Namespace ns = Namespace.of("ns_dup");
    IdempotentEnv env = idempotentEnv(key, ns, "t_dup");
    CreateTableRequest req = createReq(env.ident);

    // First create succeeds
    LoadTableResponse first =
        env.http.post(
            ResourcePaths.forCatalogProperties(ImmutableMap.of()).tables(ns),
            req,
            LoadTableResponse.class,
            env.headers,
            ErrorHandlers.tableErrorHandler());
    assertThat(first).isNotNull();

    // Verify request shape (method, path, headers including Idempotency-Key)
    verifyCreatePost(ns, env.headers);

    // Duplicate with same key returns cached 200 OK
    LoadTableResponse second =
        env.http.post(
            ResourcePaths.forCatalogProperties(ImmutableMap.of()).tables(ns),
            req,
            LoadTableResponse.class,
            env.headers,
            ErrorHandlers.tableErrorHandler());
    assertThat(second).isNotNull();
  }

  @Test
  public void testIdempotencyKeyLifetimeExpiredTreatsAsNew() {
    // Set TTL to 0 so cached success expires immediately
    CatalogHandlers.setIdempotencyLifetimeFromIso("PT0S");
    try {
      String key = "expired-create-key";
      Namespace ns = Namespace.of("ns_exp");
      IdempotentEnv env = idempotentEnv(key, ns, "t_exp");
      CreateTableRequest req = createReq(env.ident);

      // First create succeeds
      LoadTableResponse created =
          env.http.post(
              ResourcePaths.forCatalogProperties(ImmutableMap.of()).tables(ns),
              req,
              LoadTableResponse.class,
              env.headers,
              ErrorHandlers.tableErrorHandler());
      assertThat(created).isNotNull();

      // Verify request shape (method, path, headers including Idempotency-Key)
      verifyCreatePost(ns, env.headers);

      // TTL expired -> duplicate with same key should be treated as new and fail with AlreadyExists
      assertThatThrownBy(
              () ->
                  env.http.post(
                      ResourcePaths.forCatalogProperties(ImmutableMap.of()).tables(ns),
                      req,
                      LoadTableResponse.class,
                      env.headers,
                      ErrorHandlers.tableErrorHandler()))
          .isInstanceOf(AlreadyExistsException.class)
          .hasMessageContaining(env.ident.toString());
    } finally {
      // Restore default TTL for other tests
      CatalogHandlers.setIdempotencyLifetimeFromIso("PT30M");
    }
  }

  @Test
  public void testIdempotentCreateReplayAfterSimulated503() {
    // Use a fixed key and simulate 503 after first success for that key
    String key = "idemp-create-503";
    adapterForRESTServer.simulate503OnFirstSuccessForKey(key);
    Namespace ns = Namespace.of("ns_idemp");
    IdempotentEnv env = idempotentEnv(key, ns, "t_idemp");
    CreateTableRequest req = createReq(env.ident);

    // First attempt: server finalizes success but responds 503
    assertThatThrownBy(
            () ->
                env.http.post(
                    ResourcePaths.forCatalogProperties(ImmutableMap.of()).tables(ns),
                    req,
                    LoadTableResponse.class,
                    env.headers,
                    ErrorHandlers.tableErrorHandler()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("simulated transient 503");

    // Verify request shape (method, path, headers including Idempotency-Key)
    verifyCreatePost(ns, env.headers);

    // Retry with same key: server should replay 200 OK
    LoadTableResponse replay =
        env.http.post(
            ResourcePaths.forCatalogProperties(ImmutableMap.of()).tables(ns),
            req,
            LoadTableResponse.class,
            env.headers,
            ErrorHandlers.tableErrorHandler());
    assertThat(replay).isNotNull();
  }

  @Test
  public void testIdempotentDropDuplicateNoop() {
    String key = "idemp-drop-void";
    Namespace ns = Namespace.of("ns_void");
    IdempotentEnv env = idempotentEnv(key, ns, "t_void");

    // Create a table to drop
    restCatalog.createTable(
        env.ident,
        new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
        PartitionSpec.unpartitioned());

    String path = ResourcePaths.forCatalogProperties(ImmutableMap.of()).table(env.ident);

    // First drop: table exists -> drop succeeds
    env.http.delete(path, null, env.headers, ErrorHandlers.tableErrorHandler());
    assertThat(restCatalog.tableExists(env.ident)).isFalse();

    // Second drop with the same key: should be a no-op (no exception)
    assertThatCode(
            () -> env.http.delete(path, null, env.headers, ErrorHandlers.tableErrorHandler()))
        .doesNotThrowAnyException();
  }

  @Test
  public void nestedNamespaceWithLegacySeparator() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    // Simulate that the server doesn't send the namespace separator in the overrides
    Mockito.doAnswer(
            invocation -> {
              ConfigResponse configResponse = (ConfigResponse) invocation.callRealMethod();

              Map<String, String> overridesWithoutNamespaceSeparator = configResponse.overrides();
              overridesWithoutNamespaceSeparator.remove(RESTCatalogProperties.NAMESPACE_SEPARATOR);

              return ConfigResponse.builder()
                  .withDefaults(configResponse.defaults())
                  .withOverrides(overridesWithoutNamespaceSeparator)
                  .withEndpoints(configResponse.endpoints())
                  .withIdempotencyKeyLifetime(configResponse.idempotencyKeyLifetime())
                  .build();
            })
        .when(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config()),
            eq(ConfigResponse.class),
            any(),
            any());

    RESTCatalog catalog = catalog(adapter);

    ResourcePaths pathsWithLegacySeparator = ResourcePaths.forCatalogProperties(ImmutableMap.of());

    runConfigurableNamespaceSeparatorTest(
        catalog, adapter, pathsWithLegacySeparator, RESTUtil.NAMESPACE_SEPARATOR_URLENCODED_UTF_8);
  }

  @Test
  public void nestedNamespaceWithOverriddenSeparator() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));

    // When initializing the catalog, the adapter always sends an override for the namespace
    // separator.
    Mockito.doAnswer(
            invocation -> {
              ConfigResponse configResponse = (ConfigResponse) invocation.callRealMethod();

              assertThat(configResponse.overrides())
                  .containsEntry(
                      RESTCatalogProperties.NAMESPACE_SEPARATOR,
                      RESTCatalogAdapter.NAMESPACE_SEPARATOR_URLENCODED_UTF_8);

              return configResponse;
            })
        .when(adapter)
        .execute(
            matches(HTTPMethod.GET, ResourcePaths.config()),
            eq(ConfigResponse.class),
            any(),
            any());

    RESTCatalog catalog = catalog(adapter);

    runConfigurableNamespaceSeparatorTest(
        catalog, adapter, RESOURCE_PATHS, RESTCatalogAdapter.NAMESPACE_SEPARATOR_URLENCODED_UTF_8);
  }

  private void runConfigurableNamespaceSeparatorTest(
      RESTCatalog catalog,
      RESTCatalogAdapter adapter,
      ResourcePaths expectedPaths,
      String expectedSeparator) {
    Namespace nestedNamespace = Namespace.of("ns1", "ns2", "ns3");
    Namespace parentNamespace = Namespace.of("ns1", "ns2");
    TableIdentifier table = TableIdentifier.of(nestedNamespace, "tbl");

    catalog.createNamespace(nestedNamespace);

    catalog.createTable(table, SCHEMA);

    assertThat(catalog.listNamespaces(parentNamespace)).containsExactly(nestedNamespace);

    // Verify the namespace separator in the path
    Mockito.verify(adapter)
        .execute(
            matches(HTTPMethod.POST, expectedPaths.tables(nestedNamespace)),
            eq(LoadTableResponse.class),
            any(),
            any());

    // Verify the namespace separator in query parameters
    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.GET,
                expectedPaths.namespaces(),
                Map.of(),
                Map.of(
                    "parent",
                    RESTUtil.namespaceToQueryParam(parentNamespace, expectedSeparator),
                    "pageToken",
                    "")),
            eq(ListNamespacesResponse.class),
            any(),
            any());
  }

  private RESTCatalog createCatalogWithIdempAdapter(ConfigResponse cfg, boolean expectOnMutations) {
    RESTCatalogAdapter adapter =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                if (ResourcePaths.config().equals(request.path())) {
                  return castResponse(responseType, cfg);
                }

                boolean isMutation =
                    request.method() == HTTPMethod.POST || request.method() == HTTPMethod.DELETE;
                boolean hasIdemp = request.headers().contains(RESTUtil.IDEMPOTENCY_KEY_HEADER);

                if (isMutation) {
                  assertThat(hasIdemp)
                      .as("Idempotency-Key presence on mutations did not match expectation")
                      .isEqualTo(expectOnMutations);
                } else {
                  assertThat(hasIdemp).as("Idempotency-Key must NOT be sent on reads").isFalse();
                }

                return super.execute(request, responseType, errorHandler, responseHeaders);
              }
            });

    RESTCatalog local =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    local.initialize("test", ImmutableMap.of());
    return local;
  }

  private Pair<RESTClient, Map<String, String>> httpAndHeaders(String idempotencyKey) {
    Map<String, String> headers =
        ImmutableMap.of(
            RESTUtil.IDEMPOTENCY_KEY_HEADER,
            idempotencyKey,
            "Authorization",
            "Bearer client-credentials-token:sub=user",
            "test-header",
            "test-value");

    Map<String, String> conf =
        ImmutableMap.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            HTTPClient.REST_SOCKET_TIMEOUT_MS,
            "600000",
            HTTPClient.REST_CONNECTION_TIMEOUT_MS,
            "600000",
            "header.test-header",
            "test-value");
    RESTClient httpBase =
        HTTPClient.builder(conf)
            .uri(conf.get(CatalogProperties.URI))
            .withHeaders(RESTUtil.configHeaders(conf))
            .build();
    AuthManager am = AuthManagers.loadAuthManager("test", conf);
    AuthSession httpSession = am.initSession(httpBase, conf);
    RESTClient http = httpBase.withAuthSession(httpSession);
    return Pair.of(http, headers);
  }

  private Pair<TableIdentifier, Pair<RESTClient, Map<String, String>>> prepareIdempotentEnv(
      String key, Namespace ns, String tableName) {
    TableIdentifier ident = TableIdentifier.of(ns, tableName);
    restCatalog.createNamespace(ns, ImmutableMap.of());
    return Pair.of(ident, httpAndHeaders(key));
  }

  private IdempotentEnv idempotentEnv(String key, Namespace ns, String tableName) {
    Pair<TableIdentifier, Pair<RESTClient, Map<String, String>>> env =
        prepareIdempotentEnv(key, ns, tableName);
    Pair<RESTClient, Map<String, String>> httpAndHeaders = env.second();
    return new IdempotentEnv(env.first(), httpAndHeaders.first(), httpAndHeaders.second());
  }

  private static CreateTableRequest createReq(TableIdentifier ident) {
    return CreateTableRequest.builder()
        .withName(ident.name())
        .withSchema(new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())))
        .withPartitionSpec(PartitionSpec.unpartitioned())
        .build();
  }

  private void verifyCreatePost(Namespace ns, Map<String, String> headers) {
    verify(adapterForRESTServer, atLeastOnce())
        .execute(
            containsHeaders(
                HTTPMethod.POST,
                ResourcePaths.forCatalogProperties(ImmutableMap.of()).tables(ns),
                headers),
            eq(LoadTableResponse.class),
            any(),
            any());
  }

  @Test
  @Override
  public void testLoadTableWithMissingMetadataFile(@TempDir Path tempDir) {

    if (requiresNamespaceCreate()) {
      restCatalog.createNamespace(TBL.namespace());
    }

    restCatalog.buildTable(TBL, SCHEMA).create();
    assertThat(restCatalog.tableExists(TBL)).as("Table should exist").isTrue();

    Table table = restCatalog.loadTable(TBL);
    String metadataFileLocation =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    table.io().deleteFile(metadataFileLocation);

    assertThatThrownBy(() -> restCatalog.loadTable(TBL))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("No in-memory file found for location: " + metadataFileLocation);
  }

  @Test
  public void testNumLoadTableCallsForMergeAppend() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog = catalog(adapter);

    catalog.createNamespace(TABLE.namespace());
    BaseTable table = (BaseTable) catalog.createTable(TABLE, SCHEMA);
    table.newAppend().appendFile(FILE_A).commit();

    // loadTable is executed once
    Mockito.verify(adapter)
        .execute(matches(HTTPMethod.GET, RESOURCE_PATHS.table(TABLE)), any(), any(), any());

    // CommitReport reflects the table state after the commit
    Mockito.verify(adapter)
        .execute(
            matches(
                HTTPMethod.POST,
                RESOURCE_PATHS.metrics(TABLE),
                Map.of(),
                Map.of(),
                requestObj ->
                    requestObj instanceof ReportMetricsRequest reportRequest
                        && reportRequest.report() instanceof CommitReport commitReport
                        && commitReport.tableName().equals(table.name())
                        && commitReport.snapshotId() == table.currentSnapshot().snapshotId()
                        && commitReport.sequenceNumber() == table.currentSnapshot().sequenceNumber()
                        && commitReport.operation().equals("append")
                        && commitReport.commitMetrics().addedDataFiles().value() == 1),
            any(),
            any(),
            any());
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

  private RESTCatalog catalog(
      RESTCatalogAdapter adapter,
      Function<Function<Map<String, String>, RESTClient>, RESTSessionCatalog>
          sessionCatalogFactory) {
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter) {
          @Override
          protected RESTSessionCatalog newSessionCatalog(
              Function<Map<String, String>, RESTClient> clientBuilder) {
            return sessionCatalogFactory.apply(clientBuilder);
          }
        };
    catalog.initialize(
        "test",
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
    return catalog;
  }

  /**
   * Test concurrent appends on multiple branches simultaneously to verify proper handling of
   * sequence number conflicts.
   *
   * <p>Uses a barrier to synchronize threads so they all load the same table state and commit
   * simultaneously, creating deterministic conflicts. With retries disabled, we can verify exact
   * counts: one success per round, and (numBranches - 1) CommitFailedExceptions per round.
   *
   * <p>This verifies that: 1. Sequence number conflicts are caught by AssertLastSequenceNumber
   * requirement 2. Conflicts result in CommitFailedException (retryable) not ValidationException
   * (non-retryable) 3. The REST catalog properly handles concurrent modifications across branches
   */
  @Test
  public void testConcurrentAppendsOnMultipleBranches() {
    int numBranches = 5;
    int numRounds = 10;

    RESTCatalog restCatalog = catalog();

    Namespace ns = Namespace.of("concurrent_test");
    TableIdentifier tableIdent = TableIdentifier.of(ns, "test_table");

    restCatalog.createNamespace(ns);
    Table table = restCatalog.buildTable(tableIdent, SCHEMA).withPartitionSpec(SPEC).create();

    // Disable retries so we can count exact conflicts
    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "-1").commit();

    // Create branches from the main branch
    String[] branchNames = new String[numBranches];
    for (int i = 0; i < numBranches; i++) {
      branchNames[i] = "branch-" + i;
      table.manageSnapshots().createBranch(branchNames[i]).commit();
    }

    AtomicInteger barrier = new AtomicInteger(0);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger commitFailedCount = new AtomicInteger(0);
    AtomicInteger validationFailureCount = new AtomicInteger(0);

    ExecutorService executor =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(numBranches));

    Tasks.range(numBranches)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(executor)
        .run(
            branchIdx -> {
              String branchName = branchNames[branchIdx];

              for (int round = 0; round < numRounds; round++) {
                int currentRound = round;

                // Load table
                Table localTable = restCatalog.loadTable(tableIdent);

                DataFile newFile =
                    DataFiles.builder(SPEC)
                        .withPath(
                            String.format("/path/to/branch-%d-round-%d.parquet", branchIdx, round))
                        .withFileSizeInBytes(15)
                        .withPartitionPath(String.format("id_bucket=%d", branchIdx % 16))
                        .withRecordCount(3)
                        .build();

                // Wait for all threads to be ready to commit
                barrier.incrementAndGet();
                Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> barrier.get() >= (currentRound + 1) * numBranches);

                try {
                  // All threads commit simultaneously
                  localTable.newFastAppend().appendFile(newFile).toBranch(branchName).commit();
                  successCount.incrementAndGet();
                } catch (CommitFailedException e) {
                  // Expected for conflicts - this is the correct behavior with the fix
                  commitFailedCount.incrementAndGet();
                } catch (ValidationException e) {
                  // This indicates the fix is not working
                  validationFailureCount.incrementAndGet();
                  throw e;
                }
              }
            });

    int totalAttempts = numBranches * numRounds;

    // Verify: no ValidationException should have been thrown
    assertThat(validationFailureCount.get())
        .as(
            "Sequence number conflicts should throw CommitFailedException (retryable), "
                + "not ValidationException")
        .isEqualTo(0);

    // Verify at least one commit succeeded per round
    assertThat(successCount.get())
        .as("At least one commit should succeed per round")
        .isGreaterThanOrEqualTo(numRounds);

    // Verify some conflicts happened (proves concurrent contention occurred)
    assertThat(commitFailedCount.get())
        .as("Some commits should fail with CommitFailedException due to conflicts")
        .isGreaterThan(0);

    // Verify no attempts were lost
    assertThat(successCount.get() + commitFailedCount.get())
        .as("All commit attempts should either succeed or fail with CommitFailedException")
        .isEqualTo(totalAttempts);
  }

  private static List<HTTPRequest> allRequests(RESTCatalogAdapter adapter) {
    ArgumentCaptor<HTTPRequest> captor = ArgumentCaptor.forClass(HTTPRequest.class);
    verify(adapter, atLeastOnce()).execute(captor.capture(), any(), any(), any());
    return captor.getAllValues();
  }
}
