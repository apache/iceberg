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

import static org.apache.iceberg.rest.TestRESTCatalog.reqMatcher;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.iceberg.view.ViewMetadata;
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
import org.mockito.Mockito;

public class TestRESTViewCatalog extends ViewCatalogTests<RESTCatalog> {
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();

  @TempDir protected Path temp;

  protected RESTCatalog restCatalog;
  protected InMemoryCatalog backendCatalog;
  protected Server httpServer;

  @BeforeEach
  public void createCatalog() throws Exception {
    File warehouse = temp.toFile();

    this.backendCatalog = new InMemoryCatalog();
    this.backendCatalog.initialize(
        "in-memory",
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath())
            .put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key1", "catalog-default-key1")
            .put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key2", "catalog-default-key2")
            .put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key3", "catalog-default-key3")
            .put(CatalogProperties.VIEW_OVERRIDE_PREFIX + "key3", "catalog-override-key3")
            .put(CatalogProperties.VIEW_OVERRIDE_PREFIX + "key4", "catalog-override-key4")
            .build());

    RESTCatalogAdapter adaptor =
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          public <T extends RESTResponse> T execute(
              HTTPRequest request,
              Class<T> responseType,
              Consumer<ErrorResponse> errorHandler,
              Consumer<Map<String, String>> responseHeaders) {
            Object body = roundTripSerialize(request.body(), "request");
            HTTPRequest req = ImmutableHTTPRequest.builder().from(request).body(body).build();
            T response = super.execute(req, responseType, errorHandler, responseHeaders);
            T responseAfterSerialization = roundTripSerialize(response, "response");
            return responseAfterSerialization;
          }
        };

    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.setContextPath("/");
    servletContext.addServlet(new ServletHolder(new RESTCatalogServlet(adaptor)), "/*");
    servletContext.setHandler(new GzipHandler());

    this.httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
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

  @ParameterizedTest
  @ValueSource(ints = {21, 30})
  public void testPaginationForListViews(int numberOfItems) {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of(RESTCatalogProperties.PAGE_SIZE, "10"));

    String namespaceName = "newdb";
    String viewName = "newview";

    // create initial namespace
    catalog().createNamespace(Namespace.of(namespaceName));

    // create several views under namespace, based off a table for listing and verify
    for (int i = 0; i < numberOfItems; i++) {
      TableIdentifier viewIndentifier = TableIdentifier.of(namespaceName, viewName + i);
      catalog
          .buildView(viewIndentifier)
          .withSchema(SCHEMA)
          .withDefaultNamespace(viewIndentifier.namespace())
          .withQuery("spark", "select * from ns.tbl")
          .create();
    }
    List<TableIdentifier> views = catalog.listViews(Namespace.of(namespaceName));
    assertThat(views).hasSize(numberOfItems);

    Mockito.verify(adapter)
        .execute(reqMatcher(HTTPMethod.GET, "v1/config"), eq(ConfigResponse.class), any(), any());

    Mockito.verify(adapter, times(numberOfItems))
        .execute(
            reqMatcher(HTTPMethod.POST, String.format("v1/namespaces/%s/views", namespaceName)),
            eq(LoadViewResponse.class),
            any(),
            any());

    // verify initial request with empty pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(Route.LIST_VIEWS),
            eq(ImmutableMap.of("pageToken", "", "pageSize", "10", "namespace", namespaceName)),
            any(),
            eq(ListTablesResponse.class),
            any());

    // verify second request with update pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(Route.LIST_VIEWS),
            eq(ImmutableMap.of("pageToken", "10", "pageSize", "10", "namespace", namespaceName)),
            any(),
            eq(ListTablesResponse.class),
            any());

    // verify third request with update pageToken
    Mockito.verify(adapter)
        .handleRequest(
            eq(Route.LIST_VIEWS),
            eq(ImmutableMap.of("pageToken", "20", "pageSize", "10", "namespace", namespaceName)),
            any(),
            eq(ListTablesResponse.class),
            any());
  }

  @Test
  public void viewExistsViaHEADRequest() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of());

    catalog.createNamespace(Namespace.of("ns"));

    assertThat(catalog.viewExists(TableIdentifier.of("ns", "view"))).isFalse();

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.HEAD, "v1/namespaces/ns/views/view", Map.of(), Map.of()),
            any(),
            any(),
            any());
  }

  @Test
  public void viewExistsFallbackToGETRequest() {
    // server indicates support of loading a view only via GET, which is
    // what older REST servers would send back too
    verifyViewExistsFallbackToGETRequest(
        ConfigResponse.builder().withEndpoints(ImmutableList.of(Endpoint.V1_LOAD_VIEW)).build(),
        ImmutableMap.of());
  }

  private void verifyViewExistsFallbackToGETRequest(
      ConfigResponse configResponse, Map<String, String> catalogProperties) {
    RESTCatalogAdapter adapter =
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
              @Override
              public <T extends RESTResponse> T execute(
                  HTTPRequest request,
                  Class<T> responseType,
                  Consumer<ErrorResponse> errorHandler,
                  Consumer<Map<String, String>> responseHeaders) {
                if ("v1/config".equals(request.path())) {
                  return castResponse(responseType, configResponse);
                }

                return super.execute(request, responseType, errorHandler, responseHeaders);
              }
            });

    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", catalogProperties);
    assertThat(catalog.viewExists(TableIdentifier.of("ns", "view"))).isFalse();

    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/config", Map.of(), Map.of()),
            eq(ConfigResponse.class),
            any(),
            any());
    Mockito.verify(adapter)
        .execute(
            reqMatcher(HTTPMethod.GET, "v1/namespaces/ns/views/view", Map.of(), Map.of()),
            any(),
            any(),
            any());
  }

  @Test
  public void viewExistsFallbackToGETRequestWithLegacyServer() {
    // simulate a legacy server that doesn't send back supported endpoints, thus the
    // client relies on the default endpoints
    verifyViewExistsFallbackToGETRequest(
        ConfigResponse.builder().build(),
        ImmutableMap.of(RESTCatalogProperties.VIEW_ENDPOINTS_SUPPORTED, "true"));
  }

  @Test
  public void testCustomViewOperationsInjection() throws Exception {
    AtomicBoolean customViewOpsCalled = new AtomicBoolean();
    AtomicReference<RESTViewOperations> capturedOps = new AtomicReference<>();
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    Map<String, String> customHeaders =
        ImmutableMap.of("X-Custom-View-Header", "custom-value-12345");

    // Custom RESTViewOperations that adds a custom header
    class CustomRESTViewOperations extends RESTViewOperations {
      CustomRESTViewOperations(
          RESTClient client,
          String path,
          Supplier<Map<String, String>> headers,
          ViewMetadata current,
          Set<Endpoint> supportedEndpoints) {
        super(client, path, () -> customHeaders, current, supportedEndpoints);
        customViewOpsCalled.set(true);
      }
    }

    // Custom RESTSessionCatalog that overrides view operations creation
    class CustomRESTSessionCatalog extends RESTSessionCatalog {
      CustomRESTSessionCatalog(
          Function<Map<String, String>, RESTClient> clientBuilder,
          BiFunction<SessionCatalog.SessionContext, Map<String, String>, FileIO> ioBuilder) {
        super(clientBuilder, ioBuilder);
      }

      @Override
      protected RESTViewOperations newViewOps(
          RESTClient restClient,
          String path,
          Supplier<Map<String, String>> readHeaders,
          Supplier<Map<String, String>> mutationHeaders,
          ViewMetadata current,
          Set<Endpoint> supportedEndpoints) {
        RESTViewOperations ops =
            new CustomRESTViewOperations(
                restClient, path, mutationHeaders, current, supportedEndpoints);
        RESTViewOperations spy = Mockito.spy(ops);
        capturedOps.set(spy);
        return spy;
      }
    }

    try (RESTCatalog catalog =
        catalog(adapter, clientBuilder -> new CustomRESTSessionCatalog(clientBuilder, null))) {
      Namespace namespace = Namespace.of("ns");
      catalog.createNamespace(namespace);

      // Test view operations
      assertThat(customViewOpsCalled).isFalse();
      TableIdentifier viewIdentifier = TableIdentifier.of(namespace, "view1");
      org.apache.iceberg.view.View view =
          catalog
              .buildView(viewIdentifier)
              .withSchema(SCHEMA)
              .withDefaultNamespace(namespace)
              .withQuery("spark", "select * from ns.table")
              .create();

      // Verify custom operations was created
      assertThat(customViewOpsCalled).isTrue();

      // Update view properties to trigger a commit through the custom operations
      view.updateProperties().set("test-key", "test-value").commit();

      // Verify the custom operations object was created and used
      assertThat(capturedOps.get()).isNotNull();
      Mockito.verify(capturedOps.get(), Mockito.atLeastOnce()).current();
      Mockito.verify(capturedOps.get(), Mockito.atLeastOnce()).commit(any(), any());

      // Verify the custom operations were used with custom headers
      ResourcePaths resourcePaths = ResourcePaths.forCatalogProperties(Maps.newHashMap());
      Mockito.verify(adapter, Mockito.atLeastOnce())
          .execute(
              reqMatcher(HTTPMethod.POST, resourcePaths.view(viewIdentifier), customHeaders),
              eq(LoadViewResponse.class),
              any(),
              any());
    }
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

  @Override
  protected RESTCatalog catalog() {
    return restCatalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return restCatalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }
}
