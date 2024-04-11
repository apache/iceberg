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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalogAdapter.HTTPMethod;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.view.ViewCatalogTests;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

public class TestRESTViewCatalog extends ViewCatalogTests<RESTCatalog> {
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();

  @TempDir private Path temp;

  private RESTCatalog restCatalog;
  private InMemoryCatalog backendCatalog;
  private Server httpServer;

  @BeforeEach
  public void createCatalog() throws Exception {
    File warehouse = temp.toFile();

    this.backendCatalog = new InMemoryCatalog();
    this.backendCatalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.getAbsolutePath()));

    RESTCatalogAdapter adaptor =
        new RESTCatalogAdapter(backendCatalog) {
          @Override
          public <T extends RESTResponse> T execute(
              HTTPMethod method,
              String path,
              Map<String, String> queryParams,
              Object body,
              Class<T> responseType,
              Map<String, String> headers,
              Consumer<ErrorResponse> errorHandler) {
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

  @Test
  public void testPaginationForListViews() {
    RESTCatalogAdapter adapter = Mockito.spy(new RESTCatalogAdapter(backendCatalog));
    RESTCatalog catalog =
        new RESTCatalog(SessionCatalog.SessionContext.createEmpty(), (config) -> adapter);
    catalog.initialize("test", ImmutableMap.of(RESTSessionCatalog.REST_PAGE_SIZE, "10"));

    int numberOfItems = 100;
    String namespaceName = "newdb";
    String tableName = "newtable";
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
          .withQuery("spark", "select * from " + namespaceName + "." + tableName)
          .create();
    }

    List<TableIdentifier> views = catalog.listViews(Namespace.of(namespaceName));
    assertThat(views).hasSize(numberOfItems);
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
