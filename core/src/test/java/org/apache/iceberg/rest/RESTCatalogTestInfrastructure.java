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
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.mockito.Mockito;

public class RESTCatalogTestInfrastructure {
  protected static final ObjectMapper MAPPER = RESTObjectMapper.mapper();

  protected RESTCatalog restCatalog;
  protected InMemoryCatalog backendCatalog;
  protected Server httpServer;
  protected RESTCatalogAdapter adapterForRESTServer;
  protected ParserContext parserContext;

  public void before(Path temp) throws Exception {
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
        Mockito.spy(
            new RESTCatalogAdapter(backendCatalog) {
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
            });

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

  public void after() throws Exception {
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

  public RESTCatalog initCatalog(String catalogName, Map<String, String> additionalProperties) {
    Configuration conf = new Configuration();
    SessionCatalog.SessionContext context =
        new SessionCatalog.SessionContext(
            UUID.randomUUID().toString(),
            "user",
            ImmutableMap.of("credential", "user:12345"),
            ImmutableMap.of());

    RESTCatalog catalog =
        new RESTCatalog(
            context,
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

  public RESTCatalog catalog() {
    return restCatalog;
  }

  public InMemoryCatalog backendCatalog() {
    return backendCatalog;
  }

  public Server httpServer() {
    return httpServer;
  }

  public RESTCatalogAdapter adapter() {
    return adapterForRESTServer;
  }

  @SuppressWarnings("unchecked")
  public <T> T roundTripSerialize(T payload, String description) {
    if (payload == null) {
      return null;
    }

    try {
      if (payload instanceof RESTMessage) {
        RESTMessage message = (RESTMessage) payload;
        ObjectReader reader = MAPPER.readerFor(message.getClass());
        if (parserContext != null && !parserContext.isEmpty()) {
          reader = reader.with(parserContext.toInjectableValues());
        }
        return (T) reader.readValue(MAPPER.writeValueAsString(message));
      } else {
        return payload;
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format("Failed to serialize and deserialize %s: %s", description, payload), e);
    }
  }

  public void setParserContext(org.apache.iceberg.Table table) {
    parserContext =
        ParserContext.builder().add("specsById", table.specs()).add("caseSensitive", false).build();
  }

  public ParserContext parserContext() {
    return parserContext;
  }
}
