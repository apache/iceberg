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

import static org.apache.iceberg.rest.RESTCatalogAdapter.Route.CONFIG;

import java.io.File;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.BeforeEach;

public class TestRESTViewCatalogWithAssumedViewSupport extends TestRESTViewCatalog {

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
          public <T extends RESTResponse> T handleRequest(
              Route route, Map<String, String> vars, Object body, Class<T> responseType) {
            if (CONFIG == route) {
              // simulate a legacy server that doesn't send back supported endpoints
              return castResponse(responseType, ConfigResponse.builder().build());
            }

            return super.handleRequest(route, vars, body, responseType);
          }
        };

    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.setContextPath("/");
    servletContext.addServlet(new ServletHolder(new RESTCatalogServlet(adaptor)), "/*");
    servletContext.setHandler(new GzipHandler());

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
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            "credential",
            "catalog:12345",
            // assume that the server supports view endpoints
            RESTSessionCatalog.VIEW_ENDPOINTS_SUPPORTED,
            "true"));
  }
}
