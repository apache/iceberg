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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.util.PropertyUtil;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogServer.class);

  static final String REST_PORT = "rest.port";
  static final int REST_PORT_DEFAULT = 8181;

  private Server httpServer;

  RESTCatalogServer() {}

  static class CatalogContext {
    private final Catalog catalog;
    private final Map<String, String> configuration;

    CatalogContext(Catalog catalog, Map<String, String> configuration) {
      this.catalog = catalog;
      this.configuration = configuration;
    }

    public Catalog catalog() {
      return catalog;
    }

    public Map<String, String> configuration() {
      return configuration;
    }
  }

  private CatalogContext initializeBackendCatalog() throws IOException {
    // Translate environment variables to catalog properties
    Map<String, String> catalogProperties = RCKUtils.environmentCatalogConfig();

    // Fallback to a JDBCCatalog impl if one is not set
    catalogProperties.putIfAbsent(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
    catalogProperties.putIfAbsent(CatalogProperties.URI, "jdbc:sqlite::memory:");
    catalogProperties.putIfAbsent("jdbc.schema-version", "V1");

    // Configure a default location if one is not specified
    String warehouseLocation = catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION);

    if (warehouseLocation == null) {
      File tmp = java.nio.file.Files.createTempDirectory("iceberg_warehouse").toFile();
      tmp.deleteOnExit();
      warehouseLocation = tmp.toPath().resolve("iceberg_data").toFile().getAbsolutePath();
      catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

      LOG.info("No warehouse location set. Defaulting to temp location: {}", warehouseLocation);
    }

    LOG.info("Creating catalog with properties: {}", catalogProperties);
    return new CatalogContext(
        CatalogUtil.buildIcebergCatalog("rest_backend", catalogProperties, new Configuration()),
        catalogProperties);
  }

  public void start(boolean join) throws Exception {
    CatalogContext catalogContext = initializeBackendCatalog();

    RESTCatalogAdapter adapter = new RESTServerCatalogAdapter(catalogContext);
    RESTCatalogServlet servlet = new RESTCatalogServlet(adapter);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    ServletHolder servletHolder = new ServletHolder(servlet);
    context.addServlet(servletHolder, "/*");
    context.insertHandler(new GzipHandler());

    this.httpServer =
        new Server(
            PropertyUtil.propertyAsInt(catalogContext.configuration, REST_PORT, REST_PORT_DEFAULT));
    httpServer.setHandler(context);
    httpServer.start();

    if(join) {
      httpServer.join();
    }
  }

  public void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  public static void main(String[] args) throws Exception {
    new RESTCatalogServer().start(true);
  }
}
