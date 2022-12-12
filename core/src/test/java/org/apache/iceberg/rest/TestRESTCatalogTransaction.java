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

import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogTransactionTests;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class TestRESTCatalogTransaction extends CatalogTransactionTests<RESTCatalog> {
  private RESTCatalog catalog;
  private JdbcCatalog jdbcCatalog;
  private Server httpServer;

  @Override
  protected RESTCatalog catalog() {
    return catalog;
  }

  @BeforeEach
  public void before() throws Exception {
    jdbcCatalog = new JdbcCatalog();

    String sqliteDb =
        "jdbc:sqlite:file::memory:?ic" + UUID.randomUUID().toString().replace("-", "");
    jdbcCatalog.setConf(new Configuration());
    jdbcCatalog.initialize(
        "jdbc-catalog",
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION,
            metadataDir.toFile().getAbsolutePath(),
            CatalogProperties.URI,
            sqliteDb,
            JdbcCatalog.PROPERTY_PREFIX + "username",
            "user",
            JdbcCatalog.PROPERTY_PREFIX + "password",
            "password"));

    RESTCatalogAdapter adaptor = new RESTCatalogAdapter(jdbcCatalog);

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

    this.catalog =
        new RESTCatalog(
            context,
            (config) -> HTTPClient.builder().uri(config.get(CatalogProperties.URI)).build());
    catalog.setConf(new Configuration());
    catalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI, httpServer.getURI().toString(), "credential", "catalog:12345"));
  }

  @AfterEach
  public void closeCatalog() throws Exception {
    if (null != catalog) {
      catalog.close();
    }

    if (null != jdbcCatalog) {
      jdbcCatalog.close();
    }

    if (null != httpServer) {
      httpServer.stop();
      httpServer.join();
    }
  }
}
