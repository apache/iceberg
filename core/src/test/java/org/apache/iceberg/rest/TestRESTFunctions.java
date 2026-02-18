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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRESTFunctions {
  private static final Namespace NS = Namespace.of("ns");

  private InMemoryCatalog backendCatalog;
  private Server httpServer;
  private RESTCatalogAdapter adapterForRESTServer;
  private RESTCatalog restCatalog;

  @BeforeEach
  public void before() throws Exception {
    this.backendCatalog = new InMemoryCatalog();
    this.backendCatalog.initialize(
        "in-memory", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "/tmp"));

    this.adapterForRESTServer = new RESTCatalogAdapter(backendCatalog);

    ServletContextHandler servletContext =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContext.addServlet(
        new ServletHolder(new RESTCatalogServlet(adapterForRESTServer)), "/*");
    servletContext.setHandler(new GzipHandler());

    this.httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    httpServer.setHandler(servletContext);
    httpServer.start();

    this.restCatalog =
        new RESTCatalog(
            SessionCatalog.SessionContext.createEmpty(),
            config ->
                HTTPClient.builder(config)
                    .uri(config.get(CatalogProperties.URI))
                    .withHeaders(RESTUtil.configHeaders(config))
                    .build());
    restCatalog.setConf(new Configuration());
    restCatalog.initialize(
        "prod",
        ImmutableMap.of(
            CatalogProperties.URI,
            httpServer.getURI().toString(),
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO"));
  }

  @AfterEach
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

  @Test
  public void testListAndLoadFunctionSpec() {
    ObjectNode spec = RESTObjectMapper.mapper().createObjectNode().put("format-version", 1);
    adapterForRESTServer.putFunction(NS, "add_one", spec);

    List<String> names = restCatalog.listFunctions(NS);
    assertThat(names).containsExactly("add_one");

    ObjectNode loaded = restCatalog.loadFunctionSpec(NS, "add_one");
    assertThat(loaded.get("format-version").asInt()).isEqualTo(1);
  }
}
