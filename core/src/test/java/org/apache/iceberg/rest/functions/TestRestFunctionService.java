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
package org.apache.iceberg.rest.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.exceptions.RESTException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRestFunctionService {

  private HttpServer server;
  private String baseUrl;

  @BeforeEach
  public void before() throws Exception {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    int port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;

    // /v1/functions?namespace=
    server.createContext(
        "/v1/functions",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
              exchange.sendResponseHeaders(405, -1);
              exchange.close();
              return;
            }
            String query = exchange.getRequestURI().getQuery();
            if (query != null && query.contains("namespace=")) {
              String body = "{\"names\":[\"add_one_file\",\"fruits_by_color\"]}";
              byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
              exchange.sendResponseHeaders(200, bytes.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
              }
              return;
            }
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
          }
        });

    // /v1/functions/default/add_one_file
    server.createContext(
        "/v1/functions/default/add_one_file",
        ex -> {
          if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            ex.sendResponseHeaders(405, -1);
            ex.close();
            return;
          }
          String body = "{\"spec\": {\"format-version\": 1}}";
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          ex.sendResponseHeaders(200, bytes.length);
          try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
          }
        });

    server.start();
  }

  @AfterEach
  public void after() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  public void testListAndLoad() {
    RestFunctionService svc = new RestFunctionService(baseUrl, null);
    List<String> names = svc.listFunctions(new String[] {"default"});
    assertThat(names).contains("add_one_file", "fruits_by_color");

    String spec = svc.getFunctionSpecJson(new String[] {"default"}, "add_one_file");
    assertThat(spec).contains("\"format-version\":1");
  }

  @Test
  public void testListNotFound() throws Exception {
    // New server context to force 404
    HttpServer nf = HttpServer.create(new InetSocketAddress(0), 0);
    String nfBase = "http://localhost:" + nf.getAddress().getPort();
    nf.createContext(
        "/v1/functions",
        ex -> {
          ex.sendResponseHeaders(404, -1);
          ex.close();
        });
    nf.start();
    try {
      RestFunctionService svc = new RestFunctionService(nfBase, null);
      assertThatThrownBy(() -> svc.listFunctions(new String[] {"default"}))
          .isInstanceOf(RESTException.class)
          .hasMessageContaining("Unable to process");
    } finally {
      nf.stop(0);
    }
  }

  @Test
  public void testBadJson() throws Exception {
    HttpServer bad = HttpServer.create(new InetSocketAddress(0), 0);
    String badBase = "http://localhost:" + bad.getAddress().getPort();
    bad.createContext(
        "/v1/functions",
        ex -> {
          byte[] bytes = "not-json".getBytes(StandardCharsets.UTF_8);
          ex.sendResponseHeaders(200, bytes.length);
          try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
          }
        });
    bad.start();
    try {
      RestFunctionService svc = new RestFunctionService(badBase, null);
      assertThatThrownBy(() -> svc.listFunctions(new String[] {"default"}))
          .isInstanceOf(RESTException.class)
          .hasMessageContaining("failed to parse response body");
    } finally {
      bad.stop(0);
    }
  }
}
