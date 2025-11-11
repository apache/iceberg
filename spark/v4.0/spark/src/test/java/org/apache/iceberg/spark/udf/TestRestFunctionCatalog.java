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
package org.apache.iceberg.spark.udf;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestRestFunctionCatalog extends TestBaseWithCatalog {

  private HttpServer server;
  private String baseUrl;

  @BeforeEach
  public void setUp() throws Exception {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    int port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;

    // /v1/functions?namespace=
    server.createContext(
        "/v1/functions",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws IOException {
            String query = exchange.getRequestURI().getQuery();
            if (query != null && query.contains("namespace=")) {
              // Return a JSON array of names
              String body = "[\"add_one_file\", \"fruits_by_color\"]";
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

    // /v1/functions/{namespace}/{name}
    server.createContext(
        "/v1/functions/default/add_one_file",
        ex -> respondWithFile(ex, pathToExample("add_one.json")));
    server.createContext(
        "/v1/functions/default/fruits_by_color",
        ex -> respondWithFile(ex, pathToExample("fruits_by_color.json")));

    server.start();

    // Point the catalog to the REST server BEFORE base setup so the catalog picks it up
    spark.conf().set("spark.sql.catalog." + catalogName + ".functions.rest.uri", baseUrl);

    super.before();
  }

  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  @TestTemplate
  public void testListAndExecuteFunctionsFromRest() throws Exception {
    // Ensure referenced table exists before function registration runs
    sql("CREATE TABLE fruits(name STRING, color STRING) USING PARQUET");

    FunctionCatalog functionCatalog = castToFunctionCatalog(catalogName);
    Identifier[] functions = functionCatalog.listFunctions(new String[] {});
    assertThat(functions).anyMatch(id -> id.name().equals("add_one_file"));
    assertThat(functions).anyMatch(id -> id.name().equals("fruits_by_color"));

    Object result = scalarSql("SELECT add_one_file(41)");
    assertThat(((Number) result).intValue()).isEqualTo(42);
    sql("INSERT INTO fruits VALUES ('apple','red'), ('plum','red'), ('banana','yellow')");
    List<Object[]> rows = sql("SELECT * FROM fruits_by_color('red') ORDER BY name");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[0]).isEqualTo("apple");
    assertThat(rows.get(0)[1]).isEqualTo("red");
    assertThat(rows.get(1)[0]).isEqualTo("plum");
    assertThat(rows.get(1)[1]).isEqualTo("red");
  }

  private void respondWithFile(HttpExchange exchange, Path path) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      exchange.sendResponseHeaders(405, -1);
      exchange.close();
      return;
    }
    byte[] bytes = Files.readAllBytes(path);
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private Path pathToExample(String file) {
    return java.nio.file.Paths.get(
            "..",
            "..",
            "..",
            "core",
            "src",
            "main",
            "java",
            "org",
            "apache",
            "iceberg",
            "udf",
            "examples",
            file)
        .toAbsolutePath()
        .normalize();
  }

  private FunctionCatalog castToFunctionCatalog(String name) {
    return (FunctionCatalog) spark.sessionState().catalogManager().catalog(name);
  }
}


