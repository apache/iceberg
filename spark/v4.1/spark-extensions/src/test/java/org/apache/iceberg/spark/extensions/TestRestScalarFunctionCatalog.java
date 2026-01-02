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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * End-to-end test for REST-backed scalar SQL UDFs executed via Iceberg Spark extensions.
 *
 * <p>Stage 1: scalar only. UDTF/table functions will be added in a follow-up (stage 2).
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestRestScalarFunctionCatalog extends ExtensionsTestBase {

  // REST-backed functions are not currently supported for Spark's v1 session catalog
  // (`spark_catalog`)
  // because Spark resolves those via v1 persistent function lookup.
  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties()
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      },
      {
        SparkCatalogConfig.REST.catalogName(),
        SparkCatalogConfig.REST.implementation(),
        ImmutableMap.builder()
            .putAll(SparkCatalogConfig.REST.properties())
            .put(CatalogProperties.URI, restCatalog.properties().get(CatalogProperties.URI))
            .build()
      }
    };
  }

  private HttpServer server;
  private String baseUrl;

  private static final String ADD_ONE_SPEC =
      "{"
          + "\"function-uuid\":\"123\","
          + "\"format-version\":1,"
          + "\"doc\":\"Add one UDF\","
          + "\"parameter-names\":[{\"name\":\"x\"}],"
          + "\"definitions\":[{"
          + "  \"definition-id\":\"(int)\","
          + "  \"parameters\":[{\"type\":\"int\"}],"
          + "  \"return-type\":\"int\","
          + "  \"doc\":\"Adds one to x\","
          + "  \"current-version-id\":1,"
          + "  \"versions\":[{"
          + "    \"version-id\":1,"
          + "    \"timestamp-ms\":0,"
          + "    \"deterministic\":true,"
          + "    \"representations\":[{"
          + "      \"type\":\"sql\","
          + "      \"dialect\":\"spark\","
          + "      \"body\":\"x + 1\""
          + "    }]"
          + "  }]"
          + "}],"
          + "\"definition-log\":[{\"timestamp-ms\":0,\"definition-versions\":[{\"definition-id\":\"(int)\",\"version-id\":1}]}],"
          + "\"secure\":false"
          + "}";

  @BeforeEach
  public void before() {
    try {
      server = HttpServer.create(new InetSocketAddress(0), 0);
    } catch (IOException e) {
      throw new RuntimeException("Failed to start test HTTP server", e);
    }

    int port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;

    // /v1/functions?namespace=
    server.createContext(
        "/v1/functions",
        exchange -> {
          String query = exchange.getRequestURI().getQuery();
          if (query != null && query.contains("namespace=")) {
            String body = "{\"names\":[\"add_one_file\"]}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(bytes);
            }
            return;
          }
          exchange.sendResponseHeaders(400, -1);
          exchange.close();
        });

    // /v1/functions/{namespace}/{name}
    server.createContext(
        "/v1/functions/default/add_one_file", ex -> respondWithWrappedJson(ex, ADD_ONE_SPEC));

    server.start();

    // Point the configured Iceberg catalog to the REST function service before base setup
    spark.conf().set("spark.sql.catalog." + catalogName + ".functions.rest.uri", baseUrl);
    super.before();

    // Ensure unqualified function names resolve against the parameterized Iceberg catalog
    spark.conf().set("spark.sql.defaultCatalog", catalogName);
  }

  @AfterEach
  public void after() {
    if (server != null) {
      server.stop(0);
    }
  }

  @TestTemplate
  public void testListAndExecuteScalarFromRest() throws Exception {
    FunctionCatalog functionCatalog = castToFunctionCatalog(catalogName);
    Identifier[] functions = functionCatalog.listFunctions(new String[] {});
    long userCount =
        Arrays.stream(functions).filter(id -> id.name().equals("add_one_file")).count();
    assertThat(userCount).isEqualTo(1);

    Object result = scalarSql("SELECT add_one_file(41)");
    assertThat(((Number) result).intValue()).isEqualTo(42);
  }

  private void respondWithWrappedJson(HttpExchange exchange, String json) throws IOException {
    if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      exchange.sendResponseHeaders(405, -1);
      exchange.close();
      return;
    }
    String body = "{\"spec\":" + json + "}";
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }

  private FunctionCatalog castToFunctionCatalog(String name) {
    return (FunctionCatalog) spark.sessionState().catalogManager().catalog(name);
  }
}
