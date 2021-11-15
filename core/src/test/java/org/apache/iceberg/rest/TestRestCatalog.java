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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.http.HttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.model.Header;
import org.mockserver.model.HttpResponse;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.StringBody.exact;

@ExtendWith(MockServerExtension.class)
public class TestRestCatalog {

  private static final String WAREHOUSE_PATH = "s3://bucket";
  private static final String CATALOG_NAME = "rest";
  private static final Map<String, String> properties = ImmutableMap.of(
      "baseUrl", "localhost:9000",
      "io-impl", "org.apache.iceberg.TestTables.LocalFileIO"
  );

  private ClientAndServer client;
  private HttpClient httpClient;
  private RestCatalog restCatalog;
  private MockServerClient mockServerClient;

  @Before
  public void before(MockServerClient mockServerClient) {
    this.httpClient = Mockito.mock(HttpClient.class);
    this.restCatalog = Mockito.mock(RestCatalog.class);
    restCatalog.initialize(CATALOG_NAME, properties);
    this.mockServerClient = mockServerClient;
  }

  @Test
  public void noOpTest() {
    Assert.assertTrue(true);
  }

  @Test
  public void testCreateNamespaceRequest() {
    mockServerClient.when(
        request()
            .withMethod("POST")
            .withPath("/namespace")
            .withHeader("\"Content-type\", \"application/json\"")
            .withBody(exact("{namespace: ['prod', 'accounting'], password: 'bar'}"))
    ).respond(
        response()
            .withStatusCode(401)
            .withHeaders(
                new Header("Content-Type", "application/json; charset=utf-8"),
                new Header("Cache-Control", "public, max-age=86400"))
            .withBody("{ message: 'incorrect username and password combination' }")
            .withDelay(TimeUnit.SECONDS,1)
    )
  }

  // @Test
  // public void createNamespace() {
  //
  //   Mockito.doReturn(CreateNamespaceResponse.builder().build())
  //       .when(restCatalog).createDatabase(Mockito.any(CreateDatabaseRequest.class));
  //   glueCatalog.createNamespace(Namespace.of("db"));
  // }
}
