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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.http.HttpClient;
import org.apache.iceberg.rest.http.RequestResponseSerializers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.StringBody.exact;

public class TestRestCatalog {

  private static final String WAREHOUSE_PATH = "s3://bucket";
  private static final String CATALOG_NAME = "rest";
  private static final Map<String, String> properties = ImmutableMap.of(
      "baseUrl", "http://localhost:1080",
      "type", "hadoop",
      "io-impl", "org.apache.iceberg.hadoop.HadoopFileIO"
  );

  private HttpClient httpClient;
  private ObjectMapper mapper;
  private RestCatalog restCatalog;
  private ClientAndServer mockServer;

  @Before
  public void before() {
    mockServer = startClientAndServer(1080);
    ObjectMapper mapper = new ObjectMapper();
    RequestResponseSerializers.registerAll(mapper);
    // this.httpClient = HttpClient
    //     .builder()
    //     .baseUrl("http://localhost:1080")
    //     .mapper(mapper)
    //     .build();
    // this.restCatalog = new RestCatalog();
    // restCatalog.initialize(CATALOG_NAME, properties, httpClient);
    this.restCatalog = Mockito.mock(RestCatalog.class);
  }

  @After
  public void after() {
    if (mockServer != null) {
      if (mockServer.isRunning()) {
        mockServer.stop();
      }
      mockServer.close();
    }
  }

  @Test
  public void noOpTest() {
    Assert.assertTrue(true);
  }

  // TODO - Figure out how to get this to actually use req and resp.
  //        Right now this passes but I have to specify everything manually.
  //
  //       This will be great for the OpenAPI spec but not as much for testing.
  //       Should just use a mock I guess?
  @Test
  public void testCreateNamespaceSpec() {
    mockServer
        .when(
            request()
            .withMethod("POST")
            .withSocketAddress("http://localhost", 1080)
            .withPath("/namespace")
            .withHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            .withBody(exact("{ namespace: 'prod.accounting', properties: { owner: 'hank' }"))
    ).respond(
        response()
            .withStatusCode(200)
            .withHeaders(
                new Header("Content-Type", "application/json; charset=utf-8")
            )
            .withBody("{ namespace: 'prod.accounting', properties: { owner: 'hank' }  }")
    );
  }

  @Test
  public void createNamespaceWithMocks() throws JsonProcessingException {
    CreateNamespaceRequest req = CreateNamespaceRequest
        .builder()
        .withNamespace(Namespace.of("prod", "accounting"))
        .withProperties(ImmutableMap.of("owner", "hank"))
        .build();

    CreateNamespaceResponse expected = CreateNamespaceResponse
        .builder()
        .withNamespace(Namespace.of("prod", "accounting"))
        .withProperties(ImmutableMap.of("owner", "hank"))
        .build();

    Mockito.doReturn(expected)
        .when(restCatalog).createDatabase(req.getNamespace(), req.getProperties());
    CreateNamespaceResponse actual = restCatalog.createDatabase(
        Namespace.of("prod", "accounting"), ImmutableMap.of("owner", "hank"));

    Assert.assertEquals("The response body of a create database request should be expected JSON",
        expected, actual);
  }
}
