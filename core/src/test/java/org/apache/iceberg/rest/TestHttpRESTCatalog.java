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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class TestHttpRESTCatalog {

  private static final String catalogName = "rest";
  private static final int port = 1080;
  private static final String uri = String.format("http://127.0.0.1:%d/v1", port);
  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  private static RESTCatalog restCatalog;
  private static ClientAndServer mockServer;
  private static RESTClient restClient;

  @BeforeClass
  public static void beforeClass() {
    RESTSerializers.registerAll(MAPPER);
    // This is a workaround for Jackson since Iceberg doesn't use the standard get/set bean notation.
    // This allows Jackson to work with the fields directly (both public and private) and not require
    // custom serializers for all the request/response objects.
    MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mockServer = startClientAndServer(port);
    restClient = HttpRESTClient
        .builder()
        .uri(uri)
        .mapper(MAPPER)
        .withBearerAuth("auth_token")
        .defaultErrorHandler(ErrorHandlers.defaultErrorHandler())
        .build();

    restCatalog = new RESTCatalog();
    restCatalog.initialize(catalogName, restClient, null /* io */);
  }

  @AfterClass
  public static void stopServer() throws IOException {
    mockServer.stop();
    restClient.close();
  }

  @Test
  public void testCreateNamespace_200() throws Exception {
    Namespace namespace = Namespace.of("accounting", "tax");
    Map<String, String> props = ImmutableMap.of("owner", "Hank");
    CreateNamespaceRequest request = CreateNamespaceRequest
        .builder()
        .withNamespace(namespace)
        .setProperties(props)
        .build();

    CreateNamespaceResponse response = CreateNamespaceResponse
        .builder()
        .withNamespace(namespace)
        .setProperties(props)
        .build();

    mockServer
        .when(
            request("/v1/namespaces")
                .withMethod("POST")
                .withBody(MAPPER.writeValueAsString(request)),
            exactly(1))
        .respond(
            response()
                .withStatusCode(200)
                .withHeader("Content-Type", "application/json")
                .withBody(MAPPER.writeValueAsString(response)));

    CreateNamespaceResponse mockedResponse =
        restClient.post("namespaces", request, CreateNamespaceResponse.class);
    Assertions.assertThat(mockedResponse.namespace()).isEqualTo(request.namespace());
    Assertions.assertThat(mockedResponse.properties()).isEqualTo(request.properties());
  }

  @Test
  public void testCreateNamespace_AlreadyExists() throws JsonProcessingException {
    Namespace namespace = Namespace.of("already_exists");
    CreateNamespaceRequest request = CreateNamespaceRequest
        .builder()
        .withNamespace(namespace)
        .build();

    ErrorResponse response = ErrorResponse.builder()
        .withMessage("Namespace already_exists already exists")
        .withType(AlreadyExistsException.class.getSimpleName())
        .responseCode(409)
        .build();

    mockServer
        .when(
            request()
                .withMethod("POST")
                .withPath(eq("/v1/namespaces"))
                .withBody(MAPPER.writeValueAsString(request)),
            exactly(1))
        .respond(
            response()
                .withStatusCode(409)
                .withHeader("Content-Type", "application/json")
                .withBody(MAPPER.writeValueAsString(response)));

    AssertHelpers.assertThrows(
        "The RESTCatalog should throw AlreadyExistsException when trying to create an existing namespace",
        AlreadyExistsException.class,
        response.message(),
        () -> restCatalog.createNamespace(namespace));
  }

  @Test
  public void testListTables_200() throws JsonProcessingException {
    Namespace ns = Namespace.of("db1");
    List<TableIdentifier> tables = ImmutableList.of("t1", "t2", "t3")
        .stream()
        .map(tbl -> TableIdentifier.of(ns, tbl))
        .collect(Collectors.toList());
    String path = "/v1/namespaces/" + RESTUtil.asURLVariable(ns) + "/tables";

    ListTablesResponse response = ListTablesResponse.builder()
        .addAll(tables).build();

    mockServer
        .when(
            request(path)
                .withMethod("GET")
                .withHeader(new Header("Authorization", "Bearer auth_token")))
        .respond(
            response()
                .withStatusCode(200)
                .withHeader("Content-Type", "application/json")
                .withBody(MAPPER.writeValueAsString(response)));

    Assert.assertEquals(
        Sets.newHashSet(tables),
        Sets.newHashSet(restCatalog.listTables(ns))
    );
  }
}
