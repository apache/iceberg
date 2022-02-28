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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.entity.EntityBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.mock.Expectation;
import org.mockserver.model.Header;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.StringBody.subString;

public class TestRESTCatalogViaMockServer {

  private static final String uri = "localhost";
  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);
  private static ClientAndServer mockServer;

  // @Rule
  // public MockServerRule mockServerRule = new MockServerRule(this);
  // @Mock
  // private CloseableHttpClient httpClient;
  //
  private RESTClient restClient;
  private MockServerClient mockServerClient;
  private MockServerClient client;

  @BeforeClass
  public static void beforeClass() {
    RESTSerializers.registerAll(MAPPER);
    // This is a workaround for Jackson since Iceberg doesn't use the standard get/set bean notation.
    // This allows Jackson to work with the fields directly (both public and private) and not require
    // custom serializers for all the request/response objects.
    MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mockServer = startClientAndServer(1080);
  }

  @AfterClass
  public static void stopServer() {
    mockServer.stop();
  }

  @BeforeEach
  public void beforeEach(MockServerClient client) {
    this.client = client;
  }

  // @Before
  // public void before() {
  //   MockitoAnnotations.initMocks(TestErrorHandlers.class);
  //   httpClient = Mockito.mock(CloseableHttpClient.class);
  //   restClient = RestClient
  //       .builder()
  //       .withBearerAuth("any")
  //       .httpClient(httpClient)
  //       .mapper(mapper)
  //       .build();
  // }

  // @Test
  // public void testErrorHandlerReturnsNullOnNullResponseBody() throws IOException {
  //   HttpEntity entity = Mockito.mock(HttpEntity.class);
  //   Mockito.when(response.getEntity()).thenReturn(entity);
  //   Mockito.when(entity.getContent()).thenReturn(null);
  //   String actual = ErrorHandlers.extractResponseBodyAsString(response);
  //   Assertions.assertThat(actual).isNull();
  // }

  // @Test
  // public void testMockErrorHandler() throws Exception {
  //   ErrorResponse errorResponse = new ErrorResponse("Unable to find namespace test", "NotFound", 404);
  //   String json = ErrorResponse.toJson(errorResponse);
  //   InputStream jsonInputStream = Mockito.spy(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
  //   HttpEntity entity = EntityBuilder.create()
  //       .setStream(jsonInputStream)
  //       .build();
  //   ClassicHttpResponse mockResponse = Mockito.mock(ClassicHttpResponse.class);
  //   Mockito.when(mockResponse.getEntity()).thenReturn(entity);
  //   Mockito.when(httpClient.execute(any())).thenReturn((CloseableHttpResponse) mockResponse);
  //   Mockito.spy(restClient);
  //   restClient.post("unknown", null, GetNamespaceResponse.class);
  // }

  // This mock test works. It's very very specific about matching on
  // headers exactly etc.
  @Test
  @Ignore
  public void workingRawHTTPClientTest() throws Exception {
    String json = "{}";
    new MockServerClient("127.0.0.1", 1080)
        .when(
            request()
                .withMethod("POST")
                .withPath("/validate")
                .withHeaders(
                    new Header("Content-Type", "application/json"),
                    new Header("Accept", "application/json")
                )
                .withBody(subString(json)),
            exactly(1))
        .respond(
            response()
                .withStatusCode(401)
                .withHeader("Content-Type", "application/json")
                .withBody("{ message: 'incorrect username and password combination' }")
                .withDelay(TimeUnit.SECONDS, 1)
        );

    HttpUriRequestBase request = new HttpUriRequestBase("POST", URI.create("http://127.0.0.1:1080/validate"));
    request.setEntity(
        EntityBuilder
            .create()
            .setStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))).build());
    request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
    request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());

    Assert.assertEquals(
        401,
        HttpClients.createDefault().execute(request).getCode());
  }

  @Test
  public void createExpectationForInvalidAuth() throws Exception {
    CreateNamespaceResponse response = CreateNamespaceResponse
        .builder()
        .withNamespace(Namespace.of("accounting", "tax"))
        .setProperties(ImmutableMap.of("owner", "Hank"))
        .build();
    String json = MAPPER.writeValueAsString(response);
    Expectation[] expectations = new MockServerClient("127.0.0.1", 1080)
        .when(
            request()
                .withMethod("POST")
                .withHeaders(
                    new Header("Content-Type", "application/json"),
                    new Header("Accept", "application/json"),
                    new Header("Content-Length", String.valueOf(json.getBytes(StandardCharsets.UTF_8).length))
                )
                .removeHeader("Accept-Encoding")
                .withPath("/v1/namespaces")
                .withBody(subString(json)),
            exactly(1))
        .respond(
            response()
                .withStatusCode(200)
                .withHeader("Content-Type", "application/json")
                .withBody(json)
                .withBody(json)
                .withDelay(TimeUnit.SECONDS, 1)
        );

    RESTClient client = HttpRESTClient
        .builder()
        .uri("http://127.0.0.1:1080/v1")
        .httpClient(HttpClients.createDefault())
        .mapper(MAPPER)
        .build();

    // HttpUriRequestBase request = new HttpUriRequestBase("POST", URI.create("http://127.0.0.1:1080/validate"));
    // request.setEntity(
    //     EntityBuilder
    //         .create()
    //         .setStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))).build());
    // request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
    // request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
    //
    // Assert.assertEquals(401,
    //     HttpClients.createDefault().execute(request).getCode());

    CreateNamespaceResponse mockedResponse =
        client.post("namespaces", response, CreateNamespaceResponse.class);
    Assertions.assertThat(mockedResponse.namespace()).isEqualTo(response.namespace());
  }
}
