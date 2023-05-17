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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

/**
 * * Exercises the RESTClient interface, specifically over a mocked-server using the actual
 * HttpRESTClient code.
 */
public class TestHTTPClient {

  private static final int PORT = 1080;
  private static final String BEARER_AUTH_TOKEN = "auth_token";
  private static final String URI = String.format("http://127.0.0.1:%d", PORT);
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();

  private static String icebergBuildGitCommitShort;
  private static String icebergBuildFullVersion;
  private static ClientAndServer mockServer;
  private static RESTClient restClient;

  @BeforeClass
  public static void beforeClass() {
    mockServer = startClientAndServer(PORT);
    restClient = HTTPClient.builder(ImmutableMap.of()).uri(URI).build();
    icebergBuildGitCommitShort = IcebergBuild.gitCommitShortId();
    icebergBuildFullVersion = IcebergBuild.fullVersion();
  }

  @AfterClass
  public static void stopServer() throws IOException {
    mockServer.stop();
    restClient.close();
  }

  @Test
  public void testPostSuccess() throws Exception {
    testHttpMethodOnSuccess(HttpMethod.POST);
  }

  @Test
  public void testPostFailure() throws Exception {
    testHttpMethodOnFailure(HttpMethod.POST);
  }

  @Test
  public void testGetSuccess() throws Exception {
    testHttpMethodOnSuccess(HttpMethod.GET);
  }

  @Test
  public void testGetFailure() throws Exception {
    testHttpMethodOnFailure(HttpMethod.GET);
  }

  @Test
  public void testDeleteSuccess() throws Exception {
    testHttpMethodOnSuccess(HttpMethod.DELETE);
  }

  @Test
  public void testDeleteFailure() throws Exception {
    testHttpMethodOnFailure(HttpMethod.DELETE);
  }

  @Test
  public void testHeadSuccess() throws JsonProcessingException {
    testHttpMethodOnSuccess(HttpMethod.HEAD);
  }

  @Test
  public void testHeadFailure() throws JsonProcessingException {
    testHttpMethodOnFailure(HttpMethod.HEAD);
  }

  @Test
  public void testDynamicHttpRequestInterceptorLoading() {
    Map<String, String> properties = ImmutableMap.of("key", "val");

    HttpRequestInterceptor interceptor =
        HTTPClient.loadInterceptorDynamically(
            TestHttpRequestInterceptor.class.getName(), properties);

    assertThat(interceptor).isInstanceOf(TestHttpRequestInterceptor.class);
    assertThat(((TestHttpRequestInterceptor) interceptor).properties).isEqualTo(properties);
  }

  public static void testHttpMethodOnSuccess(HttpMethod method) throws JsonProcessingException {
    Item body = new Item(0L, "hank");
    int statusCode = 200;

    ErrorHandler onError = mock(ErrorHandler.class);
    doThrow(new RuntimeException("Failure response")).when(onError).accept(any());

    String path = addRequestTestCaseAndGetPath(method, body, statusCode);

    Item successResponse =
        doExecuteRequest(method, path, body, onError, h -> assertThat(h).isNotEmpty());

    if (method.usesRequestBody()) {
      Assert.assertEquals(
          "On a successful " + method + ", the correct response body should be returned",
          successResponse,
          body);
    }

    verify(onError, never()).accept(any());
  }

  public static void testHttpMethodOnFailure(HttpMethod method) throws JsonProcessingException {
    Item body = new Item(0L, "hank");
    int statusCode = 404;

    ErrorHandler onError = mock(ErrorHandler.class);
    doThrow(
            new RuntimeException(
                String.format(
                    "Called error handler for method %s due to status code: %d",
                    method, statusCode)))
        .when(onError)
        .accept(any());

    String path = addRequestTestCaseAndGetPath(method, body, statusCode);

    Assertions.assertThatThrownBy(() -> doExecuteRequest(method, path, body, onError, h -> {}))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            String.format(
                "Called error handler for method %s due to status code: %d", method, statusCode));

    verify(onError).accept(any());
  }

  // Adds a request that the mock-server can match against, based on the method, path, body, and
  // headers.
  // Return the path generated for the test case, so that the client can call that path to exercise
  // it.
  private static String addRequestTestCaseAndGetPath(HttpMethod method, Item body, int statusCode)
      throws JsonProcessingException {

    // Build the path route, which must be unique per test case.
    boolean isSuccess = statusCode == 200;
    // Using different paths keeps the expectations unique for the test's mock server
    String pathName = isSuccess ? "success" : "failure";
    String path = String.format("%s_%s", method, pathName);

    // Build the expected request
    String asJson = body != null ? MAPPER.writeValueAsString(body) : null;
    HttpRequest mockRequest =
        request("/" + path)
            .withMethod(method.name().toUpperCase(Locale.ROOT))
            .withHeader("Authorization", "Bearer " + BEARER_AUTH_TOKEN)
            .withHeader(HTTPClient.CLIENT_VERSION_HEADER, icebergBuildFullVersion)
            .withHeader(HTTPClient.CLIENT_GIT_COMMIT_SHORT_HEADER, icebergBuildGitCommitShort);

    if (method.usesRequestBody()) {
      mockRequest = mockRequest.withBody(asJson);
    }

    // Build the expected response
    HttpResponse mockResponse = response().withStatusCode(statusCode);

    if (method.usesResponseBody()) {
      if (isSuccess) {
        // Simply return the passed in item in the success case.
        mockResponse = mockResponse.withBody(asJson);
      } else {
        ErrorResponse response =
            ErrorResponse.builder().responseCode(statusCode).withMessage("Not found").build();
        mockResponse = mockResponse.withBody(ErrorResponseParser.toJson(response));
      }
    }

    mockServer.when(mockRequest).respond(mockResponse);

    return path;
  }

  private static Item doExecuteRequest(
      HttpMethod method,
      String path,
      Item body,
      ErrorHandler onError,
      Consumer<Map<String, String>> responseHeaders) {
    Map<String, String> headers = ImmutableMap.of("Authorization", "Bearer " + BEARER_AUTH_TOKEN);
    switch (method) {
      case POST:
        return restClient.post(path, body, Item.class, headers, onError, responseHeaders);
      case GET:
        return restClient.get(path, Item.class, headers, onError);
      case HEAD:
        restClient.head(path, headers, onError);
        return null;
      case DELETE:
        return restClient.delete(path, Item.class, () -> headers, onError);
      default:
        throw new IllegalArgumentException(String.format("Invalid method: %s", method));
    }
  }

  public static class Item implements RESTRequest, RESTResponse {
    private Long id;
    private String data;

    // Required for Jackson deserialization
    @SuppressWarnings("unused")
    public Item() {}

    public Item(Long id, String data) {
      this.id = id;
      this.data = data;
    }

    @Override
    public void validate() {}

    @Override
    public int hashCode() {
      return Objects.hash(id, data);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Item item = (Item) o;
      return Objects.equals(id, item.id) && Objects.equals(data, item.data);
    }
  }

  public static class TestHttpRequestInterceptor implements HttpRequestInterceptor {
    private Map<String, String> properties;

    public void initialize(Map<String, String> props) {
      this.properties = props;
    }

    @Override
    public void process(
        org.apache.hc.core5.http.HttpRequest request, EntityDetails entity, HttpContext context)
        throws HttpException, IOException {}
  }
}
