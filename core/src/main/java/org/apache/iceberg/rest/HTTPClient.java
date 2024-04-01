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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.impl.EnglishReasonPhraseCatalog;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An HttpClient for usage with the REST catalog. */
public class HTTPClient implements RESTClient {

  private static final Logger LOG = LoggerFactory.getLogger(HTTPClient.class);
  private static final String SIGV4_ENABLED = "rest.sigv4-enabled";
  private static final String SIGV4_REQUEST_INTERCEPTOR_IMPL =
      "org.apache.iceberg.aws.RESTSigV4Signer";
  @VisibleForTesting static final String CLIENT_VERSION_HEADER = "X-Client-Version";

  @VisibleForTesting
  static final String CLIENT_GIT_COMMIT_SHORT_HEADER = "X-Client-Git-Commit-Short";

  private static final String REST_MAX_RETRIES = "rest.client.max-retries";
  private static final String REST_MAX_CONNECTIONS = "rest.client.max-connections";
  private static final int REST_MAX_CONNECTIONS_DEFAULT = 100;
  private static final String REST_MAX_CONNECTIONS_PER_ROUTE = "rest.client.connections-per-route";
  private static final int REST_MAX_CONNECTIONS_PER_ROUTE_DEFAULT = 100;

  @VisibleForTesting
  static final String REST_CONNECTION_TIMEOUT_MS = "rest.client.connection-timeout-ms";

  @VisibleForTesting static final String REST_SOCKET_TIMEOUT_MS = "rest.client.socket-timeout-ms";

  private final String uri;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper mapper;

  private HTTPClient(
      String uri,
      Map<String, String> baseHeaders,
      ObjectMapper objectMapper,
      HttpRequestInterceptor requestInterceptor,
      Map<String, String> properties,
      HttpClientConnectionManager connectionManager) {
    this.uri = uri;
    this.mapper = objectMapper;

    HttpClientBuilder clientBuilder = HttpClients.custom();

    clientBuilder.setConnectionManager(connectionManager);

    if (baseHeaders != null) {
      clientBuilder.setDefaultHeaders(
          baseHeaders.entrySet().stream()
              .map(e -> new BasicHeader(e.getKey(), e.getValue()))
              .collect(Collectors.toList()));
    }

    if (requestInterceptor != null) {
      clientBuilder.addRequestInterceptorLast(requestInterceptor);
    }

    int maxRetries = PropertyUtil.propertyAsInt(properties, REST_MAX_RETRIES, 5);
    clientBuilder.setRetryStrategy(new ExponentialHttpRequestRetryStrategy(maxRetries));

    this.httpClient = clientBuilder.build();
  }

  private static String extractResponseBodyAsString(CloseableHttpResponse response) {
    try {
      if (response.getEntity() == null) {
        return null;
      }

      // EntityUtils.toString returns null when HttpEntity.getContent returns null.
      return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
    } catch (IOException | ParseException e) {
      throw new RESTException(e, "Failed to convert HTTP response body to string");
    }
  }

  // Per the spec, the only currently defined / used "success" responses are 200 and 202.
  private static boolean isSuccessful(CloseableHttpResponse response) {
    int code = response.getCode();
    return code == HttpStatus.SC_OK
        || code == HttpStatus.SC_ACCEPTED
        || code == HttpStatus.SC_NO_CONTENT;
  }

  private static ErrorResponse buildDefaultErrorResponse(CloseableHttpResponse response) {
    String responseReason = response.getReasonPhrase();
    String message =
        responseReason != null && !responseReason.isEmpty()
            ? responseReason
            : EnglishReasonPhraseCatalog.INSTANCE.getReason(response.getCode(), null /* ignored */);
    String type = "RESTException";
    return ErrorResponse.builder()
        .responseCode(response.getCode())
        .withMessage(message)
        .withType(type)
        .build();
  }

  // Process a failed response through the provided errorHandler, and throw a RESTException if the
  // provided error handler doesn't already throw.
  private static void throwFailure(
      CloseableHttpResponse response, String responseBody, Consumer<ErrorResponse> errorHandler) {
    ErrorResponse errorResponse = null;

    if (responseBody != null) {
      try {
        if (errorHandler instanceof ErrorHandler) {
          errorResponse =
              ((ErrorHandler) errorHandler).parseResponse(response.getCode(), responseBody);
        } else {
          LOG.warn(
              "Unknown error handler {}, response body won't be parsed",
              errorHandler.getClass().getName());
          errorResponse =
              ErrorResponse.builder()
                  .responseCode(response.getCode())
                  .withMessage(responseBody)
                  .build();
        }

      } catch (UncheckedIOException | IllegalArgumentException e) {
        // It's possible to receive a non-successful response that isn't a properly defined
        // ErrorResponse
        // without any bugs in the server implementation. So we ignore this exception and build an
        // error
        // response for the user.
        //
        // For example, the connection could time out before every reaching the server, in which
        // case we'll
        // likely get a 5xx with the load balancers default 5xx response.
        LOG.error("Failed to parse an error response. Will create one instead.", e);
      }
    }

    if (errorResponse == null) {
      errorResponse = buildDefaultErrorResponse(response);
    }

    errorHandler.accept(errorResponse);

    // Throw an exception in case the provided error handler does not throw.
    throw new RESTException("Unhandled error: %s", errorResponse);
  }

  private URI buildUri(String path, Map<String, String> params) {
    // if full path is provided, use the input path as path
    if (path.startsWith("/")) {
      throw new RESTException(
          "Received a malformed path for a REST request: %s. Paths should not start with /", path);
    }
    String fullPath =
        (path.startsWith("https://") || path.startsWith("http://"))
            ? path
            : String.format("%s/%s", uri, path);
    try {
      URIBuilder builder = new URIBuilder(fullPath);
      if (params != null) {
        params.forEach(builder::addParameter);
      }
      return builder.build();
    } catch (URISyntaxException e) {
      throw new RESTException(
          "Failed to create request URI from base %s, params %s", fullPath, params);
    }
  }

  /**
   * Method to execute an HTTP request and process the corresponding response.
   *
   * @param method - HTTP method, such as GET, POST, HEAD, etc.
   * @param queryParams - A map of query parameters
   * @param path - URI to send the request to
   * @param requestBody - Content to place in the request body
   * @param responseType - Class of the Response type. Needs to have serializer registered with
   *     ObjectMapper
   * @param errorHandler - Error handler delegated for HTTP responses which handles server error
   *     responses
   * @param <T> - Class type of the response for deserialization. Must be registered with the
   *     ObjectMapper.
   * @return The response entity, parsed and converted to its type T
   */
  private <T> T execute(
      Method method,
      String path,
      Map<String, String> queryParams,
      Object requestBody,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(
        method, path, queryParams, requestBody, responseType, headers, errorHandler, h -> {});
  }

  /**
   * Method to execute an HTTP request and process the corresponding response.
   *
   * @param method - HTTP method, such as GET, POST, HEAD, etc.
   * @param queryParams - A map of query parameters
   * @param path - URL to send the request to
   * @param requestBody - Content to place in the request body
   * @param responseType - Class of the Response type. Needs to have serializer registered with
   *     ObjectMapper
   * @param errorHandler - Error handler delegated for HTTP responses which handles server error
   *     responses
   * @param responseHeaders The consumer of the response headers
   * @param <T> - Class type of the response for deserialization. Must be registered with the
   *     ObjectMapper.
   * @return The response entity, parsed and converted to its type T
   */
  private <T> T execute(
      Method method,
      String path,
      Map<String, String> queryParams,
      Object requestBody,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    HttpUriRequestBase request = new HttpUriRequestBase(method.name(), buildUri(path, queryParams));

    if (requestBody instanceof Map) {
      // encode maps as form data, application/x-www-form-urlencoded
      addRequestHeaders(request, headers, ContentType.APPLICATION_FORM_URLENCODED.getMimeType());
      request.setEntity(toFormEncoding((Map<?, ?>) requestBody));
    } else if (requestBody != null) {
      // other request bodies are serialized as JSON, application/json
      addRequestHeaders(request, headers, ContentType.APPLICATION_JSON.getMimeType());
      request.setEntity(toJson(requestBody));
    } else {
      addRequestHeaders(request, headers, ContentType.APPLICATION_JSON.getMimeType());
    }

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      Map<String, String> respHeaders = Maps.newHashMap();
      for (Header header : response.getHeaders()) {
        respHeaders.put(header.getName(), header.getValue());
      }

      responseHeaders.accept(respHeaders);

      // Skip parsing the response stream for any successful request not expecting a response body
      if (response.getCode() == HttpStatus.SC_NO_CONTENT
          || (responseType == null && isSuccessful(response))) {
        return null;
      }

      String responseBody = extractResponseBodyAsString(response);

      if (!isSuccessful(response)) {
        // The provided error handler is expected to throw, but a RESTException is thrown if not.
        throwFailure(response, responseBody, errorHandler);
      }

      if (responseBody == null) {
        throw new RESTException(
            "Invalid (null) response body for request (expected %s): method=%s, path=%s, status=%d",
            responseType.getSimpleName(), method.name(), path, response.getCode());
      }

      try {
        return mapper.readValue(responseBody, responseType);
      } catch (JsonProcessingException e) {
        throw new RESTException(
            e,
            "Received a success response code of %d, but failed to parse response body into %s",
            response.getCode(),
            responseType.getSimpleName());
      }
    } catch (IOException e) {
      throw new RESTException(e, "Error occurred while processing %s request", method);
    }
  }

  @Override
  public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    execute(Method.HEAD, path, null, null, null, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.GET, path, queryParams, null, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.POST, path, null, body, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    return execute(
        Method.POST, path, null, body, responseType, headers, errorHandler, responseHeaders);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.DELETE, path, null, null, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.DELETE, path, queryParams, null, responseType, headers, errorHandler);
  }

  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    return execute(Method.POST, path, null, formData, responseType, headers, errorHandler);
  }

  private void addRequestHeaders(
      HttpUriRequest request, Map<String, String> requestHeaders, String bodyMimeType) {
    request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
    // Many systems require that content type is set regardless and will fail, even on an empty
    // bodied request.
    request.setHeader(HttpHeaders.CONTENT_TYPE, bodyMimeType);
    requestHeaders.forEach(request::setHeader);
  }

  @Override
  public void close() throws IOException {
    httpClient.close(CloseMode.GRACEFUL);
  }

  @VisibleForTesting
  static HttpRequestInterceptor loadInterceptorDynamically(
      String impl, Map<String, String> properties) {
    HttpRequestInterceptor instance;

    DynConstructors.Ctor<HttpRequestInterceptor> ctor;
    try {
      ctor =
          DynConstructors.builder(HttpRequestInterceptor.class)
              .loader(HTTPClient.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot initialize RequestInterceptor, missing no-arg constructor: %s", impl),
          e);
    }

    try {
      instance = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize, %s does not implement RequestInterceptor", impl), e);
    }

    DynMethods.builder("initialize")
        .hiddenImpl(impl, Map.class)
        .orNoop()
        .build(instance)
        .invoke(properties);

    return instance;
  }

  static HttpClientConnectionManager configureConnectionManager(
      Map<String, String> properties, ConnectionConfig connectionConfig) {
    PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder =
        PoolingHttpClientConnectionManagerBuilder.create();

    if (connectionConfig != null) {
      connectionManagerBuilder.setDefaultConnectionConfig(connectionConfig);
    }

    return connectionManagerBuilder
        .useSystemProperties()
        .setMaxConnTotal(Integer.getInteger(REST_MAX_CONNECTIONS, REST_MAX_CONNECTIONS_DEFAULT))
        .setMaxConnPerRoute(
            PropertyUtil.propertyAsInt(
                properties, REST_MAX_CONNECTIONS_PER_ROUTE, REST_MAX_CONNECTIONS_PER_ROUTE_DEFAULT))
        .build();
  }

  @VisibleForTesting
  static ConnectionConfig configureConnectionConfig(Map<String, String> properties) {
    Long connectionTimeout =
        PropertyUtil.propertyAsNullableLong(properties, REST_CONNECTION_TIMEOUT_MS);
    Integer socketTimeout = PropertyUtil.propertyAsNullableInt(properties, REST_SOCKET_TIMEOUT_MS);

    if (connectionTimeout == null && socketTimeout == null) {
      return null;
    }

    ConnectionConfig.Builder connConfigBuilder = ConnectionConfig.custom();

    if (connectionTimeout != null) {
      connConfigBuilder.setConnectTimeout(connectionTimeout, TimeUnit.MILLISECONDS);
    }

    if (socketTimeout != null) {
      connConfigBuilder.setSocketTimeout(socketTimeout, TimeUnit.MILLISECONDS);
    }

    return connConfigBuilder.build();
  }

  public static Builder builder(Map<String, String> properties) {
    return new Builder(properties);
  }

  public static class Builder {
    private final Map<String, String> properties;
    private final Map<String, String> baseHeaders = Maps.newHashMap();
    private String uri;
    private ObjectMapper mapper = RESTObjectMapper.mapper();

    private Builder(Map<String, String> properties) {
      this.properties = properties;
    }

    public Builder uri(String path) {
      Preconditions.checkNotNull(path, "Invalid uri for http client: null");
      this.uri = RESTUtil.stripTrailingSlash(path);
      return this;
    }

    public Builder withHeader(String key, String value) {
      baseHeaders.put(key, value);
      return this;
    }

    public Builder withHeaders(Map<String, String> headers) {
      baseHeaders.putAll(headers);
      return this;
    }

    public Builder withObjectMapper(ObjectMapper objectMapper) {
      this.mapper = objectMapper;
      return this;
    }

    public HTTPClient build() {
      withHeader(CLIENT_VERSION_HEADER, IcebergBuild.fullVersion());
      withHeader(CLIENT_GIT_COMMIT_SHORT_HEADER, IcebergBuild.gitCommitShortId());

      HttpRequestInterceptor interceptor = null;

      if (PropertyUtil.propertyAsBoolean(properties, SIGV4_ENABLED, false)) {
        interceptor = loadInterceptorDynamically(SIGV4_REQUEST_INTERCEPTOR_IMPL, properties);
      }

      return new HTTPClient(
          uri,
          baseHeaders,
          mapper,
          interceptor,
          properties,
          configureConnectionManager(properties, configureConnectionConfig(properties)));
    }
  }

  private StringEntity toJson(Object requestBody) {
    try {
      return new StringEntity(mapper.writeValueAsString(requestBody), StandardCharsets.UTF_8);
    } catch (JsonProcessingException e) {
      throw new RESTException(e, "Failed to write request body: %s", requestBody);
    }
  }

  private StringEntity toFormEncoding(Map<?, ?> formData) {
    return new StringEntity(RESTUtil.encodeFormData(formData), StandardCharsets.UTF_8);
  }
}
