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
import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ErrorResponseParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An HttpClient for usage with the REST catalog.
 */
public class HttpRESTClient implements RESTClient {

  private static final Logger LOG = LoggerFactory.getLogger(HttpRESTClient.class);

  private final String uri;
  private final CloseableHttpClient httpClient;
  private final Consumer<ErrorResponse> defaultErrorHandler;
  private final ObjectMapper mapper;
  private final Map<String, String> additionalHeaders;
  private final Consumer<HttpUriRequest> requestInterceptor;

  private HttpRESTClient(
      String uri,
      CloseableHttpClient httpClient,
      ObjectMapper mapper,
      Map<String, String> additionalHeaders,
      Consumer<HttpUriRequest> requestInterceptor,
      Consumer<ErrorResponse> defaultErrorHandler) {
    this.uri = uri;
    this.httpClient = httpClient != null ? httpClient : HttpClients.createDefault();
    this.mapper = mapper != null ? mapper : new ObjectMapper();
    this.additionalHeaders = additionalHeaders != null ? additionalHeaders : ImmutableMap.of();
    this.requestInterceptor = requestInterceptor;
    this.defaultErrorHandler = defaultErrorHandler;
  }

  private static String extractResponseBodyAsString(CloseableHttpResponse response) {
    try {
      if (response.getEntity() == null) {
        return null;
      }
      return EntityUtils.toString(response.getEntity());
    } catch (IOException | ParseException e) {
      throw new RESTException(e, "Encountered an exception converting HTTP response body to string");
    }
  }

  /**
   * Method to execute an HTTP request and process the corresponding response.
   *
   * @param method       - HTTP method, such as GET, POST, HEAD, etc.
   * @param path         - URL path to send the request to
   * @param body         - Contents of the request body.
   * @param responseType - Class of the Response type. Needs to have serializer registered with ObjectMapper
   * @param errorHandler - Error handler delegated for HTTP responses which handles server error responses
   * @param <T>          - Class type of the response for deserialization. Must be registered with the ObjectMapper.
   * @return The response entity, parsed and converted to its type T
   */
  @Nullable
  public <T> T execute(
      Method method, String path, Object body, Class<T> responseType, Consumer<ErrorResponse> errorHandler) {
    if (path.startsWith("/")) {
      throw new RESTException(
          "Received a malformed path for a REST request: %s. Paths should not start with /", path);
    }

    String fullUri = String.format("%s/%s", uri, path);
    HttpUriRequestBase request = new HttpUriRequestBase(method.name(), URI.create(fullUri));
    addRequestHeaders(request);

    if (body != null) {
      try {
        StringEntity stringEntity = new StringEntity(mapper.writeValueAsString(body));
        request.setEntity(stringEntity);
      } catch (JsonProcessingException e) {
        throw new RESTException(e, "Failed to write request body: %s", body);
      }
    }

    requestInterceptor.accept(request);

    try (CloseableHttpResponse response = httpClient.execute(request)) {

      // HEAD request.
      if (responseType == null) {
        if (response.getCode() < 300) {
          return null;
        } else {
          throw new RESTException(
              "Received a non-2xx response for a request that was not expecting a request body");
        }
      }

      if (response.getCode() != HttpStatus.SC_OK && response.getCode() >= 400) {
        ErrorResponse parsedError;
        String responseBody = null;
        try {
          responseBody = extractResponseBodyAsString(response);
          parsedError = ErrorResponseParser.fromJson(responseBody);
          errorHandler.accept(parsedError);
        } catch (UncheckedIOException e) {
          throw new RESTException(e,
              "Received a non-200 response but could not parse a standard error response: %s", response);
        }
      }

      HttpEntity entity = response.getEntity();
      return mapper.readValue(entity.getContent(), responseType);
    } catch (IOException e) {
      String desiredType = responseType == null ? "null" : responseType.getSimpleName();
      throw new UncheckedIOException(
          String.format("Unhandled exception trying to read in data of type %s", desiredType), e);
    }
  }

  @Override
  public void head(String path) {
    execute(Method.HEAD, path, null, null, defaultErrorHandler);
  }

  @Override
  public void head(String path, Consumer<ErrorResponse> errorHandler) {
    execute(Method.HEAD, path, null, null, errorHandler);
  }

  @Override
  public <T> T get(String path, Class<T> responseType) {
    return execute(Method.GET, path, null, responseType, defaultErrorHandler);
  }

  @Override
  public <T> T get(String path, Class<T> responseType, Consumer<ErrorResponse> errorHandler) {
    return execute(Method.GET, path, null, responseType, errorHandler);
  }

  @Override
  public <T> T post(String path, Object body, Class<T> responseType) {
    return execute(Method.POST, path, body, responseType, defaultErrorHandler);
  }

  @Override
  public <T> T post(String path, Object body, Class<T> responseType, Consumer<ErrorResponse> errorHandler) {
    return execute(Method.POST, path, body, responseType, errorHandler);
  }

  @Override
  public <T> T delete(String path, Class<T> responseType) {
    return execute(Method.DELETE, path, null, responseType, defaultErrorHandler);
  }

  @Override
  public <T> T delete(String path, Class<T> responseType, Consumer<ErrorResponse> errorHandler) {
    return execute(Method.DELETE, path, null, responseType, errorHandler);
  }

  private void addRequestHeaders(HttpUriRequest request) {
    request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
    // Many systems require tht content type is set regardless and will fail, even on an empty bodied request.
    request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
    additionalHeaders.forEach(request::setHeader);
  }

  @Override
  public void close() throws IOException {
    httpClient.close(CloseMode.GRACEFUL);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<String, String> additionalHeaders = Maps.newHashMap();
    private String uri;
    private CloseableHttpClient httpClient;
    private ObjectMapper mapper;
    private Consumer<HttpUriRequest> requestInterceptor = r -> { };
    private Consumer<ErrorResponse> defaultErrorHandler = ErrorHandlers.defaultErrorHandler();

    private Builder() {
    }

    private static String asBearer(String token) {
      return String.format("Bearer %s", token);
    }

    public Builder uri(String baseUri) {
      this.uri = baseUri;
      return this;
    }

    // Visible for testing
    public Builder httpClient(CloseableHttpClient client) {
      this.httpClient = client;
      return this;
    }

    public Builder mapper(ObjectMapper objectMapper) {
      this.mapper = objectMapper;
      return this;
    }

    public Builder withHeader(String key, String value) {
      additionalHeaders.put(key, value);
      return this;
    }

    public Builder withHeaders(Map<String, String> headers) {
      additionalHeaders.putAll(headers);
      return this;
    }

    public Builder withBearerAuth(String token) {
      Preconditions.checkNotNull(token, "Invalid auth token: null");
      additionalHeaders.put(HttpHeaders.AUTHORIZATION, asBearer(token));
      return this;
    }

    public Builder requestInterceptor(Consumer<HttpUriRequest> reqInterceptor) {
      Preconditions.checkNotNull(reqInterceptor, "Invalid request interceptor: null");
      this.requestInterceptor = reqInterceptor;
      return this;
    }

    public Builder defaultErrorHandler(Consumer<ErrorResponse> errorHandler) {
      Preconditions.checkNotNull(errorHandler, "Invalid error handler: null");
      this.defaultErrorHandler = errorHandler;
      return this;
    }

    public HttpRESTClient build() {
      return new HttpRESTClient(uri, httpClient, mapper, additionalHeaders, requestInterceptor, defaultErrorHandler);
    }
  }
}
