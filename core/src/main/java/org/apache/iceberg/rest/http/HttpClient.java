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

package org.apache.iceberg.rest.http;

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
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RestClient;
import org.apache.iceberg.rest.UncheckedRestException;

/**
 * An HttpClient that can be used for the RestClient (eventually).
 * TODO - Extract out top  for all of this so that other implementations are easy to implement (e.g. gRPC).
 */
public class HttpClient implements RestClient {
  private final String baseUrl;

  // Need their own Builders?
  private final CloseableHttpClient httpClient;
  private final Consumer<CloseableHttpResponse> defaultErrorHandler;
  private final ObjectMapper mapper;
  private Map<String, String> additionalHeaders = Maps.newHashMap();
  private Consumer<HttpUriRequest> requestInterceptor = (r) -> { };

  private HttpClient(
      String baseUrl, CloseableHttpClient httpClient, ObjectMapper mapper,
      Map<String, String> additionalHeaders, Consumer<HttpUriRequest> requestIntercepter,
      Consumer<CloseableHttpResponse> defaultErrorHandler) {
    this.baseUrl = baseUrl;
    this.httpClient = httpClient != null ? httpClient : HttpClients.createDefault();
    this.mapper = mapper != null ? mapper : new ObjectMapper();
    this.additionalHeaders = additionalHeaders != null ? additionalHeaders : Maps.newHashMap();
    // TODO - Best way to handle Preconditions here.
    this.requestInterceptor = requestIntercepter;
    this.defaultErrorHandler = defaultErrorHandler;
  }

  public static Builder builder() {
    return new Builder();
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
   * @return The Response enttity, parsed and converted to its type T
   * @throws UncheckedIOException - Shouldn't throw this as the requestInterceptor should handle expected cases.
   * @throws UncheckedRestException - Wraps exceptions to avoid having to use checked exceptions everywhere.
   */
  @Nullable
  public <T> T execute(
      Method method, String path, Object body, Class<T> responseType,
      Consumer<CloseableHttpResponse> errorHandler) {
    HttpUriRequestBase request = new HttpUriRequestBase(method.name(), URI.create(String.format("%s/%s", baseUrl,
        path)));
    addRequestHeaders(request);

    if (body != null) {
      try {
        request.setEntity(new StringEntity(mapper.writeValueAsString(body)));
      } catch (JsonProcessingException e) {
        throw new UncheckedRestException(e, "Failed to write request body");
      }
    }

    requestInterceptor.accept(request);

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      if (response.getCode() != HttpStatus.SC_OK) {
        errorHandler.accept(response);
      }

      if (responseType == null || response.getEntity() == null) {
        return null;
      }

      HttpEntity entity = response.getEntity();
      return mapper.readValue(entity.getContent(), responseType);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (Exception e) {
      throw new UncheckedRestException(e, "An unhandled error occurred while executing an Http request");
    }
  }

  public <T> T head(String path) {
    return execute(Method.HEAD, path, null, null, defaultErrorHandler);
  }

  public <T> T head(String path, Class<T> responseType, Consumer<CloseableHttpResponse> errorHandler) {
    return execute(Method.HEAD, path, null, responseType, errorHandler);
  }

  public <T> T get(String path, Class<T> responseType) {
    return execute(Method.GET, path, null, responseType, defaultErrorHandler);
  }

  public <T> T get(String path, Class<T> responseType, Consumer<CloseableHttpResponse> errorHandler) {
    return execute(Method.GET, path, null, responseType, errorHandler);
  }

  public <T> T post(String path, Object body, Class<T> responseType) {
    return execute(Method.POST, path, body, responseType, defaultErrorHandler);
  }

  public <T> T post(String path, Object body, Class<T> responseType, Consumer<CloseableHttpResponse> errorHandler) {
    return execute(Method.POST, path, body, responseType, errorHandler);
  }

  public <T> T put(String path, Object body, Class<T> responseType) {
    return execute(Method.PUT, path, body, responseType, defaultErrorHandler);
  }

  public <T> T put(String path, Object body, Class<T> responseType, Consumer<CloseableHttpResponse> errorHandler) {
    return execute(Method.PUT, path, body, responseType, errorHandler);
  }

  public <T> T delete(String path, Class<T> responseType) {
    return execute(Method.DELETE, path, null, responseType, defaultErrorHandler);
  }

  public <T> T delete(String path, Class<T> responseType, Consumer<CloseableHttpResponse> errorHandler) {
    return execute(Method.DELETE, path, null, responseType, errorHandler);
  }

  private void addRequestHeaders(HttpUriRequest request) {
    request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());
    request.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());
    additionalHeaders.forEach(request::setHeader);
  }

  @Override
  public void close() throws IOException {
    httpClient.close(CloseMode.GRACEFUL);
  }

  public static class Builder {
    private String baseUrl;
    private CloseableHttpClient httpClient;
    private ObjectMapper mapper;
    private Map<String, String> additionalHeaders = Maps.newHashMap();
    private Consumer<HttpUriRequest> requestInterceptor = r -> { };
    private Consumer<CloseableHttpResponse> defaultErrorHandler = ErrorHandlers.databaseErrorHandler();

    private Builder() {

    }

    public Builder baseUrl(String url) {
      this.baseUrl = url;
      return this;
    }

    public Builder httpClient(CloseableHttpClient client) {
      this.httpClient = client;
      return this;
    }

    public Builder mapper(ObjectMapper objectMapper) {
      this.mapper = objectMapper;
      return this;
    }

    public Builder additionalHeaders(Map<String, String> headers) {
      this.additionalHeaders = headers;
      return this;
    }

    public Builder withHeader(String key, String value) {
      if (additionalHeaders == null) {
        additionalHeaders = Maps.newHashMap();
      }
      additionalHeaders.put(key, value);
      return this;
    }

    // TODO - Perhaps place auth checking on headers in here?
    public Builder requestInterceptor(Consumer<HttpUriRequest> reqInterceptor) {
      // TODO - Needs Precondition checks.
      this.requestInterceptor = reqInterceptor;
      return this;
    }

    public Builder defaultErrorHandler(Consumer<CloseableHttpResponse> errorHandler) {
      // TODO - Maybe log if this is null, but allow it as there's other ways to register.
      this.defaultErrorHandler = errorHandler;
      return this;
    }

    public HttpClient build() {
      // TODO - Put some Preconditions here.
      return new HttpClient(baseUrl, httpClient, mapper, additionalHeaders, requestInterceptor, defaultErrorHandler);
    }
  }
}
