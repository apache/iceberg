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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.io.CloseMode;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.PropertyUtil;

/** An HttpClient for usage with the REST catalog. */
public class HTTPClient extends BaseRESTClient {

  private static final String SIGV4_ENABLED = "rest.sigv4-enabled";
  private static final String SIGV4_REQUEST_INTERCEPTOR_IMPL =
      "org.apache.iceberg.aws.RESTSigV4Signer";

  private CloseableHttpClient httpClient;

  HTTPClient() {}

  @Override
  public void initialize(
      String baseUri,
      ObjectMapper objectMapper,
      Map<String, String> baseHeaders,
      Map<String, String> properties) {
    super.initialize(baseUri, objectMapper, baseHeaders, properties);

    HttpClientBuilder clientBuilder = HttpClients.custom();

    if (baseHeaders != null) {
      clientBuilder.setDefaultHeaders(
          baseHeaders.entrySet().stream()
              .map(e -> new BasicHeader(e.getKey(), e.getValue()))
              .collect(Collectors.toList()));
    }

    if (PropertyUtil.propertyAsBoolean(properties, SIGV4_ENABLED, false)) {
      HttpRequestInterceptor requestInterceptor =
          loadInterceptorDynamically(SIGV4_REQUEST_INTERCEPTOR_IMPL, properties);
      clientBuilder.addRequestInterceptorLast(requestInterceptor);
    }

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

  /**
   * Method to execute an HTTP request and process the corresponding response.
   *
   * @param method - HTTP method, such as GET, POST, HEAD, etc.
   * @param queryParams - A map of query parameters
   * @param path - URL path to send the request to
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
  @Override
  protected <T> T execute(
      Method method,
      String path,
      Map<String, String> queryParams,
      Object requestBody,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    validateInputPath(path);
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
          || (responseType == null && isSuccessful(response.getCode()))) {
        return null;
      }

      String responseBody = extractResponseBodyAsString(response);

      if (!isSuccessful(response.getCode())) {
        // The provided error handler is expected to throw, but a RESTException is thrown if not.
        throwFailure(response.getCode(), responseBody, response.getReasonPhrase(), errorHandler);
      }

      if (responseBody == null) {
        throw new RESTException(
            "Invalid (null) response body for request (expected %s): method=%s, path=%s, status=%d",
            responseType.getSimpleName(), method.name(), path, response.getCode());
      }

      return parseResponse(responseBody, responseType, response.getCode());
    } catch (IOException e) {
      throw new RESTException(e, "Error occurred while processing %s request", method);
    }
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

  /** @deprecated use {@link RESTClient#buildFrom(Map)} instead */
  @Deprecated
  public static Builder builder(Map<String, String> properties) {
    return new Builder(properties);
  }

  /** @deprecated use {@link RESTClient#buildFrom(Map)} */
  @Deprecated
  public static class Builder {

    private final RESTClientBuilder restClientBuilder;

    public Builder(Map<String, String> properties) {
      this.restClientBuilder = new RESTClientBuilder(properties);
    }

    public Builder uri(String baseUri) {
      restClientBuilder.uri(baseUri);
      return this;
    }

    public Builder withHeader(String key, String value) {
      restClientBuilder.withHeader(key, value);
      return this;
    }

    public Builder withHeaders(Map<String, String> headers) {
      restClientBuilder.withHeaders(headers);
      return this;
    }

    public Builder withObjectMapper(ObjectMapper objectMapper) {
      restClientBuilder.withObjectMapper(objectMapper);
      return this;
    }

    public HTTPClient build() {
      RESTClient restClient = restClientBuilder.build();
      return (HTTPClient) restClient;
    }
  }

  private StringEntity toJson(Object requestBody) {
    return new StringEntity(toJsonString(requestBody), StandardCharsets.UTF_8);
  }

  private StringEntity toFormEncoding(Map<?, ?> formData) {
    return new StringEntity(RESTUtil.encodeFormData(formData), StandardCharsets.UTF_8);
  }
}
