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
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.impl.EnglishReasonPhraseCatalog;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An HttpClient for usage with the REST catalog. */
public abstract class BaseRESTClient implements RESTClient {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRESTClient.class);

  private String uri;
  private ObjectMapper mapper;

  protected BaseRESTClient() {}

  @Override
  public void initialize(
      String baseUri,
      ObjectMapper objectMapper,
      Map<String, String> baseHeaders,
      Map<String, String> properties) {
    this.uri = baseUri;
    this.mapper = objectMapper;
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
  protected abstract <T> T execute(
      Method method,
      String path,
      Map<String, String> queryParams,
      Object requestBody,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders);

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

  // Per the spec, the only currently defined / used "success" responses are 200 and 202.
  protected static boolean isSuccessful(int code) {
    return code == HttpStatus.SC_OK
        || code == HttpStatus.SC_ACCEPTED
        || code == HttpStatus.SC_NO_CONTENT;
  }

  protected static ErrorResponse buildDefaultErrorResponse(int code, String responseReason) {
    String message =
        responseReason != null && !responseReason.isEmpty()
            ? responseReason
            : EnglishReasonPhraseCatalog.INSTANCE.getReason(code, null /* ignored */);
    String type = "RESTException";
    return ErrorResponse.builder().responseCode(code).withMessage(message).withType(type).build();
  }

  // Process a failed response through the provided errorHandler, and throw a RESTException if the
  // provided error handler doesn't already throw.
  protected static void throwFailure(
      int code, String responseBody, String reason, Consumer<ErrorResponse> errorHandler) {
    ErrorResponse errorResponse = null;

    if (responseBody != null) {
      try {
        if (errorHandler instanceof ErrorHandler) {
          errorResponse = ((ErrorHandler) errorHandler).parseResponse(code, responseBody);
        } else {
          LOG.warn(
              "Unknown error handler {}, response body won't be parsed",
              errorHandler.getClass().getName());
          errorResponse =
              ErrorResponse.builder().responseCode(code).withMessage(responseBody).build();
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
      errorResponse = buildDefaultErrorResponse(code, reason);
    }

    errorHandler.accept(errorResponse);

    // Throw an exception in case the provided error handler does not throw.
    throw new RESTException("Unhandled error: %s", errorResponse);
  }

  protected URI buildUri(String path, Map<String, String> params) {
    String baseUri = String.format("%s/%s", uri, path);
    try {
      URIBuilder builder = new URIBuilder(baseUri);
      if (params != null) {
        params.forEach(builder::addParameter);
      }
      return builder.build();
    } catch (URISyntaxException e) {
      throw new RESTException(
          "Failed to create request URI from base %s, params %s", baseUri, params);
    }
  }

  protected <T> T parseResponse(String responseBody, Class<T> responseType, int code) {
    try {
      return mapper.readValue(responseBody, responseType);
    } catch (JsonProcessingException e) {
      throw new RESTException(
          e,
          "Received a success response code of %d, but failed to parse response body into %s",
          code,
          responseType.getSimpleName());
    }
  }

  protected String toJsonString(Object requestBody) {
    try {
      return mapper.writeValueAsString(requestBody);
    } catch (JsonProcessingException e) {
      throw new RESTException(e, "Failed to write request body: %s", requestBody);
    }
  }

  protected void validateInputPath(String path) {
    if (path.startsWith("/")) {
      throw new RESTException(
          "Received a malformed path for a REST request: %s. Paths should not start with /", path);
    }
  }
}
