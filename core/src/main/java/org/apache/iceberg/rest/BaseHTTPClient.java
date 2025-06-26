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
import java.util.function.Consumer;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;

/**
 * A base class for {@link RESTClient} implementations.
 *
 * <p>All methods in {@link RESTClient} are implemented in the same way: first, an {@link
 * HTTPRequest} is {@linkplain #buildRequest(HTTPMethod, String, Map, Map, Object) built from the
 * method arguments}, then {@linkplain #execute(HTTPRequest, Class, Consumer, Consumer) executed}.
 *
 * <p>This allows subclasses to provide a consistent way to execute all requests, regardless of the
 * method or arguments.
 */
public abstract class BaseHTTPClient implements RESTClient {

  @Override
  public abstract RESTClient withAuthSession(AuthSession session);

  @Override
  public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    HTTPRequest request = buildRequest(HTTPMethod.HEAD, path, null, headers, null);
    execute(request, null, errorHandler, h -> {});
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    HTTPRequest request = buildRequest(HTTPMethod.DELETE, path, null, headers, null);
    return execute(request, responseType, errorHandler, h -> {});
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    HTTPRequest request = buildRequest(HTTPMethod.DELETE, path, queryParams, headers, null);
    return execute(request, responseType, errorHandler, h -> {});
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    HTTPRequest request = buildRequest(HTTPMethod.GET, path, queryParams, headers, null);
    return execute(request, responseType, errorHandler, h -> {});
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      ParserContext parserContext) {
    HTTPRequest request = buildRequest(HTTPMethod.GET, path, queryParams, headers, null);
    return execute(request, responseType, errorHandler, h -> {}, parserContext);
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    HTTPRequest request = buildRequest(HTTPMethod.POST, path, null, headers, body);
    return execute(request, responseType, errorHandler, h -> {});
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders) {
    HTTPRequest request = buildRequest(HTTPMethod.POST, path, null, headers, body);
    return execute(request, responseType, errorHandler, responseHeaders);
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders,
      ParserContext parserContext) {
    HTTPRequest request = buildRequest(HTTPMethod.POST, path, null, headers, body);
    return execute(request, responseType, errorHandler, responseHeaders, parserContext);
  }

  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    HTTPRequest request = buildRequest(HTTPMethod.POST, path, null, headers, formData);
    return execute(request, responseType, errorHandler, h -> {});
  }

  protected abstract HTTPRequest buildRequest(
      HTTPMethod method,
      String path,
      Map<String, String> queryParams,
      Map<String, String> headers,
      Object body);

  protected abstract <T extends RESTResponse> T execute(
      HTTPRequest request,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders);

  protected <T extends RESTResponse> T execute(
      HTTPRequest request,
      Class<T> responseType,
      Consumer<ErrorResponse> errorHandler,
      Consumer<Map<String, String>> responseHeaders,
      ParserContext parserContext) {
    if (null != parserContext) {
      throw new UnsupportedOperationException("Parser context is not supported");
    }

    return execute(request, responseType, errorHandler, responseHeaders);
  }
}
