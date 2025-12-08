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

import static java.lang.String.format;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.CharStreams;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RESTCatalogServlet provides a servlet implementation used in combination with a
 * RESTCatalogAdaptor to proxy the REST Spec to any Catalog implementation.
 */
public class RESTCatalogServlet extends HttpServlet {
  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogServlet.class);

  private static final Map<String, String> DEFAULT_RESPONSE_HEADERS =
      ImmutableMap.of(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.getMimeType());

  private final RESTCatalogAdapter restCatalogAdapter;

  public RESTCatalogServlet(RESTCatalogAdapter restCatalogAdapter) {
    this.restCatalogAdapter = restCatalogAdapter;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    execute(ServletRequestContext.from(request), response);
  }

  @Override
  protected void doHead(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    execute(ServletRequestContext.from(request), response);
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    execute(ServletRequestContext.from(request), response);
  }

  @Override
  protected void doDelete(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    execute(ServletRequestContext.from(request), response);
  }

  protected void execute(ServletRequestContext context, HttpServletResponse response)
      throws IOException {
    response.setStatus(HttpServletResponse.SC_OK);
    DEFAULT_RESPONSE_HEADERS.forEach(response::setHeader);

    if (context.error().isPresent()) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      RESTObjectMapper.mapper().writeValue(response.getWriter(), context.error().get());
      return;
    }

    try {
      HTTPRequest request =
          restCatalogAdapter.buildRequest(
              context.method(),
              context.path(),
              context.queryParams(),
              context.headers(),
              context.body());

      Map<String, String> responseHeaders = Maps.newHashMap();
      Object responseBody =
          restCatalogAdapter.execute(
              request, context.route().responseClass(), handle(response), responseHeaders::putAll);

      responseHeaders.forEach(response::setHeader);

      if (responseBody != null) {
        RESTObjectMapper.mapper().writeValue(response.getWriter(), responseBody);
      } else {
        Pair<Route, Map<String, String>> routeAndVars =
            Route.from(request.method(), request.path());
        if (routeAndVars != null) {
          Route route = routeAndVars.first();
          if (route == Route.LOAD_TABLE) {
            response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
          } else if (shouldReturn204(route)) {
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
          }
        }
      }
    } catch (RESTException e) {
      LOG.error("Error processing REST request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    } catch (Exception e) {
      LOG.error("Unexpected exception when processing REST request", e);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

  private boolean shouldReturn204(Route route) {
    return route == Route.NAMESPACE_EXISTS
        || route == Route.TABLE_EXISTS
        || route == Route.VIEW_EXISTS
        || route == Route.DROP_NAMESPACE
        || route == Route.DROP_TABLE
        || route == Route.DROP_VIEW
        || route == Route.RENAME_TABLE
        || route == Route.RENAME_VIEW
        || route == Route.CANCEL_PLAN_TABLE_SCAN
        || route == Route.REPORT_METRICS
        || route == Route.COMMIT_TRANSACTION;
  }

  protected Consumer<ErrorResponse> handle(HttpServletResponse response) {
    return (errorResponse) -> {
      response.setStatus(errorResponse.code());
      try {
        RESTObjectMapper.mapper().writeValue(response.getWriter(), errorResponse);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  public static class ServletRequestContext {
    private HTTPMethod method;
    private Route route;
    private String path;
    private Map<String, String> headers;
    private Map<String, String> queryParams;
    private Object body;

    private ErrorResponse errorResponse;

    private ServletRequestContext(ErrorResponse errorResponse) {
      this.errorResponse = errorResponse;
    }

    private ServletRequestContext(
        HTTPMethod method,
        Route route,
        String path,
        Map<String, String> headers,
        Map<String, String> queryParams,
        Object body) {
      this.method = method;
      this.route = route;
      this.path = path;
      this.headers = headers;
      this.queryParams = queryParams;
      this.body = body;
    }

    static ServletRequestContext from(HttpServletRequest request) throws IOException {
      HTTPMethod method = HTTPMethod.valueOf(request.getMethod());
      String path = request.getRequestURI().substring(1);
      Pair<Route, Map<String, String>> routeContext = Route.from(method, path);

      if (routeContext == null) {
        return new ServletRequestContext(
            ErrorResponse.builder()
                .responseCode(400)
                .withType("BadRequestException")
                .withMessage(format("No route for request: %s %s", method, path))
                .build());
      }

      Route route = routeContext.first();
      Object requestBody = null;
      if (route.requestClass() != null) {
        requestBody =
            RESTObjectMapper.mapper().readValue(request.getReader(), route.requestClass());
      } else if (route == Route.TOKENS) {
        try (Reader reader = new InputStreamReader(request.getInputStream())) {
          requestBody = RESTUtil.decodeFormData(CharStreams.toString(reader));
        }
      }

      Map<String, String> queryParams =
          request.getParameterMap().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()[0]));
      Map<String, String> headers =
          Collections.list(request.getHeaderNames()).stream()
              .collect(Collectors.toMap(Function.identity(), request::getHeader));

      return new ServletRequestContext(method, route, path, headers, queryParams, requestBody);
    }

    public HTTPMethod method() {
      return method;
    }

    public Route route() {
      return route;
    }

    public String path() {
      return path;
    }

    public Map<String, String> headers() {
      return headers;
    }

    public Map<String, String> queryParams() {
      return queryParams;
    }

    public Object body() {
      return body;
    }

    public Optional<ErrorResponse> error() {
      return Optional.ofNullable(errorResponse);
    }
  }
}
